"""调度窗口与到期采集任务清单测试。

核心验收：
1. 调度窗口与触发凭证原子生成，同计划同窗口只有一条清单、一个排队任务；
2. 同一窗口重复调度（重试）只能看到一笔任务，只累加 dispatch_count；
3. 新时间窗口产生新任务；
4. 迟到事件"补当前窗口"与"顺延下一窗口"都不会产生两个同窗口任务；
5. 排队任务状态联动更新清单待办；
6. 工作台可按计划、窗口、状态过滤核对凭证与迟到说明。
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.store import store
from app.services.dispatch_window import (
    DispatchWindowService,
    LATE_POLICY_CURRENT,
    LATE_POLICY_NEXT,
    _parse_window_minutes,
)
from app.services.scheduler import SchedulerService
from app.services.task_queue import TaskQueueService


@pytest.fixture(autouse=True)
def reset_store():
    """每个用例前重置内存仓库，避免全局单例串数据。"""
    store.reset()
    yield
    store.reset()


def _active_plan() -> dict:
    plans = [r for r in store.rows("scheduler") if r.get("status") == "已激活"]
    assert plans, "种子数据中应至少有一个已激活计划"
    return plans[0]


class TestWindowParsing:
    """窗口粒度解析与窗口切片。"""

    def test_window_minutes_units(self) -> None:
        assert _parse_window_minutes("15分钟") == 15
        assert _parse_window_minutes("30") == 30
        assert _parse_window_minutes("1h") == 60
        assert _parse_window_minutes("2小时") == 120
        assert _parse_window_minutes("09:00-10:00") == 60
        # 占位文本回退默认粒度，兼容既有业务口径
        assert _parse_window_minutes("调度计划样例1") == 15
        assert _parse_window_minutes(None) == 15

    def test_resolve_window_id_stable_in_same_window(self) -> None:
        svc = DispatchWindowService()
        plan = _active_plan()  # 执行窗口 15分钟
        t1 = datetime(2026, 11, 18, 10, 5, tzinfo=timezone.utc)
        t2 = datetime(2026, 11, 18, 10, 12, tzinfo=timezone.utc)
        w1 = svc.resolve_window(plan, t1)
        w2 = svc.resolve_window(plan, t2)
        assert w1.window_id == w2.window_id == "W2-000040"
        assert w1.window_start.strftime("%H:%M") == "10:00"
        assert w1.window_end.strftime("%H:%M") == "10:15"

    def test_resolve_window_changes_across_windows(self) -> None:
        svc = DispatchWindowService()
        plan = _active_plan()
        t1 = datetime(2026, 11, 18, 10, 14, tzinfo=timezone.utc)
        t2 = datetime(2026, 11, 18, 10, 16, tzinfo=timezone.utc)
        w1 = svc.resolve_window(plan, t1)
        w2 = svc.resolve_window(plan, t2)
        assert w1.window_id != w2.window_id


class TestDispatchIdempotency:
    """同窗口重复调度只能看到一笔任务。"""

    def test_first_dispatch_creates_manifest_and_task(self) -> None:
        svc = DispatchWindowService()
        fired = "2026-11-18T10:05:00+00:00"
        report = svc.run_dispatch(fired_at=fired)

        active_count = len([
            r for r in store.rows("scheduler") if r.get("status") == "已激活"
        ])
        assert report["scanned_plans"] == active_count
        assert report["tasks_created"] == active_count
        assert report["tasks_reused"] == 0
        assert report["manifest_count"] == active_count
        # 清单带窗口标识与触发凭证
        item = report["manifests"][0]
        assert item["window_id"].startswith("W")
        assert item["trigger_credential"]["credential_no"]
        assert item["task_number"]

    def test_retry_same_window_reuses_single_task(self) -> None:
        svc = DispatchWindowService()
        fired = "2026-11-18T10:05:00+00:00"
        first = svc.run_dispatch(fired_at=fired)
        second = svc.run_dispatch(fired_at=fired)
        third = svc.run_dispatch(
            fired_at="2026-11-18T10:12:00+00:00",  # 仍在同一 15 分钟窗口
        )

        # 三次调度，任务数始终等于首次创建数
        assert second["tasks_created"] == 0
        assert second["tasks_reused"] == first["manifest_count"]
        assert third["tasks_created"] == 0
        assert third["duplicate_dispatches"] == first["manifest_count"]

        plan_id = first["manifests"][0]["plan_id"]
        window_id = first["manifests"][0]["window_id"]
        window_tasks = TaskQueueService().list_due_tasks(
            plan_id=plan_id, window_id=window_id
        )
        assert len(window_tasks) == 1  # 同一窗口只有一笔排队任务

        manifest = store.due_manifests.find(plan_id, window_id)
        assert manifest is not None
        assert manifest.dispatch_count == 3

    def test_new_window_creates_new_task(self) -> None:
        svc = DispatchWindowService()
        first = svc.run_dispatch(fired_at="2026-11-18T10:05:00+00:00")
        second = svc.run_dispatch(fired_at="2026-11-18T10:20:00+00:00")

        assert second["tasks_created"] == first["manifest_count"]
        plan_id = first["manifests"][0]["plan_id"]
        windows = {
            t["window_id"]
            for t in TaskQueueService().list_due_tasks(plan_id=plan_id)
        }
        assert len(windows) == 2

    def test_atomic_manifest_and_credential(self) -> None:
        """窗口与凭证原子生成：清单存在即带完整凭证，且只生成一次。"""
        svc = DispatchWindowService()
        svc.run_dispatch(fired_at="2026-11-18T10:05:00+00:00")
        svc.run_dispatch(fired_at="2026-11-18T10:05:00+00:00")

        rows = store.due_manifests.list_all()
        for manifest in rows:
            cred = manifest.trigger_credential
            assert cred["window_id"] == manifest.window_id
            assert cred["plan_id"] == manifest.plan_id
            assert cred["credential_no"]
            assert cred["fired_at"]
        # 同计划同窗口清单唯一
        keys = [(m.plan_id, m.window_id) for m in rows]
        assert len(keys) == len(set(keys))


class TestLateDispatch:
    """迟到事件两种策略都不产生同窗口重复任务。"""

    def test_late_fill_current_window(self) -> None:
        svc = DispatchWindowService()
        scheduled = datetime(2026, 11, 18, 10, 5, tzinfo=timezone.utc)
        fired = scheduled + timedelta(minutes=30)  # 晚于 10:15 窗口结束

        report = svc.run_dispatch(
            fired_at=fired.isoformat(),
            scheduled_at=scheduled.isoformat(),
            late_policy=LATE_POLICY_CURRENT,
        )
        item = report["manifests"][0]
        assert item["late"] is True
        assert "晚于窗口结束" in item["late_note"]
        # 补当前窗口：window_id 仍是应触发窗口
        assert item["window_id"] == "W2-000040"

        # 再来一次同样的迟到重试，仍然只有一笔任务
        again = svc.run_dispatch(
            fired_at=fired.isoformat(),
            scheduled_at=scheduled.isoformat(),
            late_policy=LATE_POLICY_CURRENT,
        )
        assert again["tasks_created"] == 0
        tasks = TaskQueueService().list_due_tasks(
            plan_id=item["plan_id"], window_id=item["window_id"]
        )
        assert len(tasks) == 1

    def test_late_defer_next_window(self) -> None:
        svc = DispatchWindowService()
        scheduled = datetime(2026, 11, 18, 10, 5, tzinfo=timezone.utc)
        fired = scheduled + timedelta(minutes=30)

        report = svc.run_dispatch(
            fired_at=fired.isoformat(),
            scheduled_at=scheduled.isoformat(),
            late_policy=LATE_POLICY_NEXT,
        )
        item = report["manifests"][0]
        assert item["late"] is True
        # 顺延下一窗口：窗口序号 +1
        assert item["window_id"] == "W2-000041"
        assert "顺延至下一窗口" in item["late_note"]

        # 顺延后同窗口重复触发依旧幂等
        again = svc.run_dispatch(
            fired_at=fired.isoformat(),
            scheduled_at=scheduled.isoformat(),
            late_policy=LATE_POLICY_NEXT,
        )
        assert again["tasks_created"] == 0
        assert len(TaskQueueService().list_due_tasks(
            plan_id=item["plan_id"], window_id="W2-000041")) == 1

    def test_two_late_policies_never_duplicate_same_window(self) -> None:
        """同一迟到事件，两种策略各跑一遍，也不会在同一窗口产生两个任务。"""
        svc = DispatchWindowService()
        scheduled = datetime(2026, 11, 18, 10, 5, tzinfo=timezone.utc)
        fired = scheduled + timedelta(minutes=30)

        svc.run_dispatch(
            fired_at=fired.isoformat(),
            scheduled_at=scheduled.isoformat(),
            late_policy=LATE_POLICY_CURRENT,
        )
        svc.run_dispatch(
            fired_at=fired.isoformat(),
            scheduled_at=scheduled.isoformat(),
            late_policy=LATE_POLICY_NEXT,
        )
        for window_id in ("W2-000040", "W2-000041"):
            assert len(TaskQueueService().list_due_tasks(
                plan_id=2, window_id=window_id)) == 1


class TestStatusLinkageAndQuery:
    """状态联动与工作台查询。"""

    def test_task_status_syncs_manifest(self) -> None:
        svc = DispatchWindowService()
        report = svc.run_dispatch(fired_at="2026-11-18T10:05:00+00:00")
        item = report["manifests"][0]
        task_id = item["task_id"]

        TaskQueueService().update_queue_status(task_id, "执行中")
        manifest = store.due_manifests.find(item["plan_id"], item["window_id"])
        assert manifest.status == "执行中"

        TaskQueueService().update_queue_status(task_id, "已完成")
        manifest = store.due_manifests.find(item["plan_id"], item["window_id"])
        assert manifest.status == "已完成"

    def test_cancel_action_syncs_manifest(self) -> None:
        svc = DispatchWindowService()
        report = svc.run_dispatch(fired_at="2026-11-18T10:05:00+00:00")
        item = report["manifests"][0]
        TaskQueueService().run_action(item["task_id"], "取消任务")
        manifest = store.due_manifests.find(item["plan_id"], item["window_id"])
        assert manifest.status == "已取消"

    def test_workbench_filters_by_plan_window_status(self) -> None:
        svc = DispatchWindowService()
        svc.run_dispatch(fired_at="2026-11-18T10:05:00+00:00")
        svc.run_dispatch(fired_at="2026-11-18T10:20:00+00:00")

        plan_id = _active_plan()["id"]
        by_plan = svc.list_manifests(plan_id=plan_id)
        assert by_plan["total"] == 2
        assert "watermark" in by_plan

        window_id = by_plan["items"][0]["window_id"]
        by_window = svc.list_manifests(window_id=window_id)
        assert by_window["total"] == 1

        by_status = svc.list_manifests(status="排队中")
        assert by_status["total"] == 2
        assert svc.list_manifests(status="已完成")["total"] == 0

    def test_watermark_reflects_queue_growth(self) -> None:
        svc = DispatchWindowService()
        before = svc._queue_watermark()
        report = svc.run_dispatch(fired_at="2026-11-18T10:05:00+00:00")
        after = svc._queue_watermark()
        assert after["total"] - before["total"] == report["tasks_created"]
        assert after["排队中"] >= before.get("排队中", 0) + report["tasks_created"]


class TestSchedulerEntryCompatibility:
    """旧调度入口 schedule_crawl_jobs 保持兼容并窗口化。"""

    def test_scheduler_entry_returns_window_manifests(self) -> None:
        result = SchedulerService().schedule_crawl_jobs(
            fired_at="2026-11-18T10:05:00+00:00"
        )
        assert result["triggered"] == result["manifest_count"]
        assert "plans" in result
        for plan in result["plans"]:
            assert plan["window_id"]
            assert plan["manifest_no"]
            assert plan["task_number"]

    def test_scheduler_retry_does_not_duplicate(self) -> None:
        svc = SchedulerService()
        first = svc.schedule_crawl_jobs(fired_at="2026-11-18T10:05:00+00:00")
        second = svc.schedule_crawl_jobs(fired_at="2026-11-18T10:05:00+00:00")
        assert second["tasks_created"] == 0
        assert second["duplicate_dispatches"] == first["manifest_count"]

        # 通道里一轮扫描只发一条审计消息，不再每计划一条重复消息
        msgs = [
            m for m in store.channel.snapshot(topic="rpa.crawl_scheduled")
            if m["source"] == "dispatch_window"
        ]
        assert len(msgs) == 2  # 两轮扫描各一条


class TestDispatchHttpApi:
    """通过真实 ASGI 栈验证调度工作台接口（Query 默认值在 HTTP 下正确解析）。"""

    def test_http_workbench_end_to_end(self) -> None:
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)

        first = client.post(
            "/api/dispatch/run-dispatch",
            json={"fired_at": "2026-11-18T10:05:00+00:00"},
        ).json()
        # 同窗口重试：零新建、全部复用
        retry = client.post(
            "/api/dispatch/run-dispatch",
            json={"fired_at": "2026-11-18T10:10:00+00:00"},
        ).json()
        assert retry["tasks_created"] == 0
        assert retry["tasks_reused"] == first["manifest_count"]

        # 清单查询与过滤
        listed = client.get("/api/dispatch/manifests").json()
        assert listed["total"] == first["manifest_count"]
        wid = listed["items"][0]["window_id"]
        assert client.get(
            f"/api/dispatch/manifests?window_id={wid}"
        ).json()["total"] == 1
        assert client.get(
            "/api/dispatch/manifests?status=排队中"
        ).json()["total"] == first["manifest_count"]

        # 到期排队任务同窗口唯一
        tasks = client.get("/api/dispatch/due-tasks").json()["items"]
        keys = [(t["plan_id"], t["window_id"]) for t in tasks]
        assert len(keys) == len(set(keys))

        # 排队水位可读
        watermark = client.get("/api/dispatch/watermark").json()["watermark"]
        assert watermark["total"] >= first["manifest_count"]
