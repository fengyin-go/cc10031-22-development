"""调度计划领域服务 — 管理定时采集计划，输出 domain_snapshot。

数据工程核心：数据清洗与转换链路处理 — 跨模块数据产物

调度触发（schedule_crawl_jobs）已升级为窗口化扫描：
不再"遍历已激活计划直接发布消息"，而是委托 DispatchWindowService 解析
调度窗口、原子生成触发凭证和到期采集任务清单，同一计划同一窗口只有
一笔排队任务，重试可区分新窗口与同窗口重复触发。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.dispatch_window import DispatchWindowService, LATE_POLICY_CURRENT
from app.store import store


class SchedulerService(BaseModuleService):
    MODULE = "scheduler"
    ENTITY = "采集计划"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["计划名称", "绑定流程", "cron表达式", "执行窗口", "并发数", "优先级", "通知渠道"]
    REQUIRED_FIELDS = ["计划名称", "绑定流程", "cron表达式"]
    STATUS_ORDER = ["草稿", "已激活", "已暂停", "已归档"]
    ACTION_RULES = {"激活计划": "草稿", "暂停计划": "已激活", "归档计划": "已暂停"}
    NEGATIVE_ACTIONS = []

    def schedule_crawl_jobs(
        self,
        *,
        fired_at: str | None = None,
        scheduled_at: str | None = None,
        late_policy: str = LATE_POLICY_CURRENT,
    ) -> dict[str, Any]:
        """调度触发：生成带窗口标识的到期采集任务清单并按窗口幂等入队。

        修复前每次调用都为全部已激活计划直接发布消息，重试无法区分新窗口
        还是同窗口重复触发；现在同一计划同一窗口只对应一个排队任务，
        重复调度在清单上累加 dispatch_count 并复用既有任务。

        Returns:
            兼容旧结构的 {"triggered", "plans"}，并附窗口化扫描明细。
        """
        dispatch = DispatchWindowService()
        report = dispatch.run_dispatch(
            fired_at=fired_at,
            scheduled_at=scheduled_at,
            late_policy=late_policy,
        )

        # 兼容既有调用方：plans 仍给出 plan_id / plan_name / msg_id 口径，
        # msg_id 取本轮扫描的审计消息（一轮一条，不再每计划一条）。
        plans = [
            {
                "plan_id": item["plan_id"],
                "plan_name": item["plan_name"],
                "window_id": item["window_id"],
                "manifest_no": item["manifest_no"],
                "task_id": item["task_id"],
                "task_number": item["task_number"],
                "dispatch_count": item["dispatch_count"],
                "late": item["late"],
                "msg_id": report["audit_msg_id"],
            }
            for item in report["manifests"]
        ]
        return {
            "triggered": report["manifest_count"],
            "manifest_count": report["manifest_count"],
            "plans": plans,
            "tasks_created": report["tasks_created"],
            "tasks_reused": report["tasks_reused"],
            "duplicate_dispatches": report["duplicate_dispatches"],
            "fired_at": report["fired_at"],
            "scheduled_at": report["scheduled_at"],
            "late_policy": report["late_policy"],
            "window_manifests": report["manifests"],
        }

    def list_active_plans(self) -> list[dict[str, Any]]:
        """列出已激活的调度计划。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"), "name": r.get("计划名称"),
                "cron": r.get("cron表达式"), "bot": r.get("绑定流程"),
                "window": r.get("执行窗口"), "concurrency": r.get("并发数"),
                "priority": r.get("优先级"),
            }
            for r in rows if r.get("status") == "已激活"
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        manifests = store.due_manifests.list_all()
        return {
            **base,
            "active_plans": sum(1 for r in rows if r.get("status") == "已激活"),
            "paused_plans": sum(1 for r in rows if r.get("status") == "已暂停"),
            "archived_plans": sum(1 for r in rows if r.get("status") == "已归档"),
            "due_manifests": len(manifests),
            "duplicate_dispatches": sum(
                1 for m in manifests if m.dispatch_count > 1
            ),
            "late_manifests": sum(1 for m in manifests if m.late),
        }