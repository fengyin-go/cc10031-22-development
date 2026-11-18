"""调度窗口领域服务 — 把"调度触发"升级为带窗口标识的可核对凭证。

核心问题（修复前）：
    调度入口每次遍历所有已激活计划并直接发布消息，重试时无法区分是新的
    时间窗口还是同一窗口的重复触发，导致同一窗口出现多笔排队任务。

修复模型：
    读取 触发器(trigger_config) + 采集计划(scheduler) + 队列排队水位(task_queue)，
    为每个到期计划解析「调度窗口」并原子生成「触发凭证」，产出带窗口标识的
    到期采集任务清单；同一计划同一窗口只有一条清单、只对应一个排队任务。

不变量：
    1. 调度窗口与触发凭证原子生成（DueTaskManifestStore 单锁保证）。
    2. 同一 plan_id + window_id 只有一个排队任务（按窗口标识复用）。
    3. 迟到事件无论"补当前窗口"还是"顺延下一窗口"，都不会产生两个同窗口任务。
"""
from __future__ import annotations

import re
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from app.store import store
from app.services.task_queue import TaskQueueService

# 窗口切片默认粒度（分钟）：种子数据里的执行窗口是占位文本时回退使用
DEFAULT_WINDOW_MINUTES = 15

# 迟到事件处理策略
LATE_POLICY_CURRENT = "补当前窗口"
LATE_POLICY_NEXT = "顺延下一窗口"
VALID_LATE_POLICIES = (LATE_POLICY_CURRENT, LATE_POLICY_NEXT)


@dataclass(frozen=True)
class WindowResolution:
    """一次调度扫描解析出的调度窗口。"""
    window_id: str
    window_start: datetime
    window_end: datetime
    scheduled_at: datetime
    fired_at: datetime
    late: bool
    late_note: str
    late_policy: str
    size_minutes: int

    def as_payload(self) -> dict[str, str]:
        return {
            "window_id": self.window_id,
            "window_start": _fmt(self.window_start),
            "window_end": _fmt(self.window_end),
        }


def _parse_dt(value: str | datetime | None) -> datetime:
    """把入参归一化为带 UTC 时区的 datetime；空值取当前 UTC 时间。"""
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return datetime.now(timezone.utc)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return datetime.now(timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _fmt(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _parse_window_minutes(raw_window: Any) -> int:
    """解析计划的"执行窗口"字段为分钟粒度。

    兼容三种口径：
    - 纯数字（按分钟）："15" / 15
    - 带单位："30分钟"、"1h"、"2小时"
    - 时间区间："09:00-10:00"（取跨度，回退到该跨度内按默认粒度切片）
    无法识别时回退到 DEFAULT_WINDOW_MINUTES，兼容既有占位种子数据。
    """
    if raw_window is None:
        return DEFAULT_WINDOW_MINUTES
    text = str(raw_window).strip()
    if not text:
        return DEFAULT_WINDOW_MINUTES

    range_match = re.match(
        r"^(\d{1,2}):(\d{2})\s*[-~～]\s*(\d{1,2}):(\d{2})$", text
    )
    if range_match:
        h1, m1, h2, m2 = (int(part) for part in range_match.groups())
        span = (h2 * 60 + m2) - (h1 * 60 + m1)
        return span if span > 0 else DEFAULT_WINDOW_MINUTES

    unit_match = re.match(r"^(\d+)\s*(分钟|分|min|m|小时|时|h|小时)?$", text, re.IGNORECASE)
    if unit_match:
        amount = int(unit_match.group(1))
        unit = (unit_match.group(2) or "分钟").lower()
        if unit in ("小时", "时", "h"):
            amount *= 60
        return amount if amount > 0 else DEFAULT_WINDOW_MINUTES

    if text.isdigit():
        amount = int(text)
        return amount if amount > 0 else DEFAULT_WINDOW_MINUTES

    return DEFAULT_WINDOW_MINUTES


class DispatchWindowService:
    """调度窗口服务：扫描到期计划、原子生成窗口凭证并驱动任务入队。"""

    def __init__(self) -> None:
        self._scan_lock = threading.Lock()

    @property
    def _task_queue(self) -> TaskQueueService:
        """现取任务队列服务，避免缓存实例在 store 重置后持有陈旧引用。"""
        return TaskQueueService()

    # ---- 窗口解析 ----

    def resolve_window(
        self,
        plan: dict[str, Any],
        fired_at: datetime,
        late_policy: str = LATE_POLICY_CURRENT,
        scheduled_at: datetime | None = None,
    ) -> WindowResolution:
        """解析计划应触发时刻所属窗口，并按实际调用时刻判定迟到。

        窗口归属以计划应触发时刻（scheduled_at，缺省等于 fired_at）做 UTC
        00:00 锚点切片，window_id 可核对：W{计划ID}-{窗口序号}。
        实际调用晚于应触发窗口结束即判定迟到，按策略补当前窗口或顺延下一窗口。
        """
        if late_policy not in VALID_LATE_POLICIES:
            late_policy = LATE_POLICY_CURRENT
        if scheduled_at is None:
            scheduled_at = fired_at

        plan_id = int(plan.get("id", 0))
        size = _parse_window_minutes(plan.get("执行窗口"))
        anchor = scheduled_at.replace(hour=0, minute=0, second=0, microsecond=0)
        elapsed = int((scheduled_at - anchor).total_seconds()) // 60
        index = elapsed // size
        window_start = anchor + timedelta(minutes=index * size)
        window_end = window_start + timedelta(minutes=size)

        late = fired_at >= window_end
        target_start, target_end, target_index = window_start, window_end, index
        late_note = ""
        if late:
            if late_policy == LATE_POLICY_NEXT:
                target_index = index + 1
                target_start = window_end
                target_end = target_start + timedelta(minutes=size)
                late_note = (
                    f"应触发窗口 {_fmt(window_start)}~{_fmt(window_end)} 已结束，"
                    f"实际调用 {_fmt(fired_at)} 晚于窗口结束，按策略顺延至下一窗口 "
                    f"{_fmt(target_start)}~{_fmt(target_end)}"
                )
            else:
                late_note = (
                    f"应触发窗口 {_fmt(window_start)}~{_fmt(window_end)} 已结束，"
                    f"实际调用 {_fmt(fired_at)} 晚于窗口结束，按策略补入当前窗口 "
                    f"{_fmt(window_start)}~{_fmt(window_end)}"
                )

        window_id = f"W{plan_id}-{target_index:06d}"
        return WindowResolution(
            window_id=window_id,
            window_start=target_start,
            window_end=target_end,
            scheduled_at=scheduled_at,
            fired_at=fired_at,
            late=late,
            late_note=late_note,
            late_policy=late_policy,
            size_minutes=size,
        )

    # ---- 触发凭证 ----

    def _match_trigger(self, plan: dict[str, Any]) -> dict[str, Any] | None:
        """读取触发器：优先按计划绑定流程/名称匹配，其次取已激活触发器。"""
        triggers = store.rows("trigger_config")
        bot_ref = str(plan.get("绑定流程") or "")
        plan_name = str(plan.get("计划名称") or "")

        def _matches(trigger: dict[str, Any]) -> bool:
            haystack = " ".join(
                str(trigger.get(field) or "")
                for field in ("触发器名称", "事件源", "前置条件")
            )
            return bool(bot_ref and bot_ref in haystack) or bool(
                plan_name and plan_name in haystack
            )

        for trigger in triggers:
            if trigger.get("status") == "已激活" and _matches(trigger):
                return trigger
        for trigger in triggers:
            if trigger.get("status") == "已激活":
                return trigger
        return triggers[0] if triggers else None

    def build_credential(
        self,
        plan: dict[str, Any],
        trigger: dict[str, Any] | None,
        window: WindowResolution,
    ) -> dict[str, Any]:
        """生成触发凭证：窗口标识 + 计划/触发器指纹 + 触发时刻，可核对、可追溯。"""
        credential_no = (
            f"TC-{plan.get('id')}-{window.window_id}-"
            f"{window.fired_at.strftime('%Y%m%d%H%M%S')}"
        )
        return {
            "credential_no": credential_no,
            "window_id": window.window_id,
            "plan_id": plan.get("id"),
            "plan_name": plan.get("计划名称"),
            "cron": plan.get("cron表达式"),
            "trigger_id": trigger.get("id") if trigger else None,
            "trigger_name": trigger.get("触发器名称") if trigger else None,
            "trigger_type": trigger.get("触发类型") if trigger else None,
            "trigger_expr": trigger.get("调度表达式") if trigger else None,
            "scheduled_at": _fmt(window.scheduled_at),
            "fired_at": _fmt(window.fired_at),
            "late": window.late,
            "late_policy": window.late_policy,
        }

    # ---- 调度扫描主入口 ----

    def run_dispatch(
        self,
        *,
        fired_at: str | datetime | None = None,
        scheduled_at: str | datetime | None = None,
        late_policy: str = LATE_POLICY_CURRENT,
    ) -> dict[str, Any]:
        """读取触发器/采集计划/队列水位，生成到期采集任务清单并按窗口复用排队任务。

        原子性：整轮扫描在单锁内完成；窗口凭证由 DueTaskManifestStore 原子
        create-or-get，排队任务由 TaskQueueService 按窗口标识幂等入队。
        同一计划同一窗口的重复调度只能看到一笔任务。

        Args:
            fired_at: 实际调度调用时刻（缺省当前 UTC 时间）
            scheduled_at: 计划应触发时刻（缺省与 fired_at 一致）；模拟迟到时
                传入早于 fired_at 的时刻，使窗口归属落在已结束的窗口
            late_policy: 迟到处理策略（补当前窗口 / 顺延下一窗口）
        """
        fired = _parse_dt(fired_at)
        scheduled = _parse_dt(scheduled_at) if scheduled_at else fired
        if late_policy not in VALID_LATE_POLICIES:
            late_policy = LATE_POLICY_CURRENT

        with self._scan_lock:
            plans = [
                row for row in store.rows("scheduler")
                if row.get("status") == "已激活"
            ]
            watermark_before = self._queue_watermark()
            manifests: list[dict[str, Any]] = []
            created_tasks = 0
            reused_tasks = 0
            duplicate_dispatches = 0

            for plan in plans:
                trigger = self._match_trigger(plan)
                window = self.resolve_window(plan, fired, late_policy, scheduled)
                credential = self.build_credential(plan, trigger, window)

                manifest_no = (
                    f"DUE-{plan.get('id')}-{window.window_id}"
                )
                manifest, first_created = store.due_manifests.create_or_get(
                    manifest_no=manifest_no,
                    plan_id=int(plan.get("id", 0)),
                    plan_name=str(plan.get("计划名称") or ""),
                    window_id=window.window_id,
                    window_start=_fmt(window.window_start),
                    window_end=_fmt(window.window_end),
                    trigger_credential=credential,
                    late=window.late,
                    late_note=window.late_note,
                    late_policy=window.late_policy,
                    fired_at=_fmt(fired),
                )
                if not first_created:
                    duplicate_dispatches += 1

                # 任务队列直接使用到期采集任务清单，按窗口标识复用已存在的排队任务
                enqueue = self._task_queue.enqueue_due_task(plan, manifest, credential)
                if enqueue["created"]:
                    created_tasks += 1
                    store.due_manifests.bind_task(
                        manifest.manifest_id,
                        enqueue["task_id"],
                        enqueue["task_number"],
                    )
                else:
                    reused_tasks += 1

                manifests.append(store.due_manifests.to_dict(manifest))

            # 审计消息：一轮扫描只发一条，重试不再制造重复业务消息
            watermark_after = self._queue_watermark()
            audit = store.channel.publish(
                topic="rpa.crawl_scheduled",
                source="dispatch_window",
                payload={
                    "fired_at": _fmt(fired),
                    "scheduled_at": _fmt(scheduled),
                    "late_policy": late_policy,
                    "scanned_plans": len(plans),
                    "manifests": len(manifests),
                    "tasks_created": created_tasks,
                    "tasks_reused": reused_tasks,
                    "duplicate_dispatches": duplicate_dispatches,
                    "queue_watermark_before": watermark_before,
                    "queue_watermark_after": watermark_after,
                },
            )

            return {
                "fired_at": _fmt(fired),
                "scheduled_at": _fmt(scheduled),
                "late_policy": late_policy,
                "scanned_plans": len(plans),
                "manifests": manifests,
                "manifest_count": len(manifests),
                "tasks_created": created_tasks,
                "tasks_reused": reused_tasks,
                "duplicate_dispatches": duplicate_dispatches,
                "queue_watermark_before": watermark_before,
                "queue_watermark": watermark_after,
                "audit_msg_id": audit.msg_id,
            }

    # ---- 查询 ----

    def list_manifests(
        self,
        *,
        plan_id: int | None = None,
        window_id: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        """调度工作台查询：按计划、窗口、状态过滤到期采集任务清单。"""
        rows = store.due_manifests.list_all(
            plan_id=plan_id, window_id=window_id, status=status
        )
        items = [store.due_manifests.to_dict(m) for m in rows]
        return {
            "items": items,
            "total": len(items),
            "watermark": self._queue_watermark(),
        }

    def _queue_watermark(self) -> dict[str, int]:
        """新到达调度排队水位：各状态排队任务数量，供扫描时核对。"""
        rows = store.rows("task_queue")
        watermark: dict[str, int] = {}
        for row in rows:
            status = str(row.get("status") or "未知")
            watermark[status] = watermark.get(status, 0) + 1
        watermark["total"] = len(rows)
        return watermark
