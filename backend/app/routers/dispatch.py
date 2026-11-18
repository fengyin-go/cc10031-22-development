"""调度工作台路由：扫描到期计划、核对窗口凭证与到期采集任务清单。

与通用资源路由并列，提供窗口化调度的专用接口：
- POST /api/dispatch/run-dispatch   触发一轮调度扫描（支持模拟迟到）
- GET  /api/dispatch/manifests      按计划/窗口/状态查看到期采集任务清单
- GET  /api/dispatch/due-tasks      查看清单生成的排队任务（窗口溯源）
- GET  /api/dispatch/watermark      查看新到达调度排队水位
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from app.schemas import DispatchRunPayload
from app.services.dispatch_window import (
    DispatchWindowService,
    VALID_LATE_POLICIES,
)
from app.services.task_queue import TaskQueueService
from app.store import store

router = APIRouter(prefix="/api/dispatch", tags=["调度工作台"])


@router.post("/run-dispatch")
def run_dispatch(payload: DispatchRunPayload) -> dict[str, Any]:
    """触发一轮调度扫描：解析窗口、原子生成凭证、按窗口幂等入队。"""
    late_policy = payload.late_policy
    if late_policy not in VALID_LATE_POLICIES:
        late_policy = VALID_LATE_POLICIES[0]
    service = DispatchWindowService()
    return service.run_dispatch(
        fired_at=payload.fired_at,
        scheduled_at=payload.scheduled_at,
        late_policy=late_policy,
    )


@router.get("/manifests")
def list_manifests(
    plan_id: int | None = Query(default=None),
    window_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> dict[str, Any]:
    """到期采集任务清单：按计划、窗口、状态核对触发凭证和迟到说明。"""
    service = DispatchWindowService()
    return service.list_manifests(plan_id=plan_id, window_id=window_id, status=status)


@router.get("/due-tasks")
def list_due_tasks(
    plan_id: int | None = Query(default=None),
    window_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> dict[str, Any]:
    """排队任务中由到期清单生成的部分，带窗口标识和凭证编号。"""
    service = TaskQueueService()
    rows = service.list_due_tasks(plan_id=plan_id, window_id=window_id, status=status)
    return {"items": rows, "total": len(rows)}


@router.get("/watermark")
def queue_watermark() -> dict[str, Any]:
    """新到达调度排队水位：任务队列各状态计数。"""
    service = DispatchWindowService()
    return {"watermark": service._queue_watermark()}
