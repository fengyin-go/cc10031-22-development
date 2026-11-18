"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

跨模块数据流能力：
- DataChannel: 服务间数据发布/消费通道，支持 topic 订阅、快照读、消费确认
- DirtyDataVault: 脏数据隔离区，存储校验不通过/解析失败的脏数据记录
- FailureRecovery: 失败恢复管理器，记录失败步骤并支持重试
- CollectRecord: 采集记录存储，记录每次采集产出的结构化数据
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.seed import SEED_ROWS

# ---------------------------------------------------------------------------
# 跨模块数据通道 (DataChannel) — 服务间发布/消费
# ---------------------------------------------------------------------------

@dataclass
class ChannelMessage:
    """数据通道中的单条消息，带 topic、来源、负载和确认状态。"""
    msg_id: int
    topic: str
    source_module: str
    payload: dict[str, Any]
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    consumed_by: list[str] = field(default_factory=list)
    confirmed: bool = False

    def mark_consumed(self, consumer: str) -> None:
        if consumer not in self.consumed_by:
            self.consumed_by.append(consumer)


class DataChannel:
    """跨模块数据通道：支持 topic 发布/订阅，服务间数据流转的桥梁。

    用法::

        store.channel.publish("rpa.bot_created", "bot_definition", bot_data)
        msgs = store.channel.consume("step_editor", "rpa.bot_created")
        store.channel.confirm(msg_id, "step_editor")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._messages: list[ChannelMessage] = []

    @property
    def total_messages(self) -> int:
        with self._lock:
            return len(self._messages)

    def publish(self, topic: str, source: str, payload: dict[str, Any]) -> ChannelMessage:
        with self._lock:
            self._seq += 1
            msg = ChannelMessage(
                msg_id=self._seq, topic=topic,
                source_module=source, payload=payload,
            )
            self._messages.append(msg)
            return msg

    def consume(self, consumer: str, topic: str | None = None,
                since_msg_id: int | None = None) -> list[ChannelMessage]:
        """获取指定 consumer 尚未消费的消息。"""
        with self._lock:
            results: list[ChannelMessage] = []
            for msg in self._messages:
                if topic and msg.topic != topic:
                    continue
                if since_msg_id is not None and msg.msg_id <= since_msg_id:
                    continue
                if consumer not in msg.consumed_by:
                    results.append(msg)
                    msg.mark_consumed(consumer)
            return results

    def confirm(self, msg_id: int, consumer: str) -> bool:
        with self._lock:
            for msg in self._messages:
                if msg.msg_id == msg_id:
                    msg.confirmed = True
                    return True
            return False

    def snapshot(self, topic: str | None = None) -> list[dict[str, Any]]:
        """返回消息快照，供跨模块分析/看板消费。"""
        with self._lock:
            msgs = self._messages if topic is None else [
                m for m in self._messages if m.topic == topic
            ]
            return [{
                "msg_id": m.msg_id, "topic": m.topic,
                "source": m.source_module, "payload": m.payload,
                "created_at": m.created_at, "consumed_by": m.consumed_by,
                "confirmed": m.confirmed,
            } for m in msgs]

    def latest(self, topic: str) -> dict[str, Any] | None:
        with self._lock:
            for msg in reversed(self._messages):
                if msg.topic == topic:
                    return {
                        "msg_id": msg.msg_id, "topic": msg.topic,
                        "source": msg.source_module, "payload": msg.payload,
                        "created_at": msg.created_at, "confirmed": msg.confirmed,
                    }
            return None

    def uncomfirmed_count(self) -> int:
        with self._lock:
            return sum(1 for m in self._messages if not m.confirmed)

    def reset(self) -> None:
        with self._lock:
            self._seq = 0
            self._messages = []


# ---------------------------------------------------------------------------
# 脏数据隔离区 (DirtyDataVault)
# ---------------------------------------------------------------------------

@dataclass
class DirtyRecord:
    """脏数据记录：校验失败或解析异常的数据行。"""
    record_id: int
    source_module: str
    batch_ref: str
    raw_data: dict[str, Any]
    reason: str  # 如 "字段缺失", "格式非法", "解析超时"
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    quarantined: bool = True
    recovered_to: str | None = None


class DirtyDataVault:
    """脏数据隔离区：被校验规则或解析器标记为问题数据后移入此处。

    用法::

        vault = store.dirty_vault
        vault.quarantine("data_extract", "BATC-001", row, "缺少必填字段")
        dirty = vault.list_by_source("data_extract")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._records: list[DirtyRecord] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._records)

    def quarantine(self, source: str, batch_ref: str,
                   raw_data: dict[str, Any], reason: str) -> DirtyRecord:
        with self._lock:
            self._seq += 1
            rec = DirtyRecord(
                record_id=self._seq, source_module=source,
                batch_ref=batch_ref, raw_data=raw_data, reason=reason,
            )
            self._records.append(rec)
            return rec

    def list_by_source(self, source: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.source_module == source]

    def list_by_batch(self, batch_ref: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.batch_ref == batch_ref]

    def mark_recovered(self, record_id: int, recovered_to: str) -> bool:
        with self._lock:
            for r in self._records:
                if r.record_id == record_id:
                    r.quarantined = False
                    r.recovered_to = recovered_to
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            quarantined = sum(1 for r in self._records if r.quarantined)
            by_reason: dict[str, int] = {}
            for r in self._records:
                by_reason[r.reason] = by_reason.get(r.reason, 0) + 1
            return {
                "total": len(self._records),
                "quarantined": quarantined,
                "recovered": len(self._records) - quarantined,
                "by_reason": by_reason,
            }

    def reset(self) -> None:
        with self._lock:
            self._seq = 0
            self._records = []


# ---------------------------------------------------------------------------
# 采集记录 (CollectRecord) — 每次采集产出的结构化数据
# ---------------------------------------------------------------------------

@dataclass
class CollectEntry:
    entry_id: int
    batch_ref: str
    source_module: str
    fields: dict[str, Any]
    collected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    validated: bool = False
    quality_score: float = 1.0


class CollectRecordStore:
    """采集记录存储：每次采集流程产出的结构化数据行。

    用法::

        rec = store.collect_records.add("BATC-001", "data_extract", {"字段1": "值1", ...})
        store.collect_records.mark_validated(rec.entry_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[CollectEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def add(self, batch_ref: str, source: str,
            fields: dict[str, Any]) -> CollectEntry:
        with self._lock:
            self._seq += 1
            entry = CollectEntry(
                entry_id=self._seq, batch_ref=batch_ref,
                source_module=source, fields=dict(fields),
            )
            self._entries.append(entry)
            return entry

    def list_by_batch(self, batch_ref: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.batch_ref == batch_ref]

    def list_by_source(self, source: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.source_module == source]

    def mark_validated(self, entry_id: int, score: float = 1.0) -> bool:
        with self._lock:
            for e in self._entries:
                if e.entry_id == entry_id:
                    e.validated = True
                    e.quality_score = score
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            validated = sum(1 for e in self._entries if e.validated)
            by_source: dict[str, int] = {}
            for e in self._entries:
                by_source[e.source_module] = by_source.get(e.source_module, 0) + 1
            return {
                "total": len(self._entries),
                "validated": validated,
                "unvalidated": len(self._entries) - validated,
                "by_source": by_source,
            }

    def reset(self) -> None:
        with self._lock:
            self._seq = 0
            self._entries = []


# ---------------------------------------------------------------------------
# 失败恢复管理 (FailureRecovery)
# ---------------------------------------------------------------------------

@dataclass
class RecoveryEntry:
    recovery_id: int
    module: str
    batch_ref: str
    step: str
    error_detail: str
    attempt: int = 0
    max_attempts: int = 3
    resolved: bool = False
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class FailureRecovery:
    """失败恢复管理器：记录失败步骤并支持自动重试。

    用法::

        rec = store.recovery.log("step_editor", "BATC-001", "元素定位",
                                  "选择器超时未匹配")
        store.recovery.retry(rec.recovery_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[RecoveryEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def log(self, module: str, batch_ref: str, step: str,
            error_detail: str, max_attempts: int = 3) -> RecoveryEntry:
        with self._lock:
            self._seq += 1
            entry = RecoveryEntry(
                recovery_id=self._seq, module=module,
                batch_ref=batch_ref, step=step,
                error_detail=error_detail, max_attempts=max_attempts,
            )
            self._entries.append(entry)
            return entry

    def retry(self, recovery_id: int) -> tuple[bool, str]:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    if e.resolved:
                        return False, "已恢复，无需重试"
                    e.attempt += 1
                    if e.attempt > e.max_attempts:
                        return False, f"已达最大重试次数 {e.max_attempts}"
                    return True, f"重试 {e.attempt}/{e.max_attempts}"
            return False, "恢复记录不存在"

    def resolve(self, recovery_id: int) -> bool:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    e.resolved = True
                    return True
            return False

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries if not e.resolved)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            pending = sum(1 for e in self._entries if not e.resolved)
            return {
                "total": len(self._entries),
                "resolved": len(self._entries) - pending,
                "pending": pending,
            }

    def reset(self) -> None:
        with self._lock:
            self._seq = 0
            self._entries = []


# ---------------------------------------------------------------------------
# 到期采集任务清单 (DueTaskManifest) — 调度窗口 + 触发凭证的可核对产物
# ---------------------------------------------------------------------------

@dataclass
class DueTaskManifest:
    """到期采集任务清单：一次调度扫描中，一个到期计划在一个调度窗口的唯一凭证。

    同一 plan_id + window_id 只会生成一条清单，重复调度只累加 dispatch_count，
    不会再产生新的排队任务。
    """
    manifest_id: int
    manifest_no: str
    plan_id: int
    plan_name: str
    window_id: str
    window_start: str
    window_end: str
    trigger_credential: dict[str, Any]
    task_id: int | None = None
    task_number: str | None = None
    status: str = "待入队"  # 待入队 / 排队中 / 执行中 / 已完成 / 已取消 / 执行失败
    late: bool = False
    late_note: str = ""
    late_policy: str = "补当前窗口"
    dispatch_count: int = 1
    fired_at: str = ""
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class DueTaskManifestStore:
    """到期采集任务清单存储：调度窗口与触发凭证原子落库，支撑窗口级幂等。

    用法::

        manifest = store.due_manifests.create_or_get(plan_id, window_id, ...)
        if manifest.dispatch_count == 1: ...  # 首次调度
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._manifests: list[DueTaskManifest] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._manifests)

    def find(self, plan_id: int, window_id: str) -> DueTaskManifest | None:
        """按 计划+窗口 幂等键查找已存在清单（同窗口重复调度的判定依据）。"""
        with self._lock:
            for m in self._manifests:
                if m.plan_id == plan_id and m.window_id == window_id:
                    return m
            return None

    def create_or_get(
        self,
        *,
        manifest_no: str,
        plan_id: int,
        plan_name: str,
        window_id: str,
        window_start: str,
        window_end: str,
        trigger_credential: dict[str, Any],
        late: bool,
        late_note: str,
        late_policy: str,
        fired_at: str,
    ) -> tuple[DueTaskManifest, bool]:
        """原子生成窗口凭证：已存在则只累加调度次数，返回 (清单, 是否首次创建)。"""
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            for existing in self._manifests:
                if existing.plan_id == plan_id and existing.window_id == window_id:
                    existing.dispatch_count += 1
                    existing.updated_at = now_iso
                    return existing, False
            self._seq += 1
            manifest = DueTaskManifest(
                manifest_id=self._seq,
                manifest_no=manifest_no,
                plan_id=plan_id,
                plan_name=plan_name,
                window_id=window_id,
                window_start=window_start,
                window_end=window_end,
                trigger_credential=dict(trigger_credential),
                late=late,
                late_note=late_note,
                late_policy=late_policy,
                fired_at=fired_at,
            )
            self._manifests.append(manifest)
            return manifest, True

    def bind_task(self, manifest_id: int, task_id: int, task_number: str) -> bool:
        """清单与排队任务绑定（同窗口只允许绑定一次）。"""
        with self._lock:
            for m in self._manifests:
                if m.manifest_id == manifest_id:
                    m.task_id = task_id
                    m.task_number = task_number
                    m.status = "排队中"
                    m.updated_at = datetime.now(timezone.utc).isoformat()
                    return True
            return False

    def sync_status(self, task_id: int, status: str) -> bool:
        """排队任务状态联动回写清单待办。"""
        with self._lock:
            for m in self._manifests:
                if m.task_id == task_id:
                    m.status = status
                    m.updated_at = datetime.now(timezone.utc).isoformat()
                    return True
            return False

    def list_all(
        self,
        *,
        plan_id: int | None = None,
        window_id: str | None = None,
        status: str | None = None,
    ) -> list[DueTaskManifest]:
        with self._lock:
            result = list(self._manifests)
        if plan_id is not None:
            result = [m for m in result if m.plan_id == plan_id]
        if window_id:
            result = [m for m in result if m.window_id == window_id]
        if status:
            result = [m for m in result if m.status == status]
        return result

    def to_dict(self, manifest: DueTaskManifest) -> dict[str, Any]:
        return {
            "manifest_id": manifest.manifest_id,
            "manifest_no": manifest.manifest_no,
            "plan_id": manifest.plan_id,
            "plan_name": manifest.plan_name,
            "window_id": manifest.window_id,
            "window_start": manifest.window_start,
            "window_end": manifest.window_end,
            "trigger_credential": dict(manifest.trigger_credential),
            "task_id": manifest.task_id,
            "task_number": manifest.task_number,
            "status": manifest.status,
            "late": manifest.late,
            "late_note": manifest.late_note,
            "late_policy": manifest.late_policy,
            "dispatch_count": manifest.dispatch_count,
            "fired_at": manifest.fired_at,
            "created_at": manifest.created_at,
            "updated_at": manifest.updated_at,
        }

    def reset(self) -> None:
        with self._lock:
            self._seq = 0
            self._manifests = []


# ---------------------------------------------------------------------------
# 增强版 Store — 整合通道、脏数据、采集记录、恢复
# ---------------------------------------------------------------------------

class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块基础设施
        self.channel = DataChannel()
        self.dirty_vault = DirtyDataVault()
        self.collect_records = CollectRecordStore()
        self.recovery = FailureRecovery()
        # 到期采集任务清单：调度窗口 + 触发凭证的可核对凭证
        self.due_manifests = DueTaskManifestStore()

    # ---- 测试辅助：重置内存仓库到初始种子 ----

    def reset(self) -> None:
        """恢复初始种子数据并清空所有跨模块运行态，供测试之间隔离。

        全部采用原地清空：不替换 store 自身及其成员实例，保证模块级单例
        服务持有的引用始终有效。
        """
        self._tables.clear()
        for name, rows in SEED_ROWS.items():
            self._tables[name] = [dict(row) for row in rows]
        self.channel.reset()
        self.dirty_vault.reset()
        self.collect_records.reset()
        self.recovery.reset()
        self.due_manifests.reset()

    # ---- 基础数据操作 ----

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()