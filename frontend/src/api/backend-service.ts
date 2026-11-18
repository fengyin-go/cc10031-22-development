import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 跨模块数据流：获取全平台领域快照 */
export async function loadNodeStats(module?: string): Promise<Record<string, unknown>> {
  const query = module ? `?module=${module}` : ''
  const response = await request(`/api/nodestats${query}`)
  if (!response.ok) throw new Error('节点统计读取失败')
  return await response.json()
}

/** 跨模块数据流：获取指定 topic 的通道消息 */
export async function loadChannelSnapshot(topic: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/channel/${topic}`)
  if (!response.ok) throw new Error('通道快照读取失败')
  return await response.json()
}

/** 跨模块数据流：查询脏数据隔离区 */
export async function loadDirtyVault(source?: string): Promise<Record<string, unknown>> {
  const query = source ? `?source=${source}` : ''
  const response = await request(`/api/dirty-vault${query}`)
  if (!response.ok) throw new Error('脏数据查询失败')
  return await response.json()
}

/** 跨模块数据流：查询采集记录 */
export async function loadCollectRecords(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${batchRef}` : ''
  const response = await request(`/api/collect-records${query}`)
  if (!response.ok) throw new Error('采集记录查询失败')
  return await response.json()
}

/** 跨模块数据流：执行RPA全生命周期管道 */
export async function triggerPipeline(lifecycle: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/pipeline/${lifecycle}`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('管道执行失败')
  return await response.json()
}

/** 跨模块数据流：获取失败恢复状态 */
export async function loadRecoveryStats(): Promise<Record<string, unknown>> {
  const response = await request('/api/recovery')
  if (!response.ok) throw new Error('恢复状态查询失败')
  return await response.json()
}

// ---- 调度工作台：窗口凭证与到期采集任务清单 ----

export type DispatchRunOptions = {
  firedAt?: string
  scheduledAt?: string
  latePolicy?: string
}

export type DueManifest = {
  manifest_id: number
  manifest_no: string
  plan_id: number
  plan_name: string
  window_id: string
  window_start: string
  window_end: string
  trigger_credential: Record<string, string | number | boolean | null>
  task_id: number | null
  task_number: string | null
  status: string
  late: boolean
  late_note: string
  late_policy: string
  dispatch_count: number
  fired_at: string
}

/** 触发一轮调度扫描（可模拟迟到：scheduledAt 早于 firedAt） */
export async function runDispatch(options: DispatchRunOptions = {}): Promise<Record<string, unknown>> {
  const response = await request('/api/dispatch/run-dispatch', {
    method: 'POST',
    body: JSON.stringify({
      fired_at: options.firedAt ?? null,
      scheduled_at: options.scheduledAt ?? null,
      late_policy: options.latePolicy ?? '补当前窗口',
    }),
  })
  if (!response.ok) throw new Error('调度扫描失败')
  return await response.json()
}

/** 到期采集任务清单：按计划、窗口、状态核对凭证与迟到说明 */
export async function loadManifests(filters: {
  planId?: number
  windowId?: string
  status?: string
} = {}): Promise<{ items: DueManifest[]; total: number; watermark: Record<string, number> }> {
  const params = new URLSearchParams()
  if (filters.planId !== undefined) params.set('plan_id', String(filters.planId))
  if (filters.windowId) params.set('window_id', filters.windowId)
  if (filters.status) params.set('status', filters.status)
  const response = await request(`/api/dispatch/manifests?${params.toString()}`)
  if (!response.ok) throw new Error('到期采集任务清单读取失败')
  return await response.json()
}

/** 到期清单生成的排队任务（窗口溯源） */
export async function loadDueTasks(filters: {
  planId?: number
  windowId?: string
  status?: string
} = {}): Promise<{ items: EntryRow[]; total: number }> {
  const params = new URLSearchParams()
  if (filters.planId !== undefined) params.set('plan_id', String(filters.planId))
  if (filters.windowId) params.set('window_id', filters.windowId)
  if (filters.status) params.set('status', filters.status)
  const response = await request(`/api/dispatch/due-tasks?${params.toString()}`)
  if (!response.ok) throw new Error('到期排队任务读取失败')
  return await response.json()
}

/** 新到达调度排队水位 */
export async function loadQueueWatermark(): Promise<{ watermark: Record<string, number> }> {
  const response = await request('/api/dispatch/watermark')
  if (!response.ok) throw new Error('排队水位读取失败')
  return await response.json()
}
