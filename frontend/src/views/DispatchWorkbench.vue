<template>
  <section class="page dispatch-page">
    <header class="page-head">
      <div>
        <h2>调度工作台</h2>
        <p class="page-desc">
          读取触发器、采集计划与排队水位，解析调度窗口并原子生成触发凭证；同一计划同一窗口只有一笔排队任务，
          重复调度可在此核对。
        </p>
      </div>
    </header>

    <!-- 调度扫描操作区 -->
    <form class="analysis-form" @submit.prevent="runScan">
      <label>
        实际调用时刻
        <input v-model="firedAt" data-testid="fired-at" placeholder="留空取当前时刻，如 2026-11-18T10:05:00+00:00" />
      </label>
      <label>
        应触发时刻（模拟迟到）
        <input v-model="scheduledAt" data-testid="scheduled-at" placeholder="留空与调用时刻一致" />
      </label>
      <label>
        迟到处理策略
        <select v-model="latePolicy" data-testid="late-policy">
          <option value="补当前窗口">补当前窗口</option>
          <option value="顺延下一窗口">顺延下一窗口</option>
        </select>
      </label>
      <button class="btn primary" data-testid="run-dispatch" type="submit" :disabled="scanning">
        {{ scanning ? '扫描中…' : '执行调度扫描' }}
      </button>
      <button class="btn ghost" type="button" @click="resetScanForm">重置时刻</button>
    </form>

    <div v-if="lastScan" class="scan-summary" data-testid="scan-summary">
      <span class="summary-pill">扫描计划：{{ lastScan.scanned_plans }}</span>
      <span class="summary-pill">清单：{{ lastScan.manifest_count }}</span>
      <span class="summary-pill created">新建任务：{{ lastScan.tasks_created }}</span>
      <span class="summary-pill reused">复用任务：{{ lastScan.tasks_reused }}</span>
      <span class="summary-pill dup">同窗口重复调度：{{ lastScan.duplicate_dispatches }}</span>
      <span class="summary-pill">调用时刻：{{ lastScan.fired_at }}</span>
    </div>

    <!-- 排队水位 -->
    <div class="stat-row">
      <article v-for="item in watermarkCards" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <!-- 过滤栏 -->
    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>计划ID</span>
        <input v-model="filterPlanId" data-testid="filter-plan" placeholder="如 2" />
      </label>
      <label class="filter-item">
        <span>窗口标识</span>
        <input v-model="filterWindowId" data-testid="filter-window" placeholder="如 W2-000040" />
      </label>
      <label class="filter-item">
        <span>状态</span>
        <input v-model="filterStatus" data-testid="filter-status" placeholder="排队中/执行中/已完成" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <!-- 标签页 -->
    <div class="tab-row">
      <button
        class="tab"
        :class="{ active: tab === 'manifests' }"
        type="button"
        @click="tab = 'manifests'"
      >
        到期采集任务清单（{{ manifestTotal }}）
      </button>
      <button
        class="tab"
        :class="{ active: tab === 'tasks' }"
        type="button"
        @click="switchTab('tasks')"
      >
        排队任务（窗口溯源，{{ taskTotal }}）
      </button>
    </div>

    <!-- 清单表 -->
    <table v-if="tab === 'manifests'" class="data-table" data-testid="manifest-table">
      <thead>
        <tr>
          <th>清单编号</th>
          <th>计划</th>
          <th>窗口标识</th>
          <th>窗口区间</th>
          <th>排队任务</th>
          <th>调度次数</th>
          <th>迟到</th>
          <th>状态</th>
          <th>凭证</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in manifests" :key="row.manifest_id" :data-testid="`manifest-${row.manifest_id}`">
          <td>{{ row.manifest_no }}</td>
          <td>{{ row.plan_name }}（#{{ row.plan_id }}）</td>
          <td><code>{{ row.window_id }}</code></td>
          <td>{{ row.window_start }} ~ {{ row.window_end }}</td>
          <td>{{ row.task_number ?? '—' }}</td>
          <td>
            <span :class="{ 'dup-badge': row.dispatch_count > 1 }">{{ row.dispatch_count }}</span>
          </td>
          <td>
            <span v-if="row.late" class="late-tag" :title="row.late_note">迟到 · {{ row.late_policy }}</span>
            <span v-else class="on-time-tag">准时</span>
          </td>
          <td>{{ row.status }}</td>
          <td>
            <button class="link" type="button" @click="openCredential(row)">查看凭证</button>
          </td>
        </tr>
        <tr v-if="!manifests.length">
          <td colspan="9" class="empty-state">暂无到期采集任务清单，先执行一次调度扫描</td>
        </tr>
      </tbody>
    </table>

    <!-- 排队任务表 -->
    <table v-else class="data-table" data-testid="due-task-table">
      <thead>
        <tr>
          <th>任务编号</th>
          <th>流程名称</th>
          <th>窗口标识</th>
          <th>凭证编号</th>
          <th>优先级</th>
          <th>迟到说明</th>
          <th>当前状态</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="task in dueTasks" :key="String(task.id)" :data-testid="`due-task-${task.id}`">
          <td>{{ task['任务编号'] }}</td>
          <td>{{ task['流程名称'] }}</td>
          <td><code>{{ task.window_id }}</code></td>
          <td>{{ task.credential_no }}</td>
          <td>{{ task['优先级'] }}</td>
          <td>
            <span v-if="task.late" class="late-tag" :title="String(task.late_note ?? '')">迟到</span>
            <span v-else>—</span>
          </td>
          <td>{{ task.status }}</td>
        </tr>
        <tr v-if="!dueTasks.length">
          <td colspan="7" class="empty-state">暂无带窗口标识的排队任务</td>
        </tr>
      </tbody>
    </table>

    <!-- 凭证详情抽屉 -->
    <div v-if="active" class="modal-backdrop" data-testid="credential-modal" @click.self="active = null">
      <div class="modal credential-modal">
        <h3>触发凭证 · {{ active.manifest_no }}</h3>
        <p class="page-desc">窗口与凭证原子生成，是该窗口调度的可核对凭据。</p>
        <dl class="credential-grid">
          <template v-for="(value, key) in active.trigger_credential" :key="String(key)">
            <dt>{{ credentialLabels[String(key)] ?? String(key) }}</dt>
            <dd>{{ value === null || value === '' ? '—' : String(value) }}</dd>
          </template>
          <dt>窗口区间</dt>
          <dd>{{ active.window_start }} ~ {{ active.window_end }}</dd>
          <dt>累计调度次数</dt>
          <dd>{{ active.dispatch_count }}（同窗口重复调度只保留一笔任务）</dd>
          <dt>迟到说明</dt>
          <dd>{{ active.late_note || '准时到达，无迟到' }}</dd>
          <dt>关联排队任务</dt>
          <dd>{{ active.task_number ?? '尚未入队' }}</dd>
        </dl>
        <div class="page-actions">
          <button class="btn primary" type="button" @click="active = null">关闭</button>
        </div>
      </div>
    </div>

    <footer class="page-foot">
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
      <span>同计划同窗口仅一笔排队任务，可按计划、窗口、状态核对触发凭证与迟到说明</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  loadDueTasks,
  loadManifests,
  loadQueueWatermark,
  runDispatch,
  type DueManifest,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const manifests = ref<DueManifest[]>([])
const dueTasks = ref<EntryRow[]>([])
const manifestTotal = ref(0)
const taskTotal = ref(0)
const watermark = ref<Record<string, number>>({})
const errorMessage = ref('')
const scanning = ref(false)
const tab = ref<'manifests' | 'tasks'>('manifests')
const active = ref<DueManifest | null>(null)

const firedAt = ref('')
const scheduledAt = ref('')
const latePolicy = ref('补当前窗口')

const filterPlanId = ref('')
const filterWindowId = ref('')
const filterStatus = ref('')

const credentialLabels: Record<string, string> = {
  credential_no: '凭证编号',
  window_id: '窗口标识',
  plan_id: '计划ID',
  plan_name: '计划名称',
  cron: 'cron 表达式',
  trigger_id: '触发器ID',
  trigger_name: '触发器名称',
  trigger_type: '触发类型',
  trigger_expr: '触发表达式',
  scheduled_at: '应触发时刻',
  fired_at: '实际调用时刻',
  late: '是否迟到',
  late_policy: '迟到策略',
}

const lastScan = ref<Record<string, unknown> | null>(null)

const watermarkCards = computed(() => [
  { label: '队列总数', value: watermark.value.total ?? 0 },
  { label: '排队中', value: watermark.value['排队中'] ?? 0 },
  { label: '执行中', value: watermark.value['执行中'] ?? 0 },
  { label: '已完成', value: watermark.value['已完成'] ?? 0 },
  { label: '执行失败', value: watermark.value['执行失败'] ?? 0 },
])

function filters() {
  return {
    planId: filterPlanId.value ? Number(filterPlanId.value) : undefined,
    windowId: filterWindowId.value || undefined,
    status: filterStatus.value || undefined,
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const [manifestResult, taskResult, mark] = await Promise.all([
      loadManifests(filters()),
      loadDueTasks(filters()),
      loadQueueWatermark(),
    ])
    manifests.value = manifestResult.items
    manifestTotal.value = manifestResult.total
    dueTasks.value = taskResult.items
    taskTotal.value = taskResult.total
    watermark.value = mark.watermark
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '调度工作台读取失败'
  }
}

async function switchTab(target: 'tasks') {
  tab.value = target
  await reload()
}

async function runScan() {
  scanning.value = true
  errorMessage.value = ''
  try {
    lastScan.value = await runDispatch({
      firedAt: firedAt.value || undefined,
      scheduledAt: scheduledAt.value || undefined,
      latePolicy: latePolicy.value,
    })
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '调度扫描失败'
  } finally {
    scanning.value = false
  }
}

function resetScanForm() {
  firedAt.value = ''
  scheduledAt.value = ''
  latePolicy.value = '补当前窗口'
}

function resetFilters() {
  filterPlanId.value = ''
  filterWindowId.value = ''
  filterStatus.value = ''
  void reload()
}

function openCredential(row: DueManifest) {
  active.value = row
}

onMounted(reload)
</script>

<style scoped>
.dispatch-page { display: block; }
.scan-summary { display: flex; flex-wrap: wrap; gap: 8px; margin: 10px 0; }
.summary-pill {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 999px;
  padding: 4px 12px;
  font-size: 12px;
  color: #334155;
}
.summary-pill.created { border-color: #22c55e; color: #15803d; }
.summary-pill.reused { border-color: #3b82f6; color: #1d4ed8; }
.summary-pill.dup { border-color: #f59e0b; color: #b45309; }
.tab-row { display: flex; gap: 8px; margin: 12px 0; }
.tab {
  border: 1px solid var(--border);
  background: #fff;
  border-radius: 6px 6px 0 0;
  padding: 8px 14px;
  cursor: pointer;
  font-size: 13px;
}
.tab.active { background: var(--brand); color: #fff; border-color: var(--brand); }
.dup-badge {
  display: inline-block;
  min-width: 20px;
  text-align: center;
  background: #fef3c7;
  color: #b45309;
  border-radius: 999px;
  padding: 1px 8px;
  font-weight: 600;
}
.late-tag {
  background: #fee2e2;
  color: #b91c1c;
  border-radius: 4px;
  padding: 2px 8px;
  font-size: 12px;
}
.on-time-tag { color: #15803d; font-size: 12px; }
.credential-modal { width: min(680px, 100%); }
.credential-grid {
  display: grid;
  grid-template-columns: 140px 1fr;
  gap: 8px 12px;
  margin: 12px 0;
}
.credential-grid dt { color: var(--muted); font-size: 12px; }
.credential-grid dd { margin: 0; font-size: 13px; word-break: break-all; }
code {
  background: #f1f5f9;
  border-radius: 4px;
  padding: 1px 6px;
  font-size: 12px;
}
</style>
