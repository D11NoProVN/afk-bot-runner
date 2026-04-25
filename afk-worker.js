const bedrock = require('bedrock-protocol')
const fs = require('fs')
const http = require('http')
const path = require('path')
const { BedrockTaskHelper, createLogger, getAuthCacheDir, inspect, stripMcColorCodes, summarizeItem } = require('./main')
const { findSkeletonSpawner, isSkeletonSpawnerSummary, shouldStartSpawnerHandoff } = require('./spawner-handoff')

function buildRandomAreaPool(min = 10, max = 70) {
  const values = []
  for (let area = min; area <= max; area += 1) values.push(area)
  for (let i = values.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1))
    const tmp = values[i]
    values[i] = values[j]
    values[j] = tmp
  }
  return values
}

const DEFAULT_AREAS = buildRandomAreaPool()
const AFK_COMMAND_DELAY_MS = 5000
const AFK_AUTO_ASSIGN_GRACE_MS = 6500
const AFK_RESULT_TIMEOUT_MS = 8000
const HEARTBEAT_MS = 30000
const RECONNECT_DELAY_MS = 10000
const RECONNECT_WATCHDOG_MS = Math.max(30000, Number(process.env.RECONNECT_WATCHDOG_MS || 90000))
const RAKNET_CONNECT_WATCHDOG_MS = Math.max(8000, Number(process.env.RAKNET_CONNECT_WATCHDOG_MS || 20000))
const LOGIN_PROGRESS_WATCHDOG_MS = Math.max(15000, Number(process.env.LOGIN_PROGRESS_WATCHDOG_MS || 30000))
const WATCHDOG_RECONNECT_DELAY_MS = Math.max(1000, Number(process.env.WATCHDOG_RECONNECT_DELAY_MS || 3000))
const CONNECT_TIMEOUT_MS = Math.max(15000, Number(process.env.CONNECT_TIMEOUT_MS || 60000))
const ALREADY_LOGGED_IN_RECONNECT_DELAY_MS = 3000
const ALREADY_LOGGED_IN_MAX_RETRIES = 3
const AFK_ANTI_IDLE_MS = 45000
const AFK_AUTH_INPUT_MS = Math.max(50, Number(process.env.AFK_AUTH_INPUT_MS || 50))
const AFK_AUTH_INPUT_LOG_EVERY = Math.max(1, Number(process.env.AFK_AUTH_INPUT_LOG_EVERY || 1200))
const AFK_MOVEMENT_PACKET_MODE = String(process.env.AFK_MOVEMENT_PACKET_MODE || 'auth').toLowerCase()
const AFK_TICK_SYNC_EVERY = Math.max(1, Number(process.env.AFK_TICK_SYNC_EVERY || 10))
const AFK_LOOK_JITTER_DEGREES = Math.max(0, Number(process.env.AFK_LOOK_JITTER_DEGREES || 1.25))
const AFK_LOOK_JITTER_PERIOD_TICKS = Math.max(20, Number(process.env.AFK_LOOK_JITTER_PERIOD_TICKS || 240))
const AFK_ANCHOR_CAPTURE_DELAY_MS = 2000
const AFK_DRIFT_CHECK_MS = 15000
const AFK_DRIFT_DISTANCE = 24
const AFK_REJOIN_COOLDOWN_MS = 12000
const DASHBOARD_REFRESH_MS = 60000
const DASHBOARD_HOST = process.env.AFK_WEB_HOST || '127.0.0.1'
const DASHBOARD_PORT = Number(process.env.AFK_WEB_PORT || 3020)
const DASHBOARD_LOG_LIMIT = 240
const AUTO_SPAWNER_ENABLED = process.env.AUTO_SPAWNER_ENABLED === 'true'
const SPAWNER_SHARD_THRESHOLD = Number(process.env.SPAWNER_SHARD_THRESHOLD || 1500)
const JAVA_MAIN_USERNAME = String(process.env.JAVA_MAIN_USERNAME || '').trim()
const SPAWNER_HANDOFF_COOLDOWN_MS = Number(process.env.SPAWNER_HANDOFF_COOLDOWN_MS || 60000)
function parseAreaList(raw) {
  if (!raw) return DEFAULT_AREAS
  return raw
    .split(',')
    .map(value => Number(String(value).trim()))
    .filter(value => Number.isInteger(value) && value > 0)
}

function normalizeChat(message) {
  return stripMcColorCodes(String(message || ''))
    .replace(/[ᴀᴬⓐ🄰Ａ]/g, 'a')
    .replace(/[ꜰғᶠⓕ🄵Ｆ]/g, 'f')
    .replace(/[ᴋᵏⓚ🄺Ｋ]/g, 'k')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase()
}

function parseFormattedNumber(text) {
  const clean = stripMcColorCodes(String(text || '')).trim()
  const match = clean.match(/([0-9]+(?:\.[0-9]+)?)\s*([kmb])?/i)
  if (!match) return null
  const value = parseFloat(match[1])
  const suffix = String(match[2] || '').toLowerCase()
  const multiplier = suffix === 'k' ? 1e3 : suffix === 'm' ? 1e6 : suffix === 'b' ? 1e9 : 1
  return Math.floor(value * multiplier)
}

function cleanScoreboardText(text) {
  return stripMcColorCodes(String(text || ''))
    .replace(/§./g, ' ')
    .replace(/§+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
}

function extractShardScore(entries = []) {
  for (const scoreEntry of entries) {
    const raw = scoreEntry.custom_name || scoreEntry.objective_name || ''
    const clean = cleanScoreboardText(raw)
    const normalized = normalizeChat(clean)
    if (!normalized.includes('shard') && !normalized.includes('balance')) continue
    return {
      raw: clean,
      value: parseFormattedNumber(clean)
    }
  }
  return null
}

function extractScoreboardStat(entries = [], { keywords = [], valuePattern = null, parser = null } = {}) {
  for (const scoreEntry of entries) {
    const raw = scoreEntry.custom_name || scoreEntry.objective_name || ''
    const clean = cleanScoreboardText(raw)
    const normalized = normalizeChat(clean)
    if (!keywords.every(keyword => normalized.includes(keyword))) continue

    let value = null
    if (typeof parser === 'function') {
      value = parser(clean)
    } else if (valuePattern) {
      const match = clean.match(valuePattern)
      value = match ? match[1].trim() : null
    } else {
      value = clean
    }

    return {
      raw: clean,
      value
    }
  }
  return null
}

function buildAfkAnalyzer(targetArea) {
  return function analyzeAfkMessage(message) {
    const text = normalizeChat(message)
    const areaText = String(targetArea)
    const hasAreaNumber = text.includes(areaText)

    const fullPatterns = [
      'full',
      'is full',
      'server full',
      'afk full'
    ]

    const successPatterns = [
      'already in afk',
      'already afk',
      'joined afk',
      'sent to afk',
      'teleported to afk',
      'now in afk',
      'already in area',
      'you are already in'
    ]

    const genericFailurePatterns = [
      'invalid',
      'unknown area',
      'does not exist',
      'cooldown',
      'wait',
      'permission',
      'cannot',
      'not available'
    ]

    if (fullPatterns.some(pattern => text.includes(pattern))) {
      return { type: 'full', area: targetArea, text }
    }

    if (
      hasAreaNumber &&
      (
        text.includes('you are already in') ||
        text.includes('already in') ||
        text.includes('teleported to')
      )
    ) {
      return { type: 'success', area: targetArea, text }
    }

    if (
      successPatterns.some(pattern => text.includes(pattern)) &&
      (!/\b\d+\b/.test(text) || hasAreaNumber)
    ) {
      return { type: 'success', area: targetArea, text }
    }

    if (hasAreaNumber && text.includes('afk') && !fullPatterns.some(pattern => text.includes(pattern))) {
      if (
        text.includes('joined') ||
        text.includes('teleported') ||
        text.includes('entered') ||
        text.includes('already')
      ) {
        return { type: 'success', area: targetArea, text }
      }
    }

    if (genericFailurePatterns.some(pattern => text.includes(pattern))) {
      return { type: 'failure', area: targetArea, text }
    }

    return null
  }
}

const logger = createLogger()
const dashboardClients = new Set()
const dashboardLogBuffer = []
let dashboardBroadcastTimeout = null
let dashboardServerStarted = false

// --- Cloud Bridge: gửi log/state về webhook manager khi chạy trên GitHub Actions ---
const CLOUD_MODE = process.env.CLOUD_MODE === 'true'
const WEBHOOK_URL = process.env.WEBHOOK_URL || ''
const WEBHOOK_TOKEN = process.env.WEBHOOK_TOKEN || ''
const CLOUD_FLUSH_MS = 2000
const cloudQueue = []
let cloudFlushTimer = null
let cloudFlushing = false

function cloudEnqueue(type, data) {
  if (!CLOUD_MODE || !WEBHOOK_URL) return
  cloudQueue.push({ type, data, ts: Date.now() })
  if (cloudQueue.length > 500) cloudQueue.splice(0, cloudQueue.length - 500) // cap
  scheduleCloudFlush()
}

function scheduleCloudFlush() {
  if (cloudFlushTimer || cloudFlushing) return
  cloudFlushTimer = setTimeout(flushCloudQueue, CLOUD_FLUSH_MS)
}

async function fetchPublicIpv4(timeoutMs = 8000) {
  // Thử nhiều endpoint, dùng HTTPS, IPv4 only (family: 4)
  const endpoints = [
    'https://api.ipify.org?format=text',
    'https://ifconfig.me/ip',
    'https://ipv4.icanhazip.com'
  ]
  for (const endpoint of endpoints) {
    try {
      const url = new URL(endpoint)
      const lib = url.protocol === 'https:' ? require('https') : require('http')
      const ip = await new Promise((resolve, reject) => {
        const req = lib.request(endpoint, { method: 'GET', family: 4, timeout: timeoutMs }, res => {
          const chunks = []
          res.on('data', c => chunks.push(c))
          res.on('end', () => resolve(Buffer.concat(chunks).toString('utf8').trim()))
          res.on('error', reject)
        })
        req.on('error', reject)
        req.on('timeout', () => { try { req.destroy() } catch {}; reject(new Error('timeout')) })
        req.end()
      })
      if (/^\d{1,3}(\.\d{1,3}){3}$/.test(ip)) return ip
    } catch {}
  }
  return null
}

async function reportIpAndCheck() {
  if (!CLOUD_MODE || !WEBHOOK_URL) return { allowed: true, skipped: true }

  const ipv4 = await fetchPublicIpv4()
  if (!ipv4) {
    log('[CLOUD] [IP_LOOKUP_FAILED] — skip IP check, cho phép chạy')
    return { allowed: true, skipped: true }
  }

  log(`[CLOUD] [IP:${ipv4}]`)

  try {
    const url = new URL(WEBHOOK_URL)
    const lib = url.protocol === 'https:' ? require('https') : require('http')
    const body = JSON.stringify({
      accountId: process.env.ACCOUNT_ID || 'unknown',
      runId: process.env.GITHUB_RUN_ID || null,
      events: [{ type: 'ip_report', data: { ipv4 }, ts: Date.now() }]
    })
    const response = await new Promise((resolve, reject) => {
      const req = lib.request(WEBHOOK_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
          'X-Webhook-Token': WEBHOOK_TOKEN
        },
        timeout: 10000
      }, res => {
        const chunks = []
        res.on('data', c => chunks.push(c))
        res.on('end', () => {
          try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')) }
          catch { resolve({}) }
        })
        res.on('error', reject)
      })
      req.on('error', reject)
      req.on('timeout', () => { try { req.destroy() } catch {}; reject(new Error('timeout')) })
      req.write(body)
      req.end()
    })

    if (response?.ipCheck) {
      return { ...response.ipCheck, ipv4 }
    }
    // Nếu manager không trả về ipCheck (có thể version cũ), mặc định cho chạy
    return { allowed: true, ipv4 }
  } catch (err) {
    log(`[CLOUD] [IP_CHECK_FAILED] [REASON:${err.message}] — cho phép chạy để tránh kẹt`)
    return { allowed: true, ipv4, error: err.message }
  }
}

async function flushCloudQueue() {
  cloudFlushTimer = null
  if (cloudFlushing) return
  if (cloudQueue.length === 0) return
  cloudFlushing = true

  const batch = cloudQueue.splice(0, cloudQueue.length)
  try {
    const url = new URL(WEBHOOK_URL)
    const lib = url.protocol === 'https:' ? require('https') : require('http')
    const body = JSON.stringify({
      accountId: process.env.ACCOUNT_ID || 'unknown',
      runId: process.env.GITHUB_RUN_ID || null,
      events: batch
    })
    // Capture response body so we can pick up manager→worker directives
    // (tasks to run, cancels). This is the cloud analogue of IPC push.
    const responsePayload = await new Promise((resolve) => {
      const req = lib.request(WEBHOOK_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
          'X-Webhook-Token': WEBHOOK_TOKEN
        },
        timeout: 8000
      }, res => {
        const chunks = []
        res.on('data', c => chunks.push(c))
        res.on('end', () => {
          try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')) }
          catch { resolve(null) }
        })
        res.on('error', () => resolve(null))
      })
      req.on('error', () => resolve(null))
      req.on('timeout', () => { try { req.destroy() } catch {}; resolve(null) })
      req.write(body)
      req.end()
    })

    if (responsePayload && typeof responsePayload === 'object') {
      try { applyCloudDirectives(responsePayload) }
      catch (e) { log(`[CLOUD] [DIRECTIVE_ERR] ${compactReason(e?.message || e, 60)}`) }
    }
  } catch {
    // nuốt lỗi, không crash bot
  } finally {
    cloudFlushing = false
    if (cloudQueue.length > 0) scheduleCloudFlush()
  }
}
// --- Cloud directive dispatcher ------------------------------------------
// Called after each webhook flush with the manager's JSON response. If it
// carries { tasks:[{type:'task', kind, id, data}] } we dispatch each one.
// If it carries { cancelTask:{reason} } we abort the in-flight task.
// We track IDs we've already started to avoid double-dispatch if the
// manager re-sends on retry.
const seenCloudTaskIds = new Set()
function applyCloudDirectives(payload) {
  if (payload.cancelTask) {
    try { cancelActiveTask(payload.cancelTask.reason || 'cloud_cancel') } catch {}
  }
  if (Array.isArray(payload.tasks)) {
    for (const t of payload.tasks) {
      if (!t || !t.id || !t.kind) continue
      if (seenCloudTaskIds.has(t.id)) continue
      seenCloudTaskIds.add(t.id)
      if (seenCloudTaskIds.size > 200) {
        // trim oldest
        const first = seenCloudTaskIds.values().next().value
        seenCloudTaskIds.delete(first)
      }
      log(`[CLOUD] [TASK_RECV] kind=${t.kind} id=${String(t.id).slice(0, 8)}`)
      if (t.kind === 'deliver_spawner') {
        const taskObj = { kind: t.kind, id: t.id, data: t.data || {} }
        runDeliverSpawnerTask(taskObj).catch(err => {
          log(`[CLOUD] [TASK_ERR] ${compactReason(err?.message || err, 60)}`)
        })
      } else {
        log(`[CLOUD] [TASK_UNKNOWN] kind=${t.kind}`)
      }
    }
  }
}

let dashboardServer = null

function appendDashboardLog(message) {
  const time = new Date().toLocaleTimeString('en-GB', { hour12: false })
  dashboardLogBuffer.push(`${time} ${message}`)
  if (dashboardLogBuffer.length > DASHBOARD_LOG_LIMIT) {
    dashboardLogBuffer.splice(0, dashboardLogBuffer.length - DASHBOARD_LOG_LIMIT)
  }
}

function log(message) {
  const line = String(message)
  appendDashboardLog(line)
  if (process.env.IS_WORKER === 'true' && process.send) {
    process.send({ type: 'log', data: line })
  }
  cloudEnqueue('log', line)
  logger.log(line)
  scheduleDashboardBroadcast()
}

const areas = parseAreaList(process.env.AFK_AREAS)
const accountId = process.env.ACCOUNT_ID || 'default-account'
const authCacheDir = getAuthCacheDir(path.join('.auth-cache', accountId))

const state = {
  client: null,
  spawned: false,
  afkAttemptInFlight: false,
  afkSuccess: false,
  currentAreaIndex: 0,
  currentTargetArea: null,
  currentTimeout: null,
  anchorTimeout: null,
  reconnectTimeout: null,
  reconnectWatchdogTimeout: null,
  reconnectWatchdogReason: null,
  lastConnectProgressAt: null,
  connectPhase: 'idle',
  reconnecting: false,
  reconnectAttempt: 0,
  alreadyLoggedInRetries: 0,
  shuttingDown: false,
  waitingForAutoAssign: false,
  spawnCommandTimeout: null,
  lastStatusAt: null,
  successAt: null,
  lastKnownAreaFromChat: null,
  currentPosition: null,
  afkAnchorPosition: null,
  afkAnchorCapturedAt: null,
  lastRejoinAt: null,
  lastActivityAt: null,
  authInputInterval: null,
  authInputTick: 1n,
  authInputPacketCount: 0,
  authInputConsecutiveErrors: 0,
  pendingTeleportAckPackets: 0,
  movementAuthority: null,
  pitch: 0,
  yaw: 0,
  headYaw: 0,
  basePitch: 0,
  baseYaw: 0,
  scoreboardEntryMap: new Map(),
  scoreboardObjectiveMap: new Map(),
  scoreboardEntries: [],
  scoreboardObjectives: [],
  lastShardValue: null,
  lastShardRaw: null,
  accountUsername: null,
  accountXuid: null,
  playerEntities: {},
  spawnerHandoff: {
    phase: 'idle',
    lastRunAt: null,
    lastError: null,
    purchasedAt: null,
    droppedAt: null
  },
  // --- Manager task system -------------------------------------------------
  // activeTask: currently-executing task dispatched from manager via IPC.
  // deathSignal: flipped to true when the player dies (detected via packets)
  // and consumed by waitForDeath() inside the deliver-spawner flow.
  activeTask: null,
  deathSignal: false
}

const helper = new BedrockTaskHelper({
  log,
  snapshotFile: 'afk_snapshot.json'
})

// --- Local playtime tracker ---
// Tự đếm playtime cộng dồn qua các session, persist ra file để không mất khi restart.
// Hữu ích khi server không update scoreboard playtime trong lúc bot AFK.
const playtimeDataDir = path.join(__dirname, 'data', 'playtime')
try { fs.mkdirSync(playtimeDataDir, { recursive: true }) } catch {}
const playtimeFile = path.join(playtimeDataDir, `${accountId}.json`)

function loadPersistedPlaytime() {
  try {
    const data = JSON.parse(fs.readFileSync(playtimeFile, 'utf8'))
    return Number(data.totalSeconds) || 0
  } catch { return 0 }
}

function savePersistedPlaytime() {
  try {
    fs.writeFileSync(playtimeFile, JSON.stringify({
      accountId,
      username: state.accountUsername,
      totalSeconds: state.localPlaytimeTotalSeconds,
      updatedAt: new Date().toISOString()
    }, null, 2), 'utf8')
  } catch (err) {
    log(`[PLAYTIME] [SAVE_FAIL] [REASON:${compactReason(err.message, 28)}]`)
  }
}

state.localPlaytimeTotalSeconds = loadPersistedPlaytime()
state.localPlaytimeSessionStart = null // timestamp (ms) khi spawn session hiện tại

function startLocalPlaytimeSession() {
  if (state.localPlaytimeSessionStart != null) return
  state.localPlaytimeSessionStart = Date.now()
}

function stopLocalPlaytimeSession() {
  if (state.localPlaytimeSessionStart == null) return
  const elapsed = Math.max(0, Math.floor((Date.now() - state.localPlaytimeSessionStart) / 1000))
  state.localPlaytimeTotalSeconds += elapsed
  state.localPlaytimeSessionStart = null
  savePersistedPlaytime()
}

function getLocalPlaytimeSeconds() {
  const base = state.localPlaytimeTotalSeconds || 0
  if (state.localPlaytimeSessionStart == null) return base
  const elapsed = Math.max(0, Math.floor((Date.now() - state.localPlaytimeSessionStart) / 1000))
  return base + elapsed
}

function formatDurationHuman(totalSeconds) {
  if (!totalSeconds || totalSeconds < 0) return '0s'
  const d = Math.floor(totalSeconds / 86400)
  const h = Math.floor((totalSeconds % 86400) / 3600)
  const m = Math.floor((totalSeconds % 3600) / 60)
  const s = totalSeconds % 60
  const parts = []
  if (d > 0) parts.push(`${d}d`)
  if (h > 0) parts.push(`${h}h`)
  if (m > 0 && d === 0) parts.push(`${m}m`)
  if (s > 0 && d === 0 && h === 0) parts.push(`${s}s`)
  return parts.join(' ') || '0s'
}

// Tick + auto-save mỗi 30s khi đang spawn
setInterval(() => {
  if (state.spawned && state.localPlaytimeSessionStart != null) {
    // Commit incremental để không mất nếu worker crash
    const elapsed = Math.max(0, Math.floor((Date.now() - state.localPlaytimeSessionStart) / 1000))
    if (elapsed > 0) {
      state.localPlaytimeTotalSeconds += elapsed
      state.localPlaytimeSessionStart = Date.now()
      savePersistedPlaytime()
    }
  }
}, 30000)

log('--- Starting afk.js ---')
log('[AFK] [AREA_POOL] [RANDOM:10-70]')
log(`[PLAYTIME] [LOADED:${formatDurationHuman(state.localPlaytimeTotalSeconds)}] [TOTAL_SEC:${state.localPlaytimeTotalSeconds}]`)

process.on('uncaughtException', err => {
  log(`[UNCAUGHT] [REASON:${compactReason(err?.message || err, 40)}]`)
  log(`[DETAIL] ${inspect(err)}`)
  if (!state.shuttingDown) {
    try { state.client?.close() } catch {}
    state.client = null
    scheduleReconnect(`uncaught:${err?.message || err}`)
  }
})

process.on('unhandledRejection', reason => {
  log(`[UNHANDLED_REJECTION] [REASON:${compactReason(String(reason?.message || reason), 40)}]`)
})

function clearAfkTimeout() {
  if (state.currentTimeout) {
    clearTimeout(state.currentTimeout)
    state.currentTimeout = null
  }
}

function clearSpawnCommandTimeout() {
  if (state.spawnCommandTimeout) {
    clearTimeout(state.spawnCommandTimeout)
    state.spawnCommandTimeout = null
  }
}

function clearAnchorTimeout() {
  if (state.anchorTimeout) {
    clearTimeout(state.anchorTimeout)
    state.anchorTimeout = null
  }
}

function clearReconnectTimeout() {
  if (state.reconnectTimeout) {
    clearTimeout(state.reconnectTimeout)
    state.reconnectTimeout = null
  }
}

function clearReconnectWatchdogTimeout() {
  if (state.reconnectWatchdogTimeout) {
    clearTimeout(state.reconnectWatchdogTimeout)
    state.reconnectWatchdogTimeout = null
  }
}

function markConnectProgress(phase) {
  state.lastConnectProgressAt = Date.now()
  state.connectPhase = phase
}

function getReconnectWatchdogLimitMs() {
  if (state.connectPhase === 'auth_session') return RAKNET_CONNECT_WATCHDOG_MS
  if (state.connectPhase === 'login_sent') return LOGIN_PROGRESS_WATCHDOG_MS
  return RECONNECT_WATCHDOG_MS
}

function scheduleReconnectWatchdog(client) {
  clearReconnectWatchdogTimeout()
  state.reconnectWatchdogTimeout = setTimeout(() => {
    state.reconnectWatchdogTimeout = null
    if (state.shuttingDown) return
    if (state.client !== client) return
    if (state.spawned) return

    const lastProgressAt = state.lastConnectProgressAt || 0
    const idleMs = Date.now() - lastProgressAt
    const watchdogMs = getReconnectWatchdogLimitMs()
    if (idleMs < watchdogMs) {
      scheduleReconnectWatchdog(client)
      return
    }

    state.reconnectWatchdogReason = `watchdog:${state.connectPhase || 'unknown'}`
    log(`[RECONNECT] [WATCHDOG] [TIMEOUT:${Math.floor(watchdogMs / 1000)}s] [PHASE:${state.connectPhase || 'unknown'}] [ACTION:CLOSE]`)
    try {
      client.close()
    } catch {}
  }, Math.min(getReconnectWatchdogLimitMs(), 5000))
}

function clearAuthInputLoop() {
  if (state.authInputInterval) {
    clearInterval(state.authInputInterval)
    state.authInputInterval = null
  }
}

function resetJoinState({ keepSuccess = false } = {}) {
  clearAfkTimeout()
  clearSpawnCommandTimeout()
  clearAnchorTimeout()
  clearReconnectWatchdogTimeout()
  clearAuthInputLoop()
  stopLocalPlaytimeSession()
  state.spawned = false
  state.afkAttemptInFlight = false
  state.waitingForAutoAssign = false
  if (!keepSuccess) state.afkSuccess = false
  state.lastKnownAreaFromChat = null
  state.currentPosition = null
  state.afkAnchorPosition = null
  state.afkAnchorCapturedAt = null
  state.lastActivityAt = null
  state.lastConnectProgressAt = null
  state.connectPhase = 'idle'
  state.authInputTick = 1n
  state.authInputPacketCount = 0
  state.authInputConsecutiveErrors = 0
  state.pendingTeleportAckPackets = 0
  state.movementAuthority = null
  state.pitch = 0
  state.yaw = 0
  state.headYaw = 0
  state.basePitch = 0
  state.baseYaw = 0
  state.scoreboardEntryMap = new Map()
  state.scoreboardObjectiveMap = new Map()
  state.scoreboardEntries = []
  state.scoreboardObjectives = []
  state.lastShardValue = null
  state.lastShardRaw = null
  state.accountUsername = null
  state.accountXuid = null
  state.playerEntities = {}
  helper.initializedSent = false
  helper.currentContainer = null
  scheduleDashboardBroadcast()
}

function clonePosition(position) {
  if (!position) return null
  return { x: Number(position.x), y: Number(position.y), z: Number(position.z) }
}

function formatPosition(position) {
  if (!position) return 'unknown'
  return `${position.x.toFixed(2)}, ${position.y.toFixed(2)}, ${position.z.toFixed(2)}`
}

function compactReason(reason, maxLength = 48) {
  const text = String(reason || 'unknown')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase()
  return text.length > maxLength ? `${text.slice(0, maxLength - 3)}...` : text
}

function coerceTick(value, fallback = 1n) {
  const base = fallback > 0n ? fallback : 1n
  let parsed = null
  if (typeof value === 'bigint') parsed = value
  else if (typeof value === 'number' && Number.isFinite(value)) parsed = BigInt(Math.floor(value))
  else if (typeof value === 'string' && /^\d+$/.test(value)) parsed = BigInt(value)
  if (parsed == null || parsed <= 0n) return base
  return parsed < base ? base : parsed
}

function authInputTickStep() {
  return BigInt(Math.max(1, Math.round(AFK_AUTH_INPUT_MS / 50)))
}

function updateRotation(params = {}) {
  if (Number.isFinite(Number(params.pitch))) state.pitch = Number(params.pitch)
  if (Number.isFinite(Number(params.yaw))) state.yaw = Number(params.yaw)
  if (Number.isFinite(Number(params.head_yaw))) state.headYaw = Number(params.head_yaw)
  else if (Number.isFinite(Number(params.headYaw))) state.headYaw = Number(params.headYaw)
  else state.headYaw = state.yaw
}

function captureAfkLookBase() {
  state.basePitch = Number.isFinite(Number(state.pitch)) ? Number(state.pitch) : 0
  state.baseYaw = Number.isFinite(Number(state.yaw)) ? Number(state.yaw) : 0
}

function getHeartbeatLook() {
  if (!AFK_LOOK_JITTER_DEGREES) {
    return {
      pitch: state.basePitch || state.pitch || 0,
      yaw: state.baseYaw || state.yaw || 0,
      headYaw: state.baseYaw || state.headYaw || state.yaw || 0
    }
  }

  const phase = (Number(state.authInputPacketCount % AFK_LOOK_JITTER_PERIOD_TICKS) / AFK_LOOK_JITTER_PERIOD_TICKS) * Math.PI * 2
  const yaw = (state.baseYaw || 0) + Math.sin(phase) * AFK_LOOK_JITTER_DEGREES
  const pitch = (state.basePitch || 0) + Math.sin(phase / 2) * Math.min(AFK_LOOK_JITTER_DEGREES / 2, 0.75)
  return { pitch, yaw, headYaw: yaw }
}

function blockPositionFromPosition(position) {
  return {
    x: Math.floor(Number(position?.x) || 0),
    y: Math.floor(Number(position?.y) || 0),
    z: Math.floor(Number(position?.z) || 0)
  }
}

function shouldSendAuthInput() {
  return AFK_MOVEMENT_PACKET_MODE === 'hybrid' ||
    AFK_MOVEMENT_PACKET_MODE === 'auth' ||
    (AFK_MOVEMENT_PACKET_MODE === 'auto' && state.movementAuthority !== 'client')
}

function shouldSendMovePlayer() {
  return AFK_MOVEMENT_PACKET_MODE === 'hybrid' ||
    AFK_MOVEMENT_PACKET_MODE === 'move' ||
    (AFK_MOVEMENT_PACKET_MODE === 'auto' && state.movementAuthority === 'client')
}

function buildAuthInputPacket() {
  const position = clonePosition(state.currentPosition)
  if (!position) return null
  if (state.authInputTick <= 0n) state.authInputTick = 1n
  const look = getHeartbeatLook()
  const inputData = { received_server_data: true }
  if (state.pendingTeleportAckPackets > 0) inputData.handled_teleport = true

  return {
    pitch: look.pitch,
    yaw: look.yaw,
    position,
    move_vector: { x: 0, z: 0 },
    head_yaw: look.headYaw,
    input_data: inputData,
    input_mode: 'mouse',
    play_mode: 'normal',
    interaction_model: 'crosshair',
    interact_rotation: { x: 0, z: 0 },
    tick: state.authInputTick,
    delta: { x: 0, y: 0, z: 0 },
    analogue_move_vector: { x: 0, z: 0 },
    camera_orientation: { x: 0, y: 0, z: 0 },
    raw_move_vector: { x: 0, z: 0 }
  }
}

function buildMovePlayerPacket() {
  const position = clonePosition(state.currentPosition)
  if (!position || !state.client || state.client.entityId == null) return null
  if (state.authInputTick <= 0n) state.authInputTick = 1n
  const look = getHeartbeatLook()
  return {
    runtime_id: Number(state.client.entityId),
    position,
    pitch: look.pitch,
    yaw: look.yaw,
    head_yaw: look.headYaw,
    mode: 'normal',
    on_ground: true,
    ridden_runtime_id: 0,
    tick: state.authInputTick
  }
}

function sendTeleportAckIfNeeded() {
  if (state.pendingTeleportAckPackets <= 0) return
  if (!state.client || state.client.entityId == null || !state.currentPosition) return

  try {
    const blockPosition = blockPositionFromPosition(state.currentPosition)
    state.client.queue('player_action', {
      runtime_entity_id: BigInt(state.client.entityId),
      action: 'handled_teleport',
      position: blockPosition,
      result_position: blockPosition,
      face: 0
    })
  } catch (err) {
    log(`[AFK] [TELEPORT_ACK_FAIL] [REASON:${compactReason(err.message, 48)}]`)
  }
}

function sendAuthInputHeartbeat(reason = 'interval') {
  if (!state.spawned || !state.afkSuccess || state.reconnecting || state.shuttingDown) return false
  if (!state.client || state.client.entityId == null) return false

  const packet = buildAuthInputPacket()
  const movePacket = buildMovePlayerPacket()
  if (!packet && !movePacket) return false

  try {
    sendTeleportAckIfNeeded()
    if (packet && shouldSendAuthInput()) state.client.queue('player_auth_input', packet)
    if (movePacket && shouldSendMovePlayer()) state.client.queue('move_player', movePacket)
    if (state.authInputPacketCount % AFK_TICK_SYNC_EVERY === 0) {
      state.client.queue('tick_sync', { request_time: state.authInputTick, response_time: 0n })
    }
    state.authInputTick += authInputTickStep()
    state.authInputPacketCount += 1
    if (state.pendingTeleportAckPackets > 0) state.pendingTeleportAckPackets -= 1
    state.authInputConsecutiveErrors = 0
    state.lastActivityAt = new Date().toISOString()

    if (process.env.AFK_DEBUG_INPUT === '1' || state.authInputPacketCount === 1 || state.authInputPacketCount % AFK_AUTH_INPUT_LOG_EVERY === 0) {
      const look = getHeartbeatLook()
      log(`[AFK] [INPUT_HEARTBEAT] [COUNT:${state.authInputPacketCount}] [TICK:${state.authInputTick}] [MODE:${AFK_MOVEMENT_PACKET_MODE}] [AUTHORITY:${state.movementAuthority || 'unknown'}] [POS:${formatPosition(state.currentPosition)}] [YAW:${look.yaw.toFixed(2)}] [REASON:${reason}]`)
    }
    return true
  } catch (err) {
    state.authInputConsecutiveErrors += 1
    log(`[AFK] [AUTH_INPUT_FAIL] [COUNT:${state.authInputConsecutiveErrors}] [REASON:${compactReason(err.message, 48)}]`)
    if (state.authInputConsecutiveErrors >= 3) {
      clearAuthInputLoop()
      log('[AFK] [AUTH_INPUT_DISABLED] repeated serializer/write failures')
    }
    return false
  }
}

function startAuthInputLoop(reason = 'spawn') {
  if (state.authInputInterval) return
  sendAuthInputHeartbeat(reason)
  state.authInputInterval = setInterval(() => {
    sendAuthInputHeartbeat('interval')
  }, AFK_AUTH_INPUT_MS)
  log(`[AFK] [AUTH_INPUT_LOOP] [STARTED] [EVERY:${AFK_AUTH_INPUT_MS}ms] [REASON:${reason}]`)
}

function clampDisplayText(value, fallback = 'UNKNOWN') {
  const text = String(value || '').trim()
  return text || fallback
}

function getDashboardConnectionState() {
  if (state.reconnecting) return 'RECONNECTING'
  if (state.spawned && state.afkSuccess) return 'AFK_LOCKED'
  if (state.spawned) return 'ONLINE'
  if (state.client) return 'CONNECTING'
  return 'OFFLINE'
}

function isAlreadyLoggedInReason(reason) {
  const text = normalizeChat(reason)
  return text.includes('already logged in') || text.includes('already online')
}

function getScoreboardObjectiveKey(objective) {
  return `${objective.display_slot || 'unknown'}:${objective.objective_name || 'unknown'}`
}

function getScoreboardEntryKey(scoreEntry, index = 0) {
  const uniqueId = scoreEntry.scoreboard_id
    ?? scoreEntry.entry_unique_id
    ?? scoreEntry.entity_unique_id
    ?? scoreEntry.runtime_entity_id
    ?? scoreEntry.player_unique_id
  if (uniqueId != null) return `id:${String(uniqueId)}`

  const objective = scoreEntry.objective_name || 'unknown'
  const score = scoreEntry.score ?? 'unknown'
  const type = scoreEntry.type || 'unknown'
  return `fallback:${objective}:${score}:${type}:${index}`
}

function syncScoreboardSnapshots() {
  state.scoreboardObjectives = Array.from(state.scoreboardObjectiveMap.values())
  state.scoreboardEntries = Array.from(state.scoreboardEntryMap.values())
    .sort((a, b) => {
      const scoreA = Number.isFinite(Number(a.score)) ? Number(a.score) : -Infinity
      const scoreB = Number.isFinite(Number(b.score)) ? Number(b.score) : -Infinity
      if (scoreA !== scoreB) return scoreB - scoreA
      return String(a.custom_name || a.objective_name || '').localeCompare(String(b.custom_name || b.objective_name || ''))
    })
  scheduleDashboardBroadcast()
}

function updateScoreboardObjective(params) {
  if (!params) return
  state.scoreboardObjectiveMap.set(getScoreboardObjectiveKey(params), { ...params })
  syncScoreboardSnapshots()
}

let scoreboardDebugDumped = false
function updateScoreboardEntries(params) {
  const action = String(params?.action || 'change').toLowerCase()
  const entries = Array.isArray(params?.entries) ? params.entries : []
  if (entries.length === 0) return

  // DEBUG: dump scoreboard 1 lần để hỗ trợ chẩn đoán money/shards = 0
  if (!scoreboardDebugDumped && action !== 'remove') {
    scoreboardDebugDumped = true
    try {
      const lines = entries.slice(0, 20).map((e, i) => {
        const raw = e.custom_name || e.objective_name || ''
        const clean = cleanScoreboardText(raw)
        return `  #${i} score=${e.score} obj="${e.objective_name || ''}" name="${clean}"`
      })
      log(`[SCOREBOARD_DUMP] [ENTRIES:${entries.length}] [ACTION:${action}]\n${lines.join('\n')}`)
    } catch (err) {
      log(`[SCOREBOARD_DUMP] [ERR:${err.message}]`)
    }
  }

  for (let index = 0; index < entries.length; index += 1) {
    const scoreEntry = entries[index]
    const key = getScoreboardEntryKey(scoreEntry, index)
    if (action === 'remove') {
      state.scoreboardEntryMap.delete(key)
      continue
    }
    state.scoreboardEntryMap.set(key, { ...scoreEntry })
  }

  syncScoreboardSnapshots()
}

function refreshShardState() {
  const shard = extractShardScore(state.scoreboardEntries)
  state.lastShardValue = shard?.value ?? null
  state.lastShardRaw = shard?.raw ?? null
  return shard
}

function printShardScore() {
  const shard = refreshShardState()
  if (!shard) {
    log('[SCOREBOARD] [SHARD] [NOT_FOUND]')
    return
  }

  const value = shard.value == null ? 'UNKNOWN' : shard.value
  log(`[SCOREBOARD] [SHARD] [VALUE:${value}] [RAW:${shard.raw}]`)
}

function printNumericScoreboardStat(label, keywords) {
  const stat = extractScoreboardStat(state.scoreboardEntries, {
    keywords,
    parser: raw => parseFormattedNumber(raw)
  })

  if (!stat) {
    log(`[SCOREBOARD] [${label}] [NOT_FOUND]`)
    return
  }

  const value = stat.value == null ? 'UNKNOWN' : stat.value
  log(`[SCOREBOARD] [${label}] [VALUE:${value}] [RAW:${stat.raw}]`)
}

// Parse playtime string to total seconds. Supports "2h 5m 30s", "1d 2h", "125m", "3600s", "02:45:10"...
function parsePlaytimeToSeconds(raw) {
  if (!raw) return null
  const text = String(raw).toLowerCase().trim()
  // HH:MM:SS or MM:SS
  const colonMatch = text.match(/(\d{1,3}):(\d{2})(?::(\d{2}))?/)
  if (colonMatch) {
    const a = Number(colonMatch[1])
    const b = Number(colonMatch[2])
    const c = colonMatch[3] != null ? Number(colonMatch[3]) : null
    return c != null ? a * 3600 + b * 60 + c : a * 60 + b
  }
  // Token units: 2d 3h 5m 30s
  let seconds = 0
  let matched = false
  const units = [
    [/(\d+)\s*d(?:ays?)?/i, 86400],
    [/(\d+)\s*h(?:ours?|rs?)?/i, 3600],
    [/(\d+)\s*m(?:in(?:utes?)?)?/i, 60],
    [/(\d+)\s*s(?:ec(?:onds?)?)?/i, 1]
  ]
  for (const [re, mult] of units) {
    const m = text.match(re)
    if (m) {
      matched = true
      seconds += Number(m[1]) * mult
    }
  }
  return matched ? seconds : null
}

function extractPlaytimeStat() {
  // Flexible: match entries có chứa 'playtime' bằng normalizeChat, extract giá trị bằng cách bỏ phần 'playtime' ra.
  for (const scoreEntry of state.scoreboardEntries) {
    const raw = scoreEntry.custom_name || scoreEntry.objective_name || ''
    const clean = cleanScoreboardText(raw)
    const normalized = normalizeChat(clean)
    if (!normalized.includes('playtime')) continue

    // Bỏ chữ "playtime" và các dấu phân cách phổ biến (:, -, ·, |, .) rồi trim
    let value = clean.replace(/playtime/i, '').replace(/^[\s:·•\-|.,>=]+|[\s:·•\-|.,>=]+$/g, '').trim()
    // Nếu value rỗng, thử lấy score
    if (!value && scoreEntry.score != null) {
      value = String(scoreEntry.score)
    }
    const seconds = parsePlaytimeToSeconds(value)
    return { raw: clean, value: value || null, seconds }
  }
  return null
}

function printPlaytimeScore() {
  const stat = extractPlaytimeStat()
  if (!stat) {
    log('[SCOREBOARD] [PLAYTIME] [NOT_FOUND]')
    return
  }
  log(`[SCOREBOARD] [PLAYTIME] [VALUE:${stat.value || 'UNKNOWN'}] [SEC:${stat.seconds ?? 'UNKNOWN'}] [RAW:${stat.raw}]`)
}

function getScoreboardStats() {
  const shard = refreshShardState()
  const money = extractScoreboardStat(state.scoreboardEntries, {
    keywords: ['money'],
    parser: raw => parseFormattedNumber(raw)
  })
  const kills = extractScoreboardStat(state.scoreboardEntries, {
    keywords: ['kills'],
    parser: raw => parseFormattedNumber(raw)
  })
  const deaths = extractScoreboardStat(state.scoreboardEntries, {
    keywords: ['deaths'],
    parser: raw => parseFormattedNumber(raw)
  })
  const playtime = extractPlaytimeStat()
  const localSeconds = getLocalPlaytimeSeconds()
  const serverSeconds = playtime?.seconds ?? null

  // Server scoreboard can go stale; display the greater server/local value.
  const displaySeconds = Math.max(Number(serverSeconds) || 0, Number(localSeconds) || 0)
  const displayValue = formatDurationHuman(displaySeconds)
  const displaySource = (serverSeconds != null && serverSeconds >= localSeconds) ? 'server' : 'local'

  return {
    money: {
      value: money?.value ?? null,
      raw: money?.raw ?? null
    },
    shards: {
      value: shard?.value ?? null,
      raw: shard?.raw ?? null
    },
    kills: {
      value: kills?.value ?? null,
      raw: kills?.raw ?? null
    },
    deaths: {
      value: deaths?.value ?? null,
      raw: deaths?.raw ?? null
    },
    playtime: {
      value: displayValue,
      raw: playtime?.raw ?? null,
      seconds: displaySeconds,
      serverSeconds,
      localSeconds,
      source: displaySource
    }
  }
}

function getDashboardSnapshot() {
  return {
    account: {
      username: state.accountUsername,
      xuid: state.accountXuid
    },
    connection: {
      state: getDashboardConnectionState(),
      reconnecting: state.reconnecting,
      reconnectAttempt: state.reconnectAttempt,
      spawned: state.spawned,
      hasClient: Boolean(state.client)
    },
    afk: {
      success: state.afkSuccess,
      currentArea: state.currentTargetArea,
      waitingForAutoAssign: state.waitingForAutoAssign,
      attemptInFlight: state.afkAttemptInFlight,
      currentPosition: state.currentPosition ? formatPosition(state.currentPosition) : null,
      anchorPosition: state.afkAnchorPosition ? formatPosition(state.afkAnchorPosition) : null,
      lastActivityAt: state.lastActivityAt,
      lastStatusAt: state.lastStatusAt
    },
    scoreboard: getScoreboardStats(),
    objectives: state.scoreboardObjectives.map(objective => ({
      name: objective.objective_name || 'UNKNOWN',
      display: stripMcColorCodes(objective.display_name || objective.objective_name || ''),
      slot: objective.display_slot || 'UNKNOWN'
    })),
    spawnerHandoff: state.spawnerHandoff,
    logLines: dashboardLogBuffer.slice(-120)
  }
}

function broadcastDashboard(event, payload) {
  const body = `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`
  for (const client of Array.from(dashboardClients)) {
    try {
      client.write(body)
    } catch {
      dashboardClients.delete(client)
    }
  }
}

function scheduleDashboardBroadcast() {
  if (dashboardBroadcastTimeout) return
  dashboardBroadcastTimeout = setTimeout(() => {
    dashboardBroadcastTimeout = null
    const snapshot = getDashboardSnapshot()
    if (process.env.IS_WORKER === 'true' && process.send) {
      process.send({ type: 'state', data: snapshot })
    }
    cloudEnqueue('state', snapshot)
    if (dashboardClients.size === 0) return
    broadcastDashboard('state', snapshot)
  }, 120)
}

function serveDashboardAsset(filePath, contentType, response) {
  try {
    const body = fs.readFileSync(filePath)
    response.writeHead(200, {
      'Content-Type': contentType,
      'Cache-Control': 'no-store'
    })
    response.end(body)
  } catch {
    response.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' })
    response.end('Not found')
  }
}

function startDashboardServer() {
  if (dashboardServerStarted) return
  dashboardServerStarted = true

  const webRoot = path.join(__dirname, 'ui')
  const server = http.createServer((request, response) => {
    const url = request.url || '/'
    if (url === '/' || url === '/index.html') {
      serveDashboardAsset(path.join(webRoot, 'index.html'), 'text/html; charset=utf-8', response)
      return
    }

    if (url === '/app.css') {
      serveDashboardAsset(path.join(webRoot, 'app.css'), 'text/css; charset=utf-8', response)
      return
    }

    if (url === '/app.js') {
      serveDashboardAsset(path.join(webRoot, 'app.js'), 'application/javascript; charset=utf-8', response)
      return
    }

    if (url === '/api/state') {
      response.writeHead(200, {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'no-store'
      })
      response.end(JSON.stringify(getDashboardSnapshot()))
      return
    }

    if (url === '/events') {
      response.writeHead(200, {
        'Content-Type': 'text/event-stream; charset=utf-8',
        'Cache-Control': 'no-cache, no-transform',
        Connection: 'keep-alive',
        'X-Accel-Buffering': 'no'
      })
      response.write('\n')
      dashboardClients.add(response)
      response.write(`event: state\ndata: ${JSON.stringify(getDashboardSnapshot())}\n\n`)
      const heartbeat = setInterval(() => {
        try {
          response.write('event: ping\ndata: {}\n\n')
        } catch {}
      }, 15000)
      request.on('close', () => {
        clearInterval(heartbeat)
        dashboardClients.delete(response)
      })
      return
    }

    response.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' })
    response.end('Not found')
  })

  dashboardServer = server

  server.on('error', err => {
    logger.log(`[WEB] [ERROR] [REASON:${compactReason(err?.message || err, 40)}]`)
  })

  server.listen(DASHBOARD_PORT, DASHBOARD_HOST, () => {
    log(`[WEB] [LIVE] [URL:http://${DASHBOARD_HOST}:${DASHBOARD_PORT}]`)
  })
}

function printFullScoreboard() {
  const objectives = state.scoreboardObjectives || []
  const entries = state.scoreboardEntries || []

  if (objectives.length === 0 && entries.length === 0) {
    log('[SCOREBOARD] [FULL] [EMPTY]')
    return
  }

  log(`[SCOREBOARD] [FULL] [OBJECTIVES:${objectives.length}] [ENTRIES:${entries.length}]`)

  for (const objective of objectives) {
    const display = cleanScoreboardText(objective.display_name || objective.objective_name || '')
    log(`[SCOREBOARD] [OBJECTIVE] [NAME:${objective.objective_name || 'UNKNOWN'}] [DISPLAY:${display || 'NONE'}] [SLOT:${objective.display_slot || 'UNKNOWN'}]`)
  }

  for (const scoreEntry of entries) {
    const raw = cleanScoreboardText(scoreEntry.custom_name || scoreEntry.objective_name || '')
    const score = scoreEntry.score ?? 'UNKNOWN'
    const objective = scoreEntry.objective_name || 'UNKNOWN'
    const type = scoreEntry.type || 'UNKNOWN'
    log(`[SCOREBOARD] [ENTRY] [OBJECTIVE:${objective}] [TYPE:${type}] [SCORE:${score}] [RAW:${raw || 'NONE'}]`)
  }
}

function captureAccountIdentity(client) {
  const accountUsername = client?.profile?.name || client?.username || null
  const accountXuid = client?.profile?.xuid ?? null
  if (!accountUsername && accountXuid == null) return
  if (state.accountUsername === accountUsername && state.accountXuid === accountXuid) return

  state.accountUsername = accountUsername
  state.accountXuid = accountXuid
  log(`[ACCOUNT] [USER:${accountUsername || 'UNKNOWN'}] [XUID:${accountXuid ?? 'UNKNOWN'}]`)
}

function entityKey(params) {
  const id = params?.runtime_id ?? params?.runtime_entity_id ?? params?.entity_id ?? params?.unique_id
  return id == null ? null : String(id)
}

function rememberPlayerEntity(params) {
  const key = entityKey(params)
  if (!key) return
  state.playerEntities[key] = {
    runtimeId: key,
    username: params.username || params.name || params.display_name || '',
    position: clonePosition(params.position)
  }
}

function updatePlayerEntityPosition(params) {
  const key = entityKey(params)
  if (!key || !state.playerEntities[key]) return
  state.playerEntities[key].position = clonePosition(params.position)
}

function forgetPlayerEntity(params) {
  const key = entityKey(params)
  if (key) delete state.playerEntities[key]
}

function handleConsoleCommand(input) {
  const command = String(input || '').trim().toLowerCase()
  if (!command) return

  if (command === '/shard') {
    printShardScore()
    return
  }

  if (command === '/money') {
    printNumericScoreboardStat('MONEY', ['money'])
    return
  }

  if (command === '/kills') {
    printNumericScoreboardStat('KILLS', ['kills'])
    return
  }

  if (command === '/deaths') {
    printNumericScoreboardStat('DEATHS', ['deaths'])
    return
  }

  if (command === '/playtime') {
    printPlaytimeScore()
    return
  }

  if (command === '/sb') {
    printFullScoreboard()
    return
  }

  log(`[CONSOLE] [UNKNOWN] ${command}`)
}

function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function waitForPacket(name, predicate = () => true, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    const client = state.client
    if (!client) return reject(new Error('client not connected'))
    const timer = setTimeout(() => {
      client.off('packet', onPacket)
      reject(new Error(`Timeout waiting for ${name}`))
    }, timeoutMs)

    function onPacket(packet) {
      if (packet.data.name !== name) return
      const params = packet.data.params
      if (!predicate(params)) return
      clearTimeout(timer)
      client.off('packet', onPacket)
      resolve({ params })
    }

    client.on('packet', onPacket)
  })
}

function setSpawnerPhase(phase, extra = {}) {
  state.spawnerHandoff = {
    ...state.spawnerHandoff,
    phase,
    ...extra
  }
  log(`[SPAWNER] [PHASE:${phase}]`)
  scheduleDashboardBroadcast()
}

function positionDistance(a, b) {
  if (!a || !b) return Infinity
  const dx = Number(a.x) - Number(b.x)
  const dy = Number(a.y) - Number(b.y)
  const dz = Number(a.z) - Number(b.z)
  return Math.sqrt(dx * dx + dy * dy + dz * dz)
}

async function buySkeletonSpawner() {
  setSpawnerPhase('open_shop')
  helper.sendCommand('/shop')

  const mainMenu = await waitForPacket('inventory_content', params => {
    return Array.isArray(params.input) && params.input[15] && params.input[15].network_id !== 0
  }, 15000)

  const mainWindowId = mainMenu.params.window_id
  const shardItem = helper.getItemAt(mainWindowId, 15)
  if (!shardItem || shardItem.empty) throw new Error('Shard shop slot not found')

  setSpawnerPhase('click_shard_shop')
  helper.clickWindowSlot(mainWindowId, 15, shardItem)

  const shopPacket = await waitForPacket('inventory_content', params => {
    if (!Array.isArray(params.input)) return false
    const summarized = params.input.map((item, index) => summarizeItem(item, index))
    return Boolean(findSkeletonSpawner(summarized))
  }, 15000)

  const shopWindowId = shopPacket.params.window_id
  const shopItems = helper.getWindowItems(shopWindowId)

  // Full dump of the shard shop window to help tune selectors.
  log(`[SPAWNER] [SHOP_DUMP] window=${shopWindowId} slots=${shopItems.length}`)
  for (const it of shopItems) {
    if (!it || it.empty) continue
    const name = (it.custom_name_clean || '').trim() || `(id=${it.network_id})`
    const lore = Array.isArray(it.lore_clean) ? it.lore_clean.filter(Boolean).join(' | ') : ''
    log(`  slot=${String(it.slot).padStart(2, ' ')}  x${it.count}  ${name}${lore ? '  // ' + lore : ''}`)
  }

  const target = findSkeletonSpawner(shopItems)
  if (!target) throw new Error('Skeleton spawner not found in shard shop')

  const targetLabel = (target.custom_name_clean || '').trim() || `id=${target.network_id}`
  log(`[SPAWNER] [SHOP_PICK] window=${shopWindowId} slot=${target.slot} name="${targetLabel}"`)
  setSpawnerPhase('click_skeleton_spawner')
  helper.clickWindowSlot(shopWindowId, target.slot, target)

  // DonutSMP may either (a) open a confirm window, or (b) purchase instantly.
  // Wait briefly for a NEW container window; if none shows up, assume instant-buy
  // and fall through to the post-buy inventory check.
  const isPlayerOrShop = (wid) => (
    wid === shopWindowId ||
    wid === 0 || wid === '0' ||
    wid === 'inventory' || wid === 'cursor' || wid === 'offhand' || wid === 'armor' ||
    wid === -1 || wid === -2 || wid === -3
  )

  try {
    const confirmPacket = await waitForPacket('inventory_content', params => {
      if (!Array.isArray(params.input)) return false
      if (isPlayerOrShop(params.window_id)) return false
      return params.input.some(item => item && item.network_id !== 0)
    }, 3000)

    const confirmWindowId = confirmPacket.params.window_id
    const confirmItemsRaw = confirmPacket.params.input.map((item, index) => summarizeItem(item, index))
    const confirmItem = (confirmItemsRaw[15] && !confirmItemsRaw[15].empty)
      ? confirmItemsRaw[15]
      : confirmItemsRaw.find(item => item && !item.empty)

    if (confirmItem && !confirmItem.empty) {
      log(`[SPAWNER] [CONFIRM] window=${confirmWindowId} slot=${confirmItem.slot} name=${confirmItem.name || confirmItem.network_id}`)
      setSpawnerPhase('confirm_purchase')
      helper.clickWindowSlot(confirmWindowId, confirmItem.slot, confirmItem)
    } else {
      const dump = confirmItemsRaw
        .filter(it => it && !it.empty)
        .map(it => `slot=${it.slot} name=${it.name || it.network_id}`)
        .join(', ') || '(no items)'
      log(`[SPAWNER] [CONFIRM_SKIP] window=${confirmWindowId} no selectable item; dump=[${dump}]`)
    }
  } catch (err) {
    // Timeout or other — assume instant-buy variant.
    log(`[SPAWNER] [NO_CONFIRM_WINDOW] assuming instant-buy (${compactReason(err?.message || err, 28)})`)
  }

  state.spawnerHandoff.purchasedAt = new Date().toISOString()
}

async function tpaToJavaMain() {
  if (!JAVA_MAIN_USERNAME) throw new Error('JAVA_MAIN_USERNAME is not configured')
  setSpawnerPhase('tpa_to_java')
  helper.sendCommand(`/tpa ${JAVA_MAIN_USERNAME}`)

  const startedAt = Date.now()
  let tick = 0
  while (Date.now() - startedAt < 60000) {
    const players = Object.values(state.playerEntities || {})
    const target = players.find(entity => String(entity.username || '').toLowerCase().includes(JAVA_MAIN_USERNAME.toLowerCase()))
    if (target?.position && state.currentPosition && positionDistance(target.position, state.currentPosition) <= 10) {
      log(`[SPAWNER] [TPA_JOINED] java=${target.username} dist=${positionDistance(target.position, state.currentPosition).toFixed(1)}`)
      setSpawnerPhase('teleported')
      return true
    }
    // Every 5s, log who we can see, so we know why proximity isn't matching.
    if (tick % 5 === 0) {
      const visible = players
        .map(e => e.username)
        .filter(Boolean)
        .slice(0, 10)
        .join(', ') || '(none)'
      log(`[SPAWNER] [TPA_WAIT] elapsed=${Math.floor((Date.now()-startedAt)/1000)}s target="${JAVA_MAIN_USERNAME}" visible=[${visible}]`)
    }
    tick += 1
    await wait(1000)
  }

  throw new Error('TPA timeout waiting for Java main proximity')
}

async function dropSkeletonSpawnerToJava() {
  setSpawnerPhase('dropping_spawner')

  // DonutSMP teleport has a ~5s countdown after /tpaccept; wait for it to finish
  // so we actually drop the item next to Java main, not at the AFK spot.
  log('[SPAWNER] [DROP_WAIT] sleeping 6s for teleport countdown to finish')
  await wait(6000)

  const found = helper.findInventoryItem(isSkeletonSpawnerSummary)
  if (!found) throw new Error('No skeleton spawner in inventory to drop')

  const item = found.item
  const slot = item.slot
  const count = item.count || 1
  const raw = item.raw_item || {}

  log(`[SPAWNER] [DROP_ITEM_META] net=${item.network_id} has_nbt=${item.has_nbt} name="${item.custom_name_clean || ''}" slot=${slot} window=${found.windowId}`)
  try {
    const snaps = helper.containerSnapshots || {}
    const windows = Object.keys(snaps).map(k => `${k}:${(snaps[k]?.slots || []).filter(s => s && !s.empty).length}`).join(', ')
    log(`[SPAWNER] [DROP_WINDOWS] ${windows}`)
  } catch {}

  // Build Item struct matching bedrock-protocol Item definition for 1.21.x.
  // Key rules learned from the protocol JSON + Item.toBedrock() reference:
  //  - has_nbt is a MAPPER over lu16 (0='false', 65535='true') — bool OK
  //  - nbt field is only serialized when has_nbt is true; carry the exact
  //    parsed NBT from the inbound packet so the server recognises the item
  //  - extra layout is ItemExtraDataWithoutBlockingTick for non-shield items
  //    (no blocking_tick field for spawner)
  //  - Missing fields cause "sizeOf error for undefined" in the serializer
  const buildItem = (overrideCount) => {
    const extra = {
      has_nbt: !!raw.extra?.has_nbt,
      can_place_on: raw.extra?.can_place_on || [],
      can_destroy: raw.extra?.can_destroy || []
    }
    if (extra.has_nbt) {
      // Preserve the original NBT exactly as received so the server sees
      // the item as identical (display name, lore, custom data).
      extra.nbt = raw.extra.nbt
    }
    return {
      network_id: item.network_id,
      count: overrideCount,
      metadata: item.metadata || 0,
      has_stack_id: 1,
      stack_id: item.stack_id || 0,
      block_runtime_id: item.block_runtime_id || 0,
      extra
    }
  }

  const airItem = {
    network_id: 0,
    count: 0,
    metadata: 0,
    has_stack_id: 0,
    block_runtime_id: 0,
    extra: { has_nbt: false, can_place_on: [], can_destroy: [] }
  }

  const sourceItem = buildItem(count)

  // Classic Bedrock drop: inventory_transaction type="normal" with a pair
  // of balanced actions. Action 1 removes the item from the player's
  // inventory container; Action 2 releases the same item into the world.
  // item_release / drop_item action_type does NOT exist in 1.21 protocol.
  const wireWindow = slot < 9 ? 'hotbar' : 'inventory'
  const wireSlot = slot < 9 ? slot : slot - 9
  log(`[SPAWNER] [DROP_TRY] transaction_type=normal window=${wireWindow} wire_slot=${wireSlot} raw_slot=${slot} stack_id=${item.stack_id || 0}`)
  state.client.write('inventory_transaction', {
    transaction: {
      legacy: { legacy_request_id: 0, legacy_transactions: [] },
      transaction_type: 'normal',
      actions: [
        {
          source_type: 'container',
          // bedrock-protocol merges the 9 hotbar slots + 27 main inventory
          // slots into a single "inventory" snapshot window (36 slots total).
          // On the wire however, the server tracks them as SEPARATE windows:
          //   - slot 0-8  => hotbar window (id=122)
          //   - slot 9-35 => inventory window (id=0), indexed 0-26
          // Pick the correct window for our slot and translate the index.
          inventory_id: slot < 9 ? 'hotbar' : 'inventory',
          slot: slot < 9 ? slot : slot - 9,
          old_item: sourceItem,
          new_item: airItem
        },
        {
          source_type: 'world_interaction',
          flags: 0,
          slot: 0,
          old_item: airItem,
          new_item: sourceItem
        }
      ],
      transaction_data: 'void'
    }
  })

  // Poll up to 5s for the spawner to leave inventory.
  const deadline = Date.now() + 5000
  while (Date.now() < deadline) {
    if (!helper.findInventoryItem(isSkeletonSpawnerSummary)) {
      log('[SPAWNER] [DROP_OK] spawner left inventory')
      setSpawnerPhase('dropped', { droppedAt: new Date().toISOString() })
      return
    }
    await wait(400)
  }

  log('[SPAWNER] [DROP_STILL_IN_INV] spawner did not leave inventory after 5s')
  throw new Error('Drop failed — spawner still in inventory')
}

// --- Task system (IPC-driven) -------------------------------------------
// Tasks are dispatched from the manager via:
//   process.send({ type: 'task', kind: 'deliver_spawner', id, data })
// The worker executes one task at a time; manager polls with {type:'poll_tasks'}
// every 15s as a safety net if the push was missed.

function emitTaskState(extra = {}) {
  if (!state.activeTask) return
  state.activeTask = { ...state.activeTask, ...extra, updatedAt: new Date().toISOString() }
  scheduleDashboardBroadcast()
  if (process.env.IS_WORKER === 'true' && process.send) {
    process.send({ type: 'task_state', data: state.activeTask })
  }
  cloudEnqueue('task_state', state.activeTask)
}

function setTaskPhase(phase, extra = {}) {
  if (!state.activeTask) return
  log(`[TASK] [PHASE:${phase}]${Object.keys(extra).length ? ' ' + JSON.stringify(extra) : ''}`)
  emitTaskState({ phase, ...extra })
}

function taskDone(outcome, detail = null) {
  const task = state.activeTask
  if (!task) return
  const finished = { ...task, outcome, detail, finishedAt: new Date().toISOString() }
  log(`[TASK] [${String(task.kind).toUpperCase()}] [DONE:${outcome}]${detail ? ' ' + compactReason(detail, 60) : ''}`)
  state.activeTask = null
  scheduleDashboardBroadcast()
  if (process.env.IS_WORKER === 'true' && process.send) {
    process.send({ type: 'task_done', data: finished })
  }
  cloudEnqueue('task_done', finished)
}

function signalDeath(reason = 'health_zero') {
  state.deathSignal = true
  log(`[DEATH] [DETECTED] [REASON:${reason}]`)
}

async function awaitTargetProximity(targetUsername, timeoutMs = 60000) {
  const startedAt = Date.now()
  let tick = 0
  while (Date.now() - startedAt < timeoutMs) {
    if (!state.activeTask) return false // cancelled
    const players = Object.values(state.playerEntities || {})
    const entity = players.find(e => String(e.username || '').toLowerCase().includes(targetUsername.toLowerCase()))
    if (entity?.position && state.currentPosition && positionDistance(entity.position, state.currentPosition) <= 10) {
      return true
    }
    if (tick % 10 === 0) {
      const visible = players.map(e => e.username).filter(Boolean).slice(0, 8).join(', ') || '(none)'
      log(`[TASK] [TPA_WAIT] elapsed=${Math.floor((Date.now() - startedAt) / 1000)}s target="${targetUsername}" visible=[${visible}]`)
    }
    tick += 1
    await wait(1000)
  }
  return false
}

async function tpaRetryLoop(targetUsername) {
  let attempt = 0
  while (state.activeTask) {
    attempt += 1
    helper.sendCommand(`/tpa ${targetUsername}`)
    log(`[TASK] [TPA_SENT] attempt=${attempt} target=${targetUsername} waiting 60s for accept`)
    const joined = await awaitTargetProximity(targetUsername, 60000)
    if (joined) {
      log(`[TASK] [TPA_ACCEPTED] arrived near ${targetUsername}`)
      return true
    }
    log('[TASK] [TPA_TIMEOUT] no accept in 60s — retrying')
  }
  throw new Error('Task cancelled during TPA')
}

async function waitForDeath(timeoutMs = 30 * 60 * 1000) {
  state.deathSignal = false
  const deadline = Date.now() + timeoutMs
  while (state.activeTask && Date.now() < deadline) {
    if (state.deathSignal) {
      state.deathSignal = false
      return true
    }
    await wait(500)
  }
  if (!state.activeTask) throw new Error('Task cancelled while waiting for death')
  throw new Error('Timed out waiting for death (30m)')
}

async function runDeliverSpawnerTask(task) {
  if (state.activeTask) {
    log(`[TASK] [REJECT] another task active (${state.activeTask.kind})`)
    const payload = { id: task.id, reason: 'busy' }
    if (process.send) process.send({ type: 'task_rejected', data: payload })
    cloudEnqueue('task_rejected', payload)
    return
  }

  const target = String(task?.data?.targetUsername || JAVA_MAIN_USERNAME || '').trim()
  if (!target) {
    log('[TASK] [REJECT] no TPA target configured')
    const payload = { ...task, outcome: 'failed', detail: 'no tpa target' }
    if (process.send) process.send({ type: 'task_done', data: payload })
    cloudEnqueue('task_done', payload)
    return
  }

  state.activeTask = {
    ...task,
    phase: 'init',
    startedAt: new Date().toISOString(),
    target
  }
  scheduleDashboardBroadcast()
  log(`[TASK] [DELIVER_SPAWNER] [START] target=${target}`)

  // Keep legacy spawnerHandoff phase in sync so existing drift/rejoin guards
  // continue to treat us as "busy".
  setSpawnerPhase('task_active')

  try {
    if (!state.afkSuccess) throw new Error('Bot not yet in AFK area')

    // Dump current inventory for diagnostics.
    const invDump = (helper.getWindowItems('inventory') || [])
      .filter(it => it && !it.empty)
      .map(it => `slot=${it.slot} x${it.count} ${(it.custom_name_clean || '').trim() || `(id=${it.network_id})`}`)
    log(`[TASK] [INV_DUMP] items=${invDump.length}${invDump.length ? ' => ' + invDump.join(' | ') : ' (empty)'}`)

    // --- Phase 1: ensure we have a spawner ---
    let spawner = helper.findInventoryItem(isSkeletonSpawnerSummary)
    if (!spawner) {
      setTaskPhase('buying')
      await buySkeletonSpawner()
      const deadline = Date.now() + 8000
      while (Date.now() < deadline && !spawner) {
        spawner = helper.findInventoryItem(isSkeletonSpawnerSummary)
        if (spawner) break
        await wait(500)
      }
      if (!spawner) throw new Error('Purchase did not produce spawner in inventory')
      log(`[TASK] [BOUGHT_OK] slot=${spawner.item.slot}`)
    } else {
      log(`[TASK] [ALREADY_HAS] slot=${spawner.item.slot}`)
    }

    // --- Phase 2: TPA loop until accept ---
    setTaskPhase('tpa_waiting', { target })
    await tpaRetryLoop(target)

    // --- Phase 3: wait for user to kill us ---
    setTaskPhase('waiting_for_kill', { target })
    log(`[TASK] [AWAITING_KILL] you may now kill ${state.accountUsername || 'the bot'} to loot the spawner`)
    await waitForDeath()

    // --- Phase 4: respawn and resume AFK ---
    setTaskPhase('respawning')
    await wait(3000) // let respawn packet settle
    log('[TASK] [RESUME_AFK] dispatching /afk to rejoin area')
    clearAfkTimeout()
    state.afkSuccess = false
    state.afkAttemptInFlight = false
    state.lastRejoinAt = Date.now()
    setTimeout(tryJoinCurrentArea, 500)

    taskDone('success')
    setSpawnerPhase('cooldown')
  } catch (err) {
    const detail = String(err?.message || err)
    log(`[TASK] [FAILED] ${compactReason(detail, 60)}`)
    taskDone('failed', detail)
    setSpawnerPhase('failed', { lastError: detail })
  }
}

function cancelActiveTask(reason = 'cancelled') {
  if (!state.activeTask) return
  log(`[TASK] [CANCEL] ${reason}`)
  taskDone('cancelled', reason)
  setSpawnerPhase('idle')
}

function captureAfkAnchor(reason) {
  if (!state.currentPosition || !state.afkSuccess) return
  state.afkAnchorPosition = clonePosition(state.currentPosition)
  state.afkAnchorCapturedAt = new Date().toISOString()
  log(`[AFK] Anchor captured (${reason}) at ${formatPosition(state.afkAnchorPosition)}`)
  helper.saveSnapshot('afk_anchor_captured', {
    afk_state: {
      area: state.currentTargetArea,
      success: state.afkSuccess,
      anchor_position: state.afkAnchorPosition,
      anchor_reason: reason,
      anchor_captured_at: state.afkAnchorCapturedAt
    }
  })
}

function scheduleAnchorCapture(reason) {
  clearAnchorTimeout()
  state.anchorTimeout = setTimeout(() => {
    state.anchorTimeout = null
    captureAfkAnchor(reason)
  }, AFK_ANCHOR_CAPTURE_DELAY_MS)
}

function updateCurrentPosition(position, source = 'unknown') {
  if (!position) return
  state.currentPosition = clonePosition(position)
  scheduleDashboardBroadcast()

  if (state.afkSuccess && !state.afkAnchorPosition) {
    scheduleAnchorCapture(`first_position_after_success:${source}`)
  }
}

function updateCurrentPose(params = {}, source = 'unknown') {
  updateRotation(params)
  updateCurrentPosition(params.position || params.player_position, source)
}

function canTriggerRejoinNow() {
  if (state.afkAttemptInFlight || state.reconnecting || state.shuttingDown) return false
  if (!state.lastRejoinAt) return true
  return Date.now() - state.lastRejoinAt >= AFK_REJOIN_COOLDOWN_MS
}

function markAfkSuccess(reason) {
  clearAfkTimeout()
  state.afkAttemptInFlight = false
  state.afkSuccess = true
  state.successAt = new Date().toISOString()
  state.lastStatusAt = state.successAt
  log(`[AFK] [SUCCESS] [AFK:${state.currentTargetArea}] [REASON:${compactReason(reason, 36)}]`)
  captureAfkLookBase()
  startAuthInputLoop('afk_success')
  scheduleAnchorCapture('afk_success')
  helper.saveSnapshot('afk_success', {
    afk_state: {
      area: state.currentTargetArea,
      reason,
      success_at: state.successAt
    }
  })
}

function scheduleRejoin(reason, { immediate = false, allowAreaAdvance = false } = {}) {
  // Never rejoin while spawner handoff is active — the bot is intentionally
  // away from its AFK anchor (teleported to Java main). Rejoining now would
  // drag the bot off before it can drop the spawner.
  const phase = state.spawnerHandoff?.phase
  const handoffActive = phase && !['idle', 'cooldown', 'failed'].includes(phase)
  if (handoffActive) {
    log(`[AFK] [REJOIN_SKIP] handoff phase=${phase} reason=${compactReason(reason, 32)}`)
    return
  }

  if (allowAreaAdvance) {
    state.afkSuccess = false
    moveToNextArea(reason)
    return
  }

  clearAfkTimeout()
  state.afkSuccess = false
  state.afkAttemptInFlight = false
  state.lastStatusAt = new Date().toISOString()
  state.lastRejoinAt = Date.now()
  log(`[AFK] [REJOIN] [REASON:${compactReason(reason)}]`)
  setTimeout(tryJoinCurrentArea, immediate ? 500 : 2500)
}

function moveToNextArea(reason) {
  clearAfkTimeout()
  state.afkAttemptInFlight = false
  state.afkSuccess = false
  state.currentAreaIndex += 1

  if (state.currentAreaIndex >= areas.length) {
    log(`[AFK] [EXHAUSTED] [REASON:${compactReason(reason)}]`)
    helper.saveSnapshot('afk_exhausted', {
      afk_state: {
        exhausted: true,
        reason
      }
    })
    return
  }

  log(`[AFK] [NEXT_AREA] [REASON:${compactReason(reason)}]`)
  setTimeout(tryJoinCurrentArea, 1500)
}

function tryJoinCurrentArea() {
  if (state.afkSuccess || state.afkAttemptInFlight) return
  if (state.currentAreaIndex >= areas.length) return

  state.currentTargetArea = areas[state.currentAreaIndex]
  state.afkAttemptInFlight = true
  state.lastStatusAt = new Date().toISOString()

  const command = `/afk ${state.currentTargetArea}`
  log(`[AFK] [TRY] [AFK:${state.currentTargetArea}]`)

  try {
    helper.sendCommand(command)
  } catch (err) {
    state.afkAttemptInFlight = false
    log(`[AFK] [COMMAND_FAIL] [AFK:${state.currentTargetArea}] [REASON:${compactReason(err.message)}]`)
    setTimeout(tryJoinCurrentArea, 3000)
    return
  }

  state.currentTimeout = setTimeout(() => {
    if (state.afkSuccess || !state.afkAttemptInFlight) return
    log(`[AFK] [TIMEOUT] [AFK:${state.currentTargetArea}] [WAIT:${AFK_RESULT_TIMEOUT_MS}ms]`)
    state.afkAttemptInFlight = false
    helper.saveSnapshot('afk_timeout', {
      afk_state: {
        area: state.currentTargetArea,
        timeout_ms: AFK_RESULT_TIMEOUT_MS
      }
    })
    setTimeout(tryJoinCurrentArea, 5000)
  }, AFK_RESULT_TIMEOUT_MS)
}

function parseTeleportedArea(message) {
  const cleaned = stripMcColorCodes(String(message || ''))
  const match = cleaned.match(/teleported to .*?(\d+)\.?$/i)
  if (!match) return null
  return Number(match[1])
}

function detectAutoAssignedAfkArea(cleanMessage) {
  const text = normalizeChat(cleanMessage)
  const area = parseTeleportedArea(cleanMessage)
  if (area == null) return null
  if (!text.includes('teleported to')) return null
  if (!text.includes('afk')) return null
  return area
}

function markAutoAssignedAfk(area, reason) {
  clearSpawnCommandTimeout()
  state.waitingForAutoAssign = false
  state.currentTargetArea = area
  state.lastKnownAreaFromChat = area

  const existingIndex = areas.indexOf(area)
  if (existingIndex >= 0) state.currentAreaIndex = existingIndex

  markAfkSuccess(reason)
}

function handlePostSuccessChat(cleanMessage) {
  const text = normalizeChat(cleanMessage)
  const teleportedArea = parseTeleportedArea(cleanMessage)

  if (teleportedArea != null) {
    state.lastKnownAreaFromChat = teleportedArea
    if (state.currentTargetArea != null && teleportedArea !== state.currentTargetArea) {
      if (canTriggerRejoinNow()) {
        scheduleRejoin(`moved out of afk area ${state.currentTargetArea} to area ${teleportedArea}`, { immediate: true })
      }
      return
    }
  }

  const maintenancePatterns = [
    'maintenance',
    'disabled',
    'temporarily unavailable',
    'đang bảo trì',
    'bảo trì',
    'tạm đóng',
    'unavailable'
  ]

  if (maintenancePatterns.some(pattern => text.includes(pattern)) && text.includes('afk')) {
    scheduleRejoin(`afk maintenance detected: ${text}`, { allowAreaAdvance: true })
  }
}

function checkAfkPositionDrift(reason = 'interval') {
  if (!state.afkSuccess || !state.afkAnchorPosition || !state.currentPosition) return
  if (!canTriggerRejoinNow()) return

  // Skip drift check while spawner handoff is in progress — the TPA teleport
  // is INTENTIONAL movement and must not trigger an auto /afk rejoin that
  // would drag the bot away before the spawner is dropped.
  const phase = state.spawnerHandoff?.phase
  const handoffActive = phase && !['idle', 'cooldown', 'failed'].includes(phase)
  if (handoffActive) {
    log(`[AFK] [DRIFT_SKIP] handoff phase=${phase}`)
    return
  }

  const distance = positionDistance(state.currentPosition, state.afkAnchorPosition)
  if (distance < AFK_DRIFT_DISTANCE) return

  scheduleRejoin(
    `position drift detected (${reason}) distance=${distance.toFixed(2)} anchor=${formatPosition(state.afkAnchorPosition)} current=${formatPosition(state.currentPosition)}`,
    { immediate: true }
  )
}

function sendAntiIdle(reason = 'interval') {
  if (!state.afkSuccess || state.reconnecting || state.shuttingDown) return
  if (!state.client || state.client.entityId == null) return

  try {
    state.client.write('animate', {
      action_id: 'swing_arm',
      runtime_entity_id: state.client.entityId
    })
    state.lastActivityAt = new Date().toISOString()
    log(`[AFK] [SWING] [EID:${state.client.entityId}]`)
  } catch (err) {
    log(`[AFK] [SWING_FAIL] [REASON:${compactReason(err.message)}]`)
  }
}

function scheduleReconnect(reason) {
  if (state.shuttingDown || state.reconnecting || state.reconnectTimeout) return

  const watchdogReason = state.reconnectWatchdogReason
  const reconnectReason = (String(reason || '').toLowerCase() === 'close' && watchdogReason) ? watchdogReason : reason
  const alreadyLoggedIn = isAlreadyLoggedInReason(reconnectReason)
  if (alreadyLoggedIn) {
    state.alreadyLoggedInRetries += 1
    if (state.alreadyLoggedInRetries > ALREADY_LOGGED_IN_MAX_RETRIES) {
      log(`[RECONNECT] [STOP] [REASON:ALREADY_LOGGED_IN] [ATTEMPT:${state.alreadyLoggedInRetries - 1}]`)
      state.shuttingDown = true
      clearAfkTimeout()
      clearSpawnCommandTimeout()
      clearAnchorTimeout()
      clearReconnectTimeout()
      try {
        state.client?.close()
      } catch {}
      logger.close()
      process.exit(1)
    }
  } else {
    state.alreadyLoggedInRetries = 0
  }

  state.reconnecting = true
  state.reconnectAttempt += 1
  resetJoinState()
  state.reconnectWatchdogReason = null
  state.client = null
  const reconnectDelay = alreadyLoggedIn
    ? ALREADY_LOGGED_IN_RECONNECT_DELAY_MS
    : watchdogReason
      ? WATCHDOG_RECONNECT_DELAY_MS
      : RECONNECT_DELAY_MS
  log(`[RECONNECT] [SCHEDULED] [ATTEMPT:${state.reconnectAttempt}] [IN:${Math.floor(reconnectDelay / 1000)}s] [REASON:${compactReason(reconnectReason, 28)}]`)
  state.reconnectTimeout = setTimeout(() => {
    state.reconnectTimeout = null
    createAndWireClient()
  }, reconnectDelay)
}

function createAndWireClient() {
  clearReconnectTimeout()
  clearReconnectWatchdogTimeout()
  state.reconnecting = false
  state.reconnectWatchdogReason = null

  const hasProxy = Boolean(process.env.PROXY_HOST)
  const clientOptions = {
    host: 'donutsmp.net',
    port: 19132,
    profilesFolder: authCacheDir,
    offline: false,
    skipPing: true,
    connectTimeout: CONNECT_TIMEOUT_MS,
    // Dùng jsp-raknet khi có proxy (để override UDP socket qua SOCKS5).
    // Không proxy -> dùng raknet-native (C++ binding, ổn định hơn, mặc định).
    raknetBackend: hasProxy ? 'jsp-raknet' : 'raknet-native',
    useRaknetWorkers: false, // workerConnect trong rak.js chưa hoàn thiện, luôn dùng plainConnect
    onMsaCode: (data) => {
      const payload = { url: data.verification_uri, code: data.user_code }
      if (process.env.IS_WORKER === 'true' && process.send) {
        process.send({ type: 'msa_code', data: payload })
      }
      cloudEnqueue('msa_code', payload)
      log('================================================================')
      log('LOGIN XBOX REQUIRED')
      log(`URL: ${data.verification_uri}`)
      log(`CODE: ${data.user_code}`)
      log('================================================================')
    }
  }

  if (process.env.PROXY_HOST) {
    clientOptions.proxy = {
      host: process.env.PROXY_HOST,
      port: Number(process.env.PROXY_PORT),
      user: process.env.PROXY_USER,
      pass: process.env.PROXY_PASS
    }
    log(`[PROXY] Using ${clientOptions.proxy.host}:${clientOptions.proxy.port}`)
  }

  const client = bedrock.createClient(clientOptions)
  markConnectProgress('client_created')
  markConnectProgress('authenticating')

  // Safety net: gắn error listener ngay để tránh crash nếu error fire trước khi handler chính gắn
  // (hoặc sau khi client.close() gọi removeAllListeners)
  client.on('error', err => {
    log(`[ERROR] [CLIENT_SAFETY] [REASON:${compactReason(err?.message || err, 36)}]`)
  })

  state.client = client
  helper.setClient(client)
  scheduleReconnectWatchdog(client)

  const originalWrite = client.write
  client.write = function wrappedWrite(name, params) {
    if (name === 'resource_pack_client_response') return
    originalWrite.call(this, name, params)
  }

  captureAccountIdentity(client)

  client.on('connect', () => {
    markConnectProgress('connect')
    log('[EVENT] [CONNECT]')
  })

  client.on('session', () => {
    markConnectProgress('auth_session')
    captureAccountIdentity(client)
    log('[EVENT] [AUTH_SESSION]')
  })

  client.on('loggingIn', () => {
    markConnectProgress('login_sent')
    log('[EVENT] [LOGIN_SENT]')
  })

  client.on('join', () => {
    markConnectProgress('join')
    captureAccountIdentity(client)
    log('[EVENT] [JOIN]')
  })

  client.on('spawn', () => {
    markConnectProgress('spawn')
    log('[EVENT] [SPAWN]')
  })

  client.on('close', (...args) => {
    clearReconnectWatchdogTimeout()
    clearAuthInputLoop()
    log('[EVENT] [CLOSE]')
    if (args.length) log(`[DETAIL] ${inspect(args)}`)
    state.client = null
    if (!state.shuttingDown) scheduleReconnect('close')
  })

  client.on('disconnect', packet => {
    clearReconnectWatchdogTimeout()
    clearAuthInputLoop()
    const reason = packet?.message || packet?.reason || 'unknown'
    log(`[DISCONNECT] [REASON:${compactReason(reason, 36)}]`)
    log(`[DETAIL] ${inspect(packet)}`)
    state.client = null
    helper.saveSnapshot('disconnect', {
      afk_state: {
        area: state.currentTargetArea,
        success: state.afkSuccess
      }
    })
    if (!state.shuttingDown) scheduleReconnect(`disconnect:${packet?.message || packet?.reason || 'unknown'}`)
  })

  client.on('error', err => {
    clearReconnectWatchdogTimeout()
    clearAuthInputLoop()
    log(`[ERROR] [CLIENT] [REASON:${compactReason(err?.message || err, 36)}]`)
    log(`[DETAIL] ${inspect(err)}`)
    helper.saveSnapshot('error', {
      afk_state: {
        area: state.currentTargetArea,
        success: state.afkSuccess,
        error: String(err?.message || err)
      }
    })
    state.client = null
    if (!state.shuttingDown) scheduleReconnect(`error:${err?.message || err}`)
  })

  client.on('packet', packet => {
    const name = packet.data.name
    const params = packet.data.params
    if (!state.spawned) markConnectProgress(`packet:${name}`)

    if (name === 'network_stack_latency' && params.needs_response) {
      const signedTimestamp = BigInt.asIntN(64, params.timestamp)
      const responseTimestamp = BigInt.asUintN(64, signedTimestamp * 1000000n)
      originalWrite.call(client, 'network_stack_latency', {
        timestamp: responseTimestamp,
        needs_response: false
      })
    }

    if (name === 'resource_packs_info') {
      log('[PKT] [RESOURCE_PACKS_INFO]')
      originalWrite.call(client, 'resource_pack_client_response', {
        response_status: 'have_all_packs',
        resourcepackids: []
      })
    }

    if (name === 'resource_pack_stack') {
      log('[PKT] [RESOURCE_PACK_STACK]')
      originalWrite.call(client, 'resource_pack_client_response', {
        response_status: 'completed',
        resourcepackids: []
      })
    }

    if (name === 'start_game') {
      clearReconnectWatchdogTimeout()
      log(`[PKT] [START_GAME] [EID:${params.runtime_entity_id}]`)
      client.startGameData = params
      state.authInputTick = coerceTick(params.current_tick, state.authInputTick)
      updateCurrentPose({
        position: params.player_position,
        pitch: params.rotation?.x ?? params.pitch,
        yaw: params.rotation?.y ?? params.yaw,
        head_yaw: params.rotation?.y ?? params.head_yaw
      }, 'start_game')
      helper.sendInitializedOnce('start_game')
    }

    if (name === 'set_movement_authority') {
      state.movementAuthority = params.movement_authority || null
      log(`[PKT] [MOVEMENT_AUTHORITY] [${String(state.movementAuthority || 'unknown').toUpperCase()}]`)
    }

    if (name === 'play_status') {
      log(`[PKT] [PLAY_STATUS] [${String(params.status || 'unknown').toUpperCase()}]`)
      if (params.status === 'player_spawn' && !state.spawned) {
        state.spawned = true
        state.alreadyLoggedInRetries = 0
        startLocalPlaytimeSession()
        helper.sendInitializedOnce('player_spawn')
        state.waitingForAutoAssign = true
        clearSpawnCommandTimeout()
        state.spawnCommandTimeout = setTimeout(() => {
          state.spawnCommandTimeout = null
          if (state.afkSuccess) return
          state.waitingForAutoAssign = false
          tryJoinCurrentArea()
        }, Math.max(AFK_COMMAND_DELAY_MS, AFK_AUTO_ASSIGN_GRACE_MS))
      }
    }

    if (name === 'container_open') {
      helper.openContainer(params)
      helper.saveSnapshot('container_open')
    }

    if (name === 'inventory_content' && Array.isArray(params.input)) {
      helper.updateInventoryContent(params.window_id, params.input)
    }

    if (name === 'inventory_slot') {
      helper.updateInventorySlot(params.window_id, params.slot, params.item || params)
    }

    if (name === 'add_player') {
      rememberPlayerEntity(params)
    }

    if (name === 'remove_entity') {
      forgetPlayerEntity(params)
    }

    if (name === 'move_player') {
      const runtimeId = params.runtime_id ?? params.runtime_entity_id
      if (client.entityId != null && String(runtimeId) === String(client.entityId)) {
        state.authInputTick = coerceTick(params.tick, state.authInputTick)
        if (params.mode === 'teleport' || params.mode === 'reset') {
          state.pendingTeleportAckPackets = Math.max(state.pendingTeleportAckPackets, 20)
          log(`[AFK] [TELEPORT_ACK_PENDING] [MODE:${params.mode}] [PACKETS:${state.pendingTeleportAckPackets}]`)
        }
        updateCurrentPose(params, `move_player:${params.mode || 'unknown'}`)
        checkAfkPositionDrift(`move_player:${params.mode || 'unknown'}`)
      } else {
        updatePlayerEntityPosition(params)
      }
    }

    if (name === 'correct_player_move_prediction') {
      state.authInputTick = coerceTick(params.tick, state.authInputTick)
      updateCurrentPosition(params.position, 'correct_player_move_prediction')
    }

    if (name === 'set_display_objective') {
      updateScoreboardObjective(params)
    }

    // --- Death detection (for IPC deliver_spawner task) ---
    // Bedrock signals death through either `set_health` with health=0 or
    // through the server sending an `event` packet / respawn flow. Flag
    // the deathSignal whenever health drops to 0 while a task is awaiting
    // it. Also treat receipt of a `respawn` packet with state=search as a
    // death confirmation in case we missed the health edge.
    if (name === 'set_health' && Number(params?.health) <= 0) {
      if (state.activeTask?.phase === 'waiting_for_kill') signalDeath('set_health=0')
    }
    if (name === 'respawn' && state.activeTask?.phase === 'waiting_for_kill') {
      signalDeath(`respawn:${params?.state || 'unknown'}`)
    }

    if (name === 'set_score') {
      updateScoreboardEntries(params)
      refreshShardState()
      // Auto-spawner trigger removed — spawner delivery is now manager-driven
      // via IPC tasks (see runDeliverSpawnerTask).
    }

    if (name === 'text') {
      const rawMessage = params.message || ''
      const cleanMessage = stripMcColorCodes(rawMessage)
      const sourceName = String(params.source_name || 'System').toUpperCase()
      // Tắt toàn bộ log [CHAT] [SYSTEM] — quá spam, không cần thiết.
      if (sourceName !== 'SYSTEM') {
        log(`[CHAT] [${sourceName}] ${cleanMessage}`)
      }

      if (!state.afkSuccess && state.waitingForAutoAssign) {
        const autoAssignedArea = detectAutoAssignedAfkArea(cleanMessage)
        if (autoAssignedArea != null) {
          log(`[AFK] [AUTO_ASSIGN] [AFK:${autoAssignedArea}]`)
          markAutoAssignedAfk(autoAssignedArea, normalizeChat(cleanMessage))
          return
        }
      }

      if (state.afkSuccess) {
        handlePostSuccessChat(cleanMessage)
      }

      if (state.afkSuccess) return
      if (!state.afkAttemptInFlight || state.currentTargetArea == null) return

      const result = buildAfkAnalyzer(state.currentTargetArea)(cleanMessage)
      if (!result) return

      if (result.type === 'success') {
        markAfkSuccess(result.text)
        return
      }

      if (result.type === 'full') {
        moveToNextArea(result.text)
        return
      }

      if (result.type === 'failure') {
        log(`[AFK] [FAILED] [AFK:${state.currentTargetArea}] [REASON:${compactReason(result.text)}]`)
        moveToNextArea(result.text)
      }
    }
  })
  return client
}

setInterval(() => {
  const current = state.currentTargetArea == null ? 'none' : state.currentTargetArea
  const status = state.afkSuccess ? 'SUCCESS' : (state.afkAttemptInFlight ? 'PENDING' : 'IDLE')
  const reconnectCount = state.reconnectAttempt || 0
  const playtime = getScoreboardStats().playtime
  log(`[HEARTBEAT] [${status}] [AFK:${current}] [POS:${formatPosition(state.currentPosition)}] [PLAYTIME:${playtime.value}] [SRC:${playtime.source}] [SERVER:${playtime.serverSeconds ?? '-'}] [LOCAL:${playtime.localSeconds ?? '-'}] [PHASE:${state.connectPhase || 'idle'}] [RECONNECT:${reconnectCount}]`)
}, HEARTBEAT_MS)

setInterval(() => {
  sendAntiIdle('interval')
}, AFK_ANTI_IDLE_MS)

setInterval(() => {
  checkAfkPositionDrift('interval')
}, AFK_DRIFT_CHECK_MS)

// --- IPC: receive tasks from manager ------------------------------------
if (process.env.IS_WORKER === 'true') {
  process.on('message', (msg) => {
    if (!msg || typeof msg !== 'object') return
    try {
      if (msg.type === 'task' && msg.kind === 'deliver_spawner') {
        const task = { kind: 'deliver_spawner', id: msg.id, data: msg.data || {} }
        runDeliverSpawnerTask(task).catch(err => {
          log(`[TASK] [HANDLER_ERR] ${compactReason(err?.message || err, 60)}`)
        })
      } else if (msg.type === 'cancel_task') {
        cancelActiveTask(msg.reason || 'manager_cancel')
      }
    } catch (err) {
      log(`[IPC] [ERR] ${compactReason(err?.message || err, 60)}`)
    }
  })

  // Every 15s ping the manager for any pending task (safety net if the
  // IPC push was missed, e.g. worker restart). Manager replies with a
  // {type:'task', kind, id, data} message when a task is queued.
  setInterval(() => {
    if (!process.send) return
    try {
      process.send({
        type: 'poll_tasks',
        hasActiveTask: Boolean(state.activeTask),
        activeTaskKind: state.activeTask?.kind || null
      })
    } catch {}
  }, 15000)
}

setInterval(() => {
  scheduleDashboardBroadcast()
}, DASHBOARD_REFRESH_MS)

process.on('SIGINT', () => {
  log('--- SIGINT ---')
  state.shuttingDown = true
  stopLocalPlaytimeSession()
  clearAfkTimeout()
  clearSpawnCommandTimeout()
  clearAnchorTimeout()
  clearReconnectTimeout()
  clearReconnectWatchdogTimeout()
  clearAuthInputLoop()
  helper.saveSnapshot('sigint', {
    afk_state: {
      area: state.currentTargetArea,
      success: state.afkSuccess
    }
  })
  try {
    state.client?.close()
  } catch {}
  try {
    dashboardServer?.close()
  } catch {}
  logger.close()
  process.exit(0)
})

if (process.stdin && process.stdin.isTTY) {
  process.stdin.setEncoding('utf8')
  process.stdin.on('data', chunk => {
    const commands = String(chunk || '')
      .split(/\r?\n/)
      .map(line => line.trim())
      .filter(Boolean)

    for (const command of commands) {
      handleConsoleCommand(command)
    }
  })
}

// Chỉ mở dashboard HTTP server khi chạy local standalone (không phải worker IPC, không phải cloud)
if (process.env.IS_WORKER !== 'true' && !CLOUD_MODE) {
  startDashboardServer()
}

// Cloud mode: lên lịch tự thoát trước khi GitHub Actions kill (max 6h)
if (CLOUD_MODE) {
  const durationSec = Number(process.env.RUN_DURATION_SEC || 20400) // mặc định 5h40m
  log(`[CLOUD] [MODE:ACTIVE] [DURATION:${durationSec}s] [WEBHOOK:${WEBHOOK_URL ? 'SET' : 'NONE'}]`)

  setTimeout(async () => {
    log(`[CLOUD] [AUTO_EXIT] [ELAPSED:${durationSec}s] [REASON:DURATION_REACHED]`)
    state.shuttingDown = true
    stopLocalPlaytimeSession()
    clearAuthInputLoop()
    try { state.client?.close() } catch {}
    await flushCloudQueue()
    setTimeout(() => process.exit(0), 1500)
  }, durationSec * 1000)
}

// SIGTERM handler (GitHub Actions gửi khi timeout-minutes reached, hoặc khi cancel run)
process.on('SIGTERM', async () => {
  log('[CLOUD] [SIGTERM] Graceful shutdown')
  state.shuttingDown = true
  stopLocalPlaytimeSession()
  clearAuthInputLoop()
  try { state.client?.close() } catch {}
  await flushCloudQueue()
  setTimeout(() => process.exit(0), 1500)
})

;(async () => {
  if (CLOUD_MODE) {
    const result = await reportIpAndCheck()
    if (result && result.allowed === false) {
      log(`[CLOUD] [IP_BLOCKED] [REASON:${result.reason || 'duplicate IP'}]`)
      log('[CLOUD] [ABORT] Không kết nối Minecraft, thoát ngay')
      await flushCloudQueue()
      setTimeout(() => process.exit(2), 1500)
      return
    }
  }
  createAndWireClient()
})()
