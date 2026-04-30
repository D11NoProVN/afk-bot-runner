import mineflayer from 'mineflayer'
import chalk from 'chalk'
import moment from 'moment'
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

/* ================= CLOUD BRIDGE ================= */

const CLOUD_MODE = process.env.CLOUD_MODE === 'true'
const WEBHOOK_URL = process.env.WEBHOOK_URL || ''
const WEBHOOK_TOKEN = process.env.WEBHOOK_TOKEN || ''
const ACCOUNT_ID = process.env.ACCOUNT_ID || 'default'
const CLOUD_FLUSH_MS = 2000

let cloudQueue = []
let cloudFlushTimer = null
let cloudFlushing = false

function cloudEnqueue(type, data) {
  if (!CLOUD_MODE || !WEBHOOK_URL) return
  cloudQueue.push({ type, data, ts: Date.now() })
  if (cloudQueue.length > 500) cloudQueue.splice(0, cloudQueue.length - 500)
  scheduleCloudFlush()
}

function scheduleCloudFlush() {
  if (cloudFlushTimer || cloudFlushing) return
  cloudFlushTimer = setTimeout(flushCloudQueue, CLOUD_FLUSH_MS)
}

async function flushCloudQueue() {
  cloudFlushTimer = null
  if (cloudFlushing || cloudQueue.length === 0) return
  cloudFlushing = true

  const batch = cloudQueue.splice(0, cloudQueue.length)
  try {
    const response = await fetch(WEBHOOK_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Webhook-Token': WEBHOOK_TOKEN
      },
      body: JSON.stringify({
        accountId: ACCOUNT_ID,
        runId: process.env.GITHUB_RUN_ID || null,
        events: batch
      })
    })

    if (response.ok) {
      const payload = await response.json()
      if (payload && payload.tasks) {
         // Directives từ manager (nếu có)
         applyDirectives(payload)
      }
    }
  } catch (err) {
    // console.error('Cloud flush failed:', err.message)
  } finally {
    cloudFlushing = false
    if (cloudQueue.length > 0) scheduleCloudFlush()
  }
}

function applyDirectives(payload) {
  if (payload.cancelTask) {
    console.log(time(), chalk.red('🛑 Received cancel directive from manager'))
    process.exit(0)
  }
}

function log(message) {
  const line = String(message)
  cloudEnqueue('log', line)
  console.log(line)
}

/* ================= CONFIG ================= */

// Trên Cloud, ta lấy config từ Env hoặc Secret
const config = {
  server: {
    host: process.env.SERVER_HOST || 'kingmc.vn',
    port: parseInt(process.env.SERVER_PORT || '25565'),
    version: process.env.SERVER_VERSION || '1.21'
  },
  account: {
    username: process.env.MC_USERNAME || 'ZenHihi',
    auth: process.env.MC_AUTH || 'offline',
    password: process.env.MC_PASSWORD || 'zennopro11'
  },
  ui: {
    showTime: true
  }
}

/* ================= HELPERS ================= */

function time() {
  return config.ui?.showTime
    ? chalk.gray(`[${moment().format('HH:mm:ss')}]`)
    : ''
}

function extractText(obj) {
  if (obj === null || obj === undefined) return ""
  if (typeof obj === "string") return obj.replace(/§./g, '')
  
  try {
    if (obj.toString && typeof obj.toString === 'function') {
      const str = obj.toString()
      if (str !== '[object Object]') return str.replace(/§./g, '')
    }
  } catch {}

  let result = ""
  if (Array.isArray(obj)) return obj.map(extractText).join("")
  if (obj.text !== undefined) result += extractText(obj.text)
  if (obj.extra !== undefined) result += extractText(obj.extra)
  return result
}

/* ================= BOT LOGIC ================= */

let bot
let hasSentHub = false
let smpFallbackTimeout = null
let shardReportInterval = null

function createBot() {
  log(`${time()} ${chalk.yellow(`🚀 Connecting to ${config.server.host} as ${config.account.username}...`)}`)
  
  bot = mineflayer.createBot({
    host: config.server.host,
    port: config.server.port,
    username: config.account.username,
    auth: config.account.auth,
    version: config.server.version || false,
    hideErrors: true
  })

  bot.inSMP = false
  bot.smpTransition = false
  bot.isLobbyReady = false

  bot.on('login', () => {
    log(`${time()} ${chalk.green(`✔ Logged in`)}`)
    if (bot.smpTransition) {
      bot.inSMP = true
      bot.smpTransition = false
      bot.isLobbyReady = false
      hasSentHub = false
      if (smpFallbackTimeout) clearTimeout(smpFallbackTimeout)
      
      log(`${time()} ${chalk.green('✔ Chuyển server thành công (Vào SMP). Chờ 1.5s rồi /afk...')}`)
      setTimeout(() => {
        if (bot && bot.chat) {
           log(`${time()} ${chalk.yellow('[AUTO] Sending /afk')}`)
           bot.chat('/afk')
        }
      }, 1500)
    }
  })

  bot.on('spawn', () => {
    log(`${time()} ${chalk.cyan('✔ Spawned')}`)
    setTimeout(() => {
       if (bot.entity) {
         bot.setControlState('forward', true)
         setTimeout(() => bot.setControlState('forward', false), 250)
       }
    }, 500)
    
    // Bắt đầu báo cáo shard định kỳ khi đã vào game
    startReporting()
  })

  bot.on('resourcePack', () => {
     log(`${time()} ${chalk.yellow('📦 Chấp nhận Resource Pack...')}`)
     bot.acceptResourcePack()
  })

  bot.on('message', (msg) => {
    const text = msg.toString().replace(/§./g, '')
    if (text.trim()) log(`${time()} ${chalk.white(text)}`)

    if (text.includes('/dk <mật khẩu>') || text.includes('Đăng ký')) {
       const password = config.account.password || 'zennopro11'
       setTimeout(() => bot.chat(`/dk ${password}`), 1000)
    }

    if (text.includes('/dn <mật khẩu>') || text.includes('Đăng nhập')) {
       const password = config.account.password || 'zennopro11'
       setTimeout(() => bot.chat(`/dn ${password}`), 1000)
    }

    if (text.includes('Phiên đăng nhập đã được kết nối trở lại.') || text.includes('SẢNH ➞ Đăng nhập thành công')) {
       if (!bot.isLobbyReady && !bot.inSMP && !hasSentHub) {
           hasSentHub = true
           log(`${time()} ${chalk.magenta('ℹ Tự động gửi /hub...')}`)
           setTimeout(() => bot.chat('/hub'), 1000)
       }
    }

    if (text.includes('SẢNH ➞ Bạn đã đăng nhập!') || text.includes('Bạn đã được gửi thành công đến sảnh')) {
       bot.isLobbyReady = true
       hasSentHub = false
       setTimeout(() => {
          if (bot && bot.isLobbyReady && !bot.inSMP) {
             log(`${time()} ${chalk.yellow('[AUTO] Sending /menu')}`)
             bot.chat('/menu')
          }
       }, 2000)
    }
  })

  bot.on('windowOpen', async (window) => {
    const title = extractText(window.title)
    log(`${time()} ${chalk.yellow(`📦 Opened GUI: ${title}`)}`)

    await new Promise(r => setTimeout(r, 800))
    if (!bot.currentWindow) return

    const items = window.slots
    
    // 1. Captcha
    const limeGlass = items.find(item => item && item.name === 'lime_stained_glass_pane')
    if (limeGlass) {
      log(`${time()} ${chalk.blue(`🎯 Click Captcha: ${limeGlass.slot}`)}`)
      bot.clickWindow(limeGlass.slot, 0, 0)
      return
    }

    // 2. Menu chính
    if (title.toUpperCase().includes('MENU')) {
      log(`${time()} ${chalk.blue('🎯 Menu Chính -> Slot 24 (SMP)')}`)
      bot.smpTransition = true
      smpFallbackTimeout = setTimeout(() => {
         if (bot && bot.smpTransition && !bot.inSMP) {
            bot.inSMP = true
            bot.smpTransition = false
            bot.chat('/afk')
         }
      }, 8000)
      bot.clickWindow(24, 0, 0)
      return
    }

    // 3. Menu AFK
    const netherStar = items.find(item => item && item.name === 'nether_star')
    if (netherStar) {
      log(`${time()} ${chalk.blue(`🎯 Menu AFK -> Slot: ${netherStar.slot}`)}`)
      bot.clickWindow(netherStar.slot, 0, 0)
      return
    }
  })

  bot.on("kicked", (reason) => {
    log(`${time()} ${chalk.red("⚠ Kicked: " + extractText(reason))}`)
  })

  bot.on('end', () => {
    log(`${time()} ${chalk.red('✖ Disconnected. Reconnecting in 10s...')}`)
    stopReporting()
    setTimeout(createBot, 10000)
  })

  bot.on('error', (err) => {
    log(`${time()} ${chalk.red('❌ Error: ' + err.message)}`)
  })
}

/* ================= SCOREBOARD REPORTING ================= */

function getScoreboardData() {
  if (!bot || !bot.scoreboards) return null
  let sb = Object.values(bot.scoreboards).find(s => s.position === 1 || s.position === 'sidebar') || Object.values(bot.scoreboards)[0]
  if (!sb) return null
  
  const results = { shards: null, money: null }
  if (bot.teamMap) {
     const sbName = sb.name || ""
     let sidebarTeams = Object.values(bot.teamMap).filter(t => {
         const tName = extractText(t.name) || ""
         const dName = extractText(t.displayName) || ""
         if (sbName && (tName.includes(sbName) || dName.includes(sbName))) return true
         if (tName.match(/^§[0-9a-fk-or]/i)) return true
         return false
     })
     
     if (bot.inSMP) {
         sidebarTeams = sidebarTeams.filter(t => !extractText(t.name).includes('TAB-Sidebar'))
     }
     
     for (const team of sidebarTeams) {
         const cleanText = (extractText(team.prefix) + extractText(team.suffix)).trim().toLowerCase()
         const valueMatch = cleanText.match(/[-+]?\d[\d,]*(?:\.\d+)?/)
         const value = valueMatch ? valueMatch[0].replace(/,/g, '') : null

         if (cleanText.includes('shard') || cleanText.includes('ꜱʜᴀʀᴅ')) {
             results.shards = value
         } else if (cleanText.includes('money') || cleanText.includes('xu') || cleanText.includes('$')) {
             results.money = value
         }
     }
  }
  return (results.shards || results.money) ? results : null
}

function startReporting() {
  if (shardReportInterval) return
  shardReportInterval = setInterval(() => {
    const data = getScoreboardData()
    if (data) {
      cloudEnqueue('state', data)
    }
  }, 30000)
}

function stopReporting() {
  if (shardReportInterval) clearInterval(shardReportInterval)
  shardReportInterval = null
}

/* ================= START ================= */

createBot()

// Tự động chain khi gần hết giờ (GitHub Actions limit ~6h)
if (CLOUD_MODE) {
  const RUN_DURATION_SEC = parseInt(process.env.RUN_DURATION_SEC || '20400') // 5h40m
  setTimeout(() => {
    log(`${time()} ${chalk.cyan('⏰ Time limit reached. Shutting down for chain restart...')}`)
    process.exit(0)
  }, RUN_DURATION_SEC * 1000)
}
