/* PolymarketFarm frontend */

const API = '';
let botRunning = false;

// ── Bootstrap ────────────────────────────────────────────────────────────────

// init defined at bottom of file

async function poll() {
  await Promise.all([fetchStatus(), fetchPositions(), fetchMarketsAuto()]);
}

async function fetchMarketsAuto() {
  const data = await get('/api/markets');
  if (data && data.length > 0) renderMarkets(data);
  document.getElementById('markets-count').textContent = (data || []).length;
}

// ── Status ───────────────────────────────────────────────────────────────────

async function fetchStatus() {
  const data = await get('/api/status');
  if (!data) return;

  botRunning = data.running;
  const balEl = document.getElementById('balance');
  if (!data.api_reachable && data.balance === 0) {
    balEl.textContent = '⚠ нет связи';
    balEl.style.color = 'var(--yellow)';
  } else {
    balEl.textContent = '$' + fmt2(data.balance);
    balEl.style.color = '';
  }
  document.getElementById('active-count').textContent = data.active_positions;
  document.getElementById('earned').textContent = '$' + fmt4(data.total_earned);

  const badge = document.getElementById('bot-status');
  const btn   = document.getElementById('btn-toggle');
  if (data.running) {
    badge.textContent = 'РАБОТАЕТ'; badge.className = 'badge badge-on';
    btn.textContent = 'Остановить'; btn.className = 'btn btn-danger';
  } else {
    badge.textContent = 'СТОП'; badge.className = 'badge badge-off';
    btn.textContent = 'Запустить'; btn.className = 'btn btn-primary';
  }

  const errSec = document.getElementById('errors-section');
  const errList = document.getElementById('errors-list');
  if (data.errors && data.errors.length) {
    errSec.style.display = '';
    errList.innerHTML = data.errors.map(e => `<li>${esc(e)}</li>`).join('');
  } else {
    errSec.style.display = 'none';
  }
}

// ── Markets ──────────────────────────────────────────────────────────────────

async function refreshMarkets() {
  await post('/api/markets/refresh');
  const data = await get('/api/markets');
  renderMarkets(data || []);
}

function renderMarkets(markets) {
  document.getElementById('markets-count').textContent = markets.length;
  const tbody = document.getElementById('markets-body');
  if (!markets.length) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty">Нет рынков — нажмите «Обновить»</td></tr>';
    return;
  }
  tbody.innerHTML = markets.map(m => {
    const endDate = m.end_date ? new Date(m.end_date).toLocaleDateString('ru') : '—';
    const trades = m.trade_count ?? '—';
    const tradesClass = trades <= 2 ? 'depth-dead' : trades <= 7 ? 'depth-thin' : 'depth-active';
    // reward_per_dollar × 100 = cents earned per $1 invested per day
    const rpd = m.reward_per_dollar != null ? (m.reward_per_dollar * 100).toFixed(3) : '—';
    const rpdClass = m.reward_per_dollar >= 0.1 ? 'depth-dead' : m.reward_per_dollar >= 0.01 ? 'depth-thin' : 'depth-active';
    // YES price in cents
    const yesPrice = m.mid_price != null ? (m.mid_price * 100).toFixed(2) + '¢' : '—';
    const yesPriceClass = m.mid_price <= 0.05 ? 'depth-dead' : m.mid_price <= 0.15 ? 'depth-thin' : 'depth-active';
    const zoneLiq = m.top4_liquidity_usd != null ? '$' + fmt2(m.top4_liquidity_usd) : '—';
    const zoneClass = m.top4_liquidity_usd <= 50 ? 'depth-dead' : m.top4_liquidity_usd <= 300 ? 'depth-thin' : 'depth-active';
    const minCost = m.min_order_cost != null ? '$' + fmt2(m.min_order_cost) : '—';
    const minCostClass = m.min_order_cost <= 1 ? 'depth-dead' : m.min_order_cost <= 5 ? 'depth-thin' : 'depth-active';
    return `<tr>
      <td class="${rpdClass}"><span class="score-val">${rpd}¢</span></td>
      <td class="q-cell" title="${esc(m.question)}">${esc(m.question)}</td>
      <td>$${fmt2(m.total_daily_rate)}</td>
      <td class="${yesPriceClass}" title="Цена YES-токена — вероятность события">${yesPrice}</td>
      <td class="${minCostClass}" title="Минимум USDC для входа (зелёный = доступно)">${minCost}</td>
      <td class="${zoneClass}" title="USDC в топ-4 зоне (меньше = меньше конкурентов)">${zoneLiq}</td>
      <td class="${tradesClass}" title="смен цены за 30д (0 = мёртвый)">${trades}</td>
      <td>${endDate}</td>
      <td><button class="btn btn-ghost btn-sm" onclick="enterMarket('${m.condition_id}')">Войти</button></td>
    </tr>`;
  }).join('');
}

async function enterMarket(conditionId) {
  await post('/api/bot/tick');
  await fetchPositions();
}

// ── Positions ─────────────────────────────────────────────────────────────────

async function fetchPositions() {
  const data = await get('/api/positions');
  renderPositions(data || []);
}

function renderPositions(positions) {
  const open = positions.filter(p => ['OPEN','WARNING','FILLED'].includes(p.status));
  document.getElementById('pos-count').textContent = open.length;
  document.getElementById('active-count').textContent = open.filter(p => p.status === 'OPEN').length;

  const tbody = document.getElementById('positions-body');
  if (!positions.length) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty">Нет позиций</td></tr>';
    return;
  }

  tbody.innerHTML = positions.map(p => {
    const chipClass = {
      OPEN: 'chip-open', FILLED: 'chip-filled', WARNING: 'chip-warning',
      CANCELLED: 'chip-cancelled', MOVED: 'chip-moved',
    }[p.status] || 'chip-open';

    const placed = p.placed_at ? new Date(p.placed_at).toLocaleString('ru') : '—';
    const canCancel = ['OPEN','WARNING'].includes(p.status);

    return `<tr>
      <td class="q-cell" title="${esc(p.market_question)}">${esc(p.market_question)}</td>
      <td>${esc(p.side)}</td>
      <td>${(p.price * 100).toFixed(2)}¢</td>
      <td>${fmt0(p.size)}</td>
      <td><span class="chip ${chipClass}">${esc(p.status)}</span></td>
      <td style="font-size:12px;color:var(--text-muted)">${placed}</td>
      <td>${canCancel ? `<button class="btn btn-danger btn-sm" onclick="cancelPosition('${p.order_id}')">Отмена</button>` : '—'}</td>
    </tr>`;
  }).join('');
}

async function cancelPosition(orderId) {
  if (!confirm('Отменить ордер?')) return;
  await del(`/api/positions/${orderId}`);
  await fetchPositions();
}

async function cancelAll() {
  if (!confirm('Отменить ВСЕ открытые ордера?')) return;
  await post('/api/positions/cancel_all');
  await fetchPositions();
}

// ── Bot control ───────────────────────────────────────────────────────────────

async function toggleBot() {
  if (botRunning) {
    await post('/api/bot/stop');
  } else {
    await post('/api/bot/start');
  }
  await fetchStatus();
}

async function manualTick() {
  await post('/api/bot/tick');
  await poll();
}

// ── Settings ──────────────────────────────────────────────────────────────────

async function loadSettings() {
  const data = await get('/api/settings');
  if (!data) return;
  if (data.depth)                        document.getElementById('s-depth').value = data.depth;
  if (data.slot_pct != null)             document.getElementById('s-slot-pct').value = data.slot_pct;
  if (data.max_slots_per_market != null) document.getElementById('s-max-slots-per-market').value = data.max_slots_per_market;
  if (data.min_daily_reward != null)     document.getElementById('s-min-reward').value = data.min_daily_reward;
  if (data.scan_interval_s != null)      document.getElementById('s-interval').value = data.scan_interval_s;
  if (data.volatility_threshold != null) document.getElementById('s-volatility').value = data.volatility_threshold;
  if (data.min_spread != null)           document.getElementById('s-min-spread').value = data.min_spread;
  if (data.max_ob_spread != null)        document.getElementById('s-max-ob-spread').value = data.max_ob_spread;
  if (data.max_daily_trades != null)     document.getElementById('s-max-daily-trades').value = data.max_daily_trades;
  if (data.monitor_interval_s != null)   document.getElementById('s-monitor-interval').value = data.monitor_interval_s;
  if (data.max_order_usdc != null)       document.getElementById('s-max-order-usdc').value = data.max_order_usdc;
  if (data.max_positions != null)        document.getElementById('s-max-positions').value = data.max_positions;
  if (data.word_blacklist != null)       document.getElementById('s-word-blacklist').value = (data.word_blacklist || []).join(', ');
}

async function saveSettings() {
  const rawWords = document.getElementById('s-word-blacklist').value;
  const wordList = rawWords.split(',').map(w => w.trim().toLowerCase()).filter(w => w.length > 0);
  const body = {
    depth:                document.getElementById('s-depth').value,
    slot_pct:             parseFloat(document.getElementById('s-slot-pct').value),
    max_slots_per_market: parseInt(document.getElementById('s-max-slots-per-market').value),
    min_daily_reward:     parseFloat(document.getElementById('s-min-reward').value),
    scan_interval_s:      parseInt(document.getElementById('s-interval').value),
    volatility_threshold: parseFloat(document.getElementById('s-volatility').value),
    min_spread:           parseFloat(document.getElementById('s-min-spread').value),
    max_ob_spread:        parseFloat(document.getElementById('s-max-ob-spread').value),
    max_daily_trades:     parseInt(document.getElementById('s-max-daily-trades').value),
    monitor_interval_s:   parseInt(document.getElementById('s-monitor-interval').value),
    max_order_usdc:       parseFloat(document.getElementById('s-max-order-usdc').value) || 0,
    max_positions:        parseInt(document.getElementById('s-max-positions').value) || 0,
    word_blacklist:       wordList,
  };
  await fetch(`${API}/api/settings`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const msg = document.getElementById('settings-msg');
  msg.textContent = 'Сохранено ✓';
  setTimeout(() => msg.textContent = '', 2000);
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

async function get(url) {
  try {
    const r = await fetch(API + url);
    if (!r.ok) return null;
    return await r.json();
  } catch { return null; }
}

async function post(url, body) {
  try {
    const r = await fetch(API + url, {
      method: 'POST',
      headers: body ? { 'Content-Type': 'application/json' } : {},
      body: body ? JSON.stringify(body) : undefined,
    });
    return await r.json();
  } catch { return null; }
}

async function del(url) {
  try {
    const r = await fetch(API + url, { method: 'DELETE' });
    return await r.json();
  } catch { return null; }
}

// ── Format helpers ─────────────────────────────────────────────────────────────

function fmt2(v) { return v == null ? '—' : Number(v).toFixed(2); }
function fmt4(v) { return v == null ? '—' : Number(v).toFixed(4); }
function fmt0(v) { return v == null ? '—' : Math.round(v); }
function esc(s)  { if (!s) return ''; return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

// ── Stats ─────────────────────────────────────────────────────────────────────

async function fetchStats() {
  const data = await get('/api/stats');
  if (!data) return;

  document.getElementById('st-balance').textContent = '$' + fmt2(data.current_balance);

  const delta = data.balance_delta_24h;
  const deltaEl = document.getElementById('st-delta-24h');
  if (delta == null) {
    deltaEl.textContent = 'нет данных';
  } else {
    deltaEl.textContent = (delta >= 0 ? '+' : '') + fmt4(delta) + '$';
    deltaEl.style.color = delta >= 0 ? 'var(--green)' : 'var(--red)';
  }

  const estEl = document.getElementById('st-est-daily');
  estEl.textContent = '$' + fmt4(data.est_daily_earnings) + '/день';
  estEl.style.color = data.est_daily_earnings > 0 ? 'var(--green)' : '';

  document.getElementById('st-invested').textContent = '$' + fmt4(data.invested_usdc);
  document.getElementById('st-total').textContent = data.total;
  document.getElementById('st-filled-cancelled').textContent =
    data.filled + ' / ' + data.cancelled;

  if (data.balance_history && data.balance_history.length > 1) {
    drawBalanceChart(data.balance_history);
  }
}

function drawBalanceChart(history) {
  const canvas = document.getElementById('balance-chart');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const W = canvas.offsetWidth || 600;
  const H = 60;
  canvas.width = W;
  canvas.height = H;
  ctx.clearRect(0, 0, W, H);

  const vals = history.map(h => h.balance);
  const min = Math.min(...vals);
  const max = Math.max(...vals);
  const range = max - min || 1;

  ctx.beginPath();
  ctx.strokeStyle = '#6c63ff';
  ctx.lineWidth = 1.5;
  vals.forEach((v, i) => {
    const x = (i / (vals.length - 1)) * W;
    const y = H - ((v - min) / range) * (H - 12) - 4;
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.stroke();

  ctx.fillStyle = '#7c8db0';
  ctx.font = '10px sans-serif';
  ctx.fillText('$' + fmt2(min), 2, H - 2);
  ctx.fillText('$' + fmt2(max), 2, 11);
}

// ── Start ─────────────────────────────────────────────────────────────────────
async function init() {
  await loadSettings();
  await Promise.all([poll(), fetchStats()]);
  setInterval(poll, 5000);
  setInterval(fetchStats, 30000);
}

document.addEventListener('DOMContentLoaded', init);
