// Boot sentinel (used by index.html to detect whether app.js executed)
try{ window.__APPJS_BOOTED = true; }catch(_){ /* ignore */ }

// Global config and state
const STRICT_SCHEDULE_DATES = false;
const AUTO_FALLBACK_TO_LAST_GAME = true; // If a selected date has no games, fall back to the most recent prior slate date
const PIN_DATE = '';
// Score blending: 0.5 means equal weight to model and sim points.
const SCORE_BLEND_ALPHA = 0.50;
const TEAM_ALIASES = {
  'warriors': 'GSW', 'golden state': 'GSW', 'golden state warriors': 'GSW',
  'lakers': 'LAL', 'los angeles lakers': 'LAL',
  'clippers': 'LAC', 'la clippers': 'LAC', 'los angeles clippers': 'LAC',
  'thunder': 'OKC', 'oklahoma city thunder': 'OKC',
  'rockets': 'HOU', 'houston rockets': 'HOU',
};
const state = {
  teams: {},
  byDate: new Map(),
  schedule: [],
  scheduleDates: [],
  predsByKey: new Map(),
  oddsByKey: new Map(),
  reconByKey: new Map(),
  gameCardsByKey: new Map(),
  periodLinesByKey: new Map(),
  periodLinesDate: null,
  simQuartersByKey: new Map(),
  simQuartersDate: null,
  gameStoryByKey: new Map(),
  propsPredsByGameKey: new Map(),
  propsPredsDate: null,
  gamesCalib: null,
  reconProps: [],
  propsEdges: [],
  propsRecs: [],
  propsRecsDate: null,
  propsFilters: { minEdge: 0.05, minEV: 0.0 },
  fbRecsByGid: new Map(),
  poll: {
    timer: null,
    date: null,
    lastPayload: null,
    oddsTimer: null,
    oddsHash: '',
  }
};

function escapeHtml(s){
  const t = String(s ?? '');
  return t
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// Minimal on-page debug line (useful when cards fail to render).
function ensureDebugEl(){
  try{
    let el = document.getElementById('debug');
    if (el) return el;
    el = document.createElement('div');
    el.id = 'debug';
    el.className = 'subtle';
    el.style.margin = '10px 0 0 0';
    const note = document.getElementById('note');
    if (note && note.parentNode){
      note.parentNode.insertBefore(el, note.nextSibling);
    } else {
      document.body.appendChild(el);
    }
    return el;
  }catch(_){ return null; }
}

function setDebugLine(msg){
  try{
    const el = ensureDebugEl();
    if (el) el.textContent = String(msg || '');
  }catch(_){ /* ignore */ }
}

function formatEtTimesInCards(){
  // Back-compat name: formats ISO timestamps into the user's local timezone.
  try{
    const nodes = Array.from(document.querySelectorAll('.card .js-local-time'));
    if (!nodes.length) return;
    const fmt = new Intl.DateTimeFormat('en-US', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: true,
      timeZoneName: 'short'
    });
    const isDateOnly = (s)=> /^\d{4}-\d{2}-\d{2}$/.test(String(s||'').trim());
    nodes.forEach((node)=>{
      const raw = String(node.textContent || '').trim();
      if (!raw) return;
      // Avoid UTC date-shift bugs for date-only strings like "2026-03-01".
      if (isDateOnly(raw)) return;
      const d = parseGameDateTime(raw) || new Date(raw);
      if (!isNaN(d)) node.textContent = fmt.format(d);
    });
  }catch(_){ /* ignore */ }
}

try{
  window.addEventListener('error', (ev)=>{
    try{
      const where = (ev && ev.filename) ? ` @ ${ev.filename}:${ev.lineno||''}` : '';
      setDebugLine(`JS error: ${ev.message || 'unknown'}${where}`);
    }catch(_){ /* ignore */ }
  });
  window.addEventListener('unhandledrejection', (ev)=>{
    try{
      const r = ev && ev.reason;
      const msg = (r && (r.message || r.toString)) ? (r.message || String(r)) : String(r || 'unknown');
      setDebugLine(`Promise rejection: ${msg}`);
    }catch(_){ /* ignore */ }
  });
}catch(_){ /* ignore */ }

function clamp01(x){
  const v = Number(x);
  if (!Number.isFinite(v)) return 0;
  return Math.max(0, Math.min(1, v));
}

function fmtPct(p, digits=1){
  if (p == null) return '—';
  const v = Number(p);
  if (!Number.isFinite(v)) return '—';
  return (v * 100).toFixed(digits) + '%';
}

function tierFromEv(ev){
  const v = Number(ev);
  if (!Number.isFinite(v)) return '—';
  if (v >= 0.08) return 'A';
  if (v >= 0.05) return 'B';
  if (v >= 0.03) return 'C';
  if (v >= 0.015) return 'D';
  return 'E';
}

function calibrateGamesProb(pRaw){
  const p = clamp01(pRaw);
  const c = state.gamesCalib;
  if (!c || !Array.isArray(c.x) || !Array.isArray(c.y) || c.x.length < 2 || c.x.length !== c.y.length) return p;
  const x = c.x;
  const y = c.y;
  if (p <= x[0]) return clamp01(y[0]);
  const last = x.length - 1;
  if (p >= x[last]) return clamp01(y[last]);
  let i = 0;
  while (i < last - 1 && !(x[i] <= p && p <= x[i+1])) i++;
  const x0 = Number(x[i]), x1 = Number(x[i+1]);
  const y0 = Number(y[i]), y1 = Number(y[i+1]);
  if (!Number.isFinite(x0) || !Number.isFinite(x1) || x1 === x0) return p;
  const t = (p - x0) / (x1 - x0);
  return clamp01(y0 + t * (y1 - y0));
}

async function maybeLoadGamesCalibration(){
  try{
    if (state.gamesCalib) return;
    const candidates = [
      '/data/processed/games_prob_calibration_60.json',
      '/data/processed/games_prob_calibration.json',
    ];
    for (const url of candidates){
      try{
        const r = await fetch(url, { cache: 'no-store' });
        if (!r.ok) continue;
        const j = await r.json();
        const x = Array.isArray(j?.x) ? j.x.map(Number) : null;
        const y = Array.isArray(j?.y) ? j.y.map(Number) : null;
        if (!x || !y || x.length < 2 || y.length !== x.length) continue;
        state.gamesCalib = { x, y, meta: j?.meta || null, source: url };
        return;
      }catch(_){ /* ignore */ }
    }
  }catch(_){ /* ignore */ }
}

// Toggle periods breakdown visibility
function togglePeriods(cardId){
  const content = document.getElementById(cardId);
  const toggle = document.querySelector(`[onclick="togglePeriods('${cardId}')"]`);
  if (!content) return;
  const isHidden = content.style.display === 'none';
  content.style.display = isHidden ? 'block' : 'none';
  if (toggle) toggle.textContent = isHidden ? '▼ Projected Line Score' : '▶ Projected Line Score';
}

// Toggle quarters breakdown visibility (Cards v2)
// Uses the same connected-game payload as the Write-up so quarters + box score stay consistent.
async function toggleQuarters(cardId, dateStr, home, away){
  const content = document.getElementById(cardId);
  const toggle = document.querySelector(`[data-q-toggle="${cardId}"]`);
  if (!content) return;

  const isHidden = content.style.display === 'none';
  content.style.display = isHidden ? 'block' : 'none';
  if (toggle) toggle.textContent = isHidden ? '▼ Quarters' : '▶ Quarters';
  if (!isHidden) return;

  if (content.dataset.loaded === '1') return;
  content.innerHTML = '<div class="subtle">Loading connected quarters…</div>';

  try{
    const key = `${dateStr}|${home}|${away}`;
    let payload = state.gameStoryByKey.get(key) || null;
    if (!payload){
      const url = `/api/sim/game-story?date=${encodeURIComponent(dateStr)}&home=${encodeURIComponent(home)}&away=${encodeURIComponent(away)}&n=900&alpha=${encodeURIComponent(String(SCORE_BLEND_ALPHA))}`;
      const r = await fetch(url, { cache: 'no-store' });
      if (!r.ok){
        const txt = await r.text();
        throw new Error(`HTTP ${r.status}: ${txt}`);
      }
      payload = await r.json();
      state.gameStoryByKey.set(key, payload);
    }

    const rep = payload?.sim?.rep || null;
    const means = payload?.sim?.means || null;
    const useMeans = Array.isArray(means?.quarters) && means.quarters.length;
    const q = useMeans ? means.quarters : (Array.isArray(rep?.quarters) ? rep.quarters : []);
    const qBy = (side, i)=>{
      const row = q.find(x => Number(x?.q) === Number(i));
      const v = row ? Number(row?.[side]) : null;
      return Number.isFinite(v) ? v : null;
    };
    const qAway = [1,2,3,4].map(i=>qBy('away', i));
    const qHome = [1,2,3,4].map(i=>qBy('home', i));
    const sum = (arr)=>{
      const xs = (arr||[]).map(Number).filter(Number.isFinite);
      return xs.length ? xs.reduce((a,b)=>a+b,0) : null;
    };
    const awayT = sum(qAway);
    const homeT = sum(qHome);

    // Per-quarter winner probabilities (sim-implied).
    // Backtests on processed reconciliation data show this is more accurate/calibrated
    // than the period-model quarter win probabilities.
    const simPeriods = payload?.periods?.sim || null;
    const simQuarters = Array.isArray(simPeriods?.quarters) ? simPeriods.quarters : null;
    const qWinProbHome = (i)=>{
      if (!simQuarters) return null;
      const row = simQuarters.find(x => Number(x?.q) === Number(i));
      const v = row ? Number(row?.p_home_win) : null;
      if (!Number.isFinite(v)) return null;
      return Math.max(0, Math.min(1, v));
    };
    const fmtPct = (p, digits=0)=>{
      const v = Number(p);
      if (!Number.isFinite(v)) return '—';
      return (v*100).toFixed(digits) + '%';
    };
    const qFavText = (i)=>{
      const p = qWinProbHome(i);
      if (p == null) return '—';
      const homeFav = p >= 0.5;
      const fav = homeFav ? String(home) : String(away);
      const pf = homeFav ? p : (1 - p);
      return `${escapeHtml(fav)} ${fmtPct(pf,0)}`;
    };

    // Optional per-quarter market lines
    const pl = state.periodLinesByKey ? (state.periodLinesByKey.get(key) || null) : null;
    const marketQ = (i)=>{
      const tot = pl ? toNum(pl[`q${i}_total`]) : null;
      const spr = pl ? toNum(pl[`q${i}_spread`]) : null;
      if (tot==null && spr==null) return `Q${i} —`;
      const bits = [];
      if (tot!=null) bits.push(`Tot ${fmtNum(tot,1)}`);
      if (spr!=null) bits.push(`${home} ${fmtSigned(spr,2)}`);
      return `Q${i} ${bits.join(' · ')}`;
    };

    const dQ = useMeans ? 1 : 0;
    const dT = useMeans ? 1 : 0;
    const label = useMeans ? 'Connected mean line (over sims)' : 'Connected scenario line (rep)';

    // Actual line score (quarters) from ESPN via backend, if available.
    let actualHtml = '';
    try{
      const u = new URL('/api/line-score', window.location.origin);
      u.searchParams.set('date', dateStr);
      u.searchParams.set('home', home);
      u.searchParams.set('away', away);
      const ac = new AbortController();
      const t = setTimeout(()=> ac.abort(), 6000);
      try{
        const rr = await fetch(u.toString(), { cache: 'no-store', signal: ac.signal });
        if (rr.ok){
          const jj = await rr.json();
          const periods = Array.isArray(jj?.periods) ? jj.periods : [];
          const byP = (side, i)=>{
            const row = periods.find(x => Number(x?.period) === Number(i));
            const v = row ? Number(row?.[side]) : null;
            return Number.isFinite(v) ? v : null;
          };
          const aAway = [1,2,3,4].map(i=>byP('away', i));
          const aHome = [1,2,3,4].map(i=>byP('home', i));
          const aAwayT = sum(aAway);
          const aHomeT = sum(aHome);

          // Error vs connected (actual - projected)
          const errAway = aAway.map((v,i)=> (v!=null && qAway[i]!=null) ? (v - qAway[i]) : null);
          const errHome = aHome.map((v,i)=> (v!=null && qHome[i]!=null) ? (v - qHome[i]) : null);
          const errAwayT = (aAwayT!=null && awayT!=null) ? (aAwayT - awayT) : null;
          const errHomeT = (aHomeT!=null && homeT!=null) ? (aHomeT - homeT) : null;

          if ((aAwayT!=null || aHomeT!=null) && (aAway.some(x=>x!=null) || aHome.some(x=>x!=null))){
            actualHtml = `
              <div style="height:10px;"></div>
              <div class="table-wrap">
                <table class="data-table boxscore-table">
                  <thead>
                    <tr>
                      <th>Team</th>
                      <th class="num">Q1</th>
                      <th class="num">Q2</th>
                      <th class="num">Q3</th>
                      <th class="num">Q4</th>
                      <th class="num">T</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td style="font-weight:800;">${escapeHtml(away)}</td>
                      <td class="num">${fmtNum(aAway[0],0)}</td>
                      <td class="num">${fmtNum(aAway[1],0)}</td>
                      <td class="num">${fmtNum(aAway[2],0)}</td>
                      <td class="num">${fmtNum(aAway[3],0)}</td>
                      <td class="num" style="font-weight:900;">${aAwayT!=null?fmtNum(aAwayT,0):'—'}</td>
                    </tr>
                    <tr>
                      <td style="font-weight:800;">${escapeHtml(home)}</td>
                      <td class="num">${fmtNum(aHome[0],0)}</td>
                      <td class="num">${fmtNum(aHome[1],0)}</td>
                      <td class="num">${fmtNum(aHome[2],0)}</td>
                      <td class="num">${fmtNum(aHome[3],0)}</td>
                      <td class="num" style="font-weight:900;">${aHomeT!=null?fmtNum(aHomeT,0):'—'}</td>
                    </tr>
                    <tr>
                      <td style="font-weight:800;">Err (A−P)</td>
                      <td class="num">${errAway[0]!=null?fmtSigned(errAway[0],1):'—'}</td>
                      <td class="num">${errAway[1]!=null?fmtSigned(errAway[1],1):'—'}</td>
                      <td class="num">${errAway[2]!=null?fmtSigned(errAway[2],1):'—'}</td>
                      <td class="num">${errAway[3]!=null?fmtSigned(errAway[3],1):'—'}</td>
                      <td class="num" style="font-weight:900;">${errAwayT!=null?fmtSigned(errAwayT,1):'—'}</td>
                    </tr>
                    <tr>
                      <td style="font-weight:800;">Err (H−P)</td>
                      <td class="num">${errHome[0]!=null?fmtSigned(errHome[0],1):'—'}</td>
                      <td class="num">${errHome[1]!=null?fmtSigned(errHome[1],1):'—'}</td>
                      <td class="num">${errHome[2]!=null?fmtSigned(errHome[2],1):'—'}</td>
                      <td class="num">${errHome[3]!=null?fmtSigned(errHome[3],1):'—'}</td>
                      <td class="num" style="font-weight:900;">${errHomeT!=null?fmtSigned(errHomeT,1):'—'}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <div class="subtle boxscore-sub"><div>Actual line score (quarters)</div></div>
            `;
          }
        }
      } finally {
        clearTimeout(t);
      }
    }catch(_){ /* ignore */ }

    content.innerHTML = `
      <div class="table-wrap">
        <table class="data-table boxscore-table">
          <thead>
            <tr>
              <th>Team</th>
              <th class="num">Q1</th>
              <th class="num">Q2</th>
              <th class="num">Q3</th>
              <th class="num">Q4</th>
              <th class="num">T</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td style="font-weight:800;">${escapeHtml(away)}</td>
              <td class="num">${fmtNum(qAway[0],dQ)}</td>
              <td class="num">${fmtNum(qAway[1],dQ)}</td>
              <td class="num">${fmtNum(qAway[2],dQ)}</td>
              <td class="num">${fmtNum(qAway[3],dQ)}</td>
              <td class="num" style="font-weight:900;">${awayT!=null?fmtNum(awayT,dT):'—'}</td>
            </tr>
            <tr>
              <td style="font-weight:800;">${escapeHtml(home)}</td>
              <td class="num">${fmtNum(qHome[0],dQ)}</td>
              <td class="num">${fmtNum(qHome[1],dQ)}</td>
              <td class="num">${fmtNum(qHome[2],dQ)}</td>
              <td class="num">${fmtNum(qHome[3],dQ)}</td>
              <td class="num" style="font-weight:900;">${homeT!=null?fmtNum(homeT,dT):'—'}</td>
            </tr>
            <tr>
              <td style="font-weight:800;">Favored (sim)</td>
              <td class="num" style="font-weight:700;">${qFavText(1)}</td>
              <td class="num" style="font-weight:700;">${qFavText(2)}</td>
              <td class="num" style="font-weight:700;">${qFavText(3)}</td>
              <td class="num" style="font-weight:700;">${qFavText(4)}</td>
              <td class="num">—</td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="subtle boxscore-sub">
        <div>${label}</div>
        ${pl ? `<div>Quarter markets: ${[1,2,3,4].map(marketQ).join(' · ')}</div>` : ''}
      </div>
      ${actualHtml || ''}
    `;
    content.dataset.loaded = '1';
  }catch(e){
    content.innerHTML = `<div class="subtle">Failed to load connected quarters: ${escapeHtml(String(e?.message||e))}</div>`;
  }
}

// Toggle players box score visibility (Cards v2)
function togglePlayers(cardId){
  const content = document.getElementById(cardId);
  const toggle = document.querySelector(`[onclick="togglePlayers('${cardId}')"]`);
  if (!content) return;
  const isHidden = content.style.display === 'none';
  content.style.display = isHidden ? 'block' : 'none';
  if (toggle) toggle.textContent = isHidden ? '▼ Players' : '▶ Players';
}

// Signed numeric formatting used across UI sections.
// Note: Cards v2 renderer defines its own local fmtSigned; this global helper
// is used by async toggles (e.g., quarters) that run outside that scope.
function fmtSigned(n, digits=1){
  const v = Number(n);
  if (!Number.isFinite(v)) return '—';
  const s = v > 0 ? '+' : '';
  return s + v.toFixed(digits);
}

// --- Odds helpers (UI safety) ---
// Some historical processed odds rows can contain invalid American odds when they were
// accidentally averaged in American space (e.g., -30). Guard against that here so the UI
// never displays nonsensical moneylines.
function impliedProbFromAmerican(ml){
  const x = Number(ml);
  if (!Number.isFinite(x) || x === 0) return null;
  // Already a probability
  if (x > 0 && x < 1) return Math.min(1-1e-6, Math.max(1e-6, x));
  if (x > 0) return 100 / (x + 100);
  return (-x) / ((-x) + 100);
}

function americanFromProb(p){
  const x = Number(p);
  if (!Number.isFinite(x) || x <= 0 || x >= 1) return null;
  if (x >= 0.5) return -100 * x / (1 - x);
  return 100 * (1 - x) / x;
}

function normalizeMoneylines(homeMl, awayMl){
  const h = Number(homeMl), a = Number(awayMl);
  const valid = (v)=> Number.isFinite(v) && (v >= 100 || v <= -100);
  if (valid(h) && valid(a)) return { home_ml: h, away_ml: a, normalized: false };

  // If either side is invalid, try to normalize via implied probabilities.
  const ph = impliedProbFromAmerican(h);
  const pa = impliedProbFromAmerican(a);
  if (ph==null || pa==null) return { home_ml: Number.isFinite(h)?h:null, away_ml: Number.isFinite(a)?a:null, normalized: false };
  const s = ph + pa;
  if (!Number.isFinite(s) || s <= 0) return { home_ml: Number.isFinite(h)?h:null, away_ml: Number.isFinite(a)?a:null, normalized: false };
  const phn = ph / s;
  const pan = pa / s;
  return { home_ml: americanFromProb(phn), away_ml: americanFromProb(pan), normalized: true };
}

// Toggle write-up visibility (Cards v2) + lazy-load connected sim
async function toggleWriteup(cardId, dateStr, home, away){
  const content = document.getElementById(cardId);
  const toggle = document.querySelector(`[onclick="toggleWriteup('${cardId}','${dateStr}','${home}','${away}')"]`);
  if (!content) return;

  // Use the card-level status computed during render (final/live/scheduled).
  // This prevents simulated write-up details from being mistaken as real “actuals”.
  const cardNode = (content && content.closest) ? content.closest('.card') : null;
  const cardStatus = cardNode ? String(cardNode.getAttribute('data-status') || '').toLowerCase() : '';
  const isFinalCard = (cardStatus === 'final');

  const isHidden = content.style.display === 'none';
  content.style.display = isHidden ? 'block' : 'none';
  if (toggle) toggle.textContent = isHidden ? '▼ Write-up' : '▶ Write-up';
  if (!isHidden) return;

  const key = `${dateStr}|${home}|${away}`;
  if (content.dataset.loaded === '1') return;

  content.innerHTML = '<div class="subtle">Loading connected sim…</div>';
  try{
    let payload = state.gameStoryByKey.get(key) || null;
    if (!payload){
      // Keep this endpoint fast on low-resource hosts (e.g., Render):
      // - smaller n
      // - default event_level=0 unless explicitly requested elsewhere
      const url = `/api/sim/game-story?date=${encodeURIComponent(dateStr)}&home=${encodeURIComponent(home)}&away=${encodeURIComponent(away)}&n=450&alpha=${encodeURIComponent(String(SCORE_BLEND_ALPHA))}&event_level=0`;
      const controller = new AbortController();
      const to = setTimeout(() => controller.abort(), 20000);
      const r = await fetch(url, { cache: 'no-store', signal: controller.signal });
      clearTimeout(to);
      if (!r.ok){
        const txt = await r.text();
        throw new Error(`HTTP ${r.status}: ${txt}`);
      }
      payload = await r.json();
      state.gameStoryByKey.set(key, payload);
    }

    const sim = payload?.sim || null;
    const rep = sim?.rep || null;
    const means = sim?.means || null;
    const model = payload?.model || null;
    const blend = payload?.blend || null;
    const warns = Array.isArray(sim?.diagnostics?.warnings) ? sim.diagnostics.warnings : [];
    const recap = payload?.recap || '';
    const summary = payload?.summary || null;
    const propsRecs = Array.isArray(payload?.props?.recommendations) ? payload.props.recommendations : [];

    const q = Array.isArray(rep?.quarters) ? rep.quarters : [];
    const qBy = (side, i)=>{
      const row = q.find(x => Number(x?.q) === Number(i));
      return (row && Number.isFinite(Number(row[side]))) ? Number(row[side]) : null;
    };
    const qAway = [1,2,3,4].map(i=>qBy('away', i));
    const qHome = [1,2,3,4].map(i=>qBy('home', i));
    const sum = (arr)=>{
      const xs = (arr||[]).map(Number).filter(Number.isFinite);
      return xs.length ? xs.reduce((a,b)=>a+b,0) : null;
    };

    const useMeanBox = !!(means?.away_box && means?.home_box);
    const awayBox = (useMeanBox ? (means?.away_box || null) : (rep?.away_box || null));
    const homeBox = (useMeanBox ? (means?.home_box || null) : (rep?.home_box || null));
    const injuries = payload?.injuries || null;
    const topN = 8;
    const topRows = (box)=>{
      const rows = Array.isArray(box?.players) ? box.players.slice() : [];
      rows.sort((a,b)=> (Number(b?.min)||0) - (Number(a?.min)||0));
      return rows.slice(0, topN);
    };

    const playerTable = (teamTri, box)=>{
      const rows = topRows(box);
      if (!rows.length) return '';
      const tr = (p)=>`
        <tr>
          <td style="font-weight:700;">${escapeHtml(p.player_name || '')}</td>
          <td class="num">${(p.min!=null && Number.isFinite(Number(p.min))) ? fmtNum(p.min,1) : '—'}</td>
          <td class="num">${fmtNum(p.pts,0)}</td>
          <td class="num">${fmtNum(p.reb,0)}</td>
          <td class="num">${fmtNum(p.ast,0)}</td>
          <td class="num">${fmtNum(p.threes,0)}</td>
        </tr>`;
      return `
        <div class="table-wrap">
          <table class="data-table boxscore-table player-boxscore">
            <thead>
              <tr>
                <th>${teamTri} (${useMeanBox ? 'connected mean' : 'connected rep'})</th>
                <th class="num">MIN</th>
                <th class="num">PTS</th>
                <th class="num">REB</th>
                <th class="num">AST</th>
                <th class="num">3PM</th>
              </tr>
            </thead>
            <tbody>${rows.map(tr).join('')}</tbody>
          </table>
        </div>`;
    };

    const injuryList = (teamTri, rows)=>{
      const xs = Array.isArray(rows) ? rows.slice() : [];
      if (!xs.length) return '';
      const items = xs.slice(0, 16).map(r=>{
        const nm = escapeHtml(r?.player || '');
        const st = escapeHtml(r?.status || '');
        return `<li>${nm}${st ? ` <span class="subtle">(${st})</span>` : ''}</li>`;
      }).join('');
      const more = xs.length > 16 ? `<div class="subtle">+${xs.length-16} more…</div>` : '';
      return `
        <div class="subtle" style="margin-top:8px;">
          <div style="font-weight:700; margin-bottom:4px;">Excluded (injury filter) (${escapeHtml(teamTri)})</div>
          <ul style="margin:0; padding-left:18px;">${items}</ul>
          ${more}
        </div>`;
    };

    const topPlays = ()=>{
      const items = [];
      for (const r of (propsRecs||[])){
        const plays = Array.isArray(r?.plays) ? r.plays : [];
        for (const p of plays){
          if (!p || typeof p !== 'object') continue;
          items.push({
            player: r.player,
            team: r.team,
            market: p.market,
            side: p.side,
            line: p.line,
            price: p.price,
            ev_pct: p.ev_pct,
            book: p.book,
          });
        }
      }
      items.sort((a,b)=> (Number(b.ev_pct)||-1e9) - (Number(a.ev_pct)||-1e9));
      return items.slice(0, 12);
    };
    const plays = topPlays();

    // Props picks table.
    // Important: do NOT “grade” pregame picks against a single simulated scenario.
    // Only show grading when official actuals are available (final games).
    const playsHtml = (()=>{
      try{
        if (!plays.length) return '';

        const normName = (s)=> String(s||'').trim().toLowerCase().replace(/\s+/g,' ');

        const officialByName = new Map();
        try{
          const rows = Array.isArray(state.reconProps) ? state.reconProps : [];
          for (const r of rows){
            const tm = String(r?.team_abbr||'').toUpperCase();
            if (!tm || (tm !== String(away||'').toUpperCase() && tm !== String(home||'').toUpperCase())) continue;
            const nm = normName(r?.player_name);
            if (!nm) continue;
            officialByName.set(`${tm}|${nm}`, r);
          }
        }catch(_){ /* ignore */ }

        const getOfficialActual = (p)=>{
          const tm = String(p.team||'').toUpperCase();
          const nm = normName(p.player);
          const row = officialByName.get(`${tm}|${nm}`) || null;
          if (!row) return null;

          const mkt = String(p.market||'').toLowerCase().trim();
          const pts = toNum(row.pts);
          const reb = toNum(row.reb);
          const ast = toNum(row.ast);
          const threes = toNum(row.threes);

          const m = mkt.replaceAll(' ', '').replaceAll('-', '').replaceAll('_','');

          if (m === 'pts' || m === 'points') return pts;
          if (m === 'reb' || m === 'rebs' || m === 'rebounds' || m === 'trb') return reb;
          if (m === 'ast' || m === 'asts' || m === 'assists') return ast;
          if (m === 'threes' || m === '3pm' || m === '3ptm' || m === '3pt' || m === '3p') return threes;

          if (m === 'pra'){
            if (pts==null || reb==null || ast==null) return null;
            return pts + reb + ast;
          }
          if (m === 'ra' || m === 'rebast' || m === 'reboundsassists'){
            if (reb==null || ast==null) return null;
            return reb + ast;
          }
          if (m === 'pr' || m === 'ptsreb' || m === 'pointsrebounds'){
            if (pts==null || reb==null) return null;
            return pts + reb;
          }
          if (m === 'pa' || m === 'ptsast' || m === 'pointsassists'){
            if (pts==null || ast==null) return null;
            return pts + ast;
          }
          return null;
        };

        const grade = (side, line, actual)=>{
          const s = String(side||'').toUpperCase().trim();
          const ln = toNum(line);
          const act = toNum(actual);
          if (ln==null || act==null) return null;
          if (act === ln) return 'P';
          if (s === 'OVER') return (act > ln) ? 'W' : 'L';
          if (s === 'UNDER') return (act < ln) ? 'W' : 'L';
          return null;
        };

        const subtitle = isFinalCard
          ? 'Top props picks (graded vs official actuals)'
          : 'Top props picks (no grading until final)';

        const headerCols = isFinalCard
          ? `<th class="num">Actual</th><th class="num">Result</th>`
          : ``;

        const rowsHtml = plays.map(p=>{
          const pick = `${p.market} ${p.side} ${p.line}`;
          const ev = (p.ev_pct!=null && Number.isFinite(Number(p.ev_pct))) ? `${fmtNum(p.ev_pct,1)}%` : '—';
          const book = p.book ? String(p.book).toUpperCase() : '';
          const simVal = getSimValue(p);
          const simRes = grade(p.side, p.line, simVal);
          const simTxt = (simVal!=null) ? fmtNum(simVal,0) : '—';
          const simResTxt = simRes || '—';

          const actVal = isFinalCard ? getOfficialActual(p) : null;
          const actRes = isFinalCard ? grade(p.side, p.line, actVal) : null;
          const actTxt = (actVal!=null) ? fmtNum(actVal,0) : '—';
          const actResTxt = actRes || '—';

          const midCols = isFinalCard
            ? `<td class="num">${escapeHtml(simTxt)}</td><td class="num">${escapeHtml(simResTxt)}</td><td class="num">${escapeHtml(actTxt)}</td><td class="num">${escapeHtml(actResTxt)}</td>`
            : `<td class="num">${escapeHtml(simTxt)}</td><td class="num">${escapeHtml(simResTxt)}</td>`;
          return `
            <tr>
              <td style="font-weight:700;">${escapeHtml(p.player)} <span class="subtle">(${escapeHtml(p.team)})</span></td>
              <td class="num">${escapeHtml(pick)}</td>
              ${midCols}
              <td class="num">${escapeHtml(ev)}</td>
              <td class="num">${escapeHtml(book)}</td>
            </tr>`;
        }).join('');

        return `
          <div class="mt-24"></div>
          <div class="subtle mb-6">${escapeHtml(subtitle)}</div>
          <div class="table-wrap">
            <table class="data-table boxscore-table">
              <thead>
                <tr>
                  <th>Player</th>
                  <th class="num">Pick</th>
                  ${headerCols}
                  <th class="num">EV%</th>
                  <th class="num">Book</th>
                </tr>
              </thead>
              <tbody>
                ${rowsHtml}
              </tbody>
            </table>
          </div>
        `;
      }catch(_){
        return '';
      }
    })();

    const repLine = (!isFinalCard && rep && Number.isFinite(Number(rep.home_score)) && Number.isFinite(Number(rep.away_score)))
      ? `<div class="subtle">Scenario score (rep): ${escapeHtml(away)} ${fmtNum(rep.away_score,0)} – ${escapeHtml(home)} ${fmtNum(rep.home_score,0)}</div>`
      : '';

    const meanLine = (means && Number.isFinite(Number(means.home_score)) && Number.isFinite(Number(means.away_score)))
      ? `<div class="subtle">Mean score (over sims): ${escapeHtml(away)} ${fmtNum(means.away_score,1)} – ${escapeHtml(home)} ${fmtNum(means.home_score,1)}</div>`
      : '';

    const modelLine = (!isFinalCard && model && Number.isFinite(Number(model.home_score)) && Number.isFinite(Number(model.away_score)))
      ? `<div class="subtle">Model score (quarters): ${escapeHtml(away)} ${fmtNum(model.away_score,1)} – ${escapeHtml(home)} ${fmtNum(model.home_score,1)}</div>`
      : '';

    const blendLine = (!isFinalCard && blend && Number.isFinite(Number(blend.home_score)) && Number.isFinite(Number(blend.away_score)))
      ? `<div class="subtle">Target score (blend): ${escapeHtml(away)} ${fmtNum(blend.away_score,1)} – ${escapeHtml(home)} ${fmtNum(blend.home_score,1)} (α=${Number(blend.alpha ?? SCORE_BLEND_ALPHA).toFixed(2)})</div>`
      : '';

    const warnLine = warns.length
      ? `<div class="subtle">Sanity: ${escapeHtml(warns.slice(0,3).join(' | '))}${warns.length>3?' …':''}</div>`
      : '';

    // Prefer an aggregate recap driven by mean scores + probabilities.
    const aggRecap = (()=>{
      try{
        const bits = [];
        const mTot = toNum(payload?.market?.total);
        const mSpr = toNum(payload?.market?.home_spread);

        if (means && Number.isFinite(Number(means.home_score)) && Number.isFinite(Number(means.away_score))){
          const hm = Number(means.home_score);
          const am = Number(means.away_score);
          bits.push(`Aggregate sims: mean ${away} ${fmtNum(am,1)} – ${home} ${fmtNum(hm,1)} (T ${fmtNum(am+hm,1)} · M ${fmtSigned(hm-am,1)})`);
        }

        const probs = summary?.probs || null;
        const pHome = toNum(probs?.home_win ?? probs?.p_home_win ?? probs?.home ?? probs?.p_home);
        const pOver = toNum(probs?.over ?? probs?.p_over ?? probs?.p_total_over);
        if (pHome!=null) bits.push(`${home} win ${fmtPct(pHome,1)}`);
        if (pOver!=null && mTot!=null) bits.push(`Over ${Number(mTot).toFixed(1)} ${fmtPct(pOver,1)}`);
        if (mSpr!=null && pHome!=null) bits.push(`Spread ${home} ${fmtSigned(mSpr,1)}`);

        return bits.filter(Boolean).join(' · ');
      }catch(_){ return ''; }
    })();

    const actualBlock = await (async ()=>{
      try{
        if (!isFinalCard) return '';
        const u = new URL('/api/line-score', window.location.origin);
        u.searchParams.set('date', dateStr);
        u.searchParams.set('home', home);
        u.searchParams.set('away', away);
        const ac = new AbortController();
        const t = setTimeout(()=> ac.abort(), 8000);
        try{
          const rr = await fetch(u.toString(), { cache: 'no-store', signal: ac.signal });
          if (!rr.ok) return '';
          const jj = await rr.json();
          const hp = toNum(jj?.home_pts);
          const ap = toNum(jj?.away_pts);
          const periods = Array.isArray(jj?.periods) ? jj.periods : [];
          const byP = (side, i)=>{
            const row = periods.find(x => Number(x?.period) === Number(i));
            const v = row ? Number(row?.[side]) : null;
            return Number.isFinite(v) ? v : null;
          };
          const qA = [1,2,3,4].map(i=>byP('away', i));
          const qH = [1,2,3,4].map(i=>byP('home', i));

          const meanQ = Array.isArray(means?.quarters) ? means.quarters : [];
          const meanByQ = (side, i)=>{
            const row = meanQ.find(x => Number(x?.q) === Number(i));
            const v = row ? Number(row?.[side]) : null;
            return Number.isFinite(v) ? v : null;
          };
          const qMA = [1,2,3,4].map(i=>meanByQ('away', i));
          const qMH = [1,2,3,4].map(i=>meanByQ('home', i));

          const aSum = (arr)=>{
            const xs = (arr||[]).map(Number).filter(Number.isFinite);
            return xs.length ? xs.reduce((a,b)=>a+b,0) : null;
          };
          const qAT = aSum(qA);
          const qHT = aSum(qH);
          const qMAT = aSum(qMA);
          const qMHT = aSum(qMH);
          const useHp = (hp!=null) ? hp : qHT;
          const useAp = (ap!=null) ? ap : qAT;
          const hasQuarters = qA.some(x=>x!=null) || qH.some(x=>x!=null);
          const hasFinal = (useHp!=null && useAp!=null);
          if (!hasQuarters && !hasFinal) return '';

          const meanHp = toNum(means?.home_score);
          const meanAp = toNum(means?.away_score);
          const useMeanHp = (meanHp!=null) ? meanHp : qMHT;
          const useMeanAp = (meanAp!=null) ? meanAp : qMAT;
          const hasMeanFinal = (useMeanHp!=null && useMeanAp!=null);

          const finalLine = hasFinal
            ? `<div class="subtle">Actual final: ${escapeHtml(away)} ${fmtNum(useAp,0)} – ${escapeHtml(home)} ${fmtNum(useHp,0)}</div>`
            : '';

          const finalDeltaLine = (hasFinal && hasMeanFinal)
            ? (()=>{
              const dAway = useAp - useMeanAp;
              const dHome = useHp - useMeanHp;
              const dTot = (useAp + useHp) - (useMeanAp + useMeanHp);
              const dMar = (useHp - useAp) - (useMeanHp - useMeanAp);
              return `<div class="subtle">Actual − mean (final): ${escapeHtml(away)} ${fmtSigned(dAway,1)} · ${escapeHtml(home)} ${fmtSigned(dHome,1)} · T ${fmtSigned(dTot,1)} · M ${fmtSigned(dMar,1)}</div>`;
            })()
            : '';

          const dqA = qA.map((v,i)=> (v!=null && qMA[i]!=null) ? (v - qMA[i]) : null);
          const dqH = qH.map((v,i)=> (v!=null && qMH[i]!=null) ? (v - qMH[i]) : null);
          const dqAT = (qAT!=null && qMAT!=null) ? (qAT - qMAT) : null;
          const dqHT = (qHT!=null && qMHT!=null) ? (qHT - qMHT) : null;
          const hasMeanQuarters = qMA.some(x=>x!=null) || qMH.some(x=>x!=null);

          const quartersTable = hasQuarters ? `
            <div class="table-wrap" style="margin-top:8px;">
              <table class="data-table boxscore-table">
                <thead>
                  <tr>
                    <th>Team</th>
                    <th class="num">Q1</th>
                    <th class="num">Q2</th>
                    <th class="num">Q3</th>
                    <th class="num">Q4</th>
                    <th class="num">T</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td style="font-weight:800;">${escapeHtml(away)}</td>
                    <td class="num">${fmtNum(qA[0],0)}</td>
                    <td class="num">${fmtNum(qA[1],0)}</td>
                    <td class="num">${fmtNum(qA[2],0)}</td>
                    <td class="num">${fmtNum(qA[3],0)}</td>
                    <td class="num" style="font-weight:900;">${qAT!=null?fmtNum(qAT,0):'—'}</td>
                  </tr>
                  <tr>
                    <td style="font-weight:800;">${escapeHtml(home)}</td>
                    <td class="num">${fmtNum(qH[0],0)}</td>
                    <td class="num">${fmtNum(qH[1],0)}</td>
                    <td class="num">${fmtNum(qH[2],0)}</td>
                    <td class="num">${fmtNum(qH[3],0)}</td>
                    <td class="num" style="font-weight:900;">${qHT!=null?fmtNum(qHT,0):'—'}</td>
                  </tr>
                  ${hasMeanQuarters ? `
                  <tr>
                    <td style="font-weight:800;">Δ (A−Mean)</td>
                    <td class="num">${dqA[0]!=null?fmtSigned(dqA[0],1):'—'}</td>
                    <td class="num">${dqA[1]!=null?fmtSigned(dqA[1],1):'—'}</td>
                    <td class="num">${dqA[2]!=null?fmtSigned(dqA[2],1):'—'}</td>
                    <td class="num">${dqA[3]!=null?fmtSigned(dqA[3],1):'—'}</td>
                    <td class="num" style="font-weight:900;">${dqAT!=null?fmtSigned(dqAT,1):'—'}</td>
                  </tr>
                  <tr>
                    <td style="font-weight:800;">Δ (H−Mean)</td>
                    <td class="num">${dqH[0]!=null?fmtSigned(dqH[0],1):'—'}</td>
                    <td class="num">${dqH[1]!=null?fmtSigned(dqH[1],1):'—'}</td>
                    <td class="num">${dqH[2]!=null?fmtSigned(dqH[2],1):'—'}</td>
                    <td class="num">${dqH[3]!=null?fmtSigned(dqH[3],1):'—'}</td>
                    <td class="num" style="font-weight:900;">${dqHT!=null?fmtSigned(dqHT,1):'—'}</td>
                  </tr>
                  ` : ''}
                </tbody>
              </table>
            </div>
          ` : '';

          return `
            <div class="details-block" style="margin-top:10px;">
              <div class="subtle">Reconciliation (actual)</div>
              ${finalLine}
              ${finalDeltaLine}
              ${quartersTable}
            </div>
          `;
        } finally {
          clearTimeout(t);
        }
      }catch(_){
        return '';
      }
    })();

    content.innerHTML = `
      <div class="writeup-recap">${escapeHtml(aggRecap || recap || '').replace(/\n/g,'<br>')}</div>
      ${actualBlock}
      ${warnLine}
      ${repLine}
      ${blendLine}
      ${modelLine}
      ${meanLine}
      <div class="mt-24"></div>
      ${playerTable(away, awayBox)}
      ${injuryList(away, injuries?.away)}
      <div class="mb-6"></div>
      ${playerTable(home, homeBox)}
      ${injuryList(home, injuries?.home)}
      ${playsHtml}
    `;
    content.dataset.loaded = '1';
  }catch(e){
    content.innerHTML = `<div class="subtle">Write-up failed: ${escapeHtml(e && e.message ? e.message : e)}</div>`;
  }
}

function pointsFromTotalMargin(total, margin){
  const t = toNum(total);
  const m = toNum(margin);
  if (t == null || m == null) return { home: null, away: null };
  return { home: 0.5 * (t + m), away: 0.5 * (t - m) };
}

function boolFromCell(v){
  const s = String(v ?? '').trim().toLowerCase();
  if (!s) return null;
  if (['true','1','yes','y','home'].includes(s)) return true;
  if (['false','0','no','n','away'].includes(s)) return false;
  return null;
}

// --- Persistence helpers (odds) ---
function persistOdds(dateStr, map){
  try{
    const obj = { ts: Date.now(), items: Array.from(map.entries()) };
    localStorage.setItem(`odds:${dateStr}`, JSON.stringify(obj));
  }catch(_){/* ignore */}
}
function restoreOdds(dateStr, maxAgeMs=6*60*60*1000){
  try{
    const raw = localStorage.getItem(`odds:${dateStr}`);
    if (!raw) return new Map();
    const obj = JSON.parse(raw);
    if (!obj || !obj.items || (obj.ts && (Date.now() - obj.ts) > maxAgeMs)) return new Map();
    return new Map(obj.items);
  }catch(_){ return new Map(); }
}

function getQueryParam(name){
  try{
    const url = new URL(window.location.href);
    return url.searchParams.get(name);
  }catch(_){ return null; }
}

// ---- Timezone parsing helpers ----
// Some backend artifacts include `datetime_est` like "2026-03-03T19:00:00" without an offset.
// Browsers interpret that as *local* time, which makes Central users see Eastern times.
// Treat offset-less datetimes as America/New_York and convert to a real Date.
function _tzOffsetMinutesAt(utcMs, timeZone){
  try{
    const d = new Date(Number(utcMs));
    if (isNaN(d)) return 0;
    const parts = new Intl.DateTimeFormat('en-US', {
      timeZone: String(timeZone||'UTC'),
      hour12: false,
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit'
    }).formatToParts(d);
    const get = (t)=>{
      const p = parts.find(x=>x && x.type===t);
      return p ? Number(p.value) : NaN;
    };
    const y=get('year'), mo=get('month'), da=get('day'), h=get('hour'), mi=get('minute'), se=get('second');
    if (![y,mo,da,h,mi,se].every(Number.isFinite)) return 0;
    const asUtc = Date.UTC(y, mo-1, da, h, mi, se);
    return Math.round((asUtc - Number(utcMs)) / 60000);
  }catch(_){ return 0; }
}

function parseGameDateTime(raw){
  try{
    const s0 = String(raw||'').trim();
    if (!s0) return null;
    // Date-only (YYYY-MM-DD) is treated specially by callers.
    if (/^\d{4}-\d{2}-\d{2}$/.test(s0)) return null;

    // Normalize common variants.
    const s = (s0.includes(' ') && !s0.includes('T')) ? s0.replace(' ', 'T') : s0;

    // If string already has timezone info, trust JS parsing.
    if (/Z$/.test(s) || /[+-]\d{2}:?\d{2}$/.test(s)){
      const d = new Date(s);
      return isNaN(d) ? null : d;
    }

    // If it's a naive ISO datetime, interpret it as America/New_York.
    const m = s.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/);
    if (m){
      const Y = Number(m[1]), M = Number(m[2]), D = Number(m[3]);
      const hh = Number(m[4]), mm = Number(m[5]), ss = Number(m[6]||'0');
      if (![Y,M,D,hh,mm,ss].every(Number.isFinite)) return null;

      // Iteratively solve for UTC given local NY time (handles DST changes).
      const localAsUtc = Date.UTC(Y, M-1, D, hh, mm, ss);
      let utc = localAsUtc;
      for (let i=0;i<2;i++){
        const off = _tzOffsetMinutesAt(utc, 'America/New_York');
        utc = localAsUtc - off*60000;
      }
      const d = new Date(utc);
      return isNaN(d) ? null : d;
    }

    // Last resort.
    const d = new Date(s);
    return isNaN(d) ? null : d;
  }catch(_){ return null; }
}

// Format helpers in the user's local timezone.
function fmtLocalTime(iso){
  try{
    const s = String(iso || '').trim();
    if (!s) return '';
    // If we only have a date, don't let Date("YYYY-MM-DD") shift days via UTC.
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return '';
    const d = parseGameDateTime(s) || new Date(s);
    if (isNaN(d)) return '';
    return new Intl.DateTimeFormat('en-US', {
      hour: '2-digit',
      minute: '2-digit',
      hour12: true,
      timeZoneName: 'short'
    }).format(d);
  }catch(_){ return ''; }
}

function fmtLocalDate(iso){
  try{
    const s = String(iso || '').trim();
    if (!s) return '';
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
    const d = parseGameDateTime(s) || new Date(s);
    if (isNaN(d)) return '';
    // en-CA yields YYYY-MM-DD reliably.
    return new Intl.DateTimeFormat('en-CA', { year:'numeric', month:'2-digit', day:'2-digit' }).format(d);
  }catch(_){ return ''; }
}

// Return the NBA "slate date" as YYYY-MM-DD (US/Eastern) to match backend artifacts.
function localYMD(d){
  try{
    const tz = 'America/New_York';
    const cutoffHour = 6; // Treat 12:00am–5:59am ET as the prior NBA slate day.
    const now = d instanceof Date ? d : new Date();
    const hourStr = new Intl.DateTimeFormat('en-US', { timeZone: tz, hour:'2-digit', hour12:false }).format(now);
    const hour = Number(hourStr);
    const base = (Number.isFinite(hour) && hour < cutoffHour)
      ? new Date(now.getTime() - 24*60*60*1000)
      : now;
    // Compute calendar date in US/Eastern
    return new Intl.DateTimeFormat('en-CA', { timeZone: tz, year:'numeric', month:'2-digit', day:'2-digit' }).format(base);
  }catch(_){
    try{
      const cutoffHour = 6;
      const dt = d instanceof Date ? d : new Date();
      const base = (dt.getHours() < cutoffHour) ? new Date(dt.getTime() - 24*60*60*1000) : dt;
      const y = base.getFullYear();
      const m = String(base.getMonth()+1).padStart(2,'0');
      const day = String(base.getDate()).padStart(2,'0');
      return `${y}-${m}-${day}`;
    }catch(__){
      return '';
    }
  }
}

// Convert an ISO/date into ET calendar YYYY-MM-DD
function etYMD(isoOrDate){
  try{
    const d = (isoOrDate instanceof Date) ? isoOrDate : new Date(isoOrDate);
    if (isNaN(d)) return '';
    return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year:'numeric', month:'2-digit', day:'2-digit' }).format(d);
  }catch(_){
    try{
      const dt = (isoOrDate instanceof Date) ? isoOrDate : new Date(isoOrDate);
      if (isNaN(dt)) return '';
      const y = dt.getFullYear();
      const m = String(dt.getMonth()+1).padStart(2,'0');
      const day = String(dt.getDate()).padStart(2,'0');
      return `${y}-${m}-${day}`;
    }catch(__){
      return '';
    }
  }
}

// Parse YYYY-MM-DD into a Date at local midnight to avoid ISO UTC interpretation
function parseYMDLocal(s){
  try{
    const [y,m,d] = String(s||'').split('-').map(Number);
    if (!y || !m || !d) return new Date('invalid');
    return new Date(y, m-1, d, 0, 0, 0, 0);
  }catch(_){ return new Date('invalid'); }
}

function teamLogoUrl(tri){
  const t = String(tri||'').toUpperCase();
  // This returns a local path, but we'll try CDN first in teamLineHTML.
  return `/web/assets/logos/${t}.svg`;
}

function teamLineHTML(tri){
  const t = String(tri||'').toUpperCase();
  const team = state.teams[t] || { tricode:t, name:t };
  const localSvg = teamLogoUrl(t);
  // Build a prioritized list of sources preferring CDN links
  const fallbacks = (function(){
    const id = (state.teams[t] && state.teams[t].id) ? String(state.teams[t].id) : null;
    const urls = [];
    if (id){
      urls.push(
        `https://cdn.nba.com/logos/nba/${id}/primary/L/logo.svg`,
        `https://cdn.nba.com/logos/nba/${id}/primary/L/logo.png`,
        `https://cdn.nba.com/logos/nba/${id}/global/L/logo.svg`,
        `https://cdn.nba.com/logos/nba/${id}/global/L/logo.png`,
      );
    }
    // Lastly, try local assets if present
    urls.push(localSvg);
    return urls;
  })();
  return `
    <div class="team-line-inner">
      <img src="${fallbacks[0] || localSvg}" alt="${t}" class="logo" data-tri="${t}" data-cdn="${fallbacks.join('|')}" onerror="handleLogoError(this)"/>
      <span class="name">${team.name || t}</span>
    </div>`;
}

// Generate a simple inline SVG badge fallback (used if all logo sources fail)
function svgBadgeDataUrl(tri){
  try{
    const T = String(tri||'').toUpperCase();
    const bg = '#1E293B'; // slate-800
    const fg = '#FFFFFF';
    const svg = `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="48" height="48" viewBox="0 0 48 48">
  <rect x="0" y="0" width="48" height="48" rx="8" ry="8" fill="${bg}" />
  <text x="24" y="29" text-anchor="middle" font-family="Inter, system-ui, -apple-system, 'Segoe UI', Roboto, Arial, sans-serif" font-size="16" font-weight="700" fill="${fg}">${T}</text>
  <title>${T}</title>
  <desc>Fallback badge for ${T}</desc>
  <style>text{dominant-baseline:middle;}</style>
</svg>`;
    return 'data:image/svg+xml;utf8,' + encodeURIComponent(svg);
  }catch(_){
    return 'data:image/svg+xml;utf8,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="48" height="48"><rect width="48" height="48" fill="#1E293B"/></svg>');
  }
}

async function loadTeams(){
  try{
    const res = await fetch('/web/assets/teams_nba.json');
    if (!res.ok) throw new Error('teams fetch failed');
    const arr = await res.json();
    const map = {};
    for (const t of arr){ map[String(t.tricode||'').toUpperCase()] = t; }
    state.teams = map;
  }catch(_){ state.teams = {}; }
}

async function maybeLoadOdds(dateStr){
  // Preserve prior odds for this date in case latest snapshot has fewer rows (books drop lines).
  // Also restore persisted odds across page reloads for continuity.
  const prevSession = new Map(state.oddsByKey);
  const prevPersisted = restoreOdds(dateStr);
  const prev = new Map([...prevPersisted, ...prevSession]);
  const next = new Map();
  // Load from multiple sources and merge per-game. Earlier sources take precedence,
  // so we prefer closing/consensus (OddsAPI) over Bovada.
  const sources = [
    { path: `../data/processed/closing_lines_${dateStr}.csv`, label: 'closing' },
    { path: `../data/processed/odds_${dateStr}.csv`, label: 'consensus' },
    { path: `../data/processed/market_${dateStr}.csv`, label: 'market' },
    { path: `../data/processed/game_odds_${dateStr}.csv`, label: 'bovada' },
  ];
  const files = [];
  for (const s of sources){
    try{ const r = await fetch(s.path); if (r.ok){ const t = await r.text(); if (t && t.trim().length>0) files.push({ label: s.label, text: t }); } }catch(_){/* ignore */}
  }
  if (!files.length) return;
  function mergeOdds(base, add){
    // Merge by filling missing values only so earlier sources in `sources` list take precedence.
    // This prioritizes closing/consensus (OddsAPI) over Bovada, aligning with desired behavior.
    if (!base) return add;
    const out = { ...base };
    const isNum = v => v!=='' && v!=null && !Number.isNaN(Number(v));
    const isSpread = v => isNum(v) && Math.abs(Number(v))<=50;
    const isTotal = v => isNum(v) && Number(v)>=100 && Number(v)<=330;
    const pickIf = (k, pred) => {
      const cur = out[k]; const nxt = add[k];
      const curOk = pred(cur); const nxtOk = pred(nxt);
      if (!curOk && nxtOk) out[k] = nxt;
    };
    if (!out.bookmaker && add.bookmaker) out.bookmaker = add.bookmaker;
    pickIf('home_ml', isNum); pickIf('away_ml', isNum);
    pickIf('home_spread', isSpread); pickIf('away_spread', isSpread);
    pickIf('total', isTotal);
    pickIf('home_spread_price', isNum); pickIf('away_spread_price', isNum);
    pickIf('total_over_price', isNum); pickIf('total_under_price', isNum);
    if (!out.commence_time && add.commence_time) out.commence_time = add.commence_time;
    return out;
  }
  for (const f of files){
    const rows = parseCSV(f.text);
    if (!rows || rows.length<2) continue;
    const headers = rows[0];
    const idx = Object.fromEntries(headers.map((h,i)=>[h,i]));
    const pick = (names)=>{ for (const n of names){ if (idx[n]!==undefined) return n; } return null; };
    const dateCol = pick(['date','game_date','asof_date']);
    const hCol = pick(['home_team','home_name','home','home_tricode']);
    const aCol = pick(['visitor_team','away_team','away_name','away','away_tricode']);
    const hsCol = pick(['home_spread','close_home_spread','spread_home','spread_h','home_line']);
    const asCol = pick(['away_spread','close_away_spread','spread_away','spread_a']);
    const spCol = pick(['spread','close_spread']);
    const totCol = pick(['total','close_total','ou_total','ou_close']);
    const hmlCol = pick(['home_ml','close_home_ml','ml_home']);
    const amlCol = pick(['away_ml','close_away_ml','ml_away']);
    const bookCol = pick(['bookmaker','source','consensus_source']);
    const hSprPriceCol = pick([
      'home_spread_price','spread_home_price','home_spread_odds','home_spread_ml',
      'home_handicap_price','home_handicap_odds'
    ]);
    const aSprPriceCol = pick([
      'away_spread_price','spread_away_price','away_spread_odds','away_spread_ml',
      'away_handicap_price','away_handicap_odds'
    ]);
    const totOverPriceCol = pick([
      'total_over_price','ou_over_price','total_over_ml','close_total_over_ml',
      'over_price','over_odds','ou_over_odds','over_ml'
    ]);
    const totUnderPriceCol = pick([
      'total_under_price','ou_under_price','total_under_ml','close_total_under_ml',
      'under_price','under_odds','ou_under_odds','under_ml'
    ]);
    const commenceCol = pick(['commence_time','start_time','game_time']);
    for (let i=1;i<rows.length;i++){
      const r = rows[i];
      const d = dateCol ? String(r[idx[dateCol]]||'').slice(0,10) : dateStr;
      let h = hCol ? r[idx[hCol]] : null; let a = aCol ? r[idx[aCol]] : null;
      if (!h || !a) continue;
      const home = tricodeFromName(h);
      const away = tricodeFromName(a);
      const key = `${d}|${home}|${away}`;
      let home_spread = null, away_spread = null;
      if (hsCol && asCol){
        home_spread = Number(r[idx[hsCol]]);
        away_spread = Number(r[idx[asCol]]);
      } else if (spCol){
        const s = Number(r[idx[spCol]]);
        home_spread = s;
        away_spread = (Number.isFinite(s) ? -s : null);
      }
      const mlH = hmlCol ? Number(r[idx[hmlCol]]) : null;
      const mlA = amlCol ? Number(r[idx[amlCol]]) : null;
      const totV = totCol ? Number(r[idx[totCol]]) : null;
      const hsPrice = hSprPriceCol ? Number(r[idx[hSprPriceCol]]) : null;
      const asPrice = aSprPriceCol ? Number(r[idx[aSprPriceCol]]) : null;
      const toPrice = totOverPriceCol ? Number(r[idx[totOverPriceCol]]) : null;
      const tuPrice = totUnderPriceCol ? Number(r[idx[totUnderPriceCol]]) : null;
      let totClean = (totV===0 ? null : totV);
      if (Number.isFinite(totClean) && totClean < 100) totClean = null;
      let hsClean = Number.isFinite(home_spread) ? Number(home_spread) : null;
      if (hsClean!=null && Math.abs(hsClean) > 60) hsClean = null;
      let asClean = Number.isFinite(away_spread) ? Number(away_spread) : (hsClean!=null? -hsClean : null);
      const rec = {
        home_ml: (mlH===0 ? null : mlH),
        away_ml: (mlA===0 ? null : mlA),
        home_spread: hsClean,
        away_spread: asClean,
        total: totClean,
        bookmaker: bookCol ? r[idx[bookCol]] : f.label,
        home_spread_price: (hsPrice===0 ? null : hsPrice),
        away_spread_price: (asPrice===0 ? null : asPrice),
        total_over_price: (toPrice===0 ? null : toPrice),
        total_under_price: (tuPrice===0 ? null : tuPrice),
        commence_time: commenceCol ? r[idx[commenceCol]] : null,
        source: f.label,
      };
      const prevRec = next.get(key) || prev.get(key);
      next.set(key, mergeOdds(prevRec, rec));
    }
  }
  // Fill forward: keep previous entries for this date that weren’t present in this snapshot
  const prefix = `${dateStr}|`;
  for (const [k,v] of prev.entries()){
    if (k.startsWith(prefix) && !next.has(k)){
      next.set(k, v);
    }
  }
  state.oddsByKey = next;
  // Persist for continuity across reloads
  persistOdds(dateStr, next);
}

async function loadSchedule() {
  // Try dynamic API (auto-builds if missing), then fallback to static file
  let sched = [];
  try {
  const r = await fetch('/api/schedule');
    if (r.ok) {
      sched = await r.json();
    }
  } catch(e) { /* ignore */ }
  if (!Array.isArray(sched) || sched.length === 0) {
    try{
      const res = await fetch('/data/processed/schedule_2025_26.json');
      if (res.ok) {
        sched = await res.json();
      }
    }catch(_){ /* ignore */ }
  }
  // Filter out non-NBA exhibition teams that won't have logos/mappings
  const teamsLoaded = !!(state.teams && Object.keys(state.teams).length >= 20);
  const isKnown = (tri)=> !!state.teams[String(tri||'').toUpperCase()];
  const filtered = Array.isArray(sched)
    ? (teamsLoaded ? sched.filter(g => isKnown(g.home_tricode) && isKnown(g.away_tricode)) : sched)
    : [];
  state.schedule = filtered;
  const m = new Map();
  const schedDateSet = new Set();
  for (const g of filtered) {
    // Group by US/Eastern calendar day
    let dtKey = '';
    if (g.datetime_utc) {
      // Prefer ET conversion from the UTC datetime; if that fails (Intl timezone), fall through.
      dtKey = etYMD(g.datetime_utc) || '';
    }
    if (!dtKey && g.date_est) {
      dtKey = String(g.date_est).slice(0,10);
    }
    if (!dtKey && g.datetime_est) {
      dtKey = String(g.datetime_est).slice(0,10);
    }
    if (!dtKey && g.date_utc) {
      // Interpret date-only as UTC midnight and convert to ET day; if conversion fails, slice.
      dtKey = etYMD(`${g.date_utc}T00:00:00Z`) || String(g.date_utc).slice(0,10);
    }
    if (!dtKey) continue;
    schedDateSet.add(dtKey);
    if (!m.has(dtKey)) m.set(dtKey, []);
    m.get(dtKey).push(g);
  }
  state.byDate = m;
  state.scheduleDates = Array.from(schedDateSet).sort();
}

// If the pinned date isn't present in the schedule, synthesize a slate from data/processed/predictions_{date}.csv
async function maybeInjectPinnedDate(dateStr){
  try{
    if (!dateStr) return;
    if (state.byDate.has(dateStr)) return; // already present
    const path = `/data/processed/predictions_${dateStr}.csv?v=${Date.now()}`;
    const res = await fetch(path);
    if (!res.ok) return; // no predictions to seed from
    const text = await res.text();
    const rows = parseCSV(text);
    if (!rows || rows.length < 2) return;
    const headers = rows[0];
    const idx = Object.fromEntries(headers.map((h,i)=>[h,i]));
    const seen = new Set();
    const list = [];
    for (let i=1;i<rows.length;i++){
      const r = rows[i];
      const home = r[idx.home_team];
      const away = r[idx.visitor_team];
      if (!home || !away) continue;
      const hTri = tricodeFromName(home);
      const aTri = tricodeFromName(away);
      const key = `${hTri}|${aTri}`;
      if (seen.has(key)) continue;
      seen.add(key);
      list.push({
        date_utc: dateStr,
        datetime_utc: `${dateStr}T00:00:00Z`,
        away_tricode: aTri,
        home_tricode: hTri,
        arena_name: '',
        broadcasters_national: '',
      });
    }
    if (list.length){
      state.byDate.set(dateStr, list);
      // Keep the date selector in sync
      if (Array.isArray(state.scheduleDates) && !state.scheduleDates.includes(dateStr)) {
        state.scheduleDates.push(dateStr);
        state.scheduleDates.sort();
      }
    }
  }catch(e){ /* ignore */ }
}

// (removed broken duplicate maybeLoadOdds and inline recon parsing)

async function maybeLoadPropsEdges(dateStr){
  state.propsEdges = [];
  const path = `/data/processed/props_edges_${dateStr}.csv`;
  try {
    const res = await fetch(path);
    if (!res.ok) return;
    const text = await res.text();
    const rows = parseCSV(text);
    const headers = rows[0];
    const idx = Object.fromEntries(headers.map((h,i)=>[h,i]));
    const items = [];
    for (let i=1;i<rows.length;i++){
      const r = rows[i];
      const rec = Object.fromEntries(headers.map((h,j)=>[h, r[j]]));
      // Normalize types
      rec.edge = Number(rec.edge);
      rec.ev = Number(rec.ev);
      items.push(rec);
    }
    state.propsEdges = items;
  } catch(e){ /* ignore */ }
}

// Props recommendations engine (used on Cards page).
// Uses backend parsing of props_recommendations_<date>.csv to avoid client-side CSV+AST parsing.
async function maybeLoadPropsRecommendations(dateStr){
  try{
    state.propsRecs = [];
    state.propsRecsDate = null;
    const url = new URL('/recommendations', window.location.origin);
    url.searchParams.set('format', 'json');
    url.searchParams.set('view', 'all');
    url.searchParams.set('date', dateStr);
    url.searchParams.set('categories', 'props');
    url.searchParams.set('compact', '1');
    url.searchParams.set('regular_only', '1');
    const res = await fetch(url.toString(), { cache: 'no-store' });
    if (!res.ok) return;
    const j = await res.json();
    const rows = Array.isArray(j?.props) ? j.props : [];
    state.propsRecs = rows;
    const used = j?.meta?.data_dates?.props;
    if (used) state.propsRecsDate = String(used);
  }catch(_){ /* ignore */ }
}

// (removed older single-source maybeLoadOdds)

async function maybeLoadPredictions(dateStr){
  state.predsByKey.clear();
  const path = `/data/processed/predictions_${dateStr}.csv?v=${Date.now()}`;
  try{
    const res = await fetch(path);
    if (!res.ok) return;
    const text = await res.text();
    const rows = parseCSV(text);
    if (!rows || rows.length < 2) return;
    const headers = rows[0];
    const idx = Object.fromEntries(headers.map((h,i)=>[h,i]));
    const has = (name)=> idx[name]!==undefined;
    const firstExisting = (names)=> names.find(n=>has(n)) || null;
    const getFirstVal = (r, names)=>{
      for (const n of names){
        if (!has(n)) continue;
        const v = r[idx[n]];
        if (v!==undefined && v!==null && String(v).trim()!=='') return toNum(v);
      }
      return null;
    };
    const dateCol = firstExisting(['date']);
    const hCol = firstExisting(['home_team','home']);
    const aCol = firstExisting(['visitor_team','away']);
    // Prefer model columns over market snapshots; fall back to market only if model is missing per-row
    const totPrefs = ['pred_total','totals','model_total','total'];
    const marPrefs = ['pred_margin','spread_margin','model_margin','margin'];
    const wpPrefs  = ['home_win_prob','home_win_prob_raw','home_win_prob_model','home_wp','p_home_win'];
    for (let i=1;i<rows.length;i++){
      const r = rows[i];
      // Force-key predictions to the selected slate date.
      // Some generators write UTC calendar dates causing 8pm+ ET games to appear on the next day.
      // Since this file is date-scoped by name, use dateStr for consistent matching with schedule/odds.
      const date = dateStr;
      const home = r[idx[hCol]]; const away = r[idx[aCol]];
      if (!home || !away) continue;
      const key = `${date}|${tricodeFromName(home)}|${tricodeFromName(away)}`;
      const obj = Object.fromEntries(headers.map((h,j)=>[h, r[j]]));
      // Normalize numeric fields using toNum (treat blanks/NaN as null)
      for (const k of ['pred_total','pred_margin','home_win_prob','edge_total','edge_spread']){
        if (obj[k]!==undefined) obj[k] = toNum(obj[k]);
      }
      // Per-row fallbacks: choose the first non-empty value among preferred columns
      if (obj.pred_total==null || obj.pred_total===undefined){
        const v = getFirstVal(r, totPrefs);
        if (v!=null) obj.pred_total = v;
      }
      if (obj.pred_margin==null || obj.pred_margin===undefined){
        const v = getFirstVal(r, marPrefs);
        if (v!=null) obj.pred_margin = v;
      }
      if (obj.home_win_prob==null || obj.home_win_prob===undefined){
        const v = getFirstVal(r, wpPrefs);
        if (v!=null) obj.home_win_prob = v;
      }
      state.predsByKey.set(key, obj);
    }
  }catch(_){ /* ignore */ }
}

// Load compact per-game cards merged from PBP markets and odds
async function maybeLoadGameCards(dateStr){
  try{
    state.gameCardsByKey.clear();
    const path = `/data/processed/game_cards_${dateStr}.csv?v=${Date.now()}`;
    const res = await fetch(path);
    if (!res.ok) return;
    const text = await res.text();
    if (!text || !text.trim()) return;
    const rows = parseCSV(text);
    if (!rows || rows.length < 2) return;
    const headers = rows[0];
    const idx = Object.fromEntries(headers.map((h,i)=>[h,i]));
    const pick = (names)=>{ for (const n of names){ if (idx[n]!==undefined) return n; } return null; };
    const hCol = pick(['home_team','home','home_name','home_tricode']);
    const aCol = pick(['visitor_team','away','away_name','away_tricode']);
    for (let i=1;i<rows.length;i++){
    // Allow alternate prediction column names
    const totCol = pick(['pred_total','totals','total_pred','model_total']);
    const marCol = pick(['pred_margin','spread_margin','margin_pred','model_margin']);
    const wpCol = pick(['home_win_prob','home_win_prob_raw','home_win_prob_model']);
      const r = rows[i];
      const h = hCol ? r[idx[hCol]] : null; const a = aCol ? r[idx[aCol]] : null;
      if (!h || !a) continue;
      const home = tricodeFromName(h);
      const away = tricodeFromName(a);
      const key = `${dateStr}|${home}|${away}`;
      const obj = Object.fromEntries(headers.map((h,j)=>[h, r[j]]));
      // Normalize core fields with fallbacks
      const tVal = totCol ? Number(r[idx[totCol]]) : null;
      const mVal = marCol ? Number(r[idx[marCol]]) : null;
      const wVal = wpCol ? Number(r[idx[wpCol]]) : null;
      if (tVal!=null && Number.isFinite(tVal)) obj.pred_total = tVal;
      if (mVal!=null && Number.isFinite(mVal)) obj.pred_margin = mVal;
      if (wVal!=null && Number.isFinite(wVal)) obj.home_win_prob = wVal;
      for (const k of ['edge_total','edge_spread']){
        if (obj[k]!==undefined) obj[k] = Number(obj[k]);
      }
      state.gameCardsByKey.set(key, obj);
    }
  }catch(_){ /* ignore */ }
}

// Load quarter/half market lines if available (optional)
// Tries date-specific file first, then falls back to a synthetic sample file.
async function maybeLoadPeriodLines(dateStr){
  try{
    if (state.periodLinesDate === dateStr && state.periodLinesByKey && state.periodLinesByKey.size) return;
    state.periodLinesByKey = new Map();
    state.periodLinesDate = dateStr;

    const candidates = [
      `/data/processed/period_lines_${dateStr}.csv?v=${Date.now()}`,
      `/data/processed/period_lines_synthetic.csv?v=${Date.now()}`,
    ];

    let text = null;
    for (const path of candidates){
      try{
        const res = await fetch(path);
        if (!res.ok) continue;
        const t = await res.text();
        if (t && t.trim()){ text = t; break; }
      }catch(_){ /* ignore */ }
    }
    if (!text) return;

    const rows = parseCSV(text);
    if (!rows || rows.length < 2) return;
    const headers = rows[0];
    const idx = Object.fromEntries(headers.map((h,i)=>[h,i]));
    const pick = (names)=>{ for (const n of names){ if (idx[n]!==undefined) return n; } return null; };

    const dateCol = pick(['date']);
    const hCol = pick(['home_team','home']);
    const aCol = pick(['visitor_team','away']);
    if (!dateCol || !hCol || !aCol) return;

    for (let i=1;i<rows.length;i++){
      const r = rows[i];
      const d = String(r[idx[dateCol]]||'').slice(0,10);
      if (!d) continue;
      // If the file contains multiple dates, keep only the selected date.
      if (d !== dateStr) continue;
      const h = r[idx[hCol]]; const a = r[idx[aCol]];
      if (!h || !a) continue;
      const home = tricodeFromName(h);
      const away = tricodeFromName(a);
      const key = `${dateStr}|${home}|${away}`;
      const obj = Object.fromEntries(headers.map((hh,j)=>[hh, r[j]]));
      for (const k of ['h1_total','h2_total','q1_total','q2_total','q3_total','q4_total','h1_spread','h2_spread','q1_spread','q2_spread','q3_spread','q4_spread']){
        if (obj[k] !== undefined) obj[k] = toNum(obj[k]);
      }
      state.periodLinesByKey.set(key, obj);
    }
  }catch(_){ /* ignore */ }
}

// Load per-game simulated quarter scoring (optional) from backend API.
async function maybeLoadSimQuarters(dateStr){
  try{
    if (state.simQuartersDate === dateStr && state.simQuartersByKey && state.simQuartersByKey.size) return;
    state.simQuartersByKey = new Map();
    state.simQuartersDate = dateStr;
    const url = new URL('/api/sim/quarters', window.location.origin);
    url.searchParams.set('date', dateStr);
    const res = await fetch(url.toString(), { cache: 'no-store' });
    if (!res.ok) return;
    const j = await res.json();
    const rows = Array.isArray(j?.rows) ? j.rows : [];
    for (const row of rows){
      const h = row?.home_team;
      const a = row?.away_team;
      if (!h || !a) continue;
      const home = tricodeFromName(h);
      const away = tricodeFromName(a);
      const key = `${dateStr}|${home}|${away}`;
      state.simQuartersByKey.set(key, row);
    }
  }catch(_){ /* ignore */ }
}

// Load player sim props (means) from processed CSV and group by game.
async function maybeLoadPropsPredictions(dateStr){
  try{
    if (state.propsPredsDate === dateStr && state.propsPredsByGameKey && state.propsPredsByGameKey.size) return;
    state.propsPredsByGameKey = new Map();
    state.propsPredsDate = dateStr;
    const path = `/data/processed/props_predictions_${dateStr}.csv?v=${Date.now()}`;
    const res = await fetch(path);
    if (!res.ok) return;
    const text = await res.text();
    if (!text || !text.trim()) return;
    const rows = parseCSV(text);
    if (!rows || rows.length < 2) return;
    const headers = rows[0];
    const idx = Object.fromEntries(headers.map((h,i)=>[h,i]));
    const pick = (names)=>{ for (const n of names){ if (idx[n]!==undefined) return n; } return null; };

    const playerCol = pick(['player_name','player','name']);
    const teamCol = pick(['team','team_tricode','player_team']);
    const oppCol = pick(['opponent','opp','opponent_tricode']);
    const homeCol = pick(['home','is_home','home_game']);
    if (!playerCol || !teamCol || !oppCol) return;

    const numCols = new Set([
      'pred_pts','pred_reb','pred_ast','pred_pra','pred_threes','pred_stl','pred_blk','pred_tov',
      'pred_min','roll10_min','roll5_min','roll20_min','roll30_min'
    ]);
    const boolCols = new Set(['playing_today','team_on_slate','home']);

    const seen = new Set();
    for (let i=1;i<rows.length;i++){
      const r = rows[i];
      const player = String(r[idx[playerCol]]||'').trim();
      if (!player) continue;
      const teamRaw = r[idx[teamCol]];
      const oppRaw = r[idx[oppCol]];
      if (!teamRaw || !oppRaw) continue;

      const teamTri = tricodeFromName(teamRaw);
      const oppTri = tricodeFromName(oppRaw);
      if (!teamTri || !oppTri) continue;

      const isHome = homeCol ? boolFromCell(r[idx[homeCol]]) : null;
      const homeTri = (isHome === true) ? teamTri : (isHome === false ? oppTri : null);
      const awayTri = (isHome === true) ? oppTri : (isHome === false ? teamTri : null);
      if (!homeTri || !awayTri) continue;

      const dedupeKey = `${dateStr}|${homeTri}|${awayTri}|${teamTri}|${player.toLowerCase()}`;
      if (seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);

      const obj = Object.fromEntries(headers.map((h,j)=>[h, r[j]]));
      obj.player_name = player;
      obj.team = teamTri;
      obj.opponent = oppTri;
      obj.home = (isHome === true);
      for (const k of Object.keys(obj)){
        if (numCols.has(k)) obj[k] = toNum(obj[k]);
        if (boolCols.has(k)) obj[k] = boolFromCell(obj[k]);
      }

      const key = `${dateStr}|${homeTri}|${awayTri}`;
      if (!state.propsPredsByGameKey.has(key)) state.propsPredsByGameKey.set(key, []);
      state.propsPredsByGameKey.get(key).push(obj);
    }
  }catch(_){ /* ignore */ }
}

// Load first-basket recommendations for the date and index by zero-padded game_id (gid10)
async function maybeLoadFirstBasketRecs(dateStr){
  try{
    state.fbRecsByGid = new Map();
    const path = `/data/processed/first_basket_recs_${dateStr}.csv?v=${Date.now()}`;
    const res = await fetch(path);
    if (!res.ok) return;
    const text = await res.text();
    if (!text || !text.trim()) return;
    const rows = parseCSV(text);
    if (!rows || rows.length < 2) return;
    const headers = rows[0];
    const idx = Object.fromEntries(headers.map((h,i)=>[h,i]));

    // Ensure required columns exist
    const required = ['game_id','team','player_name','prob_first_basket','fair_american','rank','cum_prob'];
    for (const c of required){
      if (idx[c] === undefined) return;
    }

    const normGid = (s)=>{
      try{
        const raw = String(s||'').trim().replace(/\.0$/, '');
        const digits = raw.replace(/[^0-9]/g,'');
        if (!digits) return null;
        return digits.padStart(10,'0');
      }catch(_){ return null; }
    };

    const map = new Map();
    for (let i=1;i<rows.length;i++){
      const r = rows[i];
      if (!r || !r.length) continue;
      const gid = normGid(r[idx.game_id]);
      if (!gid) continue;
      const obj = {
        game_id: gid,
        team: String(r[idx.team]||'').trim(),
        player_name: String(r[idx.player_name]||'').trim(),
        prob_first_basket: toNum(r[idx.prob_first_basket]),
        fair_american: toNum(r[idx.fair_american]),
        rank: toNum(r[idx.rank]),
        cum_prob: toNum(r[idx.cum_prob]),
      };
      if (!obj.player_name) continue;
      if (!map.has(gid)) map.set(gid, []);
      map.get(gid).push(obj);
    }

    // Sort each gid's picks by rank then prob desc for stability
    for (const [gid, arr] of map.entries()){
      const sorted = arr.slice().sort((a,b)=>{
        const ra = (a.rank==null? Number.POSITIVE_INFINITY : a.rank);
        const rb = (b.rank==null? Number.POSITIVE_INFINITY : b.rank);
        if (ra !== rb) return ra - rb;
        const pa = (a.prob_first_basket==null? -1 : a.prob_first_basket);
        const pb = (b.prob_first_basket==null? -1 : b.prob_first_basket);
        return pb - pa;
      });
      state.fbRecsByGid.set(gid, sorted);
    }
  }catch(_){ /* ignore */ }
}

async function maybeLoadRecon(dateStr){
  state.reconByKey.clear();
  // Game recon
  const gpath = `/data/processed/recon_games_${dateStr}.csv?v=${Date.now()}`;
  try{
    const res = await fetch(gpath);
    if (res.ok){
      const text = await res.text();
      const rows = parseCSV(text);
      const headers = rows[0];
      const idx = Object.fromEntries(headers.map((h,i)=>[h,i]));
      // Build temp map to allow finals backfill before committing to state
      const temp = new Map();
      for (let i=1;i<rows.length;i++){
        const r = rows[i];
        const date = r[idx.date];
        const home = r[idx.home_team];
        const away = r[idx.visitor_team];
        if (!date||!home||!away) continue;
        const key = `${date}|${tricodeFromName(home)}|${tricodeFromName(away)}`;
        const obj = Object.fromEntries(headers.map((h,j)=>[h, r[j]]));
        for (const k of ['home_pts','visitor_pts','actual_margin','total_actual','margin_error','total_error','pred_total','pred_margin']){
          if (obj[k]!==undefined) obj[k] = toNum(obj[k]);
        }
        temp.set(key, obj);
      }
      // Backfill missing points from finals CSV if available
      try{
        const fpath = `/data/processed/finals_${dateStr}.csv?v=${Date.now()}`;
        const rf = await fetch(fpath);
        if (rf.ok){
          const ftxt = await rf.text();
          const frows = parseCSV(ftxt);
          if (frows && frows.length > 1){
            const fh = frows[0];
            const fidx = Object.fromEntries(fh.map((h,i)=>[h,i]));
            const fmap = new Map();
            for (let i=1;i<frows.length;i++){
              const rr = frows[i];
              const d = rr[fidx.date];
              const htri = String(rr[fidx.home_tri]||'').toUpperCase();
              const atri = String(rr[fidx.away_tri]||'').toUpperCase();
              const hp = toNum(rr[fidx.home_pts]);
              const ap = toNum(rr[fidx.visitor_pts]);
              if (d && htri && atri) fmap.set(`${d}|${htri}|${atri}`, {hp, ap});
            }
            for (const [k,obj] of temp.entries()){
              const d = obj.date || dateStr;
              const htri = (obj.home_tri ? String(obj.home_tri).toUpperCase() : tricodeFromName(obj.home_team));
              const atri = (obj.away_tri ? String(obj.away_tri).toUpperCase() : tricodeFromName(obj.visitor_team));
              if (obj.home_pts==null || obj.visitor_pts==null){
                const fin = fmap.get(`${d}|${htri}|${atri}`);
                if (fin && fin.hp!=null && fin.ap!=null){
                  obj.home_pts = fin.hp; obj.visitor_pts = fin.ap;
                  obj.total_actual = fin.hp + fin.ap;
                  if (obj.pred_margin!=null) obj.margin_error = (fin.hp - fin.ap) - obj.pred_margin;
                  if (obj.pred_total!=null) obj.total_error = obj.total_actual - obj.pred_total;
                }
              }
            }
          }
        }
      }catch(_){ /* ignore finals backfill errors */ }
      // Commit results
      for (const [k,obj] of temp.entries()) state.reconByKey.set(k, obj);
    }
  }catch(_){/* ignore */}
  // Props recon (optional)
  state.reconProps = [];
  const ppath = `/data/processed/recon_props_${dateStr}.csv?v=${Date.now()}`;
  try{
    const res = await fetch(ppath);
    if (res.ok){
      const text = await res.text();
      const rows = parseCSV(text);
      const headers = rows[0];
      const idx = Object.fromEntries(headers.map((h,i)=>[h,i]));
      const items = [];
      for (let i=1;i<rows.length;i++){
        const r = rows[i];
        const rec = Object.fromEntries(headers.map((h,j)=>[h, r[j]]));
        // Normalize actual stat fields (recon_props_*.csv)
        for (const k of ['player_id','pts','reb','ast','threes','pra','pr','ra','pa']){
          if (rec[k]!==undefined) rec[k] = toNum(rec[k]);
        }
        // If this file ever includes prediction error columns, normalize those too.
        for (const k of ['pred_pts_err','pred_reb_err','pred_ast_err','pred_threes_err','pred_pra_err']){
          if (rec[k]!==undefined) rec[k] = toNum(rec[k]);
        }
        if (rec.team_abbr!=null) rec.team_abbr = String(rec.team_abbr||'').toUpperCase();
        if (rec.game_id!=null) rec.game_id = String(rec.game_id||'').trim();
        if (rec.player_name!=null) rec.player_name = String(rec.player_name||'').trim();
        items.push(rec);
      }
      state.reconProps = items;
    }
  }catch(_){/* ignore */}
}

function parseCSV(text) {
  // very basic CSV parser for simple commas without quoted commas
  const lines = text.trim().split(/\r?\n/);
  return lines.map(l => l.split(','));
}

// Parse numeric values safely; return null for blanks/NaN instead of 0
function toNum(v){
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  if (s === '' || s.toLowerCase() === 'nan' || s.toLowerCase() === 'none') return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function fmtNum(x, digits=1){
  if (x === null || x === undefined || x==='') return '';
  const n = Number(x);
  if (!Number.isFinite(n)) return '';
  return Math.abs(n) < 1000 ? n.toFixed(digits) : String(Math.round(n));
}

function fmtOddsAmerican(x){
  if (x === null || x === undefined || x==='') return '';
  const n = Number(x);
  if (!Number.isFinite(n)) return '';
  return n > 0 ? `+${Math.round(n)}` : `${Math.round(n)}`;
}

// Toggle reconciled actual props (boxscore-like) visibility (Cards v2)
// (Removed toggleActualProps: reconciliation is shown in the Write-up section.)

function impliedProbAmerican(odds){
  const o = Number(odds);
  if (!Number.isFinite(o) || o === 0) return null;
  if (o > 0) return 100 / (o + 100);
  return (-o) / ((-o) + 100);
}

function americanToB(odds){
  const o = Number(odds);
  if (!Number.isFinite(o) || o === 0) return null;
  return o > 0 ? (o / 100) : (100 / (-o)); // net decimal (excluding stake)
}

// Approximate standard normal CDF
function normCdf(z){
  // Abramowitz-Stegun approximation
  const t = 1 / (1 + 0.2316419 * Math.abs(z));
  const d = Math.exp(-0.5 * z * z) / Math.sqrt(2 * Math.PI);
  const p = 1 - d * (0.319381530 * t - 0.356563782 * Math.pow(t,2) + 1.781477937 * Math.pow(t,3) - 1.821255978 * Math.pow(t,4) + 1.330274429 * Math.pow(t,5));
  return z >= 0 ? p : 1 - p;
}

function evFromProbAndAmerican(p, odds){
  const b = americanToB(odds);
  if (b == null || p == null) return null;
  return p * b - (1 - p); // expected return per 1 unit staked
}

function evClass(ev){
  if (ev == null) return '';
  // Align to NHL confidence thresholds
  if (ev >= 0.08) return 'High';
  if (ev >= 0.04) return 'Medium';
  return 'Low';
}

function tricodeFromName(name){
  // quick lookup by team name into teams list
  const lower = String(name).toLowerCase();
  if (TEAM_ALIASES[lower]) return TEAM_ALIASES[lower];
  for (const k in state.teams){
    const t = state.teams[k];
    if (t.name.toLowerCase() === lower) return t.tricode;
  }
  // fallback: already a tricode
  return name.toUpperCase();
}

// Expose a global error handler for logo fallbacks
window.handleLogoError = function(img){
  try{
    const cdn = String(img.dataset.cdn || '').split('|').filter(Boolean);
    let i = Number(img.dataset.i || '-1');
    i += 1;
    img.dataset.i = String(i);
    if (i === 0) {
      img.src = img.src.replace('.svg', '.png');
      return;
    }
    if (i > 0 && i <= cdn.length) {
      img.src = cdn[i-1];
      return;
    }
    // final fallback: generate badge
    const tri = String(img.dataset.tri || '').toUpperCase();
    img.onerror = null;
    img.src = svgBadgeDataUrl(tri);
  } catch(e){
    img.onerror = null;
    const tri = String(img.dataset.tri || '').toUpperCase();
    img.src = svgBadgeDataUrl(tri);
  }
}

function recChips(pred){
  if (!pred) return '';
  const chips = [];
  // Win
  if (pred.home_win_prob) {
    const p = Number(pred.home_win_prob);
    const side = p >= 0.5 ? 'HOME ML' : 'AWAY ML';
    const prob = (p >= 0.5 ? p : 1-p);
    const conf = Math.round(prob*100);
    chips.push(`<span class="chip neutral">${side} ${conf}%</span>`);
  }
  // Spread edge if available
  if (pred.edge_spread) {
    const e = Number(pred.edge_spread);
    const label = e >= 0 ? `HOME ATS +${e.toFixed(1)}` : `AWAY ATS ${e.toFixed(1)}`;
    chips.push(`<span class="chip neutral">${label}</span>`);
  }
  // Total edge
  if (pred.edge_total) {
    const e = Number(pred.edge_total);
    const label = e >= 0 ? `OVER +${e.toFixed(1)}` : `UNDER ${e.toFixed(1)}`;
    chips.push(`<span class="chip neutral">${label}</span>`);
  }
  return chips.join(' ');
}

function resultChips(recon){
  if (!recon) return '';
  const chips = [];
  if (Number.isFinite(recon.home_pts) && Number.isFinite(recon.visitor_pts)){
    chips.push(`<span class="badge badge-final">FINAL ${recon.home_pts}-${recon.visitor_pts}</span>`);
  }
  if (Number.isFinite(recon.margin_error)){
    const e = Number(recon.margin_error);
    const cls = Math.abs(e) <= 2 ? 'good' : (Math.abs(e)<=5 ? 'ok' : 'bad');
    chips.push(`<span class="badge ${cls}" title="Model margin - Actual margin">ΔMargin ${e.toFixed(1)}</span>`);
  }
  if (Number.isFinite(recon.total_error)){
    const e = Number(recon.total_error);
    const cls = Math.abs(e) <= 4 ? 'good' : (Math.abs(e)<=8 ? 'ok' : 'bad');
    chips.push(`<span class="badge ${cls}" title="Model total - Actual total">ΔTotal ${e.toFixed(1)}</span>`);
  }
  return chips.join(' ');
}

function renderDateLegacy(dateStr){
  const wrap = document.getElementById('cards');
  if (!wrap) return;
  wrap.innerHTML = '';
  const isToday = (dateStr === localYMD());
  let list = state.byDate.get(dateStr) || [];
  // Sort games by commence/start time: prefer odds.commence_time, else schedule timestamps
  try{
    const keyTime = (g)=>{
      const home = g.home_tricode, away = g.away_tricode;
      const odds = state.oddsByKey.get(`${dateStr}|${home}|${away}`);
      const ct = odds && odds.commence_time ? new Date(odds.commence_time) : null;
      if (ct && !isNaN(ct)) return ct.getTime();
      const iso = g.datetime_utc || g.datetime_est || (g.date_utc?`${g.date_utc}T00:00:00Z`: (g.date_est?`${g.date_est}T00:00:00Z`: null));
      const d = iso ? new Date(iso) : null;
      return (d && !isNaN(d)) ? d.getTime() : Number.MAX_SAFE_INTEGER;
    };
    list = list.slice().sort((a,b)=> keyTime(a) - keyTime(b));
  }catch(_){ /* ignore sort errors */ }
  const showResults = document.getElementById('resultsToggle')?.checked;
  const hideOdds = document.getElementById('hideOdds')?.checked;
  // Build simple filters
  const edges = state.propsEdges || [];
  const byTeam = new Map();
  for (const e of edges){
    const t = String(e.team||'').toUpperCase();
    if (!byTeam.has(t)) byTeam.set(t, []);
    byTeam.get(t).push(e);
  }
  for (const g of list){
  const time = g.datetime_est || g.datetime_utc || g.date_est || g.date_utc;
  // Compute local date/time strings, preferring OddsAPI commence_time (UTC ISO)
  const oddsForGame = state.oddsByKey.get(`${dateStr}|${g.home_tricode}|${g.away_tricode}`);
  const dtIso = (oddsForGame && oddsForGame.commence_time) ? oddsForGame.commence_time : (g.datetime_utc || g.datetime_est || null);
  const localTime = dtIso ? fmtLocalTime(dtIso) : (typeof time === 'string' ? (time.includes('T') ? time.split('T')[1].slice(0,5) : '') : fmtLocalTime(time));
  const localDate = dtIso ? fmtLocalDate(dtIso) : (typeof g.date_est === 'string' ? g.date_est.slice(0,10) : (typeof g.date_utc === 'string' ? g.date_utc.slice(0,10) : ''));
  const locBits = [];
  if (g.arena_name) locBits.push(g.arena_name);
  if (g.arena_city) locBits.push(g.arena_city);
  if (g.arena_state) locBits.push(g.arena_state);
  const venueText = locBits.length ? locBits.join(', ') : (g.home_tricode && state.teams[g.home_tricode]?.name ? state.teams[g.home_tricode].name : 'Home');
  const venueLine = `Venue: ${venueText}${g.broadcasters_national?` • TV: ${g.broadcasters_national}`:''} • ${localDate}`;
    const away = g.away_tricode; const home = g.home_tricode;
    const key = `${dateStr}|${home}|${away}`; // note schedule is away@home
    const pred = state.predsByKey.get(key);
  const recs = recChips(pred);
  // Always load recon to determine final status/score; Results toggle governs extra details only
  const recon = state.reconByKey.get(key);
  const finals = (!isToday && showResults) ? resultChips(recon) : '';
  // Projected / Actual scores
    let projHome=null, projAway=null;
    if (pred && Number.isFinite(Number(pred.pred_total)) && Number.isFinite(Number(pred.pred_margin))){
      const T = Number(pred.pred_total), M = Number(pred.pred_margin);
      if (Number.isFinite(T) && Number.isFinite(M)){
        projHome = (T + M) / 2;
        projAway = (T - M) / 2;
      }
    }
  // Do not show finals/actuals sourced from CSV for today's date; only historical dates get finals from recon
  const actualHome = (!isToday && recon && Number.isFinite(recon.home_pts)) ? Number(recon.home_pts) : null;
  const actualAway = (!isToday && recon && Number.isFinite(recon.visitor_pts)) ? Number(recon.visitor_pts) : null;
  const totalModel = pred && Number.isFinite(Number(pred.pred_total)) ? Number(pred.pred_total) : null;
  const totalActual = (actualHome!=null && actualAway!=null) ? (actualHome + actualAway) : null;
  const diffLine = (totalModel!=null && totalActual!=null) ? `Diff: ${(totalActual - totalModel).toFixed(2)}` : '';
  const projLine = (projHome!=null && projAway!=null) ? `Projected: ${away} ${fmtNum(projAway,1)} — ${home} ${fmtNum(projHome,1)}` : '';
    const actualLine = (actualHome!=null && actualAway!=null) ? `Final: ${away} ${fmtNum(actualAway,0)} — ${home} ${fmtNum(actualHome,0)}` : '';
    // Props edges badges for the teams
    const tA = String(away||'').toUpperCase();
    const tH = String(home||'').toUpperCase();
    const edgesA = (byTeam.get(tA)||[]).slice().sort((a,b)=>b.edge-a.edge).filter(e=>e.edge>=state.propsFilters.minEdge && e.ev>=state.propsFilters.minEV).slice(0,3);
    const edgesH = (byTeam.get(tH)||[]).slice().sort((a,b)=>b.edge-a.edge).filter(e=>e.edge>=state.propsFilters.minEdge && e.ev>=state.propsFilters.minEV).slice(0,3);
    const badge = (e)=>`<span class="badge" title="${e.stat.toUpperCase()} ${e.side} ${e.line} @ ${e.bookmaker}\nEV ${e.ev.toFixed(2)} | Edge ${(e.edge*100).toFixed(1)}%">${e.player_name}: ${e.stat.toUpperCase()} ${e.side} ${e.line} (${(e.edge*100).toFixed(1)}%)</span>`;
    const propsBadges = [...edgesA.map(badge), ...edgesH.map(badge)].join(' ');

  const odds = state.oddsByKey.get(key);
  const hasAnyOdds = !!(odds && (odds.home_ml!=null || odds.away_ml!=null || Number.isFinite(Number(odds.total)) || Number.isFinite(Number(odds.home_spread))));
    // Compute detailed lines similar to NFL cards
  let oddsBlock = '';
    if (odds){
      const hML = odds.home_ml, aML = odds.away_ml;
      const hImp = impliedProbAmerican(hML); const aImp = impliedProbAmerican(aML);
  const mlLine = `Moneyline (Away / Home) ${fmtOddsAmerican(aML)} / ${fmtOddsAmerican(hML)}`;
      const impLine = (hImp!=null && aImp!=null) ? `Implied Win Prob (Away / Home) ${(aImp*100).toFixed(1)}% / ${(hImp*100).toFixed(1)}%` : '';
      const spr = Number(odds.home_spread);
      const tot = Number(odds.total);
      let spreadLine = '';
      let totalLine = '';
      if (Number.isFinite(spr)){
        const M = pred && Number.isFinite(Number(pred.pred_margin)) ? Number(pred.pred_margin) : null;
        if (M!=null){
          const edge = M + spr; // positive favors Home ATS (margin + line)
          const modelTeam = edge >= 0 ? home : away;
          spreadLine = `Spread (Home) ${fmtNum(spr)} • Model: ${modelTeam} (Edge ${edge>=0?'+':''}${edge.toFixed(2)})`;
        } else {
          spreadLine = `Spread (Home) ${fmtNum(spr)}`;
        }
      }
      if (Number.isFinite(tot)){
        const T = pred && Number.isFinite(Number(pred.pred_total)) ? Number(pred.pred_total) : null;
        if (T!=null){
          const edgeT = T - tot;
          const side = edgeT >= 0 ? 'Over' : 'Under';
          totalLine = `Total ${fmtNum(tot)} • Model: ${side} (Edge ${edgeT>=0?'+':''}${edgeT.toFixed(2)})`;
        } else {
          totalLine = `Total ${fmtNum(tot)}`;
        }
      }
      const parts = [
        `Book${odds.bookmaker?` @ ${odds.bookmaker}`:''}`,
        mlLine,
        impLine,
        spreadLine,
        totalLine,
      ].filter(Boolean);
      oddsBlock = parts.map(p=>`<div class=\"subtle\">${p}</div>`).join('');
    }
    // Win prob and predicted winner line
    let wpLine = '';
    if (pred && pred.home_win_prob){
      const pHome = Number(pred.home_win_prob);
      if (Number.isFinite(pHome)){
        const pAway = 1 - pHome;
        const winner = pHome >= 0.5 ? home : away;
        wpLine = `Win Prob: Away ${(pAway*100).toFixed(1)}% / Home ${(pHome*100).toFixed(1)}% • Winner: ${winner}`;
      }
    }

    // Accuracy summary (when results available)
    let accuracyLine = '';
    if (showResults && pred && recon && actualHome!=null && actualAway!=null){
      // Winner
      const pHome = Number(pred.home_win_prob);
      const predWinner = pHome >= 0.5 ? home : away;
      const actualWinner = actualHome > actualAway ? home : (actualAway > actualHome ? away : null);
      const winOk = (actualWinner && predWinner === actualWinner);
      // ATS
      let atsOk = null;
      if (odds && Number.isFinite(Number(odds.home_spread))){
        const spr = Number(odds.home_spread);
        const M = Number(pred.pred_margin);
        if (Number.isFinite(M)){
          const predATS = (M + spr >= 0) ? home : away;
          const actualMargin = actualHome - actualAway;
          const v = actualMargin + spr;
          const actualATS = (v > 0) ? home : (v < 0 ? away : null);
          atsOk = (actualATS && predATS === actualATS);
        }
      }
      // Totals
      let totOk = null;
      if (odds && Number.isFinite(Number(odds.total))){
        const tot = Number(odds.total);
        const T = Number(pred.pred_total);
        if (Number.isFinite(T)){
          const predSide = (T - tot >= 0) ? 'Over' : 'Under';
          const actualTotal = actualHome + actualAway;
          const actualSide = (actualTotal > tot) ? 'Over' : (actualTotal < tot ? 'Under' : null);
          totOk = (actualSide && predSide === actualSide);
        }
      }
      const tick = (v)=> v===null ? '–' : (v ? '✓' : '✗');
      accuracyLine = `Accuracy: Winner ${tick(winOk)} · ATS ${tick(atsOk)} · Total ${tick(totOk)}`;
    }

    // EV summaries (Winner/Spread/Total) and recommendation candidates
    let evWinnerLine = '';
    let evSpreadLine = '';
    let evTotalLine = '';
    const recCands = [];
    try{
      // Winner EV
      if (odds && pred && (odds.home_ml!=null || odds.away_ml!=null) && pred.home_win_prob!=null){
        const pH = Number(pred.home_win_prob);
        const pA = 1 - pH;
        const evH = evFromProbAndAmerican(pH, odds.home_ml);
        const evA = evFromProbAndAmerican(pA, odds.away_ml);
        let side = null, ev=null;
        if (evH!=null || evA!=null){
          if ((evH??-Infinity) >= (evA??-Infinity)) { side = home; ev = evH; } else { side = away; ev = evA; }
          evWinnerLine = `Winner: ${side} (EV ${(ev*100).toFixed(1)}%) • ${evClass(ev)}`;
        }
        if (evH!=null) recCands.push({market:'moneyline', bet:'home_ml', label:'Home ML', ev:evH, odds:odds.home_ml, book:odds.bookmaker});
        if (evA!=null) recCands.push({market:'moneyline', bet:'away_ml', label:'Away ML', ev:evA, odds:odds.away_ml, book:odds.bookmaker});
      }
      // Spread EV (approximate with normal, sigma assumption)
      if (odds && pred && odds.home_spread!=null && pred.pred_margin!=null){
        const sigmaMargin = 12.0; // rough NBA full-game margin sigma
        const spr = Number(odds.home_spread);
        const M = Number(pred.pred_margin);
        // Home covers if (margin + home_spread) > 0  i.e., margin > -home_spread
        const zHome = (0 - (M + spr)) / sigmaMargin;
        const pHomeCover = 1 - normCdf(zHome);
        const pAwayCover = 1 - pHomeCover; // ignoring pushes
        const priceHome = (odds.home_spread_price!=null && odds.home_spread_price!=='') ? Number(odds.home_spread_price) : -110;
        const priceAway = (odds.away_spread_price!=null && odds.away_spread_price!=='') ? Number(odds.away_spread_price) : -110;
        const evH = evFromProbAndAmerican(pHomeCover, priceHome);
        const evA = evFromProbAndAmerican(pAwayCover, priceAway);
        if (evH!=null || evA!=null){
          if ((evH??-Infinity) >= (evA??-Infinity)) { evSpreadLine = `Spread: ${home} (EV ${(evH*100).toFixed(1)}%) • ${evClass(evH)}`; }
          else { evSpreadLine = `Spread: ${away} (EV ${(evA*100).toFixed(1)}%) • ${evClass(evA)}`; }
        }
        if (evH!=null) recCands.push({market:'spread', bet:'home_spread', label:`${home} ATS`, ev:evH, odds:priceHome, book:odds.bookmaker});
        if (evA!=null) recCands.push({market:'spread', bet:'away_spread', label:`${away} ATS`, ev:evA, odds:priceAway, book:odds.bookmaker});
      }
      // Total EV (approximate with normal, sigma assumption)
      if (odds && pred && odds.total!=null && pred.pred_total!=null){
        const sigmaTotal = 20.0; // rough NBA full-game total sigma
        const tot = Number(odds.total);
        const T = Number(pred.pred_total);
        const zOver = (tot - T) / sigmaTotal; // P(Over) = 1 - CDF(z)
        const pOver = 1 - normCdf(zOver);
        const pUnder = 1 - pOver;
        const priceOver = (odds.total_over_price!=null && odds.total_over_price!=='') ? Number(odds.total_over_price) : -110;
        const priceUnder = (odds.total_under_price!=null && odds.total_under_price!=='') ? Number(odds.total_under_price) : -110;
        const evO = evFromProbAndAmerican(pOver, priceOver);
        const evU = evFromProbAndAmerican(pUnder, priceUnder);
        if (evO!=null || evU!=null){
          if ((evO??-Infinity) >= (evU??-Infinity)) { evTotalLine = `Total: Over (EV ${(evO*100).toFixed(1)}%) • ${evClass(evO)}`; }
          else { evTotalLine = `Total: Under (EV ${(evU*100).toFixed(1)}%) • ${evClass(evU)}`; }
        }
         if (evO!=null) recCands.push({market:'totals', bet:'over', label:'Over', ev:evO, odds:priceOver, book:odds.bookmaker});
         if (evU!=null) recCands.push({market:'totals', bet:'under', label:'Under', ev:evU, odds:priceUnder, book:odds.bookmaker});
      }
    }catch(e){ /* ignore EV calc errors */ }
  const tv = g.broadcasters_national || '';
  const venue = venueText;
  // Determine final result class when results are shown
  let w = 0, l = 0, psh = 0;
  let isFinal = (!isToday && actualHome!=null && actualAway!=null);
  if (isFinal) {
      // Moneyline
      if (pred && pred.home_win_prob!=null) {
        const pHome = Number(pred.home_win_prob);
        if (Number.isFinite(pHome)){
          const predWinner = pHome >= 0.5 ? home : away;
          const actWinner = actualHome > actualAway ? home : (actualAway > actualHome ? away : null);
          if (actWinner){ if (predWinner === actWinner) w++; else l++; }
        }
      }
      // Totals
      if (odds && Number.isFinite(Number(odds.total))) {
        const tot = Number(odds.total);
        if (totalActual!=null) {
          const actSide = totalActual > tot ? 'Over' : (totalActual < tot ? 'Under' : 'Push');
          if (pred && Number.isFinite(Number(pred.pred_total))){
            const side = (Number(pred.pred_total) - tot >= 0) ? 'Over' : 'Under';
            if (actSide === 'Push') psh++; else if (actSide === side) w++; else l++;
          }
        }
      }
      // ATS
      if (odds && Number.isFinite(Number(odds.home_spread)) && pred && Number.isFinite(Number(pred.pred_margin))) {
        const spr = Number(odds.home_spread);
        const M = Number(pred.pred_margin);
        const predATS = (M + spr >= 0) ? home : away;
        const actualMargin = actualHome - actualAway;
        const actualATS = (actualMargin + spr > 0) ? home : ((actualMargin + spr < 0) ? away : 'Push');
        if (actualATS === 'Push') psh++; else if (predATS === actualATS) w++; else l++;
      }
    }
  let resultClass = 'final-neutral';
  if (isFinal) {
      if (w > 0 && l === 0) resultClass = 'final-all-win';
      else if (l > 0 && w === 0 && psh === 0) resultClass = 'final-all-loss';
      else if (w > 0 && l > 0) resultClass = 'final-mixed';
      else if (w === 0 && l === 0 && psh > 0) resultClass = 'final-push';
    }
  const node = document.createElement('div');
  node.className = `card ${resultClass}`;
  if (hasAnyOdds) node.setAttribute('data-has-odds','1'); else node.setAttribute('data-has-odds','0');
    // Build detailed card body aligned to NHL example
  // Derive a single, non-duplicative status/time display
  const gst = String(g.game_status_text||'').trim();
  const isLive = /Q\d|OT|Half|End|1st|2nd|3rd|4th|LIVE|In Progress/i.test(gst);
  // Never consider today's games as final in the header status
  isFinal = (!isToday && (isFinal || /FINAL/i.test(gst)));
    const isEtTimeOnly = /\bET\b/i.test(gst) && !isLive && !/FINAL/i.test(gst) && /\d/.test(gst);
    let statusLine = '';
    if (isFinal) {
      statusLine = `FINAL ${fmtNum(actualAway,0)}-${fmtNum(actualHome,0)}`;
    } else if (isLive) {
      statusLine = gst; // live string like Q2 05:32
    } else {
      statusLine = 'Scheduled';
    }
    const awayName = state.teams[away]?.name || away;
    const homeName = state.teams[home]?.name || home;
    // Spread and ATS/Totals result if results shown
    let atsLine = '';
    if (odds && Number.isFinite(Number(odds.home_spread))){
      const spr = Number(odds.home_spread);
      const M = pred && Number.isFinite(Number(pred.pred_margin)) ? Number(pred.pred_margin) : null;
      const modelTeam = M!=null ? (M + spr >= 0 ? homeName : awayName) : null;
      let atsResult = '';
      if (showResults && actualHome!=null && actualAway!=null){
        const actualMargin = actualHome - actualAway;
        const v = actualMargin + spr;
        const coversHome = (v > 0) || (v === 0 ? null : false);
        const atsTeam = coversHome === null ? 'Push' : (coversHome ? homeName : awayName);
        atsResult = ` • ATS: ${atsTeam}`;
      }
      atsLine = `Spread: ${homeName} ${fmtNum(spr)}${modelTeam?` • Model: ${modelTeam} (Edge ${(M + spr>=0?'+':'')}${(M + spr).toFixed(2)})`:''}${atsResult}`;
    }
    let totalDetailLine = '';
    if (odds && Number.isFinite(Number(odds.total))){
      const tot = Number(odds.total);
      const T = totalModel;
      const side = (T!=null ? (T - tot >= 0 ? 'Over' : 'Under') : null);
      let totResult = '';
      if (showResults && totalActual!=null){
        const r = totalActual > tot ? 'Over' : (totalActual < tot ? 'Under' : 'Push');
        totResult = ` • Totals: ${r}`;
      }
  totalDetailLine = `O/U: ${fmtNum(tot)}${side?` • Model: ${side} (Edge ${(T - tot>=0?'+':'')}${(T - tot).toFixed(2)})`:''}${totResult}`;
    }
  // Expose sort-time for possible external sorting/debug
  const sortIso = (odds && odds.commence_time) ? odds.commence_time : (dtIso || `${dateStr}T00:00:00Z`);
  node.setAttribute('data-game-date', dtIso || dateStr);
  node.setAttribute('data-sort-time', sortIso);
  // Never mark today's games as final in card state
  node.setAttribute('data-status', (!isToday && isFinal) ? 'final' : (isLive ? 'live' : 'scheduled'));
    node.setAttribute('data-home-abbr', home);
    node.setAttribute('data-away-abbr', away);
  // Build chips: Totals, Spread, and Moneyline
    let chipsTotals = '';
  let chipsSpread = '';
    let chipsMoney = '';
    if (odds) {
      // Totals chips
      const tot = Number(odds.total);
      const T = pred && Number.isFinite(Number(pred.pred_total)) ? Number(pred.pred_total) : null;
      let pOver = null, pUnder = null;
      if (Number.isFinite(tot) && T!=null){
        const sigmaTotal = 20.0;
        const zOver = (tot - T) / sigmaTotal;
        pOver = 1 - normCdf(zOver);
        pUnder = 1 - pOver;
      }
  // Default to -110 when explicit O/U prices are missing (parity with summary section)
  const priceOver = (odds.total_over_price!=null && odds.total_over_price!=='') ? Number(odds.total_over_price) : -110;
  const priceUnder = (odds.total_under_price!=null && odds.total_under_price!=='') ? Number(odds.total_under_price) : -110;
  const evO = (pOver!=null && priceOver!=null) ? evFromProbAndAmerican(pOver, priceOver) : null;
  const evU = (pUnder!=null && priceUnder!=null) ? evFromProbAndAmerican(pUnder, priceUnder) : null;
    const clsO = (evO==null? 'neutral' : (evO>0? 'positive' : (evO<0? 'negative' : 'neutral')));
    const clsU = (evU==null? 'neutral' : (evU>0? 'positive' : (evU<0? 'negative' : 'neutral')));
      const evOBadge = (evO==null? '' : `<span class="ev-badge ${evO>0?'pos':(evO<0?'neg':'neu')}">EV ${(evO>0?'+':'')}${(evO*100).toFixed(1)}%</span>`);
      const evUBadge = (evU==null? '' : `<span class="ev-badge ${evU>0?'pos':(evU<0?'neg':'neu')}">EV ${(evU>0?'+':'')}${(evU*100).toFixed(1)}%</span>`);
      const book = (odds.bookmaker || '').toString();
      const bookAbbr = book ? (book.toUpperCase().slice(0,2)) : '';
      const bookBadge = bookAbbr ? `<span class=\"book-badge\" title=\"${book}\">${bookAbbr}</span>` : '';
      const overOddsTxt = (priceOver!=null? fmtOddsAmerican(priceOver) : '—');
      const underOddsTxt = (priceUnder!=null? fmtOddsAmerican(priceUnder) : '—');
      const overProbTxt = (pOver!=null? (pOver*100).toFixed(1)+'%' : '—');
      const underProbTxt = (pUnder!=null? (pUnder*100).toFixed(1)+'%' : '—');
      const isModelOver = (Number.isFinite(tot) && T!=null) ? ((T - tot) >= 0) : false;
      const isModelUnder = (Number.isFinite(tot) && T!=null) ? ((T - tot) < 0) : false;
      const modelBadge = `<span class=\"model-badge\" title=\"Model pick\">PICK</span>`;
      // Push probability (approximate)
      const pushProb = (pOver!=null && pUnder!=null) ? Math.max(0, 1 - pOver - pUnder) : null;
      const fairTotTxt = (T!=null ? T.toFixed(1) : '');
      chipsTotals = `
        <div class=\"row chips\">
          <div class=\"chip title\">Totals ${Number.isFinite(tot)? tot.toFixed(1): ''}</div>\
          ${T!=null ? `<div class=\"chip neutral\">Fair ${fairTotTxt}</div>` : ''}\
          <div class=\"chip ${clsO} ${isModelOver?'model-pick':''}\">Over ${overOddsTxt} · ${overProbTxt} ${bookBadge} ${evOBadge} ${isModelOver?modelBadge:''}</div>
          <div class=\"chip ${clsU} ${isModelUnder?'model-pick':''}\">Under ${underOddsTxt} · ${underProbTxt} ${bookBadge} ${evUBadge} ${isModelUnder?modelBadge:''}</div>
          ${pushProb!=null ? `<div class=\"chip neutral\">Push · ${(pushProb*100).toFixed(1)}%</div>` : ''}
        </div>`;
      // Spread chips (NBA)
      if (Number.isFinite(Number(odds.home_spread))) {
        const sprH = Number(odds.home_spread);
        const sprA = -sprH;
        const M = pred && Number.isFinite(Number(pred.pred_margin)) ? Number(pred.pred_margin) : null;
        let pHomeCover = null, pAwayCover = null;
        if (M!=null) {
          const sigmaMargin = 12.0;
          const zHome = (0 - (M + sprH)) / sigmaMargin;
          pHomeCover = 1 - normCdf(zHome);
          pAwayCover = 1 - pHomeCover;
        }
  const priceHome = (odds.home_spread_price!=null && odds.home_spread_price!=='') ? Number(odds.home_spread_price) : -110;
  const priceAway = (odds.away_spread_price!=null && odds.away_spread_price!=='') ? Number(odds.away_spread_price) : -110;
        const evH = (pHomeCover!=null && priceHome!=null) ? evFromProbAndAmerican(pHomeCover, priceHome) : null;
        const evA = (pAwayCover!=null && priceAway!=null) ? evFromProbAndAmerican(pAwayCover, priceAway) : null;
        const clsH = (evH==null? 'neutral' : (evH>0? 'positive' : (evH<0? 'negative' : 'neutral')));
        const clsA = (evA==null? 'neutral' : (evA>0? 'positive' : (evA<0? 'negative' : 'neutral')));
        const evHBadge = (evH==null? '' : `<span class="ev-badge ${evH>0?'pos':(evH<0?'neg':'neu')}">EV ${(evH>0?'+':'')}${(evH*100).toFixed(1)}%</span>`);
        const evABadge = (evA==null? '' : `<span class="ev-badge ${evA>0?'pos':(evA<0?'neg':'neu')}">EV ${(evA>0?'+':'')}${(evA*100).toFixed(1)}%</span>`);
        const aOddsTxt = (priceAway!=null? fmtOddsAmerican(priceAway): '—');
        const hOddsTxt = (priceHome!=null? fmtOddsAmerican(priceHome): '—');
        const aProbTxt = (pAwayCover!=null? (pAwayCover*100).toFixed(1)+'%': '—');
        const hProbTxt = (pHomeCover!=null? (pHomeCover*100).toFixed(1)+'%': '—');
        const modelHome = (M!=null) ? ((M + sprH) >= 0) : false;
        const modelAway = (M!=null) ? (!modelHome) : false;
        const book = (odds.bookmaker || '').toString();
        const bookAbbr = book ? (book.toUpperCase().slice(0,2)) : '';
        const bookBadge = bookAbbr ? `<span class=\"book-badge\" title=\"${book}\">${bookAbbr}</span>` : '';
        const modelBadge = `<span class=\"model-badge\" title=\"Model pick\">PICK</span>`;
        const fairHome = (M!=null ? -M : null);
        const fairHomeTxt = (fairHome!=null ? (fairHome>0?`+${fmtNum(fairHome)}`:fmtNum(fairHome)) : '');
        chipsSpread = `
          <div class=\"row chips\">
            <div class=\"chip title\">Spread</div>
            ${fairHome!=null ? `<div class=\"chip neutral\">Fair Home ${fairHomeTxt}</div>` : ''}\
            <div class=\"chip ${clsA} ${modelAway?'model-pick':''}\">Away ${sprA>0?`+${fmtNum(sprA)}`:fmtNum(sprA)} ${aOddsTxt} · ${aProbTxt} ${bookBadge} ${evABadge} ${modelAway?modelBadge:''}</div>
            <div class=\"chip ${clsH} ${modelHome?'model-pick':''}\">Home ${sprH>0?`+${fmtNum(sprH)}`:fmtNum(sprH)} ${hOddsTxt} · ${hProbTxt} ${bookBadge} ${evHBadge} ${modelHome?modelBadge:''}</div>
          </div>`;
      }

      // Moneyline chips
      const hML = odds.home_ml, aML = odds.away_ml;
      const aImp = impliedProbAmerican(aML); const hImp = impliedProbAmerican(hML);
      const pH = pred && pred.home_win_prob!=null ? Number(pred.home_win_prob) : (hImp!=null? hImp : null);
      const pA = (pH!=null) ? (1 - pH) : (aImp!=null? aImp : null);
      const evH = (pH!=null && hML!=null ? evFromProbAndAmerican(pH, hML) : null);
      const evA = (pA!=null && aML!=null ? evFromProbAndAmerican(pA, aML) : null);
      const clsH = (evH==null? 'neutral' : (evH>0? 'positive' : (evH<0? 'negative' : 'neutral')));
      const clsA = (evA==null? 'neutral' : (evA>0? 'positive' : (evA<0? 'negative' : 'neutral')));
      const aOddsTxt = (aML!=null? fmtOddsAmerican(aML): '—');
      const hOddsTxt = (hML!=null? fmtOddsAmerican(hML): '—');
      const aProbTxt = (pA!=null? (pA*100).toFixed(1)+'%': '—');
      const hProbTxt = (pH!=null? (pH*100).toFixed(1)+'%': '—');
      const evABadge = (evA==null? '' : `<span class=\"ev-badge ${evA>0?'pos':(evA<0?'neg':'neu')}\">EV ${(evA>0?'+':'')}${(evA*100).toFixed(1)}%</span>`);
      const evHBadge = (evH==null? '' : `<span class=\"ev-badge ${evH>0?'pos':(evH<0?'neg':'neu')}\">EV ${(evH>0?'+':'')}${(evH*100).toFixed(1)}%</span>`);
      const isModelHome = (pH!=null) ? (pH >= 0.5) : false;
      const isModelAway = (pH!=null) ? (!isModelHome) : false;
      // Fair American odds from probabilities (inverse of impliedProbAmerican)
      const fairFromProb = (p)=>{
        const v = Number(p);
        if (!Number.isFinite(v) || v<=0 || v>=1) return null;
        if (v >= 0.5) return - (100 * v / (1 - v));
        return 100 * (1 - v) / v;
      };
      const fairAway = (pA!=null ? fairFromProb(pA) : null);
      const fairHome = (pH!=null ? fairFromProb(pH) : null);
      const fairAwayTxt = (fairAway!=null ? fmtOddsAmerican(fairAway) : '');
      const fairHomeTxt = (fairHome!=null ? fmtOddsAmerican(fairHome) : '');
      chipsMoney = `
        <div class=\"row chips\">\
          <div class=\"chip title\">Moneyline</div>\
          ${fairAwayTxt? `<div class=\"chip neutral\">Fair Away ${fairAwayTxt}</div>` : ''}
          ${fairHomeTxt? `<div class=\"chip neutral\">Fair Home ${fairHomeTxt}</div>` : ''}
          <div class=\"chip ${clsA} ${isModelAway?'model-pick':''}\">Away ${aOddsTxt} · ${aProbTxt} ${bookBadge} ${evABadge} ${isModelAway?modelBadge:''}</div>\
          <div class=\"chip ${clsH} ${isModelHome?'model-pick':''}\">Home ${hOddsTxt} · ${hProbTxt} ${bookBadge} ${evHBadge} ${isModelHome?modelBadge:''}</div>\
        </div>`;
    }

    // Build recommendation and model pick blocks
    let recHtml = '';
    if (recCands.length){
      const best = recCands.slice().sort((a,b)=>((b.ev??-999)-(a.ev??-999)))[0];
      const conf = evClass(best.ev || 0).toLowerCase();
      let recResult = '';
      if (isFinal && odds) {
        try{
          if (best.market==='moneyline'){
            const want = best.bet==='home_ml' ? home : away;
            const act = (actualHome>actualAway)?home:((actualAway>actualHome)?away:null);
            if (act){ recResult = (act===want)?'Win':'Loss'; }
          } else if (best.market==='totals' && Number.isFinite(Number(odds.total))){
            const tot = Number(odds.total);
            const actTotal = totalActual;
            if (actTotal!=null){
              const actSide = actTotal>tot?'Over':(actTotal<tot?'Under':'Push');
              const want = best.bet==='over'?'Over':'Under';
              recResult = (actSide==='Push')?'Push':(actSide===want?'Win':'Loss');
            }
          } else if (best.market==='spread' && Number.isFinite(Number(odds.home_spread))){
            const spr = Number(odds.home_spread);
            const actualMargin = actualHome-actualAway;
            const v = actualMargin + spr;
            const actATS = (v>0)?home:((v<0)?away:'Push');
            const want = (best.bet==='home_spread')?home:away;
            recResult = (actATS==='Push')?'Push':(actATS===want?'Win':'Loss');
          }
        }catch(_){/* ignore */}
      }
      const bookAbbr = (best.book||'').toString().toUpperCase().slice(0,2);
      recHtml = `
        <div class=\"row details small\"><div class=\"detail-col\">
          Recommendation: <strong>${best.label}</strong>
          <span class=\"rec-conf ${conf}\">${evClass(best.ev||0)}</span>
          · EV ${(best.ev>0?'+':'')}${(100*(best.ev||0)).toFixed(1)}%
          ${best.odds!=null?` · ${fmtOddsAmerican(best.odds)}`:''}
          ${bookAbbr?` @ ${bookAbbr}`:''}
          ${recResult?` · Result: <strong>${recResult}</strong>`:''}
        </div></div>`;
    }

    // PBP markets summary from game cards (if available)
    let pbpHtml = '';
    try{
      const gc = state.gameCardsByKey.get(key);
      if (gc){
        const tip = (gc.prob_home_tip!=null) ? `Tip: Home ${(Number(gc.prob_home_tip)*100).toFixed(1)}%` : '';
        const thrExp = (gc.early_threes_expected!=null) ? `Early 3s E[X]: ${Number(gc.early_threes_expected).toFixed(2)}` : '';
        const thrP1 = (gc.early_threes_prob_ge_1!=null) ? `P(≥1): ${(Number(gc.early_threes_prob_ge_1)*100).toFixed(1)}%` : '';
        const thr = (thrExp || thrP1) ? `Threes 0–3m: ${[thrExp, thrP1].filter(Boolean).join(' • ')}` : '';

        // Reconciliation recap (if present in game_cards)
        let tipRec = '';
        try{
          const out = Number(gc.pbp_tip_outcome_home);
          if (Number.isFinite(out)){
            tipRec = `Tip result: ${out===1? 'Home' : (out===0? 'Away' : '—')}`;
          }
        }catch(_){ /* ignore */ }

        let fbRec = '';
        try{
          const act = (gc.pbp_first_basket_actual_name||'').toString().trim();
          if (act){
            const hit5 = String(gc.pbp_first_basket_hit_top5||'').toLowerCase();
            const tag = (hit5==='true' || hit5==='1') ? ' (Top5 hit)' : '';
            fbRec = `First Basket actual: ${act}${tag}`;
          }
        }catch(_){ /* ignore */ }

        let thrRec = '';
        try{
          const act = Number(gc.pbp_early_threes_actual);
          const err = Number(gc.pbp_early_threes_error);
          if (Number.isFinite(act)){
            thrRec = `Threes 0–3m actual: ${act}${Number.isFinite(err)?` (err ${err>0?'+':''}${err.toFixed(2)})`:''}`;
          }
        }catch(_){ /* ignore */ }

        let propsRec = '';
        try{
          const nm = (gc.props_top_pts_player||'').toString().trim();
          const pts = Number(gc.props_top_pts);
          const tm = (gc.props_top_pts_team||'').toString().trim();
          if (nm && Number.isFinite(pts)){
            propsRec = `Top scorer: ${nm}${tm?` (${tm})`:''} ${pts}`;
          }
        }catch(_){ /* ignore */ }

        // If we have detailed First Basket Picks for this game, suppress the compact summary line to avoid duplication
        let hasDetailedFB = false;
        try{
          if (gc && gc.game_id!=null){
            const raw = String(gc.game_id).trim().replace(/\.0$/, '');
            const digits = raw.replace(/[^0-9]/g,'');
            const gid = digits ? digits.padStart(10,'0') : null;
            const picks = gid ? (state.fbRecsByGid.get(gid) || []) : [];
            hasDetailedFB = picks.length > 0;
          }
        }catch(_){ /* ignore */ }
        const fb = (!hasDetailedFB && gc.first_basket_top5) ? `First Basket: ${gc.first_basket_top5}` : '';
        const lines = [tip, tipRec, thr, thrRec, fb, fbRec, propsRec].filter(Boolean);
        if (lines.length){
          pbpHtml = `<div class="row details small"><div class="detail-col">${lines.map(x=>`<div>${x}</div>`).join('')}</div></div>`;
        }
      } else {
        // Subtle placeholder for today's slate to indicate section without data yet
        if (dateStr === localYMD()){
          pbpHtml = `<div class=\"row details small\"><div class=\"detail-col subtle\">PBP markets: pending</div></div>`;
        }
      }
    }catch(_){ /* ignore */ }

    // First Basket Picks section (from first_basket_recs_<date>.csv)
    let fbRecsHtml = '';
    try{
      const gc = state.gameCardsByKey.get(key);
      let gid = null;
      if (gc && gc.game_id!=null){
        const raw = String(gc.game_id).trim().replace(/\.0$/, '');
        const digits = raw.replace(/[^0-9]/g,'');
        gid = digits ? digits.padStart(10,'0') : null;
      }
      const picks = gid ? (state.fbRecsByGid.get(gid) || []) : [];
      if (picks.length){
        const parts = picks.map(p=>{
          const pct = (p.prob_first_basket!=null) ? `${(p.prob_first_basket*100).toFixed(1)}%` : '—';
          const fair = (p.fair_american!=null) ? fmtOddsAmerican(p.fair_american) : '—';
          const tm = String(p.team||'').toUpperCase();
          return `<div>${p.player_name}${tm?` (${tm})`:''} · ${pct} · Fair ${fair}</div>`;
        }).join('');
        fbRecsHtml = `<div class="row details small"><div class="detail-col"><div>First Basket Picks:</div>${parts}</div></div>`;
      }
    }catch(_){ /* ignore */ }

    // Build quarters line score (traditional format)
    let periodsHtml = '';
    if (pred && pred.quarters_q1_total!=null){
      const cardId = `periods-${dateStr}-${home}-${away}`.replace(/[^a-zA-Z0-9-]/g, '');
      
      // Calculate team scores per quarter based on margin and total
      const calcQuarterScores = (qMargin, qTotal) => {
        const homeQ = (qTotal + qMargin) / 2;
        const awayQ = (qTotal - qMargin) / 2;
        return { home: homeQ, away: awayQ };
      };
      
      // Get quarter data
      const q1 = calcQuarterScores(Number(pred.quarters_q1_margin||0), Number(pred.quarters_q1_total||0));
      const q2 = calcQuarterScores(Number(pred.quarters_q2_margin||0), Number(pred.quarters_q2_total||0));
      const q3 = calcQuarterScores(Number(pred.quarters_q3_margin||0), Number(pred.quarters_q3_total||0));
      const q4 = calcQuarterScores(Number(pred.quarters_q4_margin||0), Number(pred.quarters_q4_total||0));

      // Calculate totals
      const awayTotal = q1.away + q2.away + q3.away + q4.away;
      const homeTotal = q1.home + q2.home + q3.home + q4.home;
      
      periodsHtml = `
        <div class="row details small">
          <div class="detail-col">
            <div class="periods-toggle cursor-pointer fw-600" onclick="togglePeriods('${cardId}')" style="color:#4a90e2; margin-bottom:6px;">
              ▼ Projected Line Score
            </div>
            <div id="${cardId}" class="periods-content d-block" style="margin-top:2px;">
              <table style="width:100%; font-size:0.9em; border-collapse:separate; border-spacing:0; background:transparent;">
                <thead>
                  <tr style="color:#8b92a8; font-size:0.75em; text-transform:uppercase; letter-spacing:0.5px;">
                    <th style="text-align:left; padding:4px 8px; font-weight:600;">TEAM</th>
                    <th style="text-align:center; padding:4px 8px; font-weight:600;">Q1</th>
                    <th style="text-align:center; padding:4px 8px; font-weight:600;">Q2</th>
                    <th style="text-align:center; padding:4px 8px; font-weight:600;">Q3</th>
                    <th style="text-align:center; padding:4px 8px; font-weight:600;">Q4</th>
                    <th style="text-align:center; padding:4px 8px; font-weight:600;">TOTAL</th>
                  </tr>
                </thead>
                <tbody>
                  <tr style="background:rgba(139,146,168,0.05);">
                    <td style="padding:8px; font-weight:600; color:#e8eaf0;">${away}</td>
                    <td style="text-align:center; padding:8px; color:#e8eaf0;">${fmtNum(q1.away, 1)}</td>
                    <td style="text-align:center; padding:8px; color:#e8eaf0;">${fmtNum(q2.away, 1)}</td>
                    <td style="text-align:center; padding:8px; color:#e8eaf0;">${fmtNum(q3.away, 1)}</td>
                    <td style="text-align:center; padding:8px; color:#e8eaf0;">${fmtNum(q4.away, 1)}</td>
                    <td style="text-align:center; padding:8px; font-weight:700; color:#e8eaf0;">${fmtNum(awayTotal, 1)}</td>
                  </tr>
                  <tr style="background:rgba(139,146,168,0.08);">
                    <td style="padding:8px; font-weight:600; color:#e8eaf0;">${home}</td>
                    <td style="text-align:center; padding:8px; color:#e8eaf0;">${fmtNum(q1.home, 1)}</td>
                    <td style="text-align:center; padding:8px; color:#e8eaf0;">${fmtNum(q2.home, 1)}</td>
                    <td style="text-align:center; padding:8px; color:#e8eaf0;">${fmtNum(q3.home, 1)}</td>
                    <td style="text-align:center; padding:8px; color:#e8eaf0;">${fmtNum(q4.home, 1)}</td>
                    <td style="text-align:center; padding:8px; font-weight:700; color:#e8eaf0;">${fmtNum(homeTotal, 1)}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>`;
    }

    node.innerHTML = `
      <div class="row head">
        <div class="game-date js-local-time">${dtIso || dateStr}</div>
        ${venue ? `<div class=\"venue\">${venue}</div>` : ''}
        <div class="state">${statusLine}</div>
  <div class="period-pill"></div>
  <div class="time-left"></div>
  ${isFinal ? `<div class=\"result-badge\">${w}W-${l}L${psh>0?`-${psh}P`:''}</div>` : ''}
      </div>
      <div class="row matchup">
        <div class="team side">
          <div class="team-line">${teamLineHTML(away)}</div>
          <div class="score-block">
            <div class="live-score js-live-away">${(actualAway!=null? fmtNum(actualAway,0) : (projAway!=null ? fmtNum(projAway,1) : '—'))}</div>
            <div class="sub proj-score">${(projAway!=null? `Model: ${fmtNum(projAway,2)}` : '')}</div>
          </div>
        </div>
  <div class="text-center fw-700">@</div>
        <div class="team side">
          <div class="team-line">${teamLineHTML(home)}</div>
          <div class="score-block">
            <div class="live-score js-live-home">${(actualHome!=null? fmtNum(actualHome,0) : (projHome!=null ? fmtNum(projHome,1) : '—'))}</div>
            <div class="sub proj-score">${(projHome!=null? `Model: ${fmtNum(projHome,2)}` : '')}</div>
          </div>
        </div>
      </div>
      ${chipsTotals}
      ${chipsSpread}
      ${chipsMoney}
      <div class="row details">
        <div class="detail-col">
          ${totalModel!=null? `<div>Model Total: <strong>${totalModel.toFixed(2)}</strong>${totalActual!=null? ` | Actual: <strong>${totalActual.toFixed(2)}</strong> | Diff: <strong>${(totalActual >= totalModel ? '+' : '')}${(totalActual - totalModel).toFixed(2)}</strong>`: ''}</div>`: ''}
          ${wpLine ? `<div>${wpLine}</div>` : ''}
          ${accuracyLine ? `<div>${accuracyLine}</div>` : ''}
        </div>
      </div>
      ${recHtml}
  ${pbpHtml}
    ${fbRecsHtml}
      ${periodsHtml}
      
      ${propsBadges ? `<div class=\"row details small\"><div class=\"detail-col\">${propsBadges}</div></div>` : ''}
      
      <div class=\"row details small\"><div class=\"detail-col\"><div>Debug: ${dateStr} | ${home}-${away} | hasOdds=${hasAnyOdds?1:0}</div></div></div>
    `;
    wrap.appendChild(node);
  }
  // Start or refresh live polling for this date
  try {
    startScoreboardPolling(dateStr);
    startOddsReload(dateStr);
  } catch(_){}
  // Format times inside legacy cards (if present)
  try{ formatEtTimesInCards(); }catch(_){ }
}

// Cards v2 renderer: clean, compact, and compatible with live polling.
function renderDate(dateStr){
  const wrap = document.getElementById('cards');
  if (!wrap) return;
  wrap.innerHTML = '';

  const isToday = (dateStr === localYMD());
  let list = state.byDate.get(dateStr) || [];

  // Sort games by commence/start time: prefer odds.commence_time, else schedule timestamps
  try{
    const keyTime = (g)=>{
      const home = g.home_tricode, away = g.away_tricode;
      const odds = state.oddsByKey.get(`${dateStr}|${home}|${away}`);
      const ct = odds && odds.commence_time ? new Date(odds.commence_time) : null;
      if (ct && !isNaN(ct)) return ct.getTime();
      const iso = g.datetime_utc || g.datetime_est || (g.date_utc?`${g.date_utc}T00:00:00Z`: (g.date_est?`${g.date_est}T00:00:00Z`: null));
      const d = iso ? new Date(iso) : null;
      return (d && !isNaN(d)) ? d.getTime() : Number.MAX_SAFE_INTEGER;
    };
    list = list.slice().sort((a,b)=> keyTime(a) - keyTime(b));
  }catch(_){ /* ignore sort errors */ }

  const hideOdds = document.getElementById('hideOdds')?.checked;

  const fmtSigned = (n, digits=1)=>{
    const v = Number(n);
    if (!Number.isFinite(v)) return '—';
    const s = v > 0 ? '+' : '';
    return s + v.toFixed(digits);
  };

  const pp = (p, imp)=>{
    const a = Number(p), b = Number(imp);
    if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
    return (a - b) * 100;
  };

  const tile = (title, main, meta)=>{
    const m = main || '—';
    const sub = meta || '';
    return `
      <div class="market-tile">
        <div class="market-title">${title}</div>
        <div class="market-main">${m}</div>
        ${sub?`<div class="subtle">${sub}</div>`:''}
      </div>`;
  };

  // Props recommendations engine (top plays) for in-card table
  const propsRowsAll = Array.isArray(state.propsRecs) ? state.propsRecs : [];

  try{
  for (const g of list){
    const away = String(g.away_tricode||'').toUpperCase();
    const home = String(g.home_tricode||'').toUpperCase();
    const key = `${dateStr}|${home}|${away}`;

    const odds = state.oddsByKey.get(key);
    const predBase = state.predsByKey.get(key) || null;
    const gc = state.gameCardsByKey.get(key) || null;
    const pl = state.periodLinesByKey ? (state.periodLinesByKey.get(key) || null) : null;

    const sim = state.simQuartersByKey ? (state.simQuartersByKey.get(key) || null) : null;
    const simSummary = sim && sim.summary ? sim.summary : null;
    const simQuarters = sim && Array.isArray(sim.quarters) ? sim.quarters : [];
    const props = state.propsPredsByGameKey ? (state.propsPredsByGameKey.get(key) || []) : [];

    const predMargin = (gc && gc.pred_margin!=null) ? toNum(gc.pred_margin) : (predBase ? toNum(predBase.pred_margin) : null);
    const predTotal  = (gc && gc.pred_total!=null)  ? toNum(gc.pred_total)  : (predBase ? toNum(predBase.pred_total)  : null);
    const pHomeRaw   = (gc && gc.home_win_prob!=null) ? toNum(gc.home_win_prob) : (predBase ? toNum(predBase.home_win_prob) : null);

    // Diagnostics (if present in predictions)
    const pHomeModelOnly = predBase ? toNum(predBase.home_win_prob_raw) : null;
    const pHomeFromSpread = predBase ? toNum(predBase.home_win_prob_from_spread) : null;
    const pHomeIso = predBase ? toNum(predBase.home_win_prob_iso) : null;

    const pHomeCal = (pHomeRaw!=null) ? calibrateGamesProb(pHomeRaw) : null;
    const pAwayCal = (pHomeCal!=null) ? (1 - pHomeCal) : null;

    // Model score projection (team points) from (total, margin)
    const homeModelPts = (predTotal!=null && predMargin!=null) ? (Number(predTotal) + Number(predMargin)) / 2 : null;
    const awayModelPts = (predTotal!=null && predMargin!=null) ? (Number(predTotal) - Number(predMargin)) / 2 : null;

    const simPts = pointsFromTotalMargin(simSummary?.final_total_mu, simSummary?.final_margin_mu);
    const homeSimPts = simPts.home;
    const awaySimPts = simPts.away;
    const homeBlendPts = (homeModelPts!=null && homeSimPts!=null)
      ? (SCORE_BLEND_ALPHA*homeModelPts + (1-SCORE_BLEND_ALPHA)*homeSimPts)
      : (homeModelPts!=null ? homeModelPts : (homeSimPts!=null ? homeSimPts : null));
    const awayBlendPts = (awayModelPts!=null && awaySimPts!=null)
      ? (SCORE_BLEND_ALPHA*awayModelPts + (1-SCORE_BLEND_ALPHA)*awaySimPts)
      : (awayModelPts!=null ? awayModelPts : (awaySimPts!=null ? awaySimPts : null));

    // Time / venue
    const dtIso = (odds && odds.commence_time) ? odds.commence_time : (g.datetime_utc || g.datetime_est || null);
    const when = dtIso ? fmtLocalTime(dtIso) : '';
    const venueBits = [];
    if (g.arena_name) venueBits.push(g.arena_name);
    if (g.arena_city) venueBits.push(g.arena_city);
    if (g.arena_state) venueBits.push(g.arena_state);
    const venue = venueBits.length ? venueBits.join(', ') : '';

    // Scores/status seed (polling will update for today)
    // Prefer recon CSV scores, but fall back to reconciled finals in game_cards when present.
    const recon = state.reconByKey.get(key) || null;
    let actualHome = recon && recon.home_pts!=null ? Number(recon.home_pts) : null;
    let actualAway = recon && recon.visitor_pts!=null ? Number(recon.visitor_pts) : null;
    try{
      if (!isToday && (actualHome==null || actualAway==null) && gc){
        const fh = toNum(gc.final_home_pts);
        const fa = toNum(gc.final_visitor_pts);
        if (fh!=null && fa!=null){
          actualHome = fh;
          actualAway = fa;
        }
      }
    }catch(_){ /* ignore */ }

    const totalActual = (actualHome!=null && actualAway!=null) ? (actualHome + actualAway) : (gc ? toNum(gc.final_total_pts) : null);
    const isFinal = (!isToday && actualHome!=null && actualAway!=null);
    const seedStatus = isFinal ? `Final ${actualAway}-${actualHome}` : 'Scheduled';


    // Reconciliation recap lines (from game_cards)
    let recapDetails = '';
    try{
      if (!isToday && gc){
        const lines = [];

        if (actualHome!=null && actualAway!=null){
          lines.push(`Final: ${away} ${Number(actualAway).toFixed(0)} — ${home} ${Number(actualHome).toFixed(0)}${totalActual!=null?` (T ${Number(totalActual).toFixed(0)})`:''}`);
        }

        // ATS / OU grading
        try{
          const spr = toNum(gc.home_spread);
          const res = String(gc.ats_home_result||'').toUpperCase().trim();
          const m = toNum(gc.ats_home_margin);
          if (spr!=null && res){
            lines.push(`ATS (${home} ${fmtSigned(spr,1)}): ${res}${m!=null?` (${fmtSigned(m,1)})`:''}`);
          }
        }catch(_){ /* ignore */ }

        try{
          const tot = toNum(gc.total);
          const res = String(gc.ou_result||'').toUpperCase().trim();
          const m = toNum(gc.ou_margin);
          if (tot!=null && res){
            lines.push(`O/U ${Number(tot).toFixed(1)}: ${res}${m!=null?` (${fmtSigned(m,1)})`:''}`);
          }
        }catch(_){ /* ignore */ }

        // Model error vs actual
        try{
          const me = toNum(gc.margin_error);
          const te = toNum(gc.total_error);
          if (me!=null || te!=null){
            const bits = [];
            if (me!=null) bits.push(`M err ${fmtSigned(me,1)}`);
            if (te!=null) bits.push(`T err ${fmtSigned(te,1)}`);
            if (bits.length) lines.push(`Errors: ${bits.join(' · ')}`);
          }
        }catch(_){ /* ignore */ }

        // PBP market reconciliation
        try{
          const out = toNum(gc.pbp_tip_outcome_home);
          if (out!=null){
            lines.push(`Tip result: ${out===1? 'Home' : (out===0? 'Away' : '—')}`);
          }
        }catch(_){ /* ignore */ }

        try{
          const act = (gc.pbp_first_basket_actual_name||'').toString().trim();
          if (act){
            const hit5 = String(gc.pbp_first_basket_hit_top5||'').toLowerCase();
            const tag = (hit5==='true' || hit5==='1') ? ' (Top5 hit)' : '';
            lines.push(`First Basket actual: ${act}${tag}`);
          }
        }catch(_){ /* ignore */ }

        try{
          const act = toNum(gc.pbp_early_threes_actual);
          const err = toNum(gc.pbp_early_threes_error);
          if (act!=null){
            lines.push(`Threes 0–3m actual: ${Number(act).toFixed(0)}${err!=null?` (err ${fmtSigned(err,2)})`:''}`);
          }
        }catch(_){ /* ignore */ }

        // Props recap highlight
        try{
          const nm = (gc.props_top_pts_player||'').toString().trim();
          const pts = toNum(gc.props_top_pts);
          const tm = (gc.props_top_pts_team||'').toString().trim();
          if (nm && pts!=null){
            lines.push(`Top scorer: ${nm}${tm?` (${tm})`:''} ${Number(pts).toFixed(0)}`);
          }
        }catch(_){ /* ignore */ }

        recapDetails = lines.length ? lines.map(x=>`<div class="subtle">${escapeHtml(x)}</div>`).join('') : '';
      }
    }catch(_){ /* ignore */ }

    // Market tiles (pick best side by EV)
    const candEvs = [];

    // ML
    let mlMain = '—';
    let mlMeta = '';
    try{
      if (pHomeCal!=null){
        // Prefer odds snapshot, then prediction base, then game-card market snapshot.
        const mHomeMlRaw = (toNum(odds?.home_ml) ?? toNum(predBase?.home_ml) ?? toNum(gc?.home_ml));
        const mAwayMlRaw = (toNum(odds?.away_ml) ?? toNum(predBase?.away_ml) ?? toNum(gc?.away_ml));
        if (mHomeMlRaw==null || mAwayMlRaw==null) throw new Error('no ML');

        // Guard against historical/invalid consensus moneylines (e.g. -30).
        const mlsTile = normalizeMoneylines(mHomeMlRaw, mAwayMlRaw);
        const hML = Number(mlsTile?.home_ml ?? mHomeMlRaw);
        const aML = Number(mlsTile?.away_ml ?? mAwayMlRaw);
        const evH = evFromProbAndAmerican(pHomeCal, hML);
        const evA = evFromProbAndAmerican(pAwayCal, aML);
        const hImp = impliedProbAmerican(hML);
        const aImp = impliedProbAmerican(aML);

        const bestHome = (evH??-Infinity) >= (evA??-Infinity);
        const pickTeam = bestHome ? home : away;
        const pickOdds = bestHome ? hML : aML;
        const pickP = bestHome ? pHomeCal : pAwayCal;
        const pickImp = bestHome ? hImp : aImp;
        const pickEv = bestHome ? evH : evA;
        const edge = pp(pickP, pickImp);
        if (pickEv!=null) candEvs.push(pickEv);

        mlMain = `${pickTeam}${hideOdds?'':` ${fmtOddsAmerican(pickOdds)}`}`;
        const evTxt = (pickEv!=null) ? `EV ${(pickEv>0?'+':'')}${(pickEv*100).toFixed(1)}%` : 'EV —';
        const pTxt = (pickP!=null) ? `P ${fmtPct(pickP,1)}` : 'P —';
        const impTxt = (pickImp!=null) ? `Imp ${fmtPct(pickImp,1)}` : 'Imp —';
        const edgeTxt = (edge!=null) ? `Edge ${(edge>0?'+':'')}${edge.toFixed(1)}pp` : 'Edge —';
        const nTxt = (mlsTile && mlsTile.normalized) ? 'ML normalized' : '';
        let gradeTxt = '';
        if (isFinal && actualHome!=null && actualAway!=null){
          const won = (pickTeam === home) ? (Number(actualHome) > Number(actualAway)) : (Number(actualAway) > Number(actualHome));
          gradeTxt = `<span class="mark ${won?'ok':'bad'}">${won?'W':'L'}</span>`;
        }
        mlMeta = [evTxt, pTxt, impTxt, edgeTxt, nTxt, gradeTxt].filter(Boolean).join(' · ');
      }
    }catch(_){ /* ignore */ }

    // ATS
    let atsMain = '—';
    let atsMeta = '';
    try{
      if (predMargin!=null){
        const sigmaMargin = 12.0;
        const sprHomeRaw = (toNum(odds?.home_spread) ?? toNum(predBase?.home_spread) ?? toNum(gc?.home_spread));
        if (sprHomeRaw==null) throw new Error('no spread');
        const sprHome = Number(sprHomeRaw);
        const sprAway = (toNum(odds?.away_spread) ?? toNum(predBase?.away_spread) ?? toNum(gc?.away_spread) ?? (Number.isFinite(sprHome)? -sprHome : null));
        // Spreads are expressed as points added to a team (e.g., favorite is -11.5).
        // Home covers if: (home_score + home_spread) > away_score  =>  margin > -home_spread.
        const threshold = -sprHome;
        const zHome = (threshold - predMargin) / sigmaMargin;
        const pHomeCover = 1 - normCdf(zHome);
        const pAwayCover = 1 - pHomeCover;
        const priceHome = (toNum(odds?.home_spread_price) ?? toNum(predBase?.home_spread_price) ?? toNum(gc?.home_spread_price) ?? -110);
        const priceAway = (toNum(odds?.away_spread_price) ?? toNum(predBase?.away_spread_price) ?? toNum(gc?.away_spread_price) ?? -110);
        const evH = evFromProbAndAmerican(pHomeCover, priceHome);
        const evA = evFromProbAndAmerican(pAwayCover, priceAway);
        const impH = impliedProbAmerican(priceHome);
        const impA = impliedProbAmerican(priceAway);

        const bestHome = (evH??-Infinity) >= (evA??-Infinity);
        const pickTeam = bestHome ? home : away;
        const pickLine = bestHome ? sprHome : sprAway;
        const pickOdds = bestHome ? priceHome : priceAway;
        const pickP = bestHome ? pHomeCover : pAwayCover;
        const pickImp = bestHome ? impH : impA;
        const pickEv = bestHome ? evH : evA;
        const edge = pp(pickP, pickImp);
        if (pickEv!=null) candEvs.push(pickEv);

        const lineTxt = Number.isFinite(pickLine) ? `${pickLine>0?'+':''}${pickLine.toFixed(1)}` : '';
        atsMain = `${pickTeam} ${lineTxt}${hideOdds?'':` ${fmtOddsAmerican(pickOdds)}`}`.trim();
        const evTxt = (pickEv!=null) ? `EV ${(pickEv>0?'+':'')}${(pickEv*100).toFixed(1)}%` : 'EV —';
        const pTxt = (pickP!=null) ? `P ${fmtPct(pickP,1)}` : 'P —';
        const impTxt = (pickImp!=null) ? `Imp ${fmtPct(pickImp,1)}` : 'Imp —';
        const edgeTxt = (edge!=null) ? `Edge ${(edge>0?'+':'')}${edge.toFixed(1)}pp` : 'Edge —';
        const mTxt = (predMargin!=null) ? `Model M ${fmtSigned(predMargin,1)}` : '';
        let gradeTxt = '';
        if (isFinal && actualHome!=null && actualAway!=null && Number.isFinite(Number(pickLine))){
          const line = Number(pickLine);
          const aH = Number(actualHome);
          const aA = Number(actualAway);
          const tScore = (pickTeam === home) ? aH : aA;
          const oScore = (pickTeam === home) ? aA : aH;
          const adj = tScore + line;
          const res = (Math.abs(adj - oScore) < 1e-9) ? 'P' : (adj > oScore ? 'W' : 'L');
          const cls = (res === 'W') ? 'ok' : (res === 'L') ? 'bad' : 'push';
          gradeTxt = `<span class="mark ${cls}">${res}</span>`;
        }
        atsMeta = [evTxt, pTxt, impTxt, edgeTxt, mTxt, gradeTxt].filter(Boolean).join(' · ');
      }
    }catch(_){ /* ignore */ }

    // TOTAL
    let totMain = '—';
    let totMeta = '';
    try{
      if (predTotal!=null){
        const sigmaTotal = 20.0;
        const totRaw = (toNum(odds?.total) ?? toNum(predBase?.total) ?? toNum(gc?.total));
        if (totRaw==null) throw new Error('no total');
        const tot = Number(totRaw);
        const zOver = (tot - predTotal) / sigmaTotal;
        const pOver = 1 - normCdf(zOver);
        const pUnder = 1 - pOver;
        const priceOver = (toNum(odds?.total_over_price) ?? toNum(predBase?.total_over_price) ?? toNum(gc?.total_over_price) ?? -110);
        const priceUnder = (toNum(odds?.total_under_price) ?? toNum(predBase?.total_under_price) ?? toNum(gc?.total_under_price) ?? -110);
        const evO = evFromProbAndAmerican(pOver, priceOver);
        const evU = evFromProbAndAmerican(pUnder, priceUnder);
        const impO = impliedProbAmerican(priceOver);
        const impU = impliedProbAmerican(priceUnder);

        const bestOver = (evO??-Infinity) >= (evU??-Infinity);
        const pickSide = bestOver ? 'Over' : 'Under';
        const pickOdds = bestOver ? priceOver : priceUnder;
        const pickP = bestOver ? pOver : pUnder;
        const pickImp = bestOver ? impO : impU;
        const pickEv = bestOver ? evO : evU;
        const edge = pp(pickP, pickImp);
        if (pickEv!=null) candEvs.push(pickEv);

        const totTxt = Number.isFinite(tot) ? tot.toFixed(1) : '';
        totMain = `${pickSide} ${totTxt}${hideOdds?'':` ${fmtOddsAmerican(pickOdds)}`}`.trim();
        const evTxt = (pickEv!=null) ? `EV ${(pickEv>0?'+':'')}${(pickEv*100).toFixed(1)}%` : 'EV —';
        const pTxt = (pickP!=null) ? `P ${fmtPct(pickP,1)}` : 'P —';
        const impTxt = (pickImp!=null) ? `Imp ${fmtPct(pickImp,1)}` : 'Imp —';
        const edgeTxt = (edge!=null) ? `Edge ${(edge>0?'+':'')}${edge.toFixed(1)}pp` : 'Edge —';
        const tTxt = (predTotal!=null) ? `Model T ${Number(predTotal).toFixed(1)}` : '';
        let gradeTxt = '';
        if (isFinal && totalActual!=null && Number.isFinite(Number(tot))){
          const act = Number(totalActual);
          const ln = Number(tot);
          const res = (Math.abs(act - ln) < 1e-9) ? 'P' : (pickSide.toUpperCase()==='OVER' ? (act > ln ? 'W' : 'L') : (act < ln ? 'W' : 'L'));
          const cls = (res === 'W') ? 'ok' : (res === 'L') ? 'bad' : 'push';
          gradeTxt = `<span class="mark ${cls}">${res}</span>`;
        }
        totMeta = [evTxt, pTxt, impTxt, edgeTxt, tTxt, gradeTxt].filter(Boolean).join(' · ');
      }
    }catch(_){ /* ignore */ }

    const bestEv = candEvs.length ? candEvs.slice().sort((a,b)=>b-a)[0] : null;
    const tier = tierFromEv(bestEv);

    const pickNum = (...vals)=>{
      for (const v of vals){
        const n = toNum(v);
        if (n!=null) return n;
      }
      return null;
    };

    // Explicit market lines (not EV tiles)
    const mHomeMlRaw = pickNum(odds?.home_ml, predBase?.home_ml, gc?.home_ml);
    const mAwayMlRaw = pickNum(odds?.away_ml, predBase?.away_ml, gc?.away_ml);
    const mls = normalizeMoneylines(mHomeMlRaw, mAwayMlRaw);
    const mHomeMl = pickNum(mls?.home_ml, mHomeMlRaw);
    const mAwayMl = pickNum(mls?.away_ml, mAwayMlRaw);
    const mHomeSpr = pickNum(odds?.home_spread, predBase?.home_spread, gc?.home_spread);
    const mAwaySpr = pickNum(odds?.away_spread, predBase?.away_spread, gc?.away_spread);
    const mTot = pickNum(odds?.total, predBase?.total, gc?.total);
    const mBook = String(odds?.bookmaker || predBase?.bookmaker || '').trim();
    const marketLine = (()=>{
      const parts = [];
      if (mHomeMl!=null && mAwayMl!=null) parts.push(`ML ${away} ${fmtOddsAmerican(mAwayMl)} / ${home} ${fmtOddsAmerican(mHomeMl)}`);
      if (mHomeSpr!=null && mAwaySpr!=null) parts.push(`Spread ${away} ${fmtSigned(mAwaySpr,1)} / ${home} ${fmtSigned(mHomeSpr,1)}`);
      if (mTot!=null) parts.push(`Total ${Number(mTot).toFixed(1)}`);
      const tail = (mBook && mBook.toLowerCase() !== 'nan') ? ` @ ${mBook.toUpperCase()}` : '';
      const note = (mls && mls.normalized) ? ' (ML normalized)' : '';
      return parts.length ? `Market: ${parts.join(' · ')}${tail}${note}` : '';
    })();

    // Avoid mixing multiple competing score concepts on the card.
    // The connected sim (Quarters/Write-up/Players) provides the canonical scenario score.

    const blendLine = (()=>{
      const parts = [];
      if (pHomeModelOnly!=null) parts.push(`Model ${fmtPct(pHomeModelOnly,1)}`);
      if (pHomeFromSpread!=null) parts.push(`From spread ${fmtPct(pHomeFromSpread,1)}`);
      if (pHomeRaw!=null) parts.push(`Blend ${fmtPct(pHomeRaw,1)}`);
      if (pHomeIso!=null) parts.push(`Sim/iso ${fmtPct(pHomeIso,1)}`);
      if (pHomeCal!=null) parts.push(`Cal ${fmtPct(pHomeCal,1)}`);
      return parts.length ? `Win prob blend: ${parts.join(' · ')}` : '';
    })();

    const gameReconHtml = (()=>{
      try{
        if (!isFinal || actualHome==null || actualAway==null) return '';

        const mkMark = (res)=>{
          if (!res || res === '—') return '<span class="subtle">—</span>';
          const cls = (res === 'W') ? 'ok' : (res === 'L') ? 'bad' : 'push';
          return `<span class="mark ${cls}">${escapeHtml(res)}</span>`;
        };

        const aH = Number(actualHome);
        const aA = Number(actualAway);
        if (!Number.isFinite(aH) || !Number.isFinite(aA)) return '';
        const actTotal = aH + aA;
        const actMargin = aH - aA;

        // Use the same pick logic as the tiles, but recompute here so we can grade.
        let mlPick = '—', mlRes = '—';
        try{
          const hML = toNum(mHomeMl);
          const aML = toNum(mAwayMl);
          if (pHomeCal!=null && hML!=null && aML!=null){
            const evH = evFromProbAndAmerican(pHomeCal, hML);
            const evA = evFromProbAndAmerican(pAwayCal, aML);
            const pickHome = (evH??-Infinity) >= (evA??-Infinity);
            mlPick = pickHome ? `${home} ${fmtOddsAmerican(hML)}` : `${away} ${fmtOddsAmerican(aML)}`;
            const won = pickHome ? (aH > aA) : (aA > aH);
            mlRes = won ? 'W' : 'L';
          }
        }catch(_){ /* ignore */ }

        let atsPick = '—', atsRes = '—';
        try{
          if (predMargin!=null){
            const sprHome = toNum(mHomeSpr);
            const sprAway = toNum(mAwaySpr);
            const priceHome = (toNum(odds?.home_spread_price) ?? toNum(predBase?.home_spread_price) ?? toNum(gc?.home_spread_price) ?? -110);
            const priceAway = (toNum(odds?.away_spread_price) ?? toNum(predBase?.away_spread_price) ?? toNum(gc?.away_spread_price) ?? -110);
            if (sprHome!=null && (sprAway!=null || Number.isFinite(Number(sprHome)))){
              const sigmaMargin = 12.0;
              const threshold = -Number(sprHome);
              const zHome = (threshold - predMargin) / sigmaMargin;
              const pHomeCover = 1 - normCdf(zHome);
              const pAwayCover = 1 - pHomeCover;
              const evH = evFromProbAndAmerican(pHomeCover, priceHome);
              const evA = evFromProbAndAmerican(pAwayCover, priceAway);
              const pickHome = (evH??-Infinity) >= (evA??-Infinity);
              const line = pickHome ? Number(sprHome) : Number(sprAway!=null ? sprAway : -Number(sprHome));
              const team = pickHome ? home : away;
              const oddsPx = pickHome ? priceHome : priceAway;
              const adj = (team === home) ? (aH + line) : (aA + line);
              const opp = (team === home) ? aA : aH;
              atsRes = (Math.abs(adj - opp) < 1e-9) ? 'P' : (adj > opp ? 'W' : 'L');
              atsPick = `${team} ${line>0?'+':''}${line.toFixed(1)} ${fmtOddsAmerican(oddsPx)}`;
            }
          }
        }catch(_){ /* ignore */ }

        let totPick = '—', totRes = '—';
        try{
          if (predTotal!=null){
            const tot = toNum(mTot);
            if (tot!=null){
              const sigmaTotal = 20.0;
              const zOver = (Number(tot) - predTotal) / sigmaTotal;
              const pOver = 1 - normCdf(zOver);
              const pUnder = 1 - pOver;
              const priceOver = (toNum(odds?.total_over_price) ?? toNum(predBase?.total_over_price) ?? toNum(gc?.total_over_price) ?? -110);
              const priceUnder = (toNum(odds?.total_under_price) ?? toNum(predBase?.total_under_price) ?? toNum(gc?.total_under_price) ?? -110);
              const evO = evFromProbAndAmerican(pOver, priceOver);
              const evU = evFromProbAndAmerican(pUnder, priceUnder);
              const pickOver = (evO??-Infinity) >= (evU??-Infinity);
              const side = pickOver ? 'Over' : 'Under';
              const px = pickOver ? priceOver : priceUnder;
              totPick = `${side} ${Number(tot).toFixed(1)} ${fmtOddsAmerican(px)}`;
              totRes = (Math.abs(actTotal - Number(tot)) < 1e-9) ? 'P' : (pickOver ? (actTotal > Number(tot) ? 'W' : 'L') : (actTotal < Number(tot) ? 'W' : 'L'));
            }
          }
        }catch(_){ /* ignore */ }

        const scoreLine = `${away} ${aA.toFixed(0)} @ ${home} ${aH.toFixed(0)}`;
        const mt = (Number.isFinite(actMargin)) ? fmtSigned(actMargin, 0) : '—';

        return `
          <div class="details-block">
            <div class="subtle">Game reconciliation (final)</div>
            <div class="subtle">Actual: ${escapeHtml(scoreLine)} · Margin ${escapeHtml(mt)} · Total ${escapeHtml(actTotal.toFixed(0))}</div>
            <div class="subtle">ML: ${escapeHtml(mlPick)} ${mkMark(mlRes)}</div>
            <div class="subtle">ATS: ${escapeHtml(atsPick)} ${mkMark(atsRes)}</div>
            <div class="subtle">TOTAL: ${escapeHtml(totPick)} ${mkMark(totRes)}</div>
          </div>`;
      }catch(_){
        return '';
      }
    })();

    let quartersHtml = '';
    try{
      // Always prefer connected rep quarters (same source as Write-up/Players).
      // We lazy-load them on expand to keep page loads fast.
      const hasAnyQuarterInputs = !!(predBase && (predBase.quarters_q1_total!=null || predBase['quarters_q1_total']!=null)) || !!(simQuarters && simQuarters.length) || !!pl;
      if (hasAnyQuarterInputs){
        const cardId = `q-${dateStr}-${home}-${away}`.replace(/[^a-zA-Z0-9-]/g, '');
        quartersHtml = `
          <div class="quarters-block">
            <div class="quarters-toggle cursor-pointer fw-600" data-q-toggle="${cardId}" onclick="toggleQuarters('${cardId}','${dateStr}','${home}','${away}')">▶ Quarters</div>
            <div id="${cardId}" class="quarters-content" style="display:none;"></div>
          </div>`;
      }
    }catch(_){ /* ignore */ }

    const writeupId = `w-${dateStr}-${home}-${away}`.replace(/[^a-zA-Z0-9-]/g, '');
    const writeupHtml = `
      <div class="writeup-block">
        <div class="writeup-toggle cursor-pointer fw-600" onclick="toggleWriteup('${writeupId}','${dateStr}','${home}','${away}')">▶ Write-up</div>
        <div id="${writeupId}" class="writeup-content" style="display:none;"></div>
      </div>`;

    const node = document.createElement('div');
    node.className = 'card card-v2';
    node.setAttribute('data-home-abbr', home);
    node.setAttribute('data-away-abbr', away);
    node.setAttribute('data-status', isFinal ? 'final' : 'live');

    const book = (odds && odds.bookmaker) ? String(odds.bookmaker) : '';
    const bookTxt = book ? ` · ${book}` : '';

    // Top props picks table (from props recommendations engine)
    let propsTableHtml = '';
    try{
      const normName = (s)=> String(s||'').trim().toLowerCase().replace(/\s+/g,' ');
      const normTri = (s)=>{
        const x = String(s||'').trim().toUpperCase();
        if (!x) return '';
        // Common 2-letter/alt-code normalizations
        const map = {
          'GS': 'GSW',
          'SA': 'SAS',
          'NO': 'NOP',
          'NY': 'NYK',
          'BK': 'BKN',
          'PHO': 'PHX',
          'BRK': 'BKN',
        };
        return map[x] || x;
      };
      const officialByName = new Map();
      try{
        const rows = Array.isArray(state.reconProps) ? state.reconProps : [];
        for (const r of rows){
          const tm = normTri(r?.team_abbr);
          if (!tm || (tm !== normTri(away) && tm !== normTri(home))) continue;
          const nm = normName(r?.player_name);
          if (!nm) continue;
          officialByName.set(`${tm}|${nm}`, r);
        }
      }catch(_){ /* ignore */ }

      const officialActualFor = (p)=>{
        try{
          const tm = String(p.team||'').toUpperCase();
          const nm = normName(p.player);
          const row = officialByName.get(`${tm}|${nm}`) || null;
          if (!row) return null;
          const m = String(p.market||'').trim().toLowerCase();
          const pts = toNum(row.pts);
          const reb = toNum(row.reb);
          const ast = toNum(row.ast);
          const threes = toNum(row.threes);
          const pra = toNum(row.pra);

          if (m === 'pts') return pts;
          if (m === 'reb') return reb;
          if (m === 'ast') return ast;
          if (m === 'threes') return threes;
          if (m === 'pra') return pra;

          // Derived combos (fallback if not explicitly present)
          if (m === 'pr'){
            const v = toNum(row.pr);
            if (v!=null) return v;
            if (pts!=null && reb!=null) return pts + reb;
            return null;
          }
          if (m === 'pa'){
            const v = toNum(row.pa);
            if (v!=null) return v;
            if (pts!=null && ast!=null) return pts + ast;
            return null;
          }
          if (m === 'ra'){
            const v = toNum(row.ra);
            if (v!=null) return v;
            if (reb!=null && ast!=null) return reb + ast;
            return null;
          }

          return null;
        }catch(_){ return null; }
      };

      const gradePick = (p)=>{
        try{
          if (!isFinal) return null;
          const actual = officialActualFor(p);
          const line = Number(p.line);
          if (actual==null || !Number.isFinite(line)) return null;
          const side = String(p.side||'').trim().toUpperCase();
          let res = '—';
          if (Math.abs(actual - line) < 1e-9) res = 'P';
          else if (side === 'OVER') res = (actual > line) ? 'W' : 'L';
          else if (side === 'UNDER') res = (actual < line) ? 'W' : 'L';
          else return null;
          const cls = (res === 'W') ? 'ok' : (res === 'L') ? 'bad' : 'push';
          return { res, cls, actual };
        }catch(_){ return null; }
      };

      const picks = [];
      const h = normTri(home);
      const a = normTri(away);

      for (const r of propsRowsAll){
        if (!r || typeof r !== 'object') continue;
        const rt = normTri(r.team);
        if (rt !== h && rt !== a) continue;

        const rh = normTri(r.home_tricode);
        const ra = normTri(r.away_tricode);
        const matchupOk = (rh && ra) ? ((rh === h && ra === a) || (rh === a && ra === h)) : true;
        if (!matchupOk) continue;

        const tp = (r.top_play && typeof r.top_play === 'object') ? r.top_play : null;
        if (!tp) continue;
        const market = String(tp.market||'').trim();
        const side = String(tp.side||'').trim();
        const line = tp.line;
        const book = String(tp.book||'').trim();
        const modelProb = toNum(tp.model_prob);
        const impliedProb = toNum(tp.implied_prob);
        const evPct = (()=>{
          const x = Number(tp.ev_pct);
          if (Number.isFinite(x)) return x;
          const y = Number(tp.ev);
          if (Number.isFinite(y)) return y * 100;
          return null;
        })();
        const pick = {
          player: String(r.player||'').trim(),
          team: rt,
          market,
          side,
          line,
          evPct,
          book,
          proj: toNum(r.top_play_baseline),
          modelProb,
          impliedProb,
        };
        const g = gradePick(pick);
        if (g) {
          pick.grade = g;
        }
        picks.push(pick);
      }

      const fmtLine = (x)=>{
        const n = Number(x);
        if (Number.isFinite(n)) return (Math.abs(n - Math.round(n)) < 1e-6) ? String(Math.round(n)) : n.toFixed(1);
        const s = String(x ?? '').trim();
        return s ? s : '—';
      };
      const fmtEvPct = (x)=>{
        const n = Number(x);
        if (!Number.isFinite(n)) return '—';
        return `${n.toFixed(1)}%`;
      };
      const fmtNum = (x)=>{
        if (x === null || x === undefined) return null;
        const s = String(x).trim();
        if (!s || s.toLowerCase() === 'nan' || s.toLowerCase() === 'none') return null;
        const n = Number(s);
        if (!Number.isFinite(n)) return null;
        return (Math.abs(n - Math.round(n)) < 1e-6) ? String(Math.round(n)) : n.toFixed(1);
      };
      const fmtProjVsLine = (proj, line)=>{
        const p = fmtNum(proj);
        const l = fmtNum(line);
        if (!p && !l) return '—';
        if (p && !l) return p;
        if (!p && l) return `— / ${l}`;
        const dp = (proj == null || line == null) ? null : (Number(proj) - Number(line));
        const dps = (Number.isFinite(dp)) ? ((dp >= 0 ? '+' : '') + (Math.abs(dp - Math.round(dp)) < 1e-6 ? String(Math.round(dp)) : dp.toFixed(1))) : null;
        const delta = dps ? ` <span class="subtle">(${escapeHtml(dps)})</span>` : '';
        return `${escapeHtml(p)} / ${escapeHtml(l)}${delta}`;
      };
      const fmtPct = (x)=>{
        const n = Number(x);
        if (!Number.isFinite(n)) return '—';
        return `${(n*100).toFixed(1)}%`;
      };
      const pickText = (p)=>{
        const mk = String(p.market||'').trim();
        const sd = String(p.side||'').trim().toUpperCase();
        const ln = fmtLine(p.line);
        const mkTxt = mk ? mk.toLowerCase() : 'prop';
        const bits = [mkTxt];
        if (sd) bits.push(sd);
        if (ln && ln !== '—') bits.push(ln);
        return bits.join(' ');
      };

      picks.sort((x,y)=> (Number(y.evPct)||-1e18) - (Number(x.evPct)||-1e18));
      const top = picks.slice(0, 10);
      const used = state.propsRecsDate && String(state.propsRecsDate) !== String(dateStr)
        ? ` <span class="subtle">(using ${escapeHtml(String(state.propsRecsDate))})</span>`
        : '';
      if (top.length){
        const rowsHtml = top.map(p=>{
          const player = escapeHtml(p.player || '');
          const team = escapeHtml(p.team || '');
          const pick = escapeHtml(pickText(p));
          const projCell = fmtProjVsLine(p.proj, p.line);
          const ev = escapeHtml(fmtEvPct(p.evPct));
          const bk = escapeHtml((p.book || '').toUpperCase());
          const grade = p.grade || null;
          const probCell = (()=>{
            const mp = p.modelProb;
            const ip = p.impliedProb;
            if (mp == null || !Number.isFinite(Number(mp))) return '<span class="subtle">—</span>';
            const title = (ip != null && Number.isFinite(Number(ip))) ? `Implied ${(Number(ip)*100).toFixed(1)}%` : '';
            return `<span title="${escapeHtml(title)}">${escapeHtml(fmtPct(mp))}</span>`;
          })();
          const resCell = (()=>{
            if (!isFinal) return '<span class="subtle">—</span>';
            if (!grade) return '<span class="subtle">—</span>';
            const title = (grade.actual!=null && Number.isFinite(Number(grade.actual))) ? `Actual ${Number(grade.actual).toFixed(1)}` : 'Actual —';
            const act = (grade.actual!=null && Number.isFinite(Number(grade.actual))) ? `<span class=\"subtle\" style=\"margin-left:6px;\">${Number(grade.actual).toFixed(1)}</span>` : '';
            return `<span class="mark ${grade.cls}" title="${escapeHtml(title)}">${escapeHtml(grade.res)}</span>${act}`;
          })();
          return `
            <tr>
              <td style="font-weight:700;">${player}${team?` <span class=\"subtle\">(${team})</span>`:''}</td>
              <td>${pick}</td>
              <td class="num">${projCell}</td>
              <td class="num">${ev}</td>
              <td class="num">${probCell}</td>
              <td>${bk || '—'}</td>
              <td class="num">${resCell}</td>
            </tr>`;
        }).join('');
        propsTableHtml = `
          <div class="details-block">
            <div class="subtle">Top props picks${used}</div>
            <div class="table-wrap">
              <table class="data-table boxscore-table" style="margin-top:8px;">
                <thead>
                  <tr>
                    <th>Player</th>
                    <th>Pick</th>
                    <th class="num">Proj/Line</th>
                    <th class="num">EV%</th>
                    <th class="num">P(win)</th>
                    <th>Book</th>
                    <th class="num">Recon</th>
                  </tr>
                </thead>
                <tbody>${rowsHtml}</tbody>
              </table>
            </div>
          </div>`;
      } else {
        const anyTeamRows = propsRowsAll.some(r=>{
          try{
            const rt = normTri(r?.team);
            return rt === h || rt === a;
          }catch(_){ return false; }
        });
        if (anyTeamRows){
          propsTableHtml = `
            <div class="details-block">
              <div class="subtle">Top props picks${used}</div>
              <div class="subtle">No standard recommendations found for this matchup.</div>
            </div>`;
        }
      }
    }catch(_){ /* ignore */ }

    node.innerHTML = `
      <div class="row head">
        <div class="matchup">
          <div class="team-line away">
            ${teamLineHTML(away)}
            <div class="score js-live-away">${(actualAway!=null && !isNaN(actualAway))?actualAway:'—'}</div>
          </div>
          <div class="team-line home">
            ${teamLineHTML(home)}
            <div class="score js-live-home">${(actualHome!=null && !isNaN(actualHome))?actualHome:'—'}</div>
          </div>
        </div>
        <div class="meta">
          <div class="state js-live-status">${seedStatus}</div>
          <div class="subtle"><span class="js-live-period"></span> <span class="js-live-tleft"></span></div>
          <div class="subtle">${when}${venue?` · ${venue}`:''}${bookTxt}</div>
        </div>
        <div class="tier-badge">Tier ${tier}</div>
      </div>

      <div class="model-strip">
        <div class="kv"><div class="k">Home WP (cal)</div><div class="v">${fmtPct(pHomeCal,1)}</div></div>
        <div class="kv"><div class="k">Away WP (cal)</div><div class="v">${fmtPct(pAwayCal,1)}</div></div>
        <div class="kv"><div class="k">Model Margin</div><div class="v">${predMargin!=null?fmtSigned(predMargin,1):'—'}</div></div>
        <div class="kv"><div class="k">Model Total</div><div class="v">${predTotal!=null?Number(predTotal).toFixed(1):'—'}</div></div>
      </div>

      ${(marketLine || blendLine || recapDetails) ? `
        <div class="details-block">
          ${marketLine?`<div class="subtle">${marketLine}</div>`:''}
          ${blendLine?`<div class="subtle">${blendLine}</div>`:''}
          ${recapDetails || ''}
        </div>
      `:''}

      ${gameReconHtml || ''}

      ${propsTableHtml || ''}

      <div class="market-grid">
        ${tile('ML', mlMain, mlMeta)}
        ${tile('ATS', atsMain, atsMeta)}
        ${tile('TOTAL', totMain, totMeta)}
      </div>

      ${quartersHtml}
      ${writeupHtml}
    `;

    wrap.appendChild(node);
  }
  }catch(e){
    try{
      console.error('renderDate error', e);
      setDebugLine(`Debug: renderDate failed (${dateStr}) ${(e && e.message) ? e.message : e}`);
      const note = document.getElementById('note');
      if (note){
        note.textContent = 'Render error (see debug line / console).';
        note.classList.remove('hidden');
      }
    }catch(_){ /* ignore */ }
  }

  // Start or refresh live polling for this date
  try {
    startScoreboardPolling(dateStr);
    startOddsReload(dateStr);
  } catch(_){ }

  // Legacy time formatter is a no-op for v2, but harmless.
  try{ formatEtTimesInCards(); }catch(_){ }
}

// --- Live polling (scoreboard) ---
async function pollScoreboardOnce(dateStr){
  try{
    const url = new URL('/api/scoreboard', window.location.origin);
    url.searchParams.set('date', dateStr);
    let j = null;
    try{
      // Add a hard timeout to avoid hanging the UI if the server call stalls
      const ac = new AbortController();
      const t = setTimeout(()=> ac.abort(), 7000);
      try{
        const r = await fetch(url.toString(), { cache: 'no-store', signal: ac.signal });
        if (r.ok) j = await r.json();
      } finally { clearTimeout(t); }
    }catch(_){ /* ignore */ }
    // If backend failed or returned empty, fallback to CDN directly
    if (!j || j.error || !Array.isArray(j.games) || j.games.length === 0){
      try{
        const alt = await fetchCdnScoreboard(dateStr);
        if (alt) j = alt;
      }catch(_){ /* ignore */ }
    }
    if (!j) throw new Error('no scoreboard payload');
    state.poll.lastPayload = j;
    // Heartbeat UI
    try{
      const hb = document.getElementById('hbDot');
      const ts = document.getElementById('last-sb');
      if (hb){ hb.classList.remove('pulse'); void hb.offsetWidth; hb.classList.add('pulse'); }
      if (ts){ ts.textContent = new Date().toLocaleTimeString(); }
    }catch(_){/* ignore */}
    if (!j || !Array.isArray(j.games)) return;
    const map = new Map();
    for (const g of j.games){
      const home = String(g.home||'').toUpperCase();
      const away = String(g.away||'').toUpperCase();
      map.set(`${home}|${away}`, g);
    }
    const cards = Array.from(document.querySelectorAll('.card'));
    const isToday = (dateStr === localYMD());
    let anyFinalized = false;
    for (const c of cards){
      const home = c.getAttribute('data-home-abbr');
      const away = c.getAttribute('data-away-abbr');
      const key = `${home}|${away}`;
      const g = map.get(key);
      if (!g) continue;
      // Update status
      const stateEl = c.querySelector('.js-live-status') || c.querySelector('.row.head .state');
      if (stateEl && g.status){
        const up = String(g.status);
        stateEl.textContent = up;
        const crit = /End|Final|FINAL|OT/i.test(up) ? false : /Q4|4th/i.test(up);
        if (crit) stateEl.classList.add('crit'); else stateEl.classList.remove('crit');
        // Parse into period/time-left
        const perEl = c.querySelector('.js-live-period') || c.querySelector('.row.head .period-pill');
        const tEl = c.querySelector('.js-live-tleft') || c.querySelector('.row.head .time-left');
        if (perEl && tEl){
          let period = '';
          let tleft = '';
          const s = up.toUpperCase();
          if (/FINAL/.test(s)) { period = 'FINAL'; tleft = ''; }
          else if (/HALF/.test(s)) { period = 'HT'; tleft = ''; }
          else if (/END\s*Q(\d)/.test(s)) { const m=/END\s*Q(\d)/.exec(s); period = `Q${m[1]}`; tleft='END'; }
          else if (/(Q\d)\s+(\d\d?:\d\d)/.test(up)) { const m=/(Q\d)\s+(\d\d?:\d\d)/.exec(up); period = m[1].toUpperCase(); tleft = m[2]; }
          else if (/OT/.test(s)) { period = 'OT'; tleft = ''; }
          perEl.textContent = period;
          tEl.textContent = tleft;
          // critical style if late Q4 or OT with small time left
          const late = (period==='Q4' && tleft) || period==='OT';
          if (late) { perEl.classList.add('crit'); tEl.classList.add('crit'); } else { perEl.classList.remove('crit'); tEl.classList.remove('crit'); }
          if (period==='HT') { perEl.classList.add('int'); } else { perEl.classList.remove('int'); }
        }
      }
      // Update scores
      const a = c.querySelector('.js-live-away');
      const h = c.querySelector('.js-live-home');
      if (a && g.away_pts!=null) a.textContent = String(g.away_pts);
      if (h && g.home_pts!=null) h.textContent = String(g.home_pts);
      // Update final badge and card class if now final (but not for today's date)
      if (!isToday && g.final){
        c.setAttribute('data-status','final');
        if (!/final/i.test(stateEl?.textContent||'')) {
          anyFinalized = true;
        }
        // Inject into recon map so results/accuracy can render without waiting for CSV
        try{
          const fullKey = `${dateStr}|${home}|${away}`;
          const recon = state.reconByKey.get(fullKey) || {};
          const hp = (g.home_pts!=null? Number(g.home_pts) : recon.home_pts);
          const ap = (g.away_pts!=null? Number(g.away_pts) : recon.visitor_pts);
          state.reconByKey.set(fullKey, { ...recon, home_pts: hp, visitor_pts: ap });
        }catch(_){/* ignore */}
        // If results toggle is on, re-render this card’s results details by forcing minimal refresh
        try{
          const showResults = document.getElementById('resultsToggle')?.checked;
          if (showResults){
            // Quick patch: append FINAL badge if missing
            const head = c.querySelector('.row.head');
            if (head && !head.querySelector('.result-badge')){
              const b = document.createElement('div');
              b.className = 'result-badge';
              b.textContent = 'Final';
              head.appendChild(b);
            }
          }
        }catch(_){/* ignore */}
      } else {
        c.setAttribute('data-status','live');
      }
    }
    // If any game just finalized and results view is enabled, refresh recon and re-render once
    if (anyFinalized){
      try{
        const showResults = document.getElementById('resultsToggle')?.checked;
        if (showResults){
          await maybeLoadRecon(dateStr);
          renderDate(dateStr);
        }
      }catch(_){/* ignore */}
    }
  }catch(e){
    // soft-fail
  }
}

async function fetchCdnScoreboard(dateStr){
  try{
    const ymd = String(dateStr||'').replaceAll('-', '');
    const u = `https://data.nba.com/data/10s/prod/v1/${ymd}/scoreboard.json`;
    const r = await fetch(u, { cache: 'no-store' });
    let ok = false; let games = [];
    if (r.ok){
      try{
        const j = await r.json();
        const arr = Array.isArray(j?.games) ? j.games : [];
        for (const g of arr){
          try{
            const home = String(g?.hTeam?.triCode||'').toUpperCase();
            const away = String(g?.vTeam?.triCode||'').toUpperCase();
            const sc_h = g?.hTeam?.score; const sc_a = g?.vTeam?.score;
            const hp = (sc_h!==undefined && sc_h!==null && sc_h!=='') ? Number(sc_h) : null;
            const ap = (sc_a!==undefined && sc_a!==null && sc_a!=='') ? Number(sc_a) : null;
            const statusNum = Number(g?.statusNum||0);
            const clock = String(g?.clock||'');
            const period = Number(g?.period?.current||0);
            const is_ht = !!g?.period?.isHalftime;
            const is_eop = !!g?.period?.isEndOfPeriod;
            let status_txt = 'Scheduled'; let is_final = false;
            if (statusNum === 3){ status_txt = 'Final'; is_final = true; }
            else if (statusNum === 2){
              if (is_ht) status_txt = 'Half';
              else if (is_eop && period) status_txt = `End Q${period}`;
              else if (period && clock) status_txt = `Q${period} ${clock}`;
              else if (period) status_txt = `Q${period}`;
              else status_txt = 'LIVE';
            } else {
              status_txt = g?.startTimeUTC || 'Scheduled';
            }
            games.push({ home, away, status: status_txt, game_id: g?.gameId, home_pts: hp, away_pts: ap, final: is_final });
          }catch(_){ /* ignore */ }
        }
        ok = true;
      }catch(_){ /* ignore */ }
    }
    // If prod/v1 empty or not ok, try liveData today's scoreboard (works for ET "today").
    if (!ok || games.length === 0){
      try{
        const u2 = 'https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json';
        const r2 = await fetch(u2, { cache: 'no-store' });
        if (r2.ok){
          const j2 = await r2.json();
          const sb = j2?.scoreboard || {};
          const arr2 = Array.isArray(sb.games) ? sb.games : [];
          const out = [];
          for (const g of arr2){
            try{
              const home = String(g?.homeTeam?.teamTricode||'').toUpperCase();
              const away = String(g?.awayTeam?.teamTricode||'').toUpperCase();
              const sc_h = g?.homeTeam?.score; const sc_a = g?.awayTeam?.score;
              const hp = (sc_h!==undefined && sc_h!==null && sc_h!=='') ? Number(sc_h) : null;
              const ap = (sc_a!==undefined && sc_a!==null && sc_a!=='') ? Number(sc_a) : null;
              const txt = String(g?.gameStatusText||'').trim();
              const is_final = /^final/i.test(txt);
              out.push({ home, away, status: txt||'Scheduled', game_id: g?.gameId, home_pts: hp, away_pts: ap, final: is_final });
            }catch(_){/* ignore */}
          }
          if (out.length){
            return { date: dateStr, games: out };
          }
        }
      }catch(_){ /* ignore */ }
    }
    return { date: dateStr, games };
  }catch(_){ return null; }
}

function startScoreboardPolling(dateStr){
  try{
    state.poll.date = dateStr;
    if (state.poll.timer) clearInterval(state.poll.timer);
    // Skip polling if there are no visible games
    const count = (state.byDate.get(dateStr) || []).length;
    if (!count) return;
    // Poll every 20s
    pollScoreboardOnce(dateStr);
    state.poll.timer = setInterval(()=> pollScoreboardOnce(dateStr), 20000);
  }catch(_){/* ignore */}
}

// Periodically reload odds CSVs for the selected date (client-only) so cards update when
// the server writes new lines; avoids requiring manual "Refresh Odds" clicks.
async function reloadOddsIfChanged(dateStr){
  try{
    // Build a tiny hash from the concatenation of text contents of available odds files
    const sources = [
      `/data/processed/closing_lines_${dateStr}.csv`,
      `/data/processed/odds_${dateStr}.csv`,
      `/data/processed/market_${dateStr}.csv`,
      `/data/processed/game_odds_${dateStr}.csv`,
    ];
    let agg = '';
    await Promise.all(sources.map(async (u)=>{
      try{ const r = await fetch(`${u}?v=${Date.now()}`, { cache: 'no-store' }); if (r.ok){ const t = await r.text(); if (t && t.trim()) agg += `\n# ${u}\n` + t.split(/\r?\n/).slice(0,5).join('\n'); } }catch(_){/* ignore */}
    }));
    const hash = (typeof btoa === 'function') ? btoa(unescape(encodeURIComponent(agg))).slice(0,64) : String(agg.length);
    if (hash && hash !== state.poll.oddsHash){
      state.poll.oddsHash = hash;
      // Reload odds and re-render
      await maybeLoadOdds(dateStr);
      renderDate(dateStr);
    }
  }catch(_){/* ignore */}
}

function startOddsReload(dateStr){
  try{
    if (state.poll.oddsTimer) clearInterval(state.poll.oddsTimer);
    // Only auto-reload for today's date, to avoid thrashing historical views
    const today = localYMD();
    if (dateStr !== today) return;
    // Kick once immediately, then every 60s
    reloadOddsIfChanged(dateStr);
    state.poll.oddsTimer = setInterval(()=> reloadOddsIfChanged(dateStr), 60000);
  }catch(_){/* ignore */}
}

function setupControls(){
  const picker = document.getElementById('datePicker');
  const applyBtn = document.getElementById('applyBtn');
  const todayBtn = document.getElementById('todayBtn');
  const refreshBtn = document.getElementById('refreshOddsBtn');
  const dates = Array.from(state.byDate.keys()).sort();
  const sched = Array.isArray(state.scheduleDates) ? state.scheduleDates : dates;
  const today = localYMD();
  const paramDate = getQueryParam('date');
  if (!picker){
    setDebugLine('Debug: missing #datePicker element');
    return;
  }

  // Always show a sane default date immediately, even if schedule loading fails.
  try{
    if (!picker.value) picker.value = (paramDate || today);
  }catch(_){ /* ignore */ }

  if (!dates.length){
    setDebugLine(`Debug: schedule not loaded (byDate.size=${state.byDate ? state.byDate.size : 'n/a'}, schedule.len=${Array.isArray(state.schedule)?state.schedule.length:0})`);
    const note = document.getElementById('note');
    if (note){
      note.textContent = 'Schedule not loaded yet; cannot render game cards.';
      note.classList.remove('hidden');
    }
    return;
  }
  picker.min = dates[0]; picker.max = dates[dates.length-1];
  // Default to the nearest scheduled date to 'today'
  const nearestScheduled = (target)=>{
    const arr = sched;
    if (!arr || arr.length === 0) return null;
    if (arr.includes(target)) return target;
    const t = parseYMDLocal(target);
    let best = arr[0];
    let bestDiff = Math.abs(parseYMDLocal(arr[0]) - t);
    for (let i=1;i<arr.length;i++){
      const diff = Math.abs(parseYMDLocal(arr[i]) - t);
      if (diff < bestDiff){ bestDiff = diff; best = arr[i]; }
    }
    return best;
  };
  // Find the next date with games (forward-looking only)
  const nextGameDate = (fromDate)=>{
    const arr = sched;
    if (!arr || arr.length === 0) return null;
    // Find the first date after fromDate that has games
    for (let i = 0; i < arr.length; i++) {
      if (arr[i] > fromDate && (state.byDate.get(arr[i]) || []).length > 0) {
        return arr[i];
      }
    }
    return null; // No future games found
  };
  // Find the most recent date on/before fromDate that has games
  const lastGameDateOnOrBefore = (fromDate)=>{
    const arr = sched;
    if (!arr || arr.length === 0) return null;
    for (let i = arr.length - 1; i >= 0; i--) {
      const ds = arr[i];
      if (ds <= fromDate && (state.byDate.get(ds) || []).length > 0) return ds;
    }
    return null;
  };
  const defaultDate = (function(){
    if (paramDate) return paramDate;
    const hasToday = (state.byDate.get(today) || []).length > 0;
    if (hasToday) return today;
    const last = lastGameDateOnOrBefore(today);
    return (last || nearestScheduled(today) || (dates.includes(PIN_DATE) ? PIN_DATE : dates[0]));
  })();
  picker.value = defaultDate;
  setDebugLine(`Debug: init selected=${defaultDate} byDate.size=${state.byDate.size} schedule.len=${Array.isArray(state.schedule)?state.schedule.length:0}`);
  // Mirror NHL UX: default "Show results" to ON for past dates on initial load
  try {
    const resToggleInit = document.getElementById('resultsToggle');
    if (resToggleInit) {
      // Compare as YYYY-MM-DD strings (lex order matches date order) using local calendar date
      if (defaultDate < today) resToggleInit.checked = true;
    }
  } catch(_) { /* ignore */ }
  const go = async ()=>{
    let d = picker.value;
    const preCount = (state.byDate.get(d) || []).length;
    setDebugLine(`Debug: go() selected=${d} preGames=${preCount} byDate.size=${state.byDate.size}`);
    // In static mode allow any requested date; if strict, snap to nearest scheduled
    if (STRICT_SCHEDULE_DATES) {
      const hasGames = (state.byDate.get(d) || []).length > 0;
      if (!hasGames) {
        const near = nearestScheduled(d);
        if (near && near !== d) {
          d = near; picker.value = near;
        }
      }
    } else if (AUTO_FALLBACK_TO_LAST_GAME) {
      // Fall back to the most recent prior slate date if selected date has no games
      const hasGames = (state.byDate.get(d) || []).length > 0;
      if (!hasGames) {
        const last = lastGameDateOnOrBefore(d);
        if (last && last !== d) {
          d = last;
          picker.value = last;
        }
      }
    }
    const requested = d;
    const stillOnDate = ()=>{
      const pickerNow = document.getElementById('datePicker');
      return !pickerNow || pickerNow.value === requested;
    };

    // Render immediately from schedule so the UI never looks empty while optional per-date
    // datasets load (predictions/odds/game_cards/etc).
    try{ renderDate(requested); }catch(_){ /* ignore */ }

    const note = document.getElementById('note');
    try{
      if (note && (state.byDate.get(requested) || []).length > 0){
        note.textContent = 'Loading market/model data…';
        note.classList.remove('hidden');
      }
    }catch(_){ /* ignore */ }

    const withTimeout = (p, ms)=>{
      try{
        return Promise.race([
          Promise.resolve(p),
          new Promise((_, rej)=> setTimeout(()=> rej(new Error('timeout')), ms))
        ]);
      }catch(e){
        return Promise.reject(e);
      }
    };

    // Load core datasets in parallel; each one gets a soft timeout so nothing can block forever.
    try{
      await Promise.allSettled([
        withTimeout(maybeLoadPredictions(requested), 8000),
        withTimeout(maybeLoadOdds(requested), 8000),
        withTimeout(maybeLoadGameCards(requested), 8000),
        withTimeout(maybeLoadPeriodLines(requested), 8000),
        withTimeout(maybeLoadGamesCalibration(), 8000),
        withTimeout(maybeLoadFirstBasketRecs(requested), 8000),
        withTimeout(maybeLoadPropsRecommendations(requested), 8000),
        withTimeout(maybeLoadRecon(requested), 8000),
      ]);
    }catch(_){ /* ignore */ }

    // Re-render now that most data should be available.
    try{ if (stillOnDate()) renderDate(requested); }catch(_){ /* ignore */ }

    // Load heavier optional datasets asynchronously and re-render when ready.
    try{
      maybeLoadSimQuarters(requested).then(()=>{ if (stillOnDate()) renderDate(requested); }).catch(()=>{});
      maybeLoadPropsPredictions(requested).then(()=>{ if (stillOnDate()) renderDate(requested); }).catch(()=>{});
    }catch(_){ /* ignore */ }
    try{
      const postCount = (state.byDate.get(requested) || []).length;
      const hasKey = state.byDate.has(requested);
      setDebugLine(`Debug: rendered=${requested} games=${postCount} hasKey=${hasKey?1:0} byDate.size=${state.byDate.size} schedule.len=${Array.isArray(state.schedule)?state.schedule.length:0}`);
    }catch(_){ /* ignore */ }
    // Also refresh the YTD summary through the selected date, if the host page exposes it
    try{
      if (typeof window.updateYtdSummary === 'function'){
        window.updateYtdSummary(requested);
      }
    }catch(_){/* ignore */}
    if (note) {
      if ((state.byDate.get(requested) || []).length === 0) {
        note.textContent = `No games on ${requested}.`;
        note.classList.remove('hidden');
      } else {
        note.classList.add('hidden');
      }
    }
  };
  const apply = ()=> { go(); };
  picker.addEventListener('change', ()=>{}); // wait for Apply to match NHL UX
  if (applyBtn) applyBtn.addEventListener('click', apply);
  const resToggle = document.getElementById('resultsToggle');
  if (resToggle) resToggle.addEventListener('change', go);
  if (refreshBtn) refreshBtn.addEventListener('click', async ()=>{
    const d = picker.value;
    try{
      const url = new URL('/api/cron/refresh-bovada', window.location.origin);
      url.searchParams.set('date', d);
      // If an admin key is configured, the server requires it; allow via prompt or skip
      const key = sessionStorage.getItem('ADMIN_KEY') || '';
      const headers = key ? {'X-Admin-Key': key} : {};
      const resp = await fetch(url.toString(), { method: 'POST', headers });
      if (!resp.ok){ console.warn('Refresh failed', await resp.text()); }
    }catch(e){ console.warn('Refresh error', e); }
    // Re-load odds and re-render
    await maybeLoadOdds(d);
    renderDate(d);
  });
  todayBtn.addEventListener('click', ()=>{
    // Prefer today if it has games; otherwise fall back to last slate date.
    const hasToday = (state.byDate.get(today) || []).length > 0;
    if (hasToday) {
      picker.value = today;
    } else {
      const last = lastGameDateOnOrBefore(today);
      if (last) {
        picker.value = last;
      } else {
        const near = (function(){
          const t = parseYMDLocal(today);
          const arr = sched;
          if (!arr || arr.length === 0) return dates[0];
          let best = arr[0]; let bestDiff = Math.abs(parseYMDLocal(arr[0]) - t);
          for (let i=1;i<arr.length;i++){
            const diff = Math.abs(parseYMDLocal(arr[i]) - t);
            if (diff < bestDiff){ bestDiff = diff; best = arr[i]; }
          }
          return best;
        })();
        picker.value = near;
      }
    }
    picker.dispatchEvent(new Event('change'));
  });
  go();
}

(async function init(){
  // Populate the date picker immediately so the UI never shows a blank date while data loads.
  try{
    const picker = document.getElementById('datePicker');
    if (picker && !picker.value) picker.value = (getQueryParam('date') || localYMD());
  }catch(_){ /* ignore */ }
  try{
    // Make renderers accessible even if bundling/scoping changes later.
    window.renderDate = renderDate;
    window.renderDateLegacy = renderDateLegacy;
  }catch(_){ /* ignore */ }
  await loadTeams();
  await loadSchedule();
  // Ensure the pinned date is available in the selector by seeding from predictions if needed
  await maybeInjectPinnedDate(PIN_DATE);
  setupControls();
})();
