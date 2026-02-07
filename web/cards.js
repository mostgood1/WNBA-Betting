// Cards page renderer (SmartSim-driven).
// Includes: odds, bet leans, quarter projections, projected boxscores, prop targets, and matchup write-up.

function localYMD() {
  try {
    return new Intl.DateTimeFormat('en-CA', {
      timeZone: 'America/New_York',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).format(new Date());
  } catch (_) {
    const d = new Date();
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }
}

function isYmd(s) {
  return typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s);
}

function n(x) {
  const v = Number(x);
  return Number.isFinite(v) ? v : null;
}

function fmt(x, digits = 1) {
  const v = n(x);
  return v == null ? '—' : v.toFixed(digits);
}

function pct(x, digits = 1) {
  const v = n(x);
  return v == null ? '—' : `${(v * 100).toFixed(digits)}%`;
}

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  }[c]));
}

function logoImg(tri) {
  const t = String(tri || '').toUpperCase().trim();
  if (!t || t.length !== 3) return '';
  return `<img class="logo" src="/web/assets/logos/${encodeURIComponent(t)}.svg" alt="${esc(t)}" loading="lazy" />`;
}

async function fetchJson(url) {
  const r = await fetch(url, { cache: 'no-store' });
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return await r.json();
}

async function fetchText(url) {
  const r = await fetch(url, { cache: 'no-store' });
  if (!r.ok) return '';
  return await r.text();
}

function csvParse(text) {
  const lines = String(text || '').trim().split(/\r?\n/);
  if (!lines.length) return [];
  const header = lines[0].split(',').map((x) => x.trim());
  const out = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;
    const parts = line.split(',');
    const row = {};
    for (let j = 0; j < header.length; j++) row[header[j]] = parts[j];
    out.push(row);
  }
  return out;
}

function setNote(msg) {
  const note = document.getElementById('note');
  if (!note) return;
  if (!msg) {
    note.classList.add('hidden');
    note.textContent = '';
    return;
  }
  note.textContent = String(msg);
  note.classList.remove('hidden');
}

function buildReconIndex(rows) {
  const idx = new Map();
  for (const r of rows || []) {
    const ht = String(r.home_tri || '').toUpperCase().trim();
    const at = String(r.away_tri || '').toUpperCase().trim();
    const home = String(r.home_team || '').trim();
    const away = String(r.visitor_team || '').trim();
    if (ht && at) idx.set(`${ht}|${at}`, r);
    if (home && away) idx.set(`${home}|${away}`, r);
  }
  return idx;
}

function fmtAmer(x) {
  const v = n(x);
  if (v == null) return '—';
  const r = Math.round(v);
  return r > 0 ? `+${r}` : `${r}`;
}

function fmtTime(iso) {
  if (!iso) return '';
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return String(iso);
    return d.toLocaleString(undefined, { hour: 'numeric', minute: '2-digit' });
  } catch (_) {
    return String(iso);
  }
}

function bestBets(b) {
  if (!b) return [];
  const cands = [
    { key: 'home_ml_ev', label: 'ML Home', p: b.p_home_win, ev: b.home_ml_ev },
    { key: 'away_ml_ev', label: 'ML Away', p: b.p_away_win, ev: b.away_ml_ev },
    { key: 'home_spread_ev', label: 'Spread Home', p: b.p_home_cover, ev: b.home_spread_ev },
    { key: 'away_spread_ev', label: 'Spread Away', p: b.p_away_cover, ev: b.away_spread_ev },
    { key: 'over_ev', label: 'Total Over', p: b.p_total_over, ev: b.over_ev },
    { key: 'under_ev', label: 'Total Under', p: b.p_total_under, ev: b.under_ev },
  ];
  const rows = cands
    .filter((x) => n(x.ev) != null)
    .sort((a, b2) => (n(b2.ev) ?? -1e9) - (n(a.ev) ?? -1e9))
    .slice(0, 3);
  return rows;
}

function renderPlayersTable(title, players) {
  const arr = Array.isArray(players) ? [...players] : [];
  // Sort by minutes first so the table reflects the expected rotation.
  arr.sort((a, b) => {
    const dm = (n(b?.min_mean) ?? -1e9) - (n(a?.min_mean) ?? -1e9);
    if (dm !== 0) return dm;
    return (n(b?.pts_mean) ?? -1e9) - (n(a?.pts_mean) ?? -1e9);
  });
  const top = arr.slice(0, 10);

  const rows = top.map((p) => {
    const nm = String(p.player_name || '').trim();
    const st = String(p.injury_status || '').trim().toUpperCase();
    const isOut = (st === 'OUT') || (p.playing_today === false);
    const inj = isOut ? ' <span class="badge bad">OUT</span>' : '';
    const play = '';
    return `
      <tr>
        <td>${esc(nm)}${inj}${play}</td>
        <td class="num">${fmt(p.min_mean, 1)}</td>
        <td class="num">${fmt(p.pts_mean, 1)}</td>
        <td class="num">${fmt(p.reb_mean, 1)}</td>
        <td class="num">${fmt(p.ast_mean, 1)}</td>
        <td class="num">${fmt(p.threes_mean, 1)}</td>
        <td class="num">${fmt(p.pra_mean, 1)}</td>
      </tr>
    `;
  }).join('');

  return `
    <div class="table-wrap">
      <table class="data-table player-boxscore">
        <thead>
          <tr>
            <th class="sortable" data-sort="text">${esc(title)}</th>
            <th class="num sortable" data-sort="num">MIN</th>
            <th class="num sortable" data-sort="num">PTS</th>
            <th class="num sortable" data-sort="num">REB</th>
            <th class="num sortable" data-sort="num">AST</th>
            <th class="num sortable" data-sort="num">3PM</th>
            <th class="num sortable" data-sort="num">PRA</th>
          </tr>
        </thead>
        <tbody>
          ${rows || '<tr><td colspan="7" class="subtle">No player projections.</td></tr>'}
        </tbody>
      </table>
    </div>
  `;
}

function renderInjurySummary(title, players) {
  const arr = Array.isArray(players) ? players : [];
  const flagged = arr
    // Only show players who are OUT (or explicitly not playing).
    .filter((p) => {
      if (!p) return false;
      const st = String(p.injury_status || '').trim().toUpperCase();
      return st === 'OUT' || p.playing_today === false;
    })
    .map((p) => {
      const name = String(p.player_name || '').trim();
      const tags = [];
      tags.push('<span class="badge bad">OUT</span>');
      return { name, tags: tags.join(' ') };
    })
    .filter((x) => x.name && x.tags)
    .sort((a, b) => a.name.localeCompare(b.name));

  return `
    <div class="injury-block">
      <div class="injury-title">${esc(title)} injury / availability</div>
      <div class="injury-lines">
        ${flagged.length
          ? flagged.map((x) => `<div class="injury-line"><span class="injury-name">${esc(x.name)}</span><span class="injury-tags">${x.tags}</span></div>`).join('')
          : '<div class="subtle">No OUT players.</div>'}
      </div>
    </div>
  `;
}

function marketLabel(m) {
  const k = String(m || '').toLowerCase().trim();
  const map = {
    pts: 'PTS',
    reb: 'REB',
    ast: 'AST',
    threes: '3PM',
    pra: 'PRA',
    pa: 'P+A',
    pr: 'P+R',
    ra: 'R+A',
  };
  return map[k] || String(m || '').toUpperCase();
}

function renderPropRecommendations(propRecs, homeTri, awayTri) {
  const recs = propRecs && typeof propRecs === 'object' ? propRecs : {};
  const home = Array.isArray(recs.home) ? recs.home : [];
  const away = Array.isArray(recs.away) ? recs.away : [];

  const rows = [...home.map((r) => ({ ...r, side: 'home' })), ...away.map((r) => ({ ...r, side: 'away' }))]
    .map((r) => {
      const sideTri = r.side === 'home' ? homeTri : awayTri;
      const player = String(r.player || '').trim();
      const b = (r && r.best && typeof r.best === 'object') ? r.best : null;
      if (!player || !b) return null;
      const picks = Array.isArray(r.picks) ? r.picks : [b];
      const pickLines = picks.slice(0, 3).map((pp) => {
        const mk = marketLabel(pp.market);
        const side = String(pp.side || '').toUpperCase();
        const line = pp.line;
        const book = String(pp.book || '').trim();
        const price = pp.price;
        const evPct = n(pp.ev_pct);
        const pwin = n(pp.p_win);
        const mu = n(pp.sim_mu);
        const bits = [
          `${esc(mk)} ${esc(side)} ${fmt(line, 1)}`,
          book ? `@ ${esc(book)}` : '',
          n(price) != null ? `(${esc(fmtAmer(price))})` : '',
          (pwin != null && mu != null) ? `p≈${pct(pwin, 0)} (μ ${fmt(mu, 1)})` : '',
          evPct != null ? `EV ${fmt(evPct, 1)}%` : '',
        ].filter(Boolean).join(' ');
        return `<div>${bits}</div>`;
      }).join('');

      return `<li><span class="badge">${esc(sideTri)}</span> ${esc(player)} — <b>${pickLines}</b></li>`;
    })
    .filter(Boolean)
    .slice(0, 18)
    .join('');

  return `
    <div class="writeup-content">
      <div class="subtle">Recommendations computed as SmartSim vs betting line (lines/books from processed recommendations).</div>
      <ul>
        ${rows || '<li class="subtle">No prop recommendations.</li>'}
      </ul>
    </div>
  `;
}

function renderQuarterTable(periods, reconQ) {
  const order = [
    ['q1', 'Q1'],
    ['q2', 'Q2'],
    ['q3', 'Q3'],
    ['q4', 'Q4'],
    ['h1', 'H1'],
    ['h2', 'H2'],
  ];

  const actualKey = {
    q1: 'actual_q1_total',
    q2: 'actual_q2_total',
    q3: 'actual_q3_total',
    q4: 'actual_q4_total',
    h1: 'actual_h1_total',
    h2: 'actual_h2_total',
  };

  const rows = order.map(([k, label]) => {
    const p = periods && periods[k] ? periods[k] : null;
    if (!p) return '';

    const act = reconQ ? n(reconQ[actualKey[k]]) : null;
    const simTot = n(p.total_mean);
    const dTot = (act != null && simTot != null) ? (simTot - act) : null;

    return `
      <tr>
        <td>${label}</td>
        <td class="num">${fmt(p.home_mean, 1)}</td>
        <td class="num">${fmt(p.away_mean, 1)}</td>
        <td class="num">${fmt(p.total_mean, 1)}</td>
        <td class="num">${act == null ? '—' : fmt(act, 1)}</td>
        <td class="num">${dTot == null ? '—' : fmt(dTot, 1)}</td>
        <td class="num">${fmt(p.market_home_spread, 1)}</td>
        <td class="num">${fmt(p.market_total, 1)}</td>
        <td class="num">${pct(p.p_home_win, 0)}</td>
        <td class="num">${pct(p.p_home_cover, 0)}</td>
        <td class="num">${pct(p.p_total_over, 0)}</td>
      </tr>
    `;
  }).filter(Boolean).join('');

  return `
    <div class="table-wrap">
      <table class="data-table boxscore-table">
        <thead>
          <tr>
            <th>Period</th>
            <th class="num">Home</th>
            <th class="num">Away</th>
            <th class="num">Total</th>
            <th class="num">Actual</th>
            <th class="num">Δ(Sim-Act)</th>
            <th class="num">Mkt Spr</th>
            <th class="num">Mkt Tot</th>
            <th class="num">P(Home)</th>
            <th class="num">P(Cover)</th>
            <th class="num">P(Over)</th>
          </tr>
        </thead>
        <tbody>
          ${rows || '<tr><td colspan="11" class="subtle">No period projections.</td></tr>'}
        </tbody>
      </table>
    </div>
  `;
}

function renderIntervalsTable(intervals) {
  const segs = intervals && Array.isArray(intervals.segments) ? intervals.segments : [];
  if (!segs.length) {
    return '<div class="subtle">No interval ladder available.</div>';
  }

  const rows = segs.map((s) => {
    const q = n(s.quarter);
    const lab = s.label || (q != null ? `Q${q}` : '');
    const mu = n(s.mu);
    const q10 = n(s.q && s.q.p10);
    const q50 = n(s.q && s.q.p50);
    const q90 = n(s.q && s.q.p90);
    const cmu = n(s.cum_mu);
    const c10 = n(s.cum_q && s.cum_q.p10);
    const c50 = n(s.cum_q && s.cum_q.p50);
    const c90 = n(s.cum_q && s.cum_q.p90);
    return `
      <tr>
        <td>${esc(lab)}</td>
        <td class="num">${fmt(mu, 2)}</td>
        <td class="num">${fmt(q10, 0)}</td>
        <td class="num">${fmt(q50, 0)}</td>
        <td class="num">${fmt(q90, 0)}</td>
        <td class="num">${fmt(cmu, 2)}</td>
        <td class="num">${fmt(c10, 0)}</td>
        <td class="num">${fmt(c50, 0)}</td>
        <td class="num">${fmt(c90, 0)}</td>
      </tr>
    `;
  }).join('');

  return `
    <div class="table-wrap">
      <table class="data-table boxscore-table">
        <thead>
          <tr>
            <th>Segment</th>
            <th class="num">μ seg</th>
            <th class="num">p10</th>
            <th class="num">p50</th>
            <th class="num">p90</th>
            <th class="num">μ cum</th>
            <th class="num">p10</th>
            <th class="num">p50</th>
            <th class="num">p90</th>
          </tr>
        </thead>
        <tbody>
          ${rows}
        </tbody>
      </table>
      <div class="subtle" style="margin-top:6px;">
        Note: segment quantiles are not additive; cumulative quantiles are computed from per-sim cumulatives.
      </div>
    </div>
  `;
}

function renderLiveLens(intervals, cardKey) {
  const segs = intervals && Array.isArray(intervals.segments) ? intervals.segments : [];
  if (!segs.length) return '';
  const id = String(cardKey || '').replace(/[^A-Za-z0-9_\-]/g, '_');

  function renderMinRemainingOptions(totalMinutes) {
    const step = 3;
    const opts = [];
    for (let m = totalMinutes; m >= 0; m -= step) {
      const lab = `${m}`;
      const sel = m === totalMinutes ? ' selected' : '';
      opts.push(`<option value="${m}"${sel}>${lab}</option>`);
    }
    return opts.join('');
  }

  function sliceSegsTable(startIdx, endIdx, title) {
    const rows = [];
    const nRows = Math.max(0, Math.min(segs.length - 1, endIdx) - startIdx + 1);
    for (let i = 0; i < nRows; i += 1) {
      const s = segs[startIdx + i];
      const lab = s && s.label ? s.label : `Seg ${startIdx + i + 1}`;
      const mu = n(s && s.mu);
      const p10 = n(s && s.q && s.q.p10);
      const p90 = n(s && s.q && s.q.p90);
      const c50 = n(s && s.cum_q && s.cum_q.p50);
      const c10 = n(s && s.cum_q && s.cum_q.p10);
      const c90 = n(s && s.cum_q && s.cum_q.p90);
      rows.push(`
        <tr>
          <td>${esc(lab)}</td>
          <td class="num">${fmt(mu, 1)}</td>
          <td class="num">${p10 == null || p90 == null ? '—' : `${fmt(p10, 0)}–${fmt(p90, 0)}`}</td>
          <td class="num">${fmt(c50, 0)}</td>
          <td class="num">${c10 == null || c90 == null ? '—' : `${fmt(c10, 0)}–${fmt(c90, 0)}`}</td>
        </tr>
      `);
    }

    return `
      <div class="subtle" style="margin-top:2px;">${esc(title)}</div>
      <div class="table-wrap" style="margin-top:6px;">
        <table class="data-table boxscore-table" style="font-size:12px;">
          <thead>
            <tr>
              <th>Seg</th>
              <th class="num">Tot μ</th>
              <th class="num">Tot (p10–p90)</th>
              <th class="num">Cum p50</th>
              <th class="num">Cum (p10–p90)</th>
            </tr>
          </thead>
          <tbody>
            ${rows.join('') || '<tr><td colspan="5" class="subtle">No segments.</td></tr>'}
          </tbody>
        </table>
      </div>
      <div class="subtle" style="margin-top:6px;">Use cumulative columns for rollups; segment quantiles are not additive.</div>
    `;
  }

  function renderCumChips(totalMinutes, finalIdx, labelPrefix) {
    const step = 3;
    const chips = [];
    for (let endMin = step; endMin <= totalMinutes; endMin += step) {
      const segEndCount = Math.floor(endMin / step);
      const idx = Math.min(finalIdx, Math.max(-1, segEndCount - 1));
      const c50 = idx < 0 ? 0 : n(segs[idx] && segs[idx].cum_q && segs[idx].cum_q.p50);
      const lab = `${endMin}${labelPrefix}`;
      chips.push(`<span class="chip neutral" style="padding:3px 7px; font-size:10px;">${esc(lab)}: <span class="fw-700">${fmt(c50, 0)}</span></span>`);
    }
    return `<div class="row chips" style="margin-top:6px;">${chips.join('')}</div>`;
  }

  return `
    <div class="market-tile live-lens" data-lens-id="${esc(id)}">
      <div class="market-title">Live lens (3-min interval ladder)</div>
      <div class="lens-columns">

        <div class="lens-col" data-scope="half">
          <div class="subtle" style="font-weight:900; letter-spacing:0.3px;">1H 3-min interval</div>
          <div class="model-strip" style="grid-template-columns: repeat(3, minmax(0,1fr));">
            <div class="kv"><span class="k">Min remaining</span><span class="v"><select class="lens-min">${renderMinRemainingOptions(24)}</select></span></div>
            <div class="kv"><span class="k">Total pts</span><span class="v"><input class="lens-total" type="number" value="0" style="width:110px;"></span></div>
            <div class="kv"><span class="k">Live total (opt)</span><span class="v"><input class="lens-live" type="number" placeholder="—" style="width:120px;"></span></div>
          </div>

          <div class="model-strip" style="grid-template-columns: repeat(3, minmax(0,1fr)); margin-top:8px;">
            <div class="kv"><span class="k">Sim q50 @ time</span><span class="v lens-sim-at">—</span></div>
            <div class="kv"><span class="k">Δ (Sim–Act)</span><span class="v lens-delta">—</span></div>
            <div class="kv"><span class="k">Pace final (24m)</span><span class="v lens-pace">—</span></div>
          </div>
          <div class="model-strip" style="grid-template-columns: repeat(3, minmax(0,1fr)); margin-top:8px;">
            <div class="kv"><span class="k">Sim q50 final</span><span class="v lens-sim-final">—</span></div>
            <div class="kv"><span class="k">Driver</span><span class="v lens-driver">—</span></div>
            <div class="kv"><span class="k">Lean</span><span class="v lens-lean">—</span></div>
          </div>

          <div class="subtle" style="margin-top:6px;">Cum total (q50) at end-minute:</div>
          ${renderCumChips(24, 7, ' (1H)')}
          ${sliceSegsTable(0, 7, '1H ladder (segments 1–8)')}
        </div>

        <div class="lens-col" data-scope="game">
          <div class="subtle" style="font-weight:900; letter-spacing:0.3px;">Full game 3-min interval</div>
          <div class="model-strip" style="grid-template-columns: repeat(3, minmax(0,1fr));">
            <div class="kv"><span class="k">Min remaining</span><span class="v"><select class="lens-min">${renderMinRemainingOptions(48)}</select></span></div>
            <div class="kv"><span class="k">Total pts</span><span class="v"><input class="lens-total" type="number" value="0" style="width:110px;"></span></div>
            <div class="kv"><span class="k">Live total (opt)</span><span class="v"><input class="lens-live" type="number" placeholder="—" style="width:120px;"></span></div>
          </div>

          <div class="model-strip" style="grid-template-columns: repeat(3, minmax(0,1fr)); margin-top:8px;">
            <div class="kv"><span class="k">Sim q50 @ time</span><span class="v lens-sim-at">—</span></div>
            <div class="kv"><span class="k">Δ (Sim–Act)</span><span class="v lens-delta">—</span></div>
            <div class="kv"><span class="k">Pace final (48m)</span><span class="v lens-pace">—</span></div>
          </div>
          <div class="model-strip" style="grid-template-columns: repeat(3, minmax(0,1fr)); margin-top:8px;">
            <div class="kv"><span class="k">Sim q50 final</span><span class="v lens-sim-final">—</span></div>
            <div class="kv"><span class="k">Driver</span><span class="v lens-driver">—</span></div>
            <div class="kv"><span class="k">Lean</span><span class="v lens-lean">—</span></div>
          </div>

          <div class="subtle" style="margin-top:6px;">Cum total (q50) at end-minute:</div>
          ${renderCumChips(48, 15, ' (G)')}
          ${sliceSegsTable(0, 15, 'Full game ladder (segments 1–16, regulation)')}
        </div>

      </div>
    </div>
  `;
}

function attachLiveLensHandlers(root, games) {
  const containers = root.querySelectorAll('.live-lens');
  if (!containers || !containers.length) return;

  // Build intervals lookup by card key (home|away)
  const idx = new Map();
  (games || []).forEach((g) => {
    const h = String(g.home_tri || '').toUpperCase().trim();
    const a = String(g.away_tri || '').toUpperCase().trim();
    const key = `${h}|${a}`;
    // API payload uses game.sim.intervals; raw smart_sim files store intervals at top-level.
    const itv = (g && g.sim && g.sim.intervals) ? g.sim.intervals : (g ? g.intervals : null);
    if (itv && Array.isArray(itv.segments)) idx.set(key, itv);
  });

  function clampInt(x, lo, hi, fallback) {
    const v = Number.parseInt(String(x ?? ''), 10);
    if (!Number.isFinite(v)) return fallback;
    return Math.min(hi, Math.max(lo, v));
  }

  containers.forEach((el) => {
    const id = el.dataset.lensId || '';
    // lensId is "HOME_AWAY" but lookup uses HOME|AWAY; derive from surrounding card text
    // We stored lens id as HOME_AWAY in render, so reverse it.
    const key = id.replace(/_/g, '|');
    const intervals = idx.get(key);
    if (!intervals || !Array.isArray(intervals.segments)) return;

    const segs = intervals.segments;

    function computeScope(scopeEl, totalMinutes, finalIdx, labelPrefix) {
      const minEl = scopeEl.querySelector('select.lens-min');
      const totEl = scopeEl.querySelector('input.lens-total');
      const liveEl = scopeEl.querySelector('input.lens-live');

      const outSimAt = scopeEl.querySelector('.lens-sim-at');
      const outDelta = scopeEl.querySelector('.lens-delta');
      const outPace = scopeEl.querySelector('.lens-pace');
      const outSimFinal = scopeEl.querySelector('.lens-sim-final');
      const outDriver = scopeEl.querySelector('.lens-driver');
      const outLean = scopeEl.querySelector('.lens-lean');

      const step = 3;
      const minRem = clampInt(minEl && minEl.value, 0, totalMinutes, totalMinutes);
      const actTot = n(totEl && totEl.value != null ? totEl.value : null);
      const liveTot = n(liveEl && liveEl.value != null && String(liveEl.value).trim() !== '' ? liveEl.value : null);

      if (actTot == null) {
        if (outSimAt) outSimAt.textContent = '—';
        if (outDelta) outDelta.textContent = '—';
        if (outPace) outPace.textContent = '—';
        if (outSimFinal) outSimFinal.textContent = '—';
        if (outDriver) outDriver.textContent = '—';
        if (outLean) outLean.textContent = '—';
        return;
      }

      const elapsed = totalMinutes - minRem; // minutes elapsed in scope
      const segEndCount = Math.floor(elapsed / step); // 0..(total/step)
      const idxAt = segEndCount - 1; // -1..finalIdx
      const simAt = (idxAt < 0) ? 0 : n(segs[idxAt] && segs[idxAt].cum_q && segs[idxAt].cum_q.p50);
      const simFinal = n(segs[finalIdx] && segs[finalIdx].cum_q && segs[finalIdx].cum_q.p50);

      const delta = (simAt == null) ? null : (simAt - actTot); // Sim - Act
      const paceFinal = (simAt != null && simFinal != null) ? (actTot + (simFinal - simAt)) : null;

      let driver = null;
      if (delta != null) {
        if (delta > 3.0) driver = 'Act ahead';
        else if (delta < -3.0) driver = 'Act behind';
        else driver = 'On track';
      }

      let lean = null;
      if (liveTot != null && paceFinal != null) {
        const diff = paceFinal - liveTot;
        if (diff > 1.0) lean = `Over (+${fmt(diff, 1)})`;
        else if (diff < -1.0) lean = `Under (${fmt(diff, 1)})`;
        else lean = 'No edge';
      }

      const mm = String(minRem).padStart(2, '0');
      const label = `${labelPrefix} @ ${mm}:00`;
      if (outSimAt) outSimAt.textContent = (simAt == null) ? '—' : `${fmt(simAt, 0)} (${label})`;
      if (outDelta) outDelta.textContent = (delta == null) ? '—' : fmt(delta, 1);
      if (outPace) outPace.textContent = (paceFinal == null) ? '—' : fmt(paceFinal, 1);
      if (outSimFinal) outSimFinal.textContent = (simFinal == null) ? '—' : fmt(simFinal, 1);
      if (outDriver) outDriver.textContent = (driver == null) ? '—' : driver;
      if (outLean) outLean.textContent = (lean == null) ? '—' : lean;
    }

    const cols = el.querySelectorAll('.lens-col');
    cols.forEach((col) => {
      const scope = col.dataset.scope;
      if (scope === 'half') {
        const minEl = col.querySelector('select.lens-min');
        const totEl = col.querySelector('input.lens-total');
        const liveEl = col.querySelector('input.lens-live');
        ['input', 'change'].forEach((evt) => {
          [minEl, totEl, liveEl].forEach((x) => {
            if (x) x.addEventListener(evt, () => computeScope(col, 24, Math.min(7, segs.length - 1), '1H'));
          });
        });
        computeScope(col, 24, Math.min(7, segs.length - 1), '1H');
      } else if (scope === 'game') {
        const minEl = col.querySelector('select.lens-min');
        const totEl = col.querySelector('input.lens-total');
        const liveEl = col.querySelector('input.lens-live');
        ['input', 'change'].forEach((evt) => {
          [minEl, totEl, liveEl].forEach((x) => {
            if (x) x.addEventListener(evt, () => computeScope(col, 48, Math.min(15, segs.length - 1), 'G'));
          });
        });
        computeScope(col, 48, Math.min(15, segs.length - 1), 'G');
      }
    });
  });
}

function betOutcome(label, odds, actualHome, actualAway) {
  const h = n(actualHome);
  const a = n(actualAway);
  if (h == null || a == null) return null;
  const total = h + a;
  const spr = n(odds && odds.home_spread);
  const tot = n(odds && odds.total);

  const lab = String(label || '');
  if (lab === 'ML Home') return h > a ? 'W' : 'L';
  if (lab === 'ML Away') return a > h ? 'W' : 'L';

  if (lab === 'Spread Home') {
    if (spr == null) return null;
    const v = h + spr - a;
    return v > 0 ? 'W' : (v === 0 ? 'P' : 'L');
  }
  if (lab === 'Spread Away') {
    if (spr == null) return null;
    const awaySpr = -spr;
    const v = a + awaySpr - h;
    return v > 0 ? 'W' : (v === 0 ? 'P' : 'L');
  }

  if (lab === 'Total Over') {
    if (tot == null) return null;
    const v = total - tot;
    return v > 0 ? 'W' : (v === 0 ? 'P' : 'L');
  }
  if (lab === 'Total Under') {
    if (tot == null) return null;
    const v = tot - total;
    return v > 0 ? 'W' : (v === 0 ? 'P' : 'L');
  }
  return null;
}

function badgeForOutcome(o) {
  if (!o) return '';
  const cls = o === 'W' ? 'good' : (o === 'P' ? 'ok' : 'bad');
  return `<span class="badge ${cls}">${esc(o)}</span>`;
}

function renderPropTargets(propTargets, homeTri, awayTri) {
  // Legacy: kept for now, but not rendered in the unified UI.
  const arr = Array.isArray(propTargets) ? propTargets : [];
  const rows = arr
    .filter((r) => r && r.player_name && r.stat)
    .slice(0, 18)
    .map((r) => {
      const side = r.side === 'home' ? homeTri : awayTri;
      const q = r.q && r.q.p50 != null ? ` (p50 ${fmt(r.q.p50, 1)})` : '';
      return `<li><span class="badge">${esc(side)}</span> ${esc(r.player_name)} — ${esc(r.stat)}: <b>${fmt(r.mean, 1)}</b>${q}</li>`;
    })
    .join('');

  return `
    <div class="writeup-content">
      <div class="subtle">Projection-based prop targets from aggregated sims (market lines not required).</div>
      <ul>
        ${rows || '<li class="subtle">No prop targets.</li>'}
      </ul>
    </div>
  `;
}

function renderCards(games, reconGameRows, reconQuarterRows, showResults, hideOdds) {
  const root = document.getElementById('cards');
  if (!root) return;
  if (!Array.isArray(games) || games.length === 0) {
    root.innerHTML = '<div class="card"><div class="subtle">No SmartSim games found for this date.</div></div>';
    return;
  }

  const reconIndex = buildReconIndex(reconGameRows);
  const reconQIndex = buildReconIndex(reconQuarterRows);

  const html = games.map((g) => {
    const homeTri = String(g.home_tri || '').toUpperCase().trim();
    const awayTri = String(g.away_tri || '').toUpperCase().trim();
    const homeName = String(g.home_name || homeTri).trim();
    const awayName = String(g.away_name || awayTri).trim();
    const odds = g.odds || {};
    const sim = g.sim || {};
    // API payload uses game.sim.*; raw smart_sim files store these at top-level.
    const score = sim.score || g.score || {};
    const periods = sim.periods || g.periods || {};
    const intervals = sim.intervals || g.intervals || null;
    const bet = g.betting || {};

    const simErr = sim && sim.error ? String(sim.error) : '';

    const recon = showResults ? (reconIndex.get(`${homeTri}|${awayTri}`) || reconIndex.get(`${homeName}|${awayName}`)) : null;
    const reconQ = showResults ? (reconQIndex.get(`${homeTri}|${awayTri}`) || reconQIndex.get(`${homeName}|${awayName}`)) : null;
    const actualHome = recon ? n(recon.home_pts) : null;
    const actualAway = recon ? n(recon.visitor_pts) : null;
    const finalLine = (actualHome != null && actualAway != null) ? `${actualAway}–${actualHome} (final)` : '';

    const actualMargin = (actualHome != null && actualAway != null) ? (actualHome - actualAway) : null;
    const actualTotal = (actualHome != null && actualAway != null) ? (actualHome + actualAway) : null;
    const simMargin = n(score.margin_mean);
    const simTotal = n(score.total_mean);
    const marginErr = (actualMargin != null && simMargin != null) ? fmt(simMargin - actualMargin, 1) : null;
    const totalErr = (actualTotal != null && simTotal != null) ? fmt(simTotal - actualTotal, 1) : null;

    const projFinalHtml = (n(score.home_mean) != null && n(score.away_mean) != null)
      ? `
        <div class="sim-scoreline" aria-label="Sim projected score">
          <span class="sim-side away">${logoImg(awayTri)}<span class="sim-num">${fmt(score.away_mean, 1)}</span></span>
          <span class="sim-dash">–</span>
          <span class="sim-side home"><span class="sim-num">${fmt(score.home_mean, 1)}</span>${logoImg(homeTri)}</span>
        </div>
      `
      : '<span class="subtle">—</span>';

    const timeStr = fmtTime(odds.commence_time);
    const oddsLine = hideOdds
      ? 'Odds hidden'
      : `ML (away/home) ${fmtAmer(odds.away_ml)} / ${fmtAmer(odds.home_ml)} • Spr (home) ${fmt(odds.home_spread, 1)} • Tot ${fmt(odds.total, 1)}`;

    const best = bestBets(bet);
    const betChips = best.map((x) => {
      const ev = n(x.ev);
      const evCls = ev == null ? 'neu' : (ev >= 0.02 ? 'pos' : (ev <= -0.02 ? 'neg' : 'neu'));
      const out = showResults && recon ? betOutcome(x.label, odds, actualHome, actualAway) : null;
      const outBadge = out ? ` • ${badgeForOutcome(out)}` : '';
      return `<span class="chip model-pick neutral">${esc(x.label)} • p=${pct(x.p, 0)} • EV <span class="ev-badge ${evCls}">${ev == null ? '—' : ev.toFixed(3)}</span>${outBadge}</span>`;
    }).join('');

    const warnLines = [];
    if (simErr) warnLines.push(`SmartSim error: ${simErr}`);
    if (Array.isArray(g.warnings) && g.warnings.length) warnLines.push(...g.warnings);
    const warn = warnLines.length
      ? `<div class="alert">${warnLines.map((w) => esc(w)).join('<br/>')}</div>`
      : '';

    const playersHome = (sim.players && sim.players.home) ? sim.players.home : [];
    const playersAway = (sim.players && sim.players.away) ? sim.players.away : [];

    const awayP10 = n(score.away_q && score.away_q.p10);
    const awayP90 = n(score.away_q && score.away_q.p90);
    const homeP10 = n(score.home_q && score.home_q.p10);
    const homeP90 = n(score.home_q && score.home_q.p90);
    const quantLine = (awayP10 != null && awayP90 != null && homeP10 != null && homeP90 != null)
      ? `Away p10/p90: ${fmt(awayP10, 0)}/${fmt(awayP90, 0)} • Home p10/p90: ${fmt(homeP10, 0)}/${fmt(homeP90, 0)}`
      : '';

    return `
      <section class="card card-v2">
        <div class="row head">
          <span class="venue">${esc(timeStr || '')}</span>
          <span class="venue">${esc(odds.bookmaker || odds.bookmaker_odds || 'odds')}</span>
          ${showResults && recon ? `<span class="result-badge">${finalLine}</span>` : ''}
        </div>

        <div class="row matchup">
          <div class="team side">
            <div class="subtle" style="margin-bottom:2px;">AWAY</div>
            <div class="team-line">${logoImg(awayTri)}<div class="name">${esc(awayName)}</div></div>
          </div>
          <div class="score-block">
            <div class="sub">Projected score</div>
            <div class="live-score">${projFinalHtml}</div>
            <div class="proj-score">Home win: <span class="fw-700">${pct(score.p_home_win, 0)}</span></div>
            ${quantLine ? `<div class="proj-score">${esc(quantLine)}</div>` : ''}
          </div>
          <div class="team side" style="justify-self:end;">
            <div class="subtle" style="margin-bottom:2px; text-align:right;">HOME</div>
            <div class="team-line">${logoImg(homeTri)}<div class="name">${esc(homeName)}</div></div>
          </div>
        </div>

        <div class="row chips">
          <span class="chip neutral">${esc(oddsLine)}</span>
          <span class="chip neutral">Total proj: <span class="fw-700">${fmt(score.total_mean, 1)}</span> (p90 ${fmt(score.total_q && score.total_q.p90, 0)})</span>
          <span class="chip neutral">Margin proj: <span class="fw-700">${fmt(score.margin_mean, 1)}</span></span>
          ${showResults && recon && actualHome != null && actualAway != null ? `<span class="chip neutral">Actual: <span class="fw-700">${actualAway}–${actualHome}</span> (Tot ${actualTotal})</span>` : ''}
          ${showResults && recon && marginErr != null ? `<span class="chip neutral">ΔMargin err: <span class="fw-700">${esc(marginErr)}</span></span>` : ''}
          ${showResults && recon && totalErr != null ? `<span class="chip neutral">ΔTotal err: <span class="fw-700">${esc(totalErr)}</span></span>` : ''}
        </div>

        ${warn}

        <div class="market-grid">
          <div class="market-tile">
            <div class="market-title">Sim-based bet leans (top EV)</div>
            <div class="market-main">${betChips || '<span class="subtle">No EV computed (missing odds/prices).</span>'}</div>
          </div>
          <div class="market-tile">
            <div class="market-title">Model probabilities</div>
            <div class="model-strip">
              <div class="kv"><span class="k">P(Home win)</span><span class="v">${pct(bet.p_home_win, 1)}</span></div>
              <div class="kv"><span class="k">P(Home cover)</span><span class="v">${pct(bet.p_home_cover, 1)}</span></div>
              <div class="kv"><span class="k">P(Over)</span><span class="v">${pct(bet.p_total_over, 1)}</span></div>
              <div class="kv"><span class="k">Sims</span><span class="v">${esc(sim.n_sims ?? '—')}</span></div>
            </div>
          </div>
          <div class="market-tile">
            <div class="market-title">Write-up</div>
            <div class="writeup-recap">${esc(g.writeup || '—')}</div>
          </div>
        </div>

        <div class="market-grid">
          ${renderLiveLens(intervals, `${homeTri}_${awayTri}`)}
        </div>

        <details class="quarters-block">
          <summary class="quarters-toggle cursor-pointer">Quarter / Half projections</summary>
          ${renderQuarterTable(periods, reconQ)}
        </details>

        <details class="players-block" open>
          <summary class="players-toggle cursor-pointer">Projected boxscore (aggregated sim means)</summary>
          ${renderPlayersTable(`HOME (${homeTri}) players`, playersHome)}
          ${renderInjurySummary(`HOME (${homeTri})`, (g && g.sim && g.sim.injuries && g.sim.injuries.home) ? g.sim.injuries.home : playersHome)}
          <div class="mb-6"></div>
          ${renderPlayersTable(`AWAY (${awayTri}) players`, playersAway)}
          ${renderInjurySummary(`AWAY (${awayTri})`, (g && g.sim && g.sim.injuries && g.sim.injuries.away) ? g.sim.injuries.away : playersAway)}
        </details>

        <details class="writeup-block">
          <summary class="writeup-toggle cursor-pointer">Recommended props (sim vs line)</summary>
          ${renderPropRecommendations(g.prop_recommendations, homeTri, awayTri)}
        </details>
      </section>
    `;
  }).join('\n');

  root.innerHTML = html;

  // Enable click-to-sort on projected boxscore tables.
  try {
    makeBoxscoreTablesSortable(root);
  } catch (_) {
    // ignore
  }

  // Attach live lens handlers (uses game intervals ladder)
  try {
    attachLiveLensHandlers(root, games);
  } catch (_) {
    // ignore
  }
}

function parseSortValue(text, type) {
  const s = String(text || '').trim();
  if (!s || s === '—') return null;
  if (type === 'text') return s.toUpperCase();
  // numeric
  const cleaned = s.replace(/[^0-9+\-\.]/g, '');
  const v = Number.parseFloat(cleaned);
  return Number.isFinite(v) ? v : null;
}

function sortTableByColumn(table, colIndex, dir, type) {
  const tbody = table.tBodies && table.tBodies[0];
  if (!tbody) return;
  const rows = Array.from(tbody.rows || []);
  rows.sort((ra, rb) => {
    const a = parseSortValue((ra.cells[colIndex] && ra.cells[colIndex].textContent) || '', type);
    const b = parseSortValue((rb.cells[colIndex] && rb.cells[colIndex].textContent) || '', type);
    if (a == null && b == null) return 0;
    if (a == null) return 1;
    if (b == null) return -1;
    if (type === 'text') return dir * String(a).localeCompare(String(b));
    return dir * ((a < b) ? -1 : (a > b ? 1 : 0));
  });
  rows.forEach((r) => tbody.appendChild(r));
}

function makeBoxscoreTablesSortable(root) {
  const tables = root.querySelectorAll('table.player-boxscore');
  tables.forEach((table) => {
    const ths = table.querySelectorAll('thead th.sortable');
    ths.forEach((th, idx) => {
      th.style.cursor = 'pointer';
      th.addEventListener('click', () => {
        const curCol = Number.parseInt(table.dataset.sortCol || '-1', 10);
        const curDir = Number.parseInt(table.dataset.sortDir || '-1', 10);
        const type = th.dataset.sort || (th.classList.contains('num') ? 'num' : 'text');
        const dir = (curCol === idx) ? (-curDir || 1) : -1; // default: descending
        table.dataset.sortCol = String(idx);
        table.dataset.sortDir = String(dir);
        sortTableByColumn(table, idx, dir, type);

        // Update visual indicator
        try {
          ths.forEach((h) => {
            const base = String(h.dataset.baseText || h.textContent || '').replace(/[\s▲▼]+$/g, '').trim();
            h.dataset.baseText = base;
            h.textContent = base;
          });
          const base = String(th.dataset.baseText || th.textContent || '').replace(/[\s▲▼]+$/g, '').trim();
          th.dataset.baseText = base;
          th.textContent = `${base} ${dir > 0 ? '▲' : '▼'}`;
        } catch (_) {
          // ignore
        }
      });
    });
  });
}

async function load(dateStr) {
  setNote('');
  const today = localYMD();
  const isHistorical = isYmd(dateStr) && dateStr < today;

  const resultsToggle = document.getElementById('resultsToggle');
  const hideOddsToggle = document.getElementById('hideOdds');
  if (resultsToggle && typeof resultsToggle.checked === 'boolean') {
    if (isHistorical) resultsToggle.checked = true;
  }

  const showResults = !!(resultsToggle && resultsToggle.checked);
  const hideOdds = !!(hideOddsToggle && hideOddsToggle.checked);

  try {
    const payload = await fetchJson(`/api/cards?date=${encodeURIComponent(dateStr)}`);
    const games = Array.isArray(payload?.games) ? payload.games : [];

    let reconGameRows = [];
    let reconQuarterRows = [];
    if (showResults) {
      const [csvG, csvQ] = await Promise.all([
        fetchText(`/api/processed/recon_games?date=${encodeURIComponent(dateStr)}`),
        fetchText(`/api/processed/recon_quarters?date=${encodeURIComponent(dateStr)}`),
      ]);
      reconGameRows = csvG ? csvParse(csvG) : [];
      reconQuarterRows = csvQ ? csvParse(csvQ) : [];
    }

    renderCards(games, reconGameRows, reconQuarterRows, showResults, hideOdds);
  } catch (e) {
    setNote(`Failed to load cards: ${String(e && e.message ? e.message : e)}`);
    renderCards([], [], [], false, false);
  }
}

function setUrlDate(dateStr) {
  const u = new URL(window.location.href);
  u.searchParams.set('date', dateStr);
  window.history.replaceState({}, '', u.toString());
}

window.addEventListener('DOMContentLoaded', () => {
  const datePicker = document.getElementById('datePicker');
  const applyBtn = document.getElementById('applyBtn');
  const todayBtn = document.getElementById('todayBtn');
  const resultsToggle = document.getElementById('resultsToggle');
  const hideOddsToggle = document.getElementById('hideOdds');

  const u = new URL(window.location.href);
  const qd = u.searchParams.get('date');
  const d0 = isYmd(qd) ? qd : localYMD();
  if (datePicker) datePicker.value = d0;

  function apply() {
    const d = (datePicker && datePicker.value) ? datePicker.value : localYMD();
    setUrlDate(d);
    load(d);
  }

  if (applyBtn) applyBtn.addEventListener('click', apply);
  if (todayBtn) todayBtn.addEventListener('click', () => {
    if (datePicker) datePicker.value = localYMD();
    apply();
  });
  if (resultsToggle) resultsToggle.addEventListener('change', apply);
  if (hideOddsToggle) hideOddsToggle.addEventListener('change', apply);

  apply();
});
