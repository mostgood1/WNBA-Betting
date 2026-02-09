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

function renderLiveLens(intervals, cardKey, gameId) {
  const segsAll = intervals && Array.isArray(intervals.segments) ? intervals.segments : [];
  // Prefer regulation segments for tables/chips; OT segments have `ot` not `quarter`.
  const segsReg = segsAll.filter((s) => s && s.quarter != null);
  const segs = segsReg.length ? segsReg : segsAll;
  if (!segs.length) return '';
  const id = String(cardKey || '').replace(/[^A-Za-z0-9_\-]/g, '_');
  const gid = canonGameId((gameId == null) ? '' : gameId);

  const segSec = n(intervals && intervals.segment_seconds) ?? 180;
  const segMin = Math.max(1, Math.round(segSec / 60));
  const ladderLabel = `${segMin}-min interval ladder`;

  function renderMinRemainingOptions(totalMinutes) {
    const step = 1;
    const opts = [];
    for (let m = totalMinutes; m >= 0; m -= step) {
      const lab = `${m}`;
      const sel = m === totalMinutes ? ' selected' : '';
      opts.push(`<option value="${m}"${sel}>${lab}</option>`);
    }
    return opts.join('');
  }

  function sliceSegsTable(segsLocal, startIdx, endIdx, title) {
    const rows = [];
    const ss = Array.isArray(segsLocal) ? segsLocal : [];
    const nRows = Math.max(0, Math.min(ss.length - 1, endIdx) - startIdx + 1);
    for (let i = 0; i < nRows; i += 1) {
      const s = ss[startIdx + i];
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

  function renderCumChips(segsLocal, totalMinutes, finalIdx, labelPrefix) {
    const ss = Array.isArray(segsLocal) ? segsLocal : [];
    if (!ss.length) return '';
    // Keep chips sparse for readability when the underlying ladder is 1-minute.
    const step = (segMin === 1) ? 3 : segMin;
    const chips = [];
    for (let endMin = step; endMin <= totalMinutes; endMin += step) {
      const segEndCount = Math.floor(endMin / segMin);
      const idx = Math.min(finalIdx, Math.max(-1, segEndCount - 1));
      const c50 = idx < 0 ? 0 : n(ss[idx] && ss[idx].cum_q && ss[idx].cum_q.p50);
      const lab = `${endMin}${labelPrefix}`;
      chips.push(`<span class="chip neutral" style="padding:3px 7px; font-size:10px;">${esc(lab)}: <span class="fw-700">${fmt(c50, 0)}</span></span>`);
    }
    return `<div class="row chips" style="margin-top:6px;">${chips.join('')}</div>`;
  }

  const halfFinalIdx = Math.min(segs.length - 1, Math.max(0, Math.round(24 / segMin) - 1));
  const gameFinalIdx = Math.min(segs.length - 1, Math.max(0, Math.round(48 / segMin) - 1));

  function buildQuarterSegs(qNum) {
    // Build a quarter-local segment list with cumulative values adjusted to start at 0.
    const qSegs = segs.filter((s) => s && Number(s.quarter) === Number(qNum));
    if (!qSegs.length) return [];
    let baseC10 = 0;
    let baseC50 = 0;
    let baseC90 = 0;
    let baseCumMu = 0;
    if (Number(qNum) > 1) {
      const prevSegs = segs.filter((s) => s && Number(s.quarter) === (Number(qNum) - 1));
      const prevLast = prevSegs.length ? prevSegs[prevSegs.length - 1] : null;
      baseC10 = n(prevLast && prevLast.cum_q && prevLast.cum_q.p10) ?? 0;
      baseC50 = n(prevLast && prevLast.cum_q && prevLast.cum_q.p50) ?? 0;
      baseC90 = n(prevLast && prevLast.cum_q && prevLast.cum_q.p90) ?? 0;
      baseCumMu = n(prevLast && prevLast.cum_mu) ?? 0;
    }
    return qSegs.map((s) => {
      const c10 = n(s && s.cum_q && s.cum_q.p10);
      const c50 = n(s && s.cum_q && s.cum_q.p50);
      const c90 = n(s && s.cum_q && s.cum_q.p90);
      const cumMu = n(s && s.cum_mu);
      const out = { ...s };
      out.cum_q = {
        p10: (c10 == null) ? null : (c10 - baseC10),
        p50: (c50 == null) ? null : (c50 - baseC50),
        p90: (c90 == null) ? null : (c90 - baseC90),
      };
      out.cum_mu = (cumMu == null) ? null : (cumMu - baseCumMu);
      return out;
    });
  }

  function renderQuarterCol(qNum) {
    const qSegs = buildQuarterSegs(qNum);
    const qFinalIdx = Math.min(qSegs.length - 1, Math.max(0, Math.round(12 / segMin) - 1));
    const title = `Q${qNum} ${segMin}-min interval`;
    return `
      <div class="lens-col" data-scope="q${qNum}">
        <div class="subtle" style="font-weight:900; letter-spacing:0.3px;">${esc(title)}</div>
        <div class="model-strip" style="grid-template-columns: repeat(3, minmax(0,1fr));">
          <div class="kv"><span class="k">Min remaining</span><span class="v"><select class="lens-min">${renderMinRemainingOptions(12)}</select></span></div>
          <div class="kv"><span class="k">Q pts</span><span class="v"><input class="lens-total" type="number" value="0" style="width:110px;"></span></div>
          <div class="kv"><span class="k">Live total (opt)</span><span class="v"><input class="lens-live" type="number" placeholder="—" style="width:120px;"></span></div>
        </div>

        <div class="model-strip" style="grid-template-columns: repeat(3, minmax(0,1fr)); margin-top:8px;">
          <div class="kv"><span class="k">Sim q50 final</span><span class="v lens-sim-final">—</span></div>
          <div class="kv"><span class="k">Sim q50 @ time</span><span class="v lens-sim-at">—</span></div>
          <div class="kv"><span class="k">Δ (Sim–Act)</span><span class="v lens-delta">—</span></div>
        </div>
        <div class="model-strip" style="grid-template-columns: repeat(3, minmax(0,1fr)); margin-top:8px;">
          <div class="kv"><span class="k">Proj final (12m)</span><span class="v lens-pace">—</span></div>
          <div class="kv"><span class="k">Driver</span><span class="v lens-driver">—</span></div>
          <div class="kv"><span class="k">Signal</span><span class="v lens-lean">—</span></div>
        </div>

        <details class="lens-details" style="margin-top:6px;">
          <summary class="subtle cursor-pointer">Interval details</summary>
          <div class="subtle" style="margin-top:6px;">Cum total (q50) at end-minute:</div>
          ${renderCumChips(qSegs, 12, qFinalIdx, ' (Q' + qNum + ')')}
          ${sliceSegsTable(qSegs, 0, qFinalIdx, 'Q' + qNum + ' ladder (segments 1–' + (qFinalIdx + 1) + ', regulation)')}
        </details>
      </div>
    `;
  }

  return `
    <div class="market-tile live-lens" data-lens-id="${esc(id)}" data-game-id="${esc(gid)}">
      <div class="market-title">Live lens (${esc(ladderLabel)})</div>
      <div class="subtle lens-live-bar" style="margin-top:2px;">Live: <span class="lens-live-status">—</span> · <span class="lens-live-score">—</span> · <span class="lens-live-lines">Lines —</span> · Updated <span class="lens-live-updated">—</span></div>
      <div class="row chips" style="margin-top:6px;">
        <span class="chip neutral lens-rec-total">Total: —</span>
        <span class="chip neutral lens-rec-half">1H: —</span>
        <span class="chip neutral lens-rec-qtr">Q: —</span>
        <span class="chip neutral lens-rec-ats">ATS: —</span>
        <span class="chip neutral lens-live-attempts">FT/2P/3P: —</span>
      </div>
      <div class="lens-columns">

        ${renderQuarterCol(1)}
        ${renderQuarterCol(2)}
        ${renderQuarterCol(3)}
        ${renderQuarterCol(4)}

        <div class="lens-col" data-scope="half">
          <div class="subtle" style="font-weight:900; letter-spacing:0.3px;">1H ${esc(segMin)}-min interval</div>
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
            <div class="kv"><span class="k">Signal</span><span class="v lens-lean">—</span></div>
          </div>
        </div>

        <div class="lens-col" data-scope="game">
          <div class="subtle" style="font-weight:900; letter-spacing:0.3px;">Full game ${esc(segMin)}-min interval</div>
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
            <div class="kv"><span class="k">Signal</span><span class="v lens-lean">—</span></div>
          </div>
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
  const gameMetaByKey = new Map();
  (games || []).forEach((g) => {
    const h = String(g.home_tri || '').toUpperCase().trim();
    const a = String(g.away_tri || '').toUpperCase().trim();
    const key = `${h}|${a}`;
    // API payload uses game.sim.intervals; raw smart_sim files store intervals at top-level.
    const itv = (g && g.sim && (g.sim.intervals_1m || g.sim.intervals))
      ? (g.sim.intervals_1m || g.sim.intervals)
      : (g ? (g.intervals_1m || g.intervals) : null);
    if (itv && Array.isArray(itv.segments)) idx.set(key, itv);

    const gid = canonGameId((g && g.sim && g.sim.game_id != null) ? g.sim.game_id : (g && g.game_id != null ? g.game_id : ''));
    const marginMean = n(g && g.sim && g.sim.score ? g.sim.score.margin_mean : null);
    gameMetaByKey.set(key, { game_id: gid, home: h, away: a, margin_mean: marginMean });
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

    // Ensure game-id attribute is present for polling
    try {
      const meta = gameMetaByKey.get(key);
      if (meta && meta.game_id && !el.dataset.gameId) el.dataset.gameId = String(meta.game_id);
    } catch (_) {
      // ignore
    }

    const segsAll = intervals.segments;
    const segsReg = segsAll.filter((s) => s && s.quarter != null);
    const segs = segsReg.length ? segsReg : segsAll;
    const segSec = n(intervals && intervals.segment_seconds) ?? 180;
    const segMin = Math.max(1, Math.round(segSec / 60));

    const halfFinalIdx = Math.min(segs.length - 1, Math.max(0, Math.round(24 / segMin) - 1));
    const gameFinalIdx = Math.min(segs.length - 1, Math.max(0, Math.round(48 / segMin) - 1));

    function buildQuarterSegs(qNum) {
      // Build a quarter-local segment list with cumulative values adjusted to start at 0.
      const qSegs = segs.filter((s) => s && Number(s.quarter) === Number(qNum));
      if (!qSegs.length) return [];

      let baseC10 = 0;
      let baseC50 = 0;
      let baseC90 = 0;
      if (Number(qNum) > 1) {
        const prevSegs = segs.filter((s) => s && Number(s.quarter) === (Number(qNum) - 1));
        const prevLast = prevSegs.length ? prevSegs[prevSegs.length - 1] : null;
        baseC10 = n(prevLast && prevLast.cum_q && prevLast.cum_q.p10) ?? 0;
        baseC50 = n(prevLast && prevLast.cum_q && prevLast.cum_q.p50) ?? 0;
        baseC90 = n(prevLast && prevLast.cum_q && prevLast.cum_q.p90) ?? 0;
      }

      return qSegs.map((s) => {
        const c10 = n(s && s.cum_q && s.cum_q.p10);
        const c50 = n(s && s.cum_q && s.cum_q.p50);
        const c90 = n(s && s.cum_q && s.cum_q.p90);
        const out = { ...s };
        out.cum_q = {
          p10: (c10 == null) ? null : (c10 - baseC10),
          p50: (c50 == null) ? null : (c50 - baseC50),
          p90: (c90 == null) ? null : (c90 - baseC90),
        };
        return out;
      });
    }

    function cumP50AtElapsedMinutes(segsLocal, segMinLocal, elapsedMin, totalMinutesLocal, finalIdxLocal) {
      // Interpolate between ladder cum_q.p50 points based on segment length.
      const ss = Array.isArray(segsLocal) ? segsLocal : [];
      const t = n(elapsedMin);
      if (t == null) return null;
      const tt = Math.max(0, Math.min(totalMinutesLocal, t));
      if (tt <= 0) return 0;

      if (!ss.length) return null;

      // Fast path: native 1-minute ladder.
      if (segMinLocal === 1) {
        const idxAt = Math.min(finalIdxLocal, Math.max(0, Math.ceil(tt) - 1));
        return n(ss[idxAt] && ss[idxAt].cum_q && ss[idxAt].cum_q.p50);
      }

      const step = segMinLocal;
      const prevEnd = Math.floor(tt / step) * step;
      const nextEnd = Math.min(totalMinutesLocal, Math.ceil(tt / step) * step);

      function cumAtEndMinute(endMin) {
        const em = Math.max(0, Math.min(totalMinutesLocal, endMin));
        if (em <= 0) return 0;
        const segEndCount = Math.floor(em / step);
        const idxAt = segEndCount - 1;
        if (idxAt < 0) return 0;
        const idxClamped = Math.min(finalIdxLocal, Math.max(0, idxAt));
        return n(ss[idxClamped] && ss[idxClamped].cum_q && ss[idxClamped].cum_q.p50);
      }

      const a = cumAtEndMinute(prevEnd);
      const b = cumAtEndMinute(nextEnd);
      if (a == null || b == null) return null;
      const denom = (nextEnd - prevEnd);
      if (denom <= 0) return a;
      const frac = (tt - prevEnd) / denom;
      return a + frac * (b - a);
    }

    function computeScope(scopeEl, segsLocal, segMinLocal, totalMinutes, finalIdx, labelPrefix) {
      const lensRoot = scopeEl && scopeEl.closest ? scopeEl.closest('.live-lens') : null;
      const isFinal = !!(lensRoot && lensRoot.dataset && lensRoot.dataset.final === '1');
      const minEl = scopeEl.querySelector('select.lens-min');
      const totEl = scopeEl.querySelector('input.lens-total');
      const liveEl = scopeEl.querySelector('input.lens-live');

      const outSimAt = scopeEl.querySelector('.lens-sim-at');
      const outDelta = scopeEl.querySelector('.lens-delta');
      const outPace = scopeEl.querySelector('.lens-pace');
      const outSimFinal = scopeEl.querySelector('.lens-sim-final');
      const outDriver = scopeEl.querySelector('.lens-driver');
      const outLean = scopeEl.querySelector('.lens-lean');

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
  const simAt = cumP50AtElapsedMinutes(segsLocal, segMinLocal, elapsed, totalMinutes, finalIdx);
  const ss = Array.isArray(segsLocal) ? segsLocal : [];
  const simFinal = n(ss[finalIdx] && ss[finalIdx].cum_q && ss[finalIdx].cum_q.p50);

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
      } else if (paceFinal != null && simFinal != null) {
        // No live line for this scope; interpret as Over/Under vs pregame sim median for the scope.
        const diff = paceFinal - simFinal;
        const watchThresh = (totalMinutes <= 12) ? 2.0 : 3.0;
        const betThresh = (totalMinutes <= 12) ? 4.0 : 6.0;
        const klass = classifyDiff(Math.abs(diff), watchThresh, betThresh);
        if (klass === 'BET') {
          if (diff > 0.5) lean = `BET Over vs pregame (+${fmt(diff, 1)})`;
          else if (diff < -0.5) lean = `BET Under vs pregame (${fmt(diff, 1)})`;
          else lean = 'No edge';
        } else if (klass === 'WATCH') {
          if (diff > 0.5) lean = `WATCH Over vs pregame (+${fmt(diff, 1)})`;
          else if (diff < -0.5) lean = `WATCH Under vs pregame (${fmt(diff, 1)})`;
          else lean = 'No edge';
        } else {
          lean = 'No edge';
        }
      }

      const mm = String(minRem).padStart(2, '0');
      const label = `${labelPrefix} @ ${mm}:00`;
      if (outSimAt) outSimAt.textContent = (simAt == null) ? '—' : `${fmt(simAt, 0)} (${label})`;
      if (outDelta) outDelta.textContent = (delta == null) ? '—' : fmt(delta, 1);
      if (outPace) outPace.textContent = (paceFinal == null) ? '—' : fmt(paceFinal, 1);
      if (outSimFinal) outSimFinal.textContent = (simFinal == null) ? '—' : fmt(simFinal, 1);
      if (outDriver) outDriver.textContent = (driver == null) ? '—' : driver;
      if (outLean) outLean.textContent = (isFinal || lean == null) ? '—' : lean;

      // Persist for polling-driven chips
      try {
        scopeEl.dataset.simAt = (simAt == null) ? '' : String(simAt);
        scopeEl.dataset.simFinal = (simFinal == null) ? '' : String(simFinal);
        scopeEl.dataset.paceFinal = (paceFinal == null) ? '' : String(paceFinal);
        scopeEl.dataset.deltaSimMinusAct = (delta == null) ? '' : String(delta);
        scopeEl.dataset.liveTotal = (liveTot == null) ? '' : String(liveTot);
      } catch (_) {
        // ignore
      }
    }

    const cols = el.querySelectorAll('.lens-col');
    cols.forEach((col) => {
      const scope = col.dataset.scope;
      if (scope && /^q[1-4]$/.test(scope)) {
        const qNum = Number(scope.replace('q', ''));
        const qSegs = buildQuarterSegs(qNum);
        const qFinalIdx = Math.min(qSegs.length - 1, Math.max(0, Math.round(12 / segMin) - 1));

        const minEl = col.querySelector('select.lens-min');
        const totEl = col.querySelector('input.lens-total');
        const liveEl = col.querySelector('input.lens-live');
        ['input', 'change'].forEach((evt) => {
          [minEl, totEl, liveEl].forEach((x) => {
            if (x) x.addEventListener(evt, () => computeScope(col, qSegs, segMin, 12, qFinalIdx, `Q${qNum}`));
          });
        });
        computeScope(col, qSegs, segMin, 12, qFinalIdx, `Q${qNum}`);
      } else if (scope === 'half') {
        const minEl = col.querySelector('select.lens-min');
        const totEl = col.querySelector('input.lens-total');
        const liveEl = col.querySelector('input.lens-live');
        ['input', 'change'].forEach((evt) => {
          [minEl, totEl, liveEl].forEach((x) => {
            if (x) x.addEventListener(evt, () => computeScope(col, segs, segMin, 24, halfFinalIdx, '1H'));
          });
        });
        computeScope(col, segs, segMin, 24, halfFinalIdx, '1H');
      } else if (scope === 'game') {
        const minEl = col.querySelector('select.lens-min');
        const totEl = col.querySelector('input.lens-total');
        const liveEl = col.querySelector('input.lens-live');
        ['input', 'change'].forEach((evt) => {
          [minEl, totEl, liveEl].forEach((x) => {
            if (x) x.addEventListener(evt, () => computeScope(col, segs, segMin, 48, gameFinalIdx, 'G'));
          });
        });
        computeScope(col, segs, segMin, 48, gameFinalIdx, 'G');
      }
    });
  });
}

function nearest3(x, maxMins) {
  const v = Number.parseFloat(String(x ?? ''));
  if (!Number.isFinite(v)) return null;
  const r = Math.floor(v / 3) * 3;
  return Math.max(0, Math.min(maxMins, r));
}

function computePaceFinalFromIntervals(intervals, totalMinutes, minRem, actTot) {
  const segsAll = intervals && Array.isArray(intervals.segments) ? intervals.segments : [];
  const segsReg = segsAll.filter((s) => s && s.quarter != null);
  const segs = segsReg.length ? segsReg : segsAll;
  if (!segs.length) return null;
  const segSec = n(intervals && intervals.segment_seconds) ?? 180;
  const segMin = Math.max(1, Math.round(segSec / 60));
  const finalIdx = Math.min(segs.length - 1, Math.max(0, Math.round(totalMinutes / segMin) - 1));
  const elapsed = totalMinutes - minRem;
  // Interpolate cum p50 for arbitrary elapsed minutes using ladder points
  function cumAtElapsed(t) {
    const tt = Math.max(0, Math.min(totalMinutes, n(t) ?? 0));
    if (tt <= 0) return 0;

    if (segMin === 1) {
      const idxAt = Math.min(finalIdx, Math.max(0, Math.ceil(tt) - 1));
      return n(segs[idxAt] && segs[idxAt].cum_q && segs[idxAt].cum_q.p50);
    }

    const step = segMin;
    const prevEnd = Math.floor(tt / step) * step;
    const nextEnd = Math.min(totalMinutes, Math.ceil(tt / step) * step);
    function cumAtEnd(endMin) {
      const em = Math.max(0, Math.min(totalMinutes, endMin));
      if (em <= 0) return 0;
      const segEndCount = Math.floor(em / step);
      const idxAt = segEndCount - 1;
      if (idxAt < 0) return 0;
      const idxClamped = Math.min(finalIdx, Math.max(0, idxAt));
      return n(segs[idxClamped] && segs[idxClamped].cum_q && segs[idxClamped].cum_q.p50);
    }
    const a = cumAtEnd(prevEnd);
    const b = cumAtEnd(nextEnd);
    if (a == null || b == null) return null;
    const denom = (nextEnd - prevEnd);
    if (denom <= 0) return a;
    const frac = (tt - prevEnd) / denom;
    return a + frac * (b - a);
  }

  const simAt = cumAtElapsed(elapsed);
  const simFinal = n(segs[finalIdx] && segs[finalIdx].cum_q && segs[finalIdx].cum_q.p50);
  if (simAt == null || simFinal == null || actTot == null) return null;
  const paceFinal = actTot + (simFinal - simAt);
  return { simAt, simFinal, paceFinal, elapsedMinutes: elapsed };
}

function classifyDiff(absDiff, watchThresh, betThresh) {
  if (absDiff == null) return 'NONE';
  if (absDiff >= betThresh) return 'BET';
  if (absDiff >= watchThresh) return 'WATCH';
  return 'NONE';
}

function clampNum(x, lo, hi) {
  const v = n(x);
  if (v == null) return null;
  return Math.max(lo, Math.min(hi, v));
}

function adjustGameTotalDiffWithContext(rawDiff, lineTotal, meta, live, curMinLeft) {
  const out = {
    diff_adj: rawDiff,
    diff_raw: rawDiff,
    pace_ratio: null,
    eff_ppp_delta: null,
    poss_live: null,
    poss_expected: null,
    elapsed_min: null,
  };

  const rd = n(rawDiff);
  const lt = n(lineTotal);
  if (rd == null || lt == null) return out;

  // Server-driven knobs (optional)
  let adjCfg = null;
  try {
    const t = (__liveLensTuning && typeof __liveLensTuning === 'object') ? __liveLensTuning : null;
    const a = t && t.adjustments && typeof t.adjustments === 'object' ? t.adjustments : null;
    const g = a && a.game_total && typeof a.game_total === 'object' ? a.game_total : null;
    adjCfg = g;
  } catch (_) {
    adjCfg = null;
  }
  if (adjCfg && adjCfg.enabled === false) return out;

  const expHomePace = n(meta && meta.home_pace);
  const expAwayPace = n(meta && meta.away_pace);
  const expPace = (expHomePace != null && expAwayPace != null) ? ((expHomePace + expAwayPace) / 2.0) : null;

  const expTotal = n(meta && (meta.total_mean != null ? meta.total_mean : null));
  const expPpp = (expPace != null && expTotal != null && expPace > 1e-6) ? (expTotal / expPace) : null;

  const cm = n(curMinLeft);
  const elapsedMin = (cm != null) ? (48.0 - cm) : null;
  out.elapsed_min = elapsedMin;

  if (expPace == null || expPpp == null || elapsedMin == null) return out;
  const minElapsed = n(adjCfg && adjCfg.min_elapsed_min);
  if (elapsedMin < ((minElapsed != null) ? minElapsed : 6.0)) return out; // avoid noisy early-game adjustments

  // Live possessions: use away/home poss_est averages.
  let apPoss = null;
  let hpPoss = null;
  try {
    const pbpPoss = (live && live.pbp_possessions) ? live.pbp_possessions : null;
    const aTri = meta && meta.away ? String(meta.away).toUpperCase().trim() : '';
    const hTri = meta && meta.home ? String(meta.home).toUpperCase().trim() : '';
    const ap = pbpPoss && (pbpPoss[aTri] || pbpPoss.away) ? (pbpPoss[aTri] || pbpPoss.away) : null;
    const hp = pbpPoss && (pbpPoss[hTri] || pbpPoss.home) ? (pbpPoss[hTri] || pbpPoss.home) : null;
    apPoss = ap ? n(ap.poss_est) : null;
    hpPoss = hp ? n(hp.poss_est) : null;
  } catch (_) {
    apPoss = null;
    hpPoss = null;
  }

  if (apPoss == null || hpPoss == null) return out;

  const possLive = (apPoss + hpPoss) / 2.0;
  out.poss_live = possLive;
  if (!(possLive > 8.0)) return out;

  const possExpected = expPace * (elapsedMin / 48.0);
  out.poss_expected = possExpected;
  if (!(possExpected > 5.0)) return out;

  out.pace_ratio = possLive / possExpected;

  const totalPts = n(live && live.score ? live.score.total_pts : null);
  if (totalPts != null) {
    const actPpp = totalPts / Math.max(1.0, possLive);
    out.eff_ppp_delta = actPpp - expPpp;
  }

  // Convert pace/eff deltas into small edge adjustments.
  // - Pace is treated as more repeatable than hot/cold shooting.
  // - Keep adjustments modest + capped so we don't whipsaw tags.
  const paceCap = n(adjCfg && adjCfg.pace_cap_points);
  const effCap = n(adjCfg && adjCfg.eff_cap_points);
  const paceW = n(adjCfg && adjCfg.pace_weight);
  const effW = n(adjCfg && adjCfg.eff_weight);
  const paceCapPts = (paceCap != null) ? paceCap : 3.0;
  const effCapPts = (effCap != null) ? effCap : 4.0;
  const paceWeight = (paceW != null) ? paceW : 0.25;
  const effWeight = (effW != null) ? effW : 0.25;

  const pacePoints = (out.pace_ratio != null)
    ? clampNum((out.pace_ratio - 1.0) * lt, -30, 30)
    : null;
  const effPoints = (out.eff_ppp_delta != null)
    ? clampNum(out.eff_ppp_delta * expPace, -30, 30)
    : null;

  const paceBoost = (pacePoints != null) ? clampNum(pacePoints * paceWeight, -paceCapPts, paceCapPts) : 0.0;
  const effPenalty = (effPoints != null) ? clampNum(effPoints * effWeight, -effCapPts, effCapPts) : 0.0;

  // Over edges get penalized for hot shooting; Under edges get penalized for cold shooting.
  let adj = rd;
  if (rd > 0.5) {
    const boost = Math.max(0.0, paceBoost || 0.0);
    const pen = Math.max(0.0, effPenalty || 0.0);
    adj = rd + boost - pen;
  } else if (rd < -0.5) {
    const boost = Math.max(0.0, -(paceBoost || 0.0));
    const pen = Math.max(0.0, -(effPenalty || 0.0));
    adj = rd - boost + pen;
  }

  out.diff_adj = adj;
  return out;
}

function canonGameId(x) {
  const s = String(x ?? '').trim();
  if (!s) return '';
  const digits = s.replace(/\D/g, '');
  if (!digits) return s;
  const norm = (digits.replace(/^0+/, '') || '0');
  if (norm.length <= 10) return norm.padStart(10, '0');
  return digits;
}

async function postJson(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body || {}),
  });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`POST ${url} failed: ${res.status} ${t}`);
  }
  return res.json();
}

let __liveLensTimer = null;
const __liveLensLastLogged = new Map();
let __liveLensTuning = null;
let __liveLensTuningAt = 0;

function roundHalf(x) {
  const v = n(x);
  if (v == null) return null;
  return Math.round(v * 2) / 2;
}

function sanitizeTotalLine(x) {
  const v = n(x);
  if (v == null) return null;
  if (!Number.isFinite(v)) return null;
  if (Math.abs(v) < 0.001) return null;
  return v;
}

function parseClockToSecondsLeft(clock) {
  if (clock == null) return null;
  if (typeof clock === 'number' && Number.isFinite(clock)) return Math.max(0, Math.round(clock));
  const s = String(clock || '').trim();
  if (!s) return null;
  // ISO-ish: PT11M45.00S
  const m = s.match(/^PT(?:(\d+)M)?(?:(\d+)(?:\.\d+)?)S$/);
  if (m) {
    const mm = Number(m[1] || 0);
    const ss = Number(m[2] || 0);
    if (Number.isFinite(mm) && Number.isFinite(ss)) return Math.max(0, Math.round(mm * 60 + ss));
  }
  // mm:ss
  if (s.includes(':')) {
    const parts = s.split(':');
    if (parts.length === 2) {
      const mm = Number(parts[0]);
      const ss = Number(parts[1]);
      if (Number.isFinite(mm) && Number.isFinite(ss)) return Math.max(0, Math.round(mm * 60 + ss));
    }
  }
  const v = Number(s);
  return Number.isFinite(v) ? Math.max(0, Math.round(v)) : null;
}

function getTuningThresholds() {
  const t = (__liveLensTuning && typeof __liveLensTuning === 'object') ? __liveLensTuning : null;
  const mk = t && t.markets ? t.markets : null;
  function thr(key, defWatch, defBet) {
    const o = mk && mk[key] ? mk[key] : null;
    const w = n(o && o.watch);
    const b = n(o && o.bet);
    return {
      watch: (w != null) ? w : defWatch,
      bet: (b != null) ? b : defBet,
    };
  }
  return {
    total: thr('total', 3.0, 6.0),
    half_total: thr('half_total', 3.0, 6.0),
    quarter_total: thr('quarter_total', 2.0, 4.0),
    ats: thr('ats', 2.0, 4.0),
    roundHalf: !!(t && t.round_live_line_to_half),
    adjustments: (t && t.adjustments && typeof t.adjustments === 'object') ? t.adjustments : null,
    logging: (t && t.logging && typeof t.logging === 'object') ? t.logging : null,
  };
}

function startLiveLensPolling(root, games, dateStr) {
  if (__liveLensTimer != null) {
    try { clearInterval(__liveLensTimer); } catch (_) { /* ignore */ }
    __liveLensTimer = null;
  }

  const byGameId = new Map();
  (games || []).forEach((g) => {
    const gid = canonGameId((g && g.sim && g.sim.game_id != null) ? g.sim.game_id : '');
    if (!gid) return;
    const intervals = (g && g.sim && (g.sim.intervals_1m || g.sim.intervals))
      ? (g.sim.intervals_1m || g.sim.intervals)
      : (g ? (g.intervals_1m || g.intervals) : null);
    const marginMean = n(g && g.sim && g.sim.score ? g.sim.score.margin_mean : null);

    const ctx = (g && g.sim && g.sim.context && typeof g.sim.context === 'object') ? g.sim.context : {};
    const sc = (g && g.sim && g.sim.score && typeof g.sim.score === 'object') ? g.sim.score : {};
    const homePace = n(ctx && (ctx.home_pace != null ? ctx.home_pace : null));
    const awayPace = n(ctx && (ctx.away_pace != null ? ctx.away_pace : null));
    const homeMean = n(sc && (sc.home_mean != null ? sc.home_mean : null));
    const awayMean = n(sc && (sc.away_mean != null ? sc.away_mean : null));
    const totalMean = n(sc && (sc.total_mean != null ? sc.total_mean : null));

    byGameId.set(gid, {
      game_id: gid,
      home: String(g.home_tri || '').toUpperCase().trim(),
      away: String(g.away_tri || '').toUpperCase().trim(),
      intervals,
      margin_mean: marginMean,
      home_pace: homePace,
      away_pace: awayPace,
      home_mean: homeMean,
      away_mean: awayMean,
      total_mean: totalMean,
    });
  });

  async function ensureTuning() {
    const now = Date.now();
    if (__liveLensTuning && (now - __liveLensTuningAt) < (5 * 60 * 1000)) return;
    try {
      const t = await fetchJson('/api/live_lens_tuning?ttl=300');
      if (t && typeof t === 'object') {
        __liveLensTuning = t;
        __liveLensTuningAt = now;
      }
    } catch (_) {
      // ignore
    }
  }

  async function pollOnce() {
    if (!dateStr || !isYmd(dateStr)) return;

    await ensureTuning();
    const thr = getTuningThresholds();

    let sb;
    try {
      sb = await fetchJson(`/api/live_state?date=${encodeURIComponent(dateStr)}&ttl=12`);
    } catch (_) {
      // Back-compat: older servers only have /api/live/scoreboard
      try {
        const legacy = await fetchJson(`/api/live/scoreboard?date=${encodeURIComponent(dateStr)}`);
        const legacyGames = Array.isArray(legacy?.games) ? legacy.games : [];
        sb = {
          date: legacy?.date,
          ttl: 12,
          source: legacy?.source,
          games: legacyGames.map((g) => ({
            game_id: g?.game_id,
            event_id: g?.espn_event_id,
            home: g?.home,
            away: g?.away,
            home_pts: g?.home_pts,
            away_pts: g?.away_pts,
            status_id: g?.status_id,
            status: g?.status,
            period: g?.period,
            clock: g?.clock,
            in_progress: !!g?.in_progress,
            final: !!g?.final,
          })),
        };
      } catch (_) {
        return;
      }
    }

    const sbGames = Array.isArray(sb?.games) ? sb.games : [];
    const sbById = new Map();
    const sbEventByGid = new Map();
    sbGames.forEach((x) => {
      const gid = canonGameId(x && x.game_id != null ? x.game_id : '');
      if (!gid) return;
      sbById.set(gid, x);
      const eid = String(x && x.event_id != null ? x.event_id : '').trim();
      if (eid) sbEventByGid.set(gid, eid);
    });

    const detailIds = [];
    const detailEventIds = [];
    const lineEventIds = [];
    byGameId.forEach((meta, gid) => {
      const s = sbById.get(gid);
      if (s && (s.in_progress || s.final)) {
        detailIds.push(gid);
        const eid = sbEventByGid.get(gid);
        if (eid) detailEventIds.push(eid);
        if (s.in_progress && eid) lineEventIds.push(eid);
      }

      // Always update status line if present
      const el = root.querySelector(`.live-lens[data-game-id="${CSS.escape(gid)}"]`);
      if (!el) return;

      // Visible confirmation that polling is active
      try {
        const upd = el.querySelector('.lens-live-updated');
        if (upd) {
          const now = new Date();
          const hh = String(now.getHours()).padStart(2, '0');
          const mm = String(now.getMinutes()).padStart(2, '0');
          const ss = String(now.getSeconds()).padStart(2, '0');
          upd.textContent = `${hh}:${mm}:${ss}`;
        }
      } catch (_) {
        // ignore
      }
      const statusEl = el.querySelector('.lens-live-status');
      const scoreEl = el.querySelector('.lens-live-score');
      if (statusEl) statusEl.textContent = (s && s.status) ? String(s.status) : '—';
      if (scoreEl) {
        const hp = (s && s.home_pts != null) ? s.home_pts : '—';
        const ap = (s && s.away_pts != null) ? s.away_pts : '—';
        scoreEl.textContent = `${meta.away} ${ap} – ${hp} ${meta.home}`;
      }
    });

    // Staged fetches (NCAAB-style): pbp stats + lines for in-progress games only
    let pbpMap = new Map();
    let linesMap = new Map();
    if (detailEventIds.length) {
      try {
        const pbpPromise = fetchJson(`/api/live_pbp_stats?ttl=20&event_ids=${encodeURIComponent(detailEventIds.join(','))}&date=${encodeURIComponent(dateStr)}`);
        const linesPromise = lineEventIds.length
          ? fetchJson(`/api/live_lines?ttl=20&date=${encodeURIComponent(dateStr)}&event_ids=${encodeURIComponent(lineEventIds.join(','))}`)
          : Promise.resolve({ games: [] });
        const [pbp, lines] = await Promise.all([pbpPromise, linesPromise]);
        const pbpGames = Array.isArray(pbp?.games) ? pbp.games : [];
        pbpGames.forEach((gg) => {
          const eid = String(gg && gg.event_id != null ? gg.event_id : '').trim();
          if (eid) pbpMap.set(eid, gg);
        });
        const lineGames = Array.isArray(lines?.games) ? lines.games : [];
        lineGames.forEach((gg) => {
          const eid = String(gg && gg.event_id != null ? gg.event_id : '').trim();
          if (eid) linesMap.set(eid, gg);
        });
      } catch (_) {
        // ignore multi-fetch failures; we still show basic state
      }
    }

    await Promise.all(detailIds.map(async (gid) => {
      const meta = byGameId.get(gid);
      if (!meta) return;

      const s = sbById.get(gid);
      if (!s) return;
      const eid = sbEventByGid.get(gid);
      const pbp = eid ? pbpMap.get(eid) : null;
      const ln = eid ? linesMap.get(eid) : null;

      const isFinal = !!(s && s.final);
      const isInProgress = !!(s && s.in_progress);

      const homePts = n(s.home_pts);
      const awayPts = n(s.away_pts);
      const totalPts = (homePts != null && awayPts != null) ? (homePts + awayPts) : null;
      const homeMargin = (homePts != null && awayPts != null) ? (homePts - awayPts) : null;

      const period = (s && s.period != null) ? Number(s.period) : null;
      let secLeftPeriod = parseClockToSecondsLeft(s.clock);
      // ESPN finals sometimes carry a placeholder clock like 12:00; clamp to 0.
      if (isFinal) secLeftPeriod = 0;
      let gameMinLeft = null;
      let halfMinLeft = null;
      if (period != null && Number.isFinite(period) && secLeftPeriod != null) {
        if (isFinal) {
          gameMinLeft = 0.0;
          halfMinLeft = 0.0;
        } else {
          if (period <= 4) gameMinLeft = (((4 - period) * 12 * 60) + secLeftPeriod) / 60.0;
          else gameMinLeft = 0.0;
          if (period <= 2) halfMinLeft = (((2 - period) * 12 * 60) + secLeftPeriod) / 60.0;
          else halfMinLeft = 0.0;
        }
      }

      let lineTotal = n(ln && ln.lines ? ln.lines.total : null);
      let homeSpr = n(ln && ln.lines ? ln.lines.home_spread : null);
      // Treat 0 totals/spreads as missing placeholders.
      if (lineTotal != null && Math.abs(lineTotal) < 0.001) lineTotal = null;
      if (homeSpr != null && Math.abs(homeSpr) < 0.001 && lineTotal == null) homeSpr = null;
      const periodTotals = (ln && ln.lines && ln.lines.period_totals) ? ln.lines.period_totals : null;

      const live = {
        status: {
          period,
          clock: s.clock,
          in_progress: !!s.in_progress,
          final: !!s.final,
        },
        score: {
          home_pts: homePts,
          away_pts: awayPts,
          total_pts: totalPts,
          home_margin: homeMargin,
        },
        time: {
          sec_left_period: secLeftPeriod,
          game_min_left: gameMinLeft,
          half_min_left: halfMinLeft,
        },
        lines: {
          total: lineTotal,
          home_spread: homeSpr,
          period_totals: periodTotals,
        },
        pbp_attempts: pbp && pbp.pbp_attempts ? pbp.pbp_attempts : null,
        pbp_possessions: pbp && pbp.pbp_possessions ? pbp.pbp_possessions : null,
        pbp_quarters: pbp && pbp.pbp_quarters ? pbp.pbp_quarters : null,
        _event_id: eid || null,
      };

      const el = root.querySelector(`.live-lens[data-game-id="${CSS.escape(gid)}"]`);
      if (!el) return;

      // Expose status for computeScope() so it can suppress Lean/Signal on finals.
      try {
        el.dataset.final = isFinal ? '1' : '0';
        el.dataset.inProgress = isInProgress ? '1' : '0';
      } catch (_) {
        // ignore
      }

      const minLeftRaw = n(live && live.time ? live.time.game_min_left : null);
      const halfMinLeftRaw = n(live && live.time ? live.time.half_min_left : null);
      const secLeftPeriodRaw = n(live && live.time ? live.time.sec_left_period : null);

      // Lines
      const linesEl = el.querySelector('.lens-live-lines');
      if (linesEl) {
        const pieces = [];
        if (lineTotal != null) pieces.push(`Tot ${fmt(lineTotal, 1)}`);
        if (homeSpr != null) pieces.push(`Spr ${fmt(homeSpr, 1)}`);

        try {
          const pNow = (period == null) ? null : Number(period);
          const h1LineRaw = periodTotals && periodTotals.h1 != null ? n(periodTotals.h1) : null;
          const h1Line = (h1LineRaw != null && Math.abs(h1LineRaw) < 0.001) ? null : h1LineRaw;
          if (h1Line != null && (pNow == null || pNow <= 2) && !isFinal) pieces.push(`1H ${fmt(h1Line, 1)}`);

          if (pNow != null && Number.isFinite(pNow) && pNow >= 1 && pNow <= 4) {
            const qLineRaw = periodTotals && periodTotals[`q${pNow}`] != null ? n(periodTotals[`q${pNow}`]) : null;
            const qLine = (qLineRaw != null && Math.abs(qLineRaw) < 0.001) ? null : qLineRaw;
            if (qLine != null && !isFinal) pieces.push(`Q${pNow} ${fmt(qLine, 1)}`);
          }
        } catch (_) {
          // ignore
        }
        linesEl.textContent = pieces.length ? pieces.join(' · ') : 'Lines —';
      }

      // Attempts (team-order)
      const pbpAttempts = (live && live.pbp_attempts) ? live.pbp_attempts : null;
      const a = pbpAttempts && (pbpAttempts[meta.away] || pbpAttempts.away) ? (pbpAttempts[meta.away] || pbpAttempts.away) : null;
      const h = pbpAttempts && (pbpAttempts[meta.home] || pbpAttempts.home) ? (pbpAttempts[meta.home] || pbpAttempts.home) : null;
      const attemptsEl = el.querySelector('.lens-live-attempts');
      if (attemptsEl && a && h) {
        const ft = `FT ${n(a.ft_made) ?? 0}/${n(a.ft_att) ?? 0}-${n(h.ft_made) ?? 0}/${n(h.ft_att) ?? 0}`;
        const p2 = `2P ${n(a.fg2_made) ?? 0}/${n(a.fg2_att) ?? 0}-${n(h.fg2_made) ?? 0}/${n(h.fg2_att) ?? 0}`;
        const p3 = `3P ${n(a.fg3_made) ?? 0}/${n(a.fg3_att) ?? 0}-${n(h.fg3_made) ?? 0}/${n(h.fg3_att) ?? 0}`;

        let possTxt = '';
        try {
          const pbpPoss = (live && live.pbp_possessions) ? live.pbp_possessions : null;
          const ap = pbpPoss && (pbpPoss[meta.away] || pbpPoss.away) ? (pbpPoss[meta.away] || pbpPoss.away) : null;
          const hp = pbpPoss && (pbpPoss[meta.home] || pbpPoss.home) ? (pbpPoss[meta.home] || pbpPoss.home) : null;
          const apPoss = ap ? n(ap.poss_est) : null;
          const hpPoss = hp ? n(hp.poss_est) : null;
          if (apPoss != null && hpPoss != null) possTxt = ` · Poss ${fmt(apPoss, 0)}-${fmt(hpPoss, 0)}`;
        } catch (_) {
          // ignore
        }

        attemptsEl.textContent = `${ft} · ${p2} · ${p3}${possTxt}`;
      }

      // Auto-fill full game scope
      try {
        const gameCol = el.querySelector('.lens-col[data-scope="game"]');
        if (gameCol) {
          const sel = gameCol.querySelector('select.lens-min');
          const tot = gameCol.querySelector('input.lens-total');
          const liv = gameCol.querySelector('input.lens-live');
          let vMin = (minLeftRaw == null) ? null : Math.round(minLeftRaw);
          if (period != null && Number(period) > 4) vMin = 0; // OT: clamp to regulation end (SmartSim ladder is regulation)
          if (isFinal) vMin = 0;
          if (vMin != null) vMin = Math.max(0, Math.min(48, vMin));
          if (sel && vMin != null) sel.value = String(vMin);
          if (tot && totalPts != null) tot.value = String(Math.round(totalPts));
          if (liv) liv.value = (lineTotal != null) ? String(lineTotal) : '';
          if (sel) sel.dispatchEvent(new Event('change'));
          if (tot) tot.dispatchEvent(new Event('input'));
          if (liv) liv.dispatchEvent(new Event('input'));
        }
      } catch (_) {
        // ignore
      }

      try {
        const qt = (live && live.pbp_quarters && live.pbp_quarters.q_totals) ? live.pbp_quarters.q_totals : null;
        const curQ = (live && live.pbp_quarters && live.pbp_quarters.current) ? live.pbp_quarters.current : null;
        for (let qNum = 1; qNum <= 4; qNum += 1) {
          const qCol = el.querySelector(`.lens-col[data-scope="q${qNum}"]`);
          if (!qCol) continue;
          const sel = qCol.querySelector('select.lens-min');
          const tot = qCol.querySelector('input.lens-total');
          const liv = qCol.querySelector('input.lens-live');

          let qMinLeft = null;
          let qAct = qt && qt[`q${qNum}`] != null ? n(qt[`q${qNum}`]) : null;

          // If this is the current quarter, prefer the running total.
          try {
            const curPer = curQ && curQ.period != null ? Number(curQ.period) : null;
            if (curPer != null && Number.isFinite(curPer) && Number(curPer) === Number(qNum)) {
              const running = n(curQ && curQ.q_total);
              if (running != null) qAct = running;
            }
          } catch (_) {
            // ignore
          }

          const pNow = (period == null) ? null : Number(period);
          if (pNow != null && Number.isFinite(pNow)) {
            if (pNow < qNum) {
              qMinLeft = 12;
              if (qAct == null) qAct = 0;
            } else if (pNow === qNum) {
              if (secLeftPeriodRaw != null) qMinLeft = Math.round(secLeftPeriodRaw / 60.0);
            } else if (pNow > qNum) {
              qMinLeft = 0;
            }
          }

          if (qMinLeft != null) qMinLeft = Math.max(0, Math.min(12, qMinLeft));
          if (sel && qMinLeft != null) sel.value = String(qMinLeft);
          if (tot && qAct != null) tot.value = String(Math.round(qAct));

          // Quarter live total line (OddsAPI) when available
          try {
            const qLine = periodTotals && periodTotals[`q${qNum}`] != null ? n(periodTotals[`q${qNum}`]) : null;
            const qLine2 = (qLine != null && Math.abs(qLine) < 0.001) ? null : qLine;
            if (liv) liv.value = (qLine2 != null) ? String(qLine2) : '';
            if (liv) liv.dispatchEvent(new Event('input'));
          } catch (_) {
            // ignore
          }

          if (sel) sel.dispatchEvent(new Event('change'));
          if (tot) tot.dispatchEvent(new Event('input'));
        }
      } catch (_) {
        // ignore
      }

      // Auto-fill 1H scope only while in 1H
      try {
        const halfCol = el.querySelector('.lens-col[data-scope="half"]');
        if (halfCol) {
          const sel = halfCol.querySelector('select.lens-min');
          const tot = halfCol.querySelector('input.lens-total');
          const liv = halfCol.querySelector('input.lens-live');
          let vMin = (halfMinLeftRaw == null) ? null : Math.round(halfMinLeftRaw);
          if (vMin != null) vMin = Math.max(0, Math.min(24, vMin));
          if (sel && vMin != null) sel.value = String(vMin);
          // Half actual total: during 1H use current total; in 2H/final use Q1+Q2.
          let halfAct = totalPts;
          try {
            if (period != null && Number.isFinite(Number(period)) && Number(period) > 2) {
              const qt = (live && live.pbp_quarters && live.pbp_quarters.q_totals) ? live.pbp_quarters.q_totals : null;
              const q1 = qt && qt.q1 != null ? n(qt.q1) : null;
              const q2 = qt && qt.q2 != null ? n(qt.q2) : null;
              if (q1 != null && q2 != null) halfAct = q1 + q2;
            }
          } catch (_) {
            // ignore
          }
          if (tot && halfAct != null) tot.value = String(Math.round(halfAct));

          // Half live total line (OddsAPI) when available (only meaningful in 1H)
          const h1LineRaw = periodTotals && periodTotals.h1 != null ? n(periodTotals.h1) : null;
          const h1Line = (h1LineRaw != null && Math.abs(h1LineRaw) < 0.001) ? null : h1LineRaw;
          if (liv) liv.value = (h1Line != null && (period == null || Number(period) <= 2) && !isFinal) ? String(h1Line) : '';
          if (sel) sel.dispatchEvent(new Event('change'));
          if (tot) tot.dispatchEvent(new Event('input'));
          if (liv) liv.dispatchEvent(new Event('input'));
        }
      } catch (_) {
        // ignore
      }

      // If game is final: do not show or log any BET/WATCH signals.
      // (Still allow status/score/attempts/lines to render.)
      if (live && live.status && live.status.final) {
        try {
          const recTotalEl = el.querySelector('.lens-rec-total');
          const recHalfEl = el.querySelector('.lens-rec-half');
          const recQtrEl = el.querySelector('.lens-rec-qtr');
          const recATSEl = el.querySelector('.lens-rec-ats');
          if (recTotalEl) recTotalEl.textContent = 'Total: —';
          if (recHalfEl) recHalfEl.textContent = '1H: —';
          if (recQtrEl) recQtrEl.textContent = 'Q: —';
          if (recATSEl) recATSEl.textContent = 'ATS: —';
        } catch (_) {
          // ignore
        }
        try {
          const card = el.closest('.card');
          if (card) {
            card.classList.remove('live-edge-ok');
            card.classList.remove('live-edge-warn');
          }
        } catch (_) {
          // ignore
        }
        return;
      }

      // Compute tags (Total)
      const recTotalEl = el.querySelector('.lens-rec-total');
      const recHalfEl = el.querySelector('.lens-rec-half');
      const recQtrEl = el.querySelector('.lens-rec-qtr');
      const recATSEl = el.querySelector('.lens-rec-ats');
      let curMinLeft = (minLeftRaw == null) ? 48 : Math.max(0, Math.min(48, Math.round(minLeftRaw)));
      if (period != null && Number(period) > 4) curMinLeft = 0;
      const lens = computePaceFinalFromIntervals(meta.intervals, 48, curMinLeft, totalPts);
      let totalDiffRaw = null;
      let totalDiff = null;
      let totalCtx = null;
      let totalClass = 'NONE';
      let totalSide = null;

      if (lens && lineTotal != null) {
        totalDiffRaw = lens.paceFinal - lineTotal;
        totalCtx = adjustGameTotalDiffWithContext(totalDiffRaw, lineTotal, meta, live, curMinLeft);
        totalDiff = (totalCtx && totalCtx.diff_adj != null) ? n(totalCtx.diff_adj) : totalDiffRaw;
        totalClass = classifyDiff(Math.abs(totalDiff), thr.total.watch, thr.total.bet);
        if (totalDiff > 1.0) totalSide = 'Over';
        else if (totalDiff < -1.0) totalSide = 'Under';
        else totalSide = 'No edge';
      }
      if (recTotalEl) {
        if (totalClass === 'BET') recTotalEl.textContent = `Total: BET ${totalSide} (${fmt(totalDiff, 1)})`;
        else if (totalClass === 'WATCH') recTotalEl.textContent = `Total: WATCH ${totalSide} (${fmt(totalDiff, 1)})`;
        else recTotalEl.textContent = 'Total: —';
      }

      // Half-level signal (vs pregame half baseline) during 1H only
      let halfClass = 'NONE';
      let halfSide = null;
      let halfDiff = null;
      try {
        if (recHalfEl && (period == null || Number(period) <= 2)) {
          const halfCol = el.querySelector('.lens-col[data-scope="half"]');
          const pf = halfCol ? n(halfCol.dataset.paceFinal) : null;
          const sf = halfCol ? n(halfCol.dataset.simFinal) : null;
          const hl = halfCol ? n(halfCol.dataset.liveTotal) : null;
          if (pf != null && hl != null) {
            halfDiff = pf - hl;
            halfClass = classifyDiff(Math.abs(halfDiff), thr.half_total.watch, thr.half_total.bet);
            if (halfDiff > 1.0) halfSide = 'Over';
            else if (halfDiff < -1.0) halfSide = 'Under';
            else halfSide = 'No edge';
          } else if (pf != null && sf != null) {
            // Fallback: vs pregame half baseline when no live half line.
            halfDiff = pf - sf;
            halfClass = classifyDiff(Math.abs(halfDiff), thr.half_total.watch, thr.half_total.bet);
            if (halfDiff > 1.0) halfSide = 'Over';
            else if (halfDiff < -1.0) halfSide = 'Under';
            else halfSide = 'No edge';
          }
          if (halfClass === 'BET') recHalfEl.textContent = `1H: BET ${halfSide} (${fmt(halfDiff, 1)})`;
          else if (halfClass === 'WATCH') recHalfEl.textContent = `1H: WATCH ${halfSide} (${fmt(halfDiff, 1)})`;
          else recHalfEl.textContent = '1H: —';
        } else if (recHalfEl) {
          recHalfEl.textContent = '1H: —';
        }
      } catch (_) {
        if (recHalfEl) recHalfEl.textContent = '1H: —';
      }

      // Quarter-level signal for the current regulation quarter (vs pregame quarter baseline)
      let qClass = 'NONE';
      let qSide = null;
      let qDiff = null;
      let qLabel = 'Q';
      try {
        const pNow = (period == null) ? null : Number(period);
        if (recQtrEl && pNow != null && Number.isFinite(pNow) && pNow >= 1 && pNow <= 4) {
          const qNum = Math.floor(pNow);
          qLabel = `Q${qNum}`;
          const qCol = el.querySelector(`.lens-col[data-scope="q${qNum}"]`);
          const pf = qCol ? n(qCol.dataset.paceFinal) : null;
          const sf = qCol ? n(qCol.dataset.simFinal) : null;
          const ql = qCol ? n(qCol.dataset.liveTotal) : null;
          if (pf != null && ql != null) {
            qDiff = pf - ql;
            qClass = classifyDiff(Math.abs(qDiff), thr.quarter_total.watch, thr.quarter_total.bet);
            if (qDiff > 1.0) qSide = 'Over';
            else if (qDiff < -1.0) qSide = 'Under';
            else qSide = 'No edge';
          } else if (pf != null && sf != null) {
            // Fallback: vs pregame quarter baseline when no live quarter line.
            qDiff = pf - sf;
            qClass = classifyDiff(Math.abs(qDiff), thr.quarter_total.watch, thr.quarter_total.bet);
            if (qDiff > 1.0) qSide = 'Over';
            else if (qDiff < -1.0) qSide = 'Under';
            else qSide = 'No edge';
          }
          if (qClass === 'BET') recQtrEl.textContent = `${qLabel}: BET ${qSide} (${fmt(qDiff, 1)})`;
          else if (qClass === 'WATCH') recQtrEl.textContent = `${qLabel}: WATCH ${qSide} (${fmt(qDiff, 1)})`;
          else recQtrEl.textContent = `${qLabel}: —`;
        } else if (recQtrEl) {
          recQtrEl.textContent = 'Q: —';
        }
      } catch (_) {
        if (recQtrEl) recQtrEl.textContent = 'Q: —';
      }

      // Compute tags (ATS) using blended margin (pregame -> live)
      let atsClass = 'NONE';
      let atsText = 'ATS: —';
      let atsEdge = null;
      if (homeSpr != null && meta.margin_mean != null && live && live.score) {
        const curMargin = n(live.score.home_margin);
        const minLeft = n(live.time ? live.time.game_min_left : null);
        const elapsed = (minLeft != null) ? (48 - minLeft) : (lens ? lens.elapsedMinutes : 0);
        const w = Math.max(0, Math.min(1, (elapsed || 0) / 48.0));
        const adjMargin = (1 - w) * meta.margin_mean + w * (curMargin ?? 0);
        // Home covers if margin > -homeSpr (e.g., -4.5 => margin > 4.5)
        const homeEdge = adjMargin + homeSpr;
        const awayEdge = -adjMargin - homeSpr;
        const pickHome = Math.abs(homeEdge) >= Math.abs(awayEdge);
        atsEdge = pickHome ? homeEdge : awayEdge;
        const side = pickHome ? meta.home : meta.away;
        atsClass = classifyDiff(Math.abs(atsEdge), thr.ats.watch, thr.ats.bet);
        if (atsClass === 'BET') atsText = `ATS: BET ${side} (${fmt(atsEdge, 1)})`;
        else if (atsClass === 'WATCH') atsText = `ATS: WATCH ${side} (${fmt(atsEdge, 1)})`;
      }
      if (recATSEl) recATSEl.textContent = atsText;

      // Card-level highlight (NCAAB parity)
      try {
        const card = el.closest('.card');
        if (card) {
          const anyBet = (totalClass === 'BET') || (halfClass === 'BET') || (qClass === 'BET') || (atsClass === 'BET');
          const anyWatch = (totalClass === 'WATCH') || (halfClass === 'WATCH') || (qClass === 'WATCH') || (atsClass === 'WATCH');
          card.classList.toggle('live-edge-ok', anyBet);
          card.classList.toggle('live-edge-warn', (!anyBet) && anyWatch);
          if (!anyBet && !anyWatch) {
            card.classList.remove('live-edge-ok');
            card.classList.remove('live-edge-warn');
          }
        }
      } catch (_) {
        // ignore
      }

      // Log watch/bet signals (throttled to once/minute per market)
      const logCfg = (thr && thr.logging && typeof thr.logging === 'object') ? thr.logging : null;
      const logMode = (logCfg && typeof logCfg.mode === 'string' && logCfg.mode.trim()) ? logCfg.mode.trim().toLowerCase() : 'bet';
      const minIntervalSecRaw = n(logCfg && logCfg.min_interval_sec);
      const minIntervalSec = (minIntervalSecRaw != null && minIntervalSecRaw >= 5) ? minIntervalSecRaw : 60;
      const bucketKey = String(Math.floor((Date.now() / 1000.0) / minIntervalSec));

      async function maybeLog(market, klass, payload) {
        // Default parity: BET only. Optional modes for tuning.
        const allow = (logMode === 'bet')
          ? (klass === 'BET')
          : (logMode === 'watch')
            ? (klass === 'WATCH' || klass === 'BET')
            : (logMode === 'all')
              ? (klass === 'NONE' || klass === 'WATCH' || klass === 'BET')
              : (klass === 'BET');
        if (!allow) return;
        const k = `${gid}:${market}`;
        const last = __liveLensLastLogged.get(k);
        if (last === bucketKey) return;
        __liveLensLastLogged.set(k, bucketKey);
        try {
          await postJson('/api/live_lens_signal', payload);
        } catch (_) {
          // ignore logging failures
        }
      }

      const shouldRound = thr.roundHalf;
      const safeLineTotal = sanitizeTotalLine(lineTotal);
      const baseLog = {
        date: dateStr,
        game_id: gid,
        event_id: live._event_id,
        home: meta.home,
        away: meta.away,
        pbp: live.pbp_attempts,
        pbp_possessions: live.pbp_possessions,
        tuning_source: 'api',
      };

      // Game total
      await maybeLog('game_total', totalClass, {
        ...baseLog,
        klass: totalClass,
        horizon: 'game',
        market: 'total',
        elapsed: (curMinLeft != null) ? (48 - curMinLeft) : null,
        remaining: curMinLeft,
        total_points: totalPts,
        live_line: shouldRound ? roundHalf(safeLineTotal) : safeLineTotal,
        side: totalSide,
        edge: totalDiff,
        edge_raw: totalDiffRaw,
        edge_adj: totalDiff,
        strength: (totalDiff != null) ? Math.abs(totalDiff) : null,
        context: (totalCtx && typeof totalCtx === 'object') ? {
          pace_ratio: totalCtx.pace_ratio,
          eff_ppp_delta: totalCtx.eff_ppp_delta,
          poss_live: totalCtx.poss_live,
          poss_expected: totalCtx.poss_expected,
          elapsed_min: totalCtx.elapsed_min,
          exp_home_pace: meta.home_pace,
          exp_away_pace: meta.away_pace,
          exp_total_mean: meta.total_mean,
        } : null,
      });

      // 1H total (only 1H)
      await maybeLog('h1_total', halfClass, {
        ...baseLog,
        klass: halfClass,
        horizon: 'h1',
        market: 'half_total',
        elapsed: (halfMinLeftRaw != null) ? (24 - Math.max(0, Math.min(24, Math.round(halfMinLeftRaw)))) : null,
        remaining: (halfMinLeftRaw != null) ? Math.max(0, Math.min(24, Math.round(halfMinLeftRaw))) : null,
        total_points: totalPts,
        live_line: shouldRound
          ? roundHalf(sanitizeTotalLine(periodTotals && periodTotals.h1 != null ? n(periodTotals.h1) : null))
          : sanitizeTotalLine(periodTotals && periodTotals.h1 != null ? n(periodTotals.h1) : null),
        side: halfSide,
        edge: halfDiff,
        strength: (halfDiff != null) ? Math.abs(halfDiff) : null,
      });

      // Current quarter total
      await maybeLog('q_total', qClass, {
        ...baseLog,
        klass: qClass,
        horizon: qLabel.toLowerCase(),
        market: 'quarter_total',
        elapsed: (secLeftPeriodRaw != null) ? (12 - Math.max(0, Math.min(12, Math.round((secLeftPeriodRaw / 60.0))))) : null,
        remaining: (secLeftPeriodRaw != null) ? Math.max(0, Math.min(12, Math.round((secLeftPeriodRaw / 60.0)))) : null,
        total_points: null,
        live_line: shouldRound
          ? roundHalf(sanitizeTotalLine(periodTotals && periodTotals[qLabel.toLowerCase()] != null ? n(periodTotals[qLabel.toLowerCase()]) : null))
          : sanitizeTotalLine(periodTotals && periodTotals[qLabel.toLowerCase()] != null ? n(periodTotals[qLabel.toLowerCase()]) : null),
        side: qSide,
        edge: qDiff,
        strength: (qDiff != null) ? Math.abs(qDiff) : null,
      });

      // ATS
      await maybeLog('game_ats', atsClass, {
        ...baseLog,
        klass: atsClass,
        horizon: 'game',
        market: 'ats',
        elapsed: (curMinLeft != null) ? (48 - curMinLeft) : null,
        remaining: curMinLeft,
        total_points: totalPts,
        live_line: shouldRound ? roundHalf(homeSpr) : homeSpr,
        side: (atsClass === 'BET' || atsClass === 'WATCH') ? atsText.replace(/^ATS:\s*(BET|WATCH)\s*/i, '').replace(/\s*\([^)]*\)\s*$/, '') : null,
        edge: atsEdge,
        strength: (atsEdge != null) ? Math.abs(atsEdge) : null,
      });
    }));
  }

  // Kick off immediately, then every 12s (NCAAB parity)
  pollOnce();
  __liveLensTimer = setInterval(pollOnce, 12 * 1000);
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

function renderCards(games, reconGameRows, reconQuarterRows, showResults, hideOdds, dateStr) {
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
    const intervals = sim.intervals_1m || sim.intervals || g.intervals_1m || g.intervals || null;
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
          ${renderLiveLens(intervals, `${homeTri}_${awayTri}`, g && g.sim ? g.sim.game_id : null)}
        </div>

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

  // Automated live polling (60s) for Live Lens
  try {
    startLiveLensPolling(root, games, dateStr);
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

    renderCards(games, reconGameRows, reconQuarterRows, showResults, hideOdds, dateStr);
  } catch (e) {
    setNote(`Failed to load cards: ${String(e && e.message ? e.message : e)}`);
    renderCards([], [], [], false, false, dateStr);
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
