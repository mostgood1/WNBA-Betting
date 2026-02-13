// Cards page renderer (SmartSim-driven).
// Includes: odds, bet leans, quarter projections, projected boxscores, prop targets, and matchup write-up.

function localYMD() {
  const tz = 'America/New_York';
  const cutoffHour = 6; // Treat 12:00am–5:59am ET as the prior NBA slate day.
  try {
    const now = new Date();
    const hourStr = new Intl.DateTimeFormat('en-US', {
      timeZone: tz,
      hour: '2-digit',
      hour12: false,
    }).format(now);
    const hour = Number(hourStr);
    const base = (Number.isFinite(hour) && hour < cutoffHour)
      ? new Date(now.getTime() - 24 * 60 * 60 * 1000)
      : now;
    return new Intl.DateTimeFormat('en-CA', {
      timeZone: tz,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).format(base);
  } catch (_) {
    const d = new Date();
    const base = (d.getHours() < cutoffHour)
      ? new Date(d.getTime() - 24 * 60 * 60 * 1000)
      : d;
    const y = base.getFullYear();
    const m = String(base.getMonth() + 1).padStart(2, '0');
    const day = String(base.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }
}

function isYmd(s) {
  return typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s);
}

function n(x) {
  if (x == null) return null;
  const s = (typeof x === 'string') ? x.trim() : x;
  if (s === '') return null;
  const v = Number(s);
  return Number.isFinite(v) ? v : null;
}

function fetchJsonWithTimeout(url, timeoutMs) {
  const ms = Number(timeoutMs);
  if (!Number.isFinite(ms) || ms <= 0) return fetchJson(url);
  const ctrl = new AbortController();
  const t = setTimeout(() => {
    try { ctrl.abort(); } catch (_) { /* ignore */ }
  }, ms);
  return fetch(url, { cache: 'no-store', signal: ctrl.signal })
    .then((r) => {
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
      return r.json();
    })
    .finally(() => {
      try { clearTimeout(t); } catch (_) { /* ignore */ }
    });
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

function canonNbaGameId10(gameId) {
  try {
    const raw = String(gameId ?? '').trim();
    const digits = raw.replace(/\D+/g, '');
    if (digits.length === 8) return `00${digits}`;
    if (digits.length === 9) return `0${digits}`;
    return digits;
  } catch (_) {
    return '';
  }
}

function buildReconPlayersIndex(reconPlayerRows) {
  const idx = {};
  const rows = Array.isArray(reconPlayerRows) ? reconPlayerRows : [];
  for (const r of rows) {
    try {
      const gid = canonNbaGameId10(r.game_id);
      const team = String(r.team_tri ?? '').trim().toUpperCase();
      const pid = String(r.player_id ?? '').trim();
      if (!gid || !team || !pid) continue;
      if (!idx[gid]) idx[gid] = {};
      if (!idx[gid][team]) idx[gid][team] = {};
      idx[gid][team][pid] = r;
    } catch (_) {
      // ignore
    }
  }
  return idx;
}

function reconTeamSummary(reconByPlayerId) {
  try {
    const rows = Object.values(reconByPlayerId || {});
    let nPts = 0, sumAbsPts = 0;
    let nPra = 0, sumAbsPra = 0;
    let missing = 0;
    for (const r of rows) {
      const miss = String(r.missing_actual ?? '').toLowerCase().trim();
      if (miss === 'true' || miss === '1') {
        missing += 1;
        continue;
      }
      const ePts = n(r.err_pts);
      if (ePts != null) { nPts += 1; sumAbsPts += Math.abs(ePts); }
      const ePra = n(r.err_pra);
      if (ePra != null) { nPra += 1; sumAbsPra += Math.abs(ePra); }
    }
    const maePts = nPts ? (sumAbsPts / nPts) : null;
    const maePra = nPra ? (sumAbsPra / nPra) : null;
    return { maePts, maePra, missing };
  } catch (_) {
    return { maePts: null, maePra: null, missing: 0 };
  }
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

function renderPlayersTable(title, players, reconByPlayerId) {
  const arr = Array.isArray(players) ? [...players] : [];
  // Sort by minutes first so the table reflects the expected rotation.
  arr.sort((a, b) => {
    const dm = (n(b?.min_mean) ?? -1e9) - (n(a?.min_mean) ?? -1e9);
    if (dm !== 0) return dm;
    return (n(b?.pts_mean) ?? -1e9) - (n(a?.pts_mean) ?? -1e9);
  });
  const top = arr.slice(0, 10);

  const hasRecon = !!(reconByPlayerId && typeof reconByPlayerId === 'object' && Object.keys(reconByPlayerId).length);

  const rows = top.map((p) => {
    const nm = String(p.player_name || '').trim();
    const st = String(p.injury_status || '').trim().toUpperCase();
    const isOut = (st === 'OUT') || (p.playing_today === false);
    const inj = isOut ? ' <span class="badge bad">OUT</span>' : '';
    const play = '';

    let actPts = '—';
    let actPra = '—';
    let actMin = '—';
    let actReb = '—';
    let actAst = '—';
    let act3pm = '—';
    let dPra = '—';
    try {
      if (hasRecon) {
        const pid = String(p && p.player_id != null ? p.player_id : '').trim();
        const rr = pid ? (reconByPlayerId[pid] || null) : null;
        if (rr && String(rr.missing_actual ?? '').toLowerCase().trim() !== 'true') {
          actMin = fmt(rr.actual_min, 1);
          actPts = fmt(rr.actual_pts, 1);
          actReb = fmt(rr.actual_reb, 1);
          actAst = fmt(rr.actual_ast, 1);
          act3pm = fmt(rr.actual_3pm, 1);
          actPra = fmt(rr.actual_pra, 1);
          const ePra = n(rr.err_pra);
          dPra = (ePra == null) ? '—' : fmt(ePra, 1);
        }
      }
    } catch (_) {
      // ignore
    }
    return `
      <tr>
        <td>${esc(nm)}${inj}${play}</td>
        <td class="num">${fmt(p.min_mean, 1)}</td>
        ${hasRecon ? `<td class="num">${esc(actMin)}</td>` : ''}
        <td class="num">${fmt(p.pts_mean, 1)}</td>
        ${hasRecon ? `<td class="num">${esc(actPts)}</td>` : ''}
        <td class="num">${fmt(p.reb_mean, 1)}</td>
        ${hasRecon ? `<td class="num">${esc(actReb)}</td>` : ''}
        <td class="num">${fmt(p.ast_mean, 1)}</td>
        ${hasRecon ? `<td class="num">${esc(actAst)}</td>` : ''}
        <td class="num">${fmt(p.threes_mean, 1)}</td>
        ${hasRecon ? `<td class="num">${esc(act3pm)}</td>` : ''}
        <td class="num">${fmt(p.pra_mean, 1)}</td>
        ${hasRecon ? `<td class="num">${esc(actPra)}</td><td class="num">${esc(dPra)}</td>` : ''}
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
            ${hasRecon ? '<th class="num sortable" data-sort="num">ACT MIN</th>' : ''}
            <th class="num sortable" data-sort="num">PTS</th>
            ${hasRecon ? '<th class="num sortable" data-sort="num">ACT PTS</th>' : ''}
            <th class="num sortable" data-sort="num">REB</th>
            ${hasRecon ? '<th class="num sortable" data-sort="num">ACT REB</th>' : ''}
            <th class="num sortable" data-sort="num">AST</th>
            ${hasRecon ? '<th class="num sortable" data-sort="num">ACT AST</th>' : ''}
            <th class="num sortable" data-sort="num">3PM</th>
            ${hasRecon ? '<th class="num sortable" data-sort="num">ACT 3PM</th>' : ''}
            <th class="num sortable" data-sort="num">PRA</th>
            ${hasRecon ? '<th class="num sortable" data-sort="num">ACT PRA</th><th class="num sortable" data-sort="num">ΔPRA</th>' : ''}
          </tr>
        </thead>
        <tbody>
          ${rows || `<tr><td colspan="${hasRecon ? 14 : 7}" class="subtle">No player projections.</td></tr>`}
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

function renderLiveLens(intervals, cardKey, gameId, actualMeta) {
  const segsAll = intervals && Array.isArray(intervals.segments) ? intervals.segments : [];
  // Prefer regulation segments for tables/chips; OT segments have `ot` not `quarter`.
  const segsReg = segsAll.filter((s) => s && s.quarter != null);
  const segs = segsReg.length ? segsReg : segsAll;
  if (!segs.length) {
    const id0 = String(cardKey || '').replace(/[^A-Za-z0-9_\-]/g, '_');
    const gid0 = canonGameId((gameId == null) ? '' : gameId);
    return `
      <div class="market-tile live-lens" data-lens-id="${esc(id0)}" data-game-id="${esc(gid0)}">
        <div class="market-title">LIVE LENS</div>
        <div class="subtle" style="margin-top:6px;">No interval ladder available for this game (intervals missing/empty).</div>
      </div>
    `;
  }
  const id = String(cardKey || '').replace(/[^A-Za-z0-9_\-]/g, '_');
  const gid = canonGameId((gameId == null) ? '' : gameId);

  const parts = String(cardKey || '').split('_');
  const homeTri = String(parts[0] || '').toUpperCase().trim();
  const awayTri = String(parts[1] || '').toUpperCase().trim();

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
    return `
      <div class="lens-col" data-scope="q${qNum}">
        <div class="subtle lens-desc">Live lens (Q${qNum}; enter minutes remaining + total points; optional live total):</div>

        <div class="lens-pillbar">
          <span class="chip neutral lens-auto">Auto: Forced</span>
          <span class="chip neutral lens-phase">Live: —</span>
        </div>

        <div class="lens-inputs lens-inputs-3">
          <div class="lens-input">
            <span class="k">Min remaining</span>
            <span class="v"><select class="lens-min">${renderMinRemainingOptions(12)}</select></span>
          </div>
          <div class="lens-input">
            <span class="k">Total pts</span>
            <span class="v"><input class="lens-total" type="number" placeholder="—"></span>
          </div>
          <div class="lens-input">
            <span class="k">Live total</span>
            <span class="v"><input class="lens-live" type="number" placeholder="optional"></span>
          </div>
        </div>

        <div class="row chips lens-out lens-out-compact">
          <span class="chip neutral">Sim q50 @ time: <span class="fw-700 lens-sim-at">—</span></span>
          <span class="chip neutral">Δ (Sim–Act): <span class="fw-700 lens-delta">—</span></span>
          <span class="chip neutral">Pace final: <span class="fw-700 lens-pace">—</span></span>
          <span class="chip neutral">Sim q50 final: <span class="fw-700 lens-sim-final">—</span></span>
          <span class="chip neutral">Driver: <span class="fw-700 lens-driver">—</span></span>
          <span class="chip neutral">Lean: <span class="fw-700 lens-lean">—</span></span>
          <span class="chip neutral">Attempts: <span class="fw-700 lens-scope-attempts">—</span></span>
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

  function renderScopeCol(scope, title, totalMinutes, labelPrefix) {
    const liveLineLabel = (scope === 'game') ? 'Live total' : 'Live total';
    const paceLabel = (scope === 'game') ? 'Pace final (48m)' : (scope === 'half' ? 'Pace final (24m)' : `Pace final (${totalMinutes}m)`);
    const simFinalLabel = (scope === 'game') ? 'Sim q50 final' : (scope === 'half' ? 'Sim q50 final' : 'Sim q50 final');
    const atLabel = (scope === 'game') ? 'Sim q50 @ time' : (scope === 'half' ? 'Sim q50 @ time' : 'Sim q50 @ time');
    const scopeLabel = (scope === 'game') ? 'full game' : (scope === 'half' ? '1H' : scope);
    return `
      <div class="lens-col" data-scope="${esc(scope)}">
        <div class="subtle lens-desc">Live lens (${esc(scopeLabel)}; enter minutes remaining + total points; optional live total):</div>

        <div class="lens-pillbar">
          <span class="chip neutral lens-auto">Auto: Forced</span>
          <span class="chip neutral lens-phase">Live: —</span>
        </div>

        <div class="lens-inputs lens-inputs-3">
          <div class="lens-input">
            <span class="k">Min remaining</span>
            <span class="v"><select class="lens-min">${renderMinRemainingOptions(totalMinutes)}</select></span>
          </div>
          <div class="lens-input">
            <span class="k">Total pts</span>
            <span class="v"><input class="lens-total" type="number" placeholder="—"></span>
          </div>
          <div class="lens-input">
            <span class="k">${esc(liveLineLabel)}</span>
            <span class="v"><input class="lens-live" type="number" placeholder="optional"></span>
          </div>
        </div>

        <div class="row chips lens-out lens-out-compact">
          <span class="chip neutral">${esc(atLabel)}: <span class="fw-700 lens-sim-at">—</span></span>
          <span class="chip neutral">Δ (Sim–Act): <span class="fw-700 lens-delta">—</span></span>
          <span class="chip neutral">${esc(paceLabel)}: <span class="fw-700 lens-pace">—</span></span>
          <span class="chip neutral">${esc(simFinalLabel)}: <span class="fw-700 lens-sim-final">—</span></span>
          <span class="chip neutral">Driver: <span class="fw-700 lens-driver">—</span></span>
          <span class="chip neutral">Lean: <span class="fw-700 lens-lean">—</span></span>
          <span class="chip neutral">Attempts: <span class="fw-700 lens-scope-attempts">—</span></span>
        </div>
      </div>
    `;
  }

  function renderSummaryInner(simTotal) {
    const st = (simTotal == null) ? '—' : fmt(simTotal, 1);
    return `
      <div class="table-wrap" style="margin-top:6px;">
        <table class="data-table boxscore-table lens-summary-table" style="font-size:12px;">
          <thead>
            <tr>
              <th>Source</th>
              <th>Winner</th>
              <th>ATS</th>
              <th class="num">Total</th>
              <th>Score</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td class="fw-700">Sim</td>
              <td class="lens-sum-sim-winner">—</td>
              <td class="lens-sum-sim-ats">—</td>
              <td class="num lens-sum-sim-total">${esc(st)}</td>
              <td class="lens-sum-sim-score">—</td>
            </tr>
            <tr>
              <td class="fw-700">Line</td>
              <td class="lens-sum-line-winner">—</td>
              <td class="lens-sum-line-ats">—</td>
              <td class="num lens-sum-line-total">—</td>
              <td class="lens-sum-line-score">—</td>
            </tr>
            <tr>
              <td class="fw-700">Live</td>
              <td class="lens-sum-live-winner">—</td>
              <td class="lens-sum-live-ats">—</td>
              <td class="num lens-sum-live-total">—</td>
              <td class="lens-sum-live-score">—</td>
            </tr>
          </tbody>
        </table>
      </div>
    `;
  }

  function renderSegmentTile(scope, title, simTotal, innerHtml) {
    return `
      <div class="market-tile lens-seg" data-scope-seg="${esc(scope)}">
        <div class="lens-panel-head">
          <div class="market-title">${esc(title)}</div>
          <div class="subtle lens-panel-sub">${esc(segMin)}-min</div>
        </div>
        <div class="lens-summary" data-scope="${esc(scope)}">
          ${renderSummaryInner(simTotal)}
        </div>
        ${innerHtml || ''}
      </div>
    `;
  }

  const halfSimTotal = n(segs[halfFinalIdx] && segs[halfFinalIdx].cum_q && segs[halfFinalIdx].cum_q.p50);
  const gameSimTotal = n(segs[gameFinalIdx] && segs[gameFinalIdx].cum_q && segs[gameFinalIdx].cum_q.p50);

  const q1Segs0 = buildQuarterSegs(1);
  const q1FinalIdx0 = Math.min(q1Segs0.length - 1, Math.max(0, Math.round(12 / segMin) - 1));
  const q1SimTotal = (q1Segs0 && q1Segs0.length)
    ? n(q1Segs0[q1FinalIdx0] && q1Segs0[q1FinalIdx0].cum_q && q1Segs0[q1FinalIdx0].cum_q.p50)
    : null;

  const q3Segs0 = buildQuarterSegs(3);
  const q3FinalIdx0 = Math.min(q3Segs0.length - 1, Math.max(0, Math.round(12 / segMin) - 1));
  const q3SimTotal = (q3Segs0 && q3Segs0.length)
    ? n(q3Segs0[q3FinalIdx0] && q3Segs0[q3FinalIdx0].cum_q && q3Segs0[q3FinalIdx0].cum_q.p50)
    : null;

  const am = (actualMeta && typeof actualMeta === 'object') ? actualMeta : {};
  const dAttrs = [];
  function addAttr(k, v) {
    if (v == null) return;
    const s = String(v);
    if (!s.trim()) return;
    dAttrs.push(`data-${k}="${esc(s)}"`);
  }
  addAttr('actual-home', am.home_pts);
  addAttr('actual-away', am.away_pts);
  addAttr('actual-game-total', am.game_total);
  addAttr('actual-h1-total', am.h1_total);
  addAttr('actual-q1-total', am.q1_total);
  addAttr('actual-q3-total', am.q3_total);

  return `
    <div class="market-tile live-lens" data-lens-id="${esc(id)}" data-game-id="${esc(gid)}" data-player-only-lines="1" ${dAttrs.join(' ')}>
      <div class="lens-top">
        <div class="market-title">LIVE LENS</div>
        <div class="subtle lens-live-bar">Live: <span class="lens-live-status">—</span> · <span class="lens-live-score">—</span> · Updated <span class="lens-live-updated">—</span></div>
      </div>

      <div class="row chips lens-market-row" style="margin-top:4px;">
        <span class="chip neutral lens-market-ml">ML: —</span>
        <span class="chip neutral lens-market-ats">ATS: —</span>
        <span class="chip neutral lens-market-total">Total: —</span>
        <span class="chip neutral lens-market-1h-ats">1H ATS: —</span>
        <span class="chip neutral lens-market-1h-total">1H Total: —</span>
        <span class="chip neutral lens-live-attempts">Attempts: —</span>
      </div>

      <div class="lens-columns" style="margin-top:8px;">
        ${renderSegmentTile('q1', '1Q', q1SimTotal, renderQuarterCol(1))}
        ${renderSegmentTile('half', '1H', halfSimTotal, renderScopeCol('half', '1H interval', 24, '1H'))}
        ${renderSegmentTile('q3', '3Q', q3SimTotal, renderQuarterCol(3))}
        ${renderSegmentTile('game', 'FULL GAME', gameSimTotal, renderScopeCol('game', 'Full game interval', 48, 'G'))}
      </div>

      <details class="lens-player-details" style="margin-top:8px;">
        <summary class="subtle cursor-pointer">Player live lens (sim vs line vs live)</summary>
        <div class="subtle" style="margin-top:6px;">Uses pregame prop lines (from recommendations) and live ESPN boxscore totals.</div>
        <label class="subtle" style="display:inline-flex; align-items:center; gap:8px; margin-top:6px;">
          <input type="checkbox" class="lens-player-only-lines" checked />
          Only rows with lines
        </label>
        <div class="lens-player-body" style="margin-top:6px;"><div class="subtle">Live player lens not loaded.</div></div>
      </details>
    </div>
  `;
}

function attachLiveLensHandlers(root, games) {
  const containers = root.querySelectorAll('.live-lens');
  if (!containers || !containers.length) return;

  // Player lens filtering (client-side; avoids re-render churn)
  try {
    if (root && root.dataset && root.dataset.playerLensToggleBound !== '1') {
      root.dataset.playerLensToggleBound = '1';
      root.addEventListener('change', (ev) => {
        try {
          const t = ev && ev.target ? ev.target : null;
          if (!t || !t.classList || !t.classList.contains('lens-player-only-lines')) return;
          const wrap = t.closest ? t.closest('.live-lens') : null;
          if (!wrap) return;
          wrap.dataset.playerOnlyLines = t.checked ? '1' : '0';
        } catch (_) {
          // ignore
        }
      });
    }
  } catch (_) {
    // ignore
  }

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
    const sim0 = (g && g.sim) ? g.sim : g;
    const score0 = (sim0 && sim0.score) ? sim0.score : (g && g.score ? g.score : null);
    const ctx0 = (sim0 && sim0.context && typeof sim0.context === 'object') ? sim0.context : {};
    const odds0 = (g && g.odds) ? g.odds : null;
    const market0 = (sim0 && sim0.market) ? sim0.market : (g && g.market ? g.market : null);

    const per0 = (sim0 && sim0.periods) ? sim0.periods : (g && g.periods ? g.periods : null);
    const perQ1 = per0 && per0.q1 ? per0.q1 : null;
    const perQ3 = per0 && per0.q3 ? per0.q3 : null;
    const perH1 = per0 && per0.h1 ? per0.h1 : null;

    const simMarginMean = n(score0 && score0.margin_mean != null ? score0.margin_mean : null);
    const simHomeMean = n(score0 && score0.home_mean != null ? score0.home_mean : null);
    const simAwayMean = n(score0 && score0.away_mean != null ? score0.away_mean : null);
    const simTotalMean = n(score0 && score0.total_mean != null ? score0.total_mean : null);
    const simPHomeWin = n(score0 && score0.p_home_win != null ? score0.p_home_win : null);
    const simPHomeCover = n(score0 && score0.p_home_cover != null ? score0.p_home_cover : null);

    const expHomePace = n(ctx0 && (ctx0.home_pace != null ? ctx0.home_pace : null));
    const expAwayPace = n(ctx0 && (ctx0.away_pace != null ? ctx0.away_pace : null));

    const oddsHomeSpr = n(odds0 && odds0.home_spread != null ? odds0.home_spread : (market0 && market0.market_home_spread != null ? market0.market_home_spread : null));
    const oddsTotal = n(odds0 && odds0.total != null ? odds0.total : (market0 && market0.market_total != null ? market0.market_total : null));
    const oddsAwayMl = n(odds0 && odds0.away_ml != null ? odds0.away_ml : null);
    const oddsHomeMl = n(odds0 && odds0.home_ml != null ? odds0.home_ml : null);

    let oddsH1Total = null;
    try {
      const cand = [
        odds0 && odds0.h1_total,
        odds0 && odds0.total_1h,
        odds0 && odds0.first_half_total,
        odds0 && odds0.total_first_half,
      ];
      for (let i = 0; i < cand.length; i += 1) {
        const v = n(cand[i]);
        if (v != null) { oddsH1Total = v; break; }
      }
    } catch (_) {
      oddsH1Total = null;
    }

    gameMetaByKey.set(key, {
      game_id: gid,
      home: h,
      away: a,
      sim_margin_mean: simMarginMean,
      sim_home_mean: simHomeMean,
      sim_away_mean: simAwayMean,
      sim_total_mean: simTotalMean,
      sim_p_home_win: simPHomeWin,
      sim_p_home_cover: simPHomeCover,
      home_pace: expHomePace,
      away_pace: expAwayPace,
      line_total: oddsTotal,
      line_home_spread: oddsHomeSpr,
      line_away_ml: oddsAwayMl,
      line_home_ml: oddsHomeMl,
      line_h1_total: oddsH1Total,
      periods: {
        q1: perQ1,
        q3: perQ3,
        h1: perH1,
      },
    });
  });

  function impliedProbFromAmer(amer) {
    const a = n(amer);
    if (a == null) return null;
    // American odds implied probability (no vig removal)
    if (a < 0) return (-a) / ((-a) + 100.0);
    if (a > 0) return 100.0 / (a + 100.0);
    return null;
  }

  function impliedScoreFromTotalAndHomeSpread(total, homeSpr) {
    const t = n(total);
    const hs = n(homeSpr);
    if (t == null || hs == null) return null;
    // Convention: homeSpr is HOME ATS spread (e.g. HOME -13.0 => hs=-13).
    // Solve: H + A = total; H - A = -homeSpr.
    const homeImp = (t - hs) / 2.0;
    const awayImp = (t + hs) / 2.0;
    if (!Number.isFinite(homeImp) || !Number.isFinite(awayImp)) return null;
    return { home: homeImp, away: awayImp };
  }

  function _periodNum(x) {
    // Normalize to numeric or null.
    const v = n(x);
    return (v == null) ? null : v;
  }

  function prefillSummaryFromPeriod(sumEl, meta, periodObj, scopeLabel) {
    if (!sumEl || !meta || !periodObj) return;
    const p = periodObj;

    const simWinner = sumEl.querySelector('.lens-sum-sim-winner');
    const simAts = sumEl.querySelector('.lens-sum-sim-ats');
    const simScore = sumEl.querySelector('.lens-sum-sim-score');

    const lineWinner = sumEl.querySelector('.lens-sum-line-winner');
    const lineAts = sumEl.querySelector('.lens-sum-line-ats');
    const lineTotEl = sumEl.querySelector('.lens-sum-line-total');
    const lineScore = sumEl.querySelector('.lens-sum-line-score');

    const sh = _periodNum(p.home_mean);
    const sa = _periodNum(p.away_mean);
    const sm = _periodNum(p.margin_mean);
    const pHome = _periodNum(p.p_home_win);

    const lineTotal = _periodNum(p.market_total);
    const homeSpr = _periodNum(p.market_home_spread);

    if (simScore) {
      if (sa != null && sh != null) simScore.textContent = `${fmt(sa, 0)}–${fmt(sh, 0)}`;
    }
    if (simWinner) {
      let w = null;
      if (pHome != null) w = (pHome >= 0.5) ? meta.home : meta.away;
      else if (sh != null && sa != null) w = (sh >= sa) ? meta.home : meta.away;
      simWinner.textContent = w || '—';
    }
    if (simAts) {
      if (homeSpr != null && sm != null) {
        const coverEdge = sm + homeSpr;
        if (coverEdge >= 0) simAts.textContent = `${meta.home} ${fmt(homeSpr, 1)} (proj +${fmt(coverEdge, 1)})`;
        else simAts.textContent = `${meta.away} ${fmt(-homeSpr, 1)} (proj +${fmt(-coverEdge, 1)})`;
      } else {
        simAts.textContent = '—';
      }
    }

    if (lineTotEl) {
      if (lineTotal != null) lineTotEl.textContent = fmt(lineTotal, 1);
    }
    if (lineAts) {
      if (homeSpr != null) lineAts.textContent = `${meta.home} ${fmt(homeSpr, 1)}`;
    }
    if (lineScore) {
      const imp = impliedScoreFromTotalAndHomeSpread(lineTotal, homeSpr);
      if (imp) lineScore.textContent = `${fmt(imp.away, 1)}–${fmt(imp.home, 1)}`;
    }
    if (lineWinner) {
      // No period ML; infer favorite from spread sign.
      if (homeSpr != null && Math.abs(homeSpr) > 1e-6) lineWinner.textContent = (homeSpr < 0) ? meta.home : meta.away;
      else lineWinner.textContent = '—';
    }

    // Optional: tag scope for debugging if needed.
    try {
      if (scopeLabel) sumEl.dataset.scopeLabel = String(scopeLabel);
    } catch (_) {
      // ignore
    }
  }

  function fmtTeamFromMeta(meta, isHome) {
    if (!meta) return '';
    return isHome ? (meta.home || '') : (meta.away || '');
  }

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
    if (!intervals || !Array.isArray(intervals.segments)) {
      // Keep the tile visible with a clear reason instead of silently doing nothing.
      try {
        const st = el.querySelector('.lens-live-status');
        if (st) st.textContent = 'No intervals';
      } catch (_) {
        // ignore
      }
      return;
    }

    // Ensure game-id attribute is present for polling
    try {
      const meta = gameMetaByKey.get(key);
      if (meta && meta.game_id && !el.dataset.gameId) el.dataset.gameId = String(meta.game_id);
    } catch (_) {
      // ignore
    }

    const meta0 = gameMetaByKey.get(key);

    // Prefill MARKET chips + summary tiles from pregame odds / SmartSim score.
    // This prevents "—" cells when live polling hasn't populated yet.
    try {
      const meta = meta0;
      if (meta) {
        const lineTotal = n(meta.line_total);
        const homeSpr = n(meta.line_home_spread);
        const awayMl = n(meta.line_away_ml);
        const homeMl = n(meta.line_home_ml);

        // Live bar (Lines …)
        try {
          const linesEl = el.querySelector('.lens-live-lines');
          if (linesEl) {
            const pieces = [];
            if (lineTotal != null) pieces.push(`Tot ${fmt(lineTotal, 1)}`);
            if (homeSpr != null) pieces.push(`Spr ${fmt(homeSpr, 1)}`);
            if (awayMl != null && homeMl != null) pieces.push(`ML ${fmtAmer(awayMl)}/${fmtAmer(homeMl)}`);
            const h1Line = n(meta.line_h1_total);
            if (h1Line != null) pieces.push(`1H ${fmt(h1Line, 1)}`);
            if (pieces.length) linesEl.textContent = `Lines ${pieces.join(' · ')}`;
          }
        } catch (_) {
          // ignore
        }

        // MARKET chips
        const mlChip = el.querySelector('.lens-market-ml');
        const atsChip = el.querySelector('.lens-market-ats');
        const totChip = el.querySelector('.lens-market-total');
        const hAtsChip = el.querySelector('.lens-market-1h-ats');
        const hTotChip = el.querySelector('.lens-market-1h-total');

        if (mlChip) {
          if (awayMl != null && homeMl != null) mlChip.textContent = `ML: ${meta.away} ${fmtAmer(awayMl)} / ${meta.home} ${fmtAmer(homeMl)}`;
          else mlChip.textContent = 'ML: —';
        }
        if (atsChip) {
          if (homeSpr != null) atsChip.textContent = `ATS: ${meta.home} ${fmt(homeSpr, 1)}`;
          else atsChip.textContent = 'ATS: —';
        }
        if (totChip) {
          if (lineTotal != null) totChip.textContent = `Total: ${fmt(lineTotal, 1)}`;
          else totChip.textContent = 'Total: —';
        }
        if (hAtsChip) hAtsChip.textContent = '1H ATS: —';
        if (hTotChip) {
          const h1Line = n(meta.line_h1_total);
          if (h1Line != null) hTotChip.textContent = `1H Total: ${fmt(h1Line, 1)}`;
          else hTotChip.textContent = '1H Total: —';
        }

        // Summary tiles
        const sumQ1 = el.querySelector('.lens-summary[data-scope="q1"]');
        const sumQ3 = el.querySelector('.lens-summary[data-scope="q3"]');
        const sumHalf = el.querySelector('.lens-summary[data-scope="half"]');
        const sumGame = el.querySelector('.lens-summary[data-scope="game"]');

        // Prefill Q1/Q3/1H from SmartSim periods if present.
        try {
          const per = meta.periods && typeof meta.periods === 'object' ? meta.periods : {};
          if (sumQ1 && per.q1) prefillSummaryFromPeriod(sumQ1, meta, per.q1, 'q1');
          if (sumQ3 && per.q3) prefillSummaryFromPeriod(sumQ3, meta, per.q3, 'q3');
          if (sumHalf && per.h1) prefillSummaryFromPeriod(sumHalf, meta, per.h1, 'h1');
        } catch (_) {
          // ignore
        }

        if (sumHalf) {
          const lineTot = sumHalf.querySelector('.lens-sum-line-total');
          if (lineTot) {
            const h1Line = n(meta.line_h1_total);
            if (h1Line != null) lineTot.textContent = fmt(h1Line, 1);
          }
        }

        if (sumGame) {
          const simWinner = sumGame.querySelector('.lens-sum-sim-winner');
          const simAts = sumGame.querySelector('.lens-sum-sim-ats');
          const simScore = sumGame.querySelector('.lens-sum-sim-score');

          const lineWinner = sumGame.querySelector('.lens-sum-line-winner');
          const lineAts = sumGame.querySelector('.lens-sum-line-ats');
          const lineTotEl = sumGame.querySelector('.lens-sum-line-total');
          const lineScore = sumGame.querySelector('.lens-sum-line-score');

          const sh = n(meta.sim_home_mean);
          const sa = n(meta.sim_away_mean);
          const sm = n(meta.sim_margin_mean);

          if (simScore) {
            if (sa != null && sh != null) simScore.textContent = `${fmt(sa, 0)}–${fmt(sh, 0)}`;
          }

          if (simWinner) {
            let w = null;
            const pHome = n(meta.sim_p_home_win);
            if (pHome != null) w = (pHome >= 0.5) ? meta.home : meta.away;
            else if (sh != null && sa != null) w = (sh >= sa) ? meta.home : meta.away;
            simWinner.textContent = w || '—';
          }

          if (simAts) {
            if (homeSpr != null && sm != null) {
              const coverEdge = sm + homeSpr; // >0 means home covers
              if (coverEdge >= 0) simAts.textContent = `${meta.home} ${fmt(homeSpr, 1)} (proj +${fmt(coverEdge, 1)})`;
              else simAts.textContent = `${meta.away} ${fmt(-homeSpr, 1)} (proj +${fmt(-coverEdge, 1)})`;
            }
          }

          if (lineTotEl) {
            if (lineTotal != null) lineTotEl.textContent = fmt(lineTotal, 1);
          }
          if (lineAts) {
            if (homeSpr != null) lineAts.textContent = `${meta.home} ${fmt(homeSpr, 1)}`;
          }
          if (lineScore) {
            const imp = impliedScoreFromTotalAndHomeSpread(lineTotal, homeSpr);
            if (imp) lineScore.textContent = `${fmt(imp.away, 1)}–${fmt(imp.home, 1)}`;
          }
          if (lineWinner) {
            let w = null;
            const pAway = impliedProbFromAmer(awayMl);
            const pHome = impliedProbFromAmer(homeMl);
            if (pAway != null && pHome != null) w = (pHome >= pAway) ? meta.home : meta.away;
            else if (homeSpr != null && Math.abs(homeSpr) > 1e-6) w = (homeSpr < 0) ? meta.home : meta.away;
            lineWinner.textContent = w || '—';
          }
        }
      }
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
      let paceFinal = (simAt != null && simFinal != null) ? (actTot + (simFinal - simAt)) : null;

      // Interval-smart adjustment: use live possessions (per-scope) + SmartSim expected pace/PPP.
      try {
        const adjCfg = _liveLensScopeTotalAdjCfg(totalMinutes);
        const possLive = n(scopeEl && scopeEl.dataset ? scopeEl.dataset.possLive : null);
        const meta = meta0;
        const expHomePace = n(meta && meta.home_pace);
        const expAwayPace = n(meta && meta.away_pace);
        const expPace = (expHomePace != null && expAwayPace != null) ? ((expHomePace + expAwayPace) / 2.0) : null;
        const expTotal = n(meta && (meta.sim_total_mean != null ? meta.sim_total_mean : null));
        const expPppGame = (expPace != null && expTotal != null && expPace > 1e-6) ? (expTotal / expPace) : null;

        const elapsedMin = totalMinutes - minRem;
        const possExpectedSoFar = (expPace != null) ? (expPace * (elapsedMin / 48.0)) : null;
        const possExpectedFull = (expPace != null) ? (expPace * (totalMinutes / 48.0)) : null;

        // Prefer the scope median as the PPP prior when possible (captures quarter/half scoring shape).
        const expPppScope = (simFinal != null && possExpectedFull != null && possExpectedFull > 1e-6) ? (simFinal / possExpectedFull) : null;
        const expPpp = (expPppScope != null) ? expPppScope : expPppGame;

        if (adjCfg && adjCfg.enabled === false) {
          // disabled via tuning
        } else if (!isFinal && paceFinal != null && simFinal != null && possLive != null && possExpectedSoFar != null && possExpectedFull != null && expPpp != null) {
          // Avoid noisy adjustments very early; scale server knob to scope length.
          const minElapsedFromTuning = n(adjCfg && adjCfg.min_elapsed_min);
          const minElapsed = (minElapsedFromTuning != null) ? Math.max(1.0, minElapsedFromTuning * (totalMinutes / 48.0)) : Math.max(1.0, 0.25 * totalMinutes);
          if (elapsedMin >= minElapsed && possLive > 2.0 && possExpectedSoFar > 1.5) {
            const paceRatio = possLive / possExpectedSoFar;
            if (paceRatio > 0.4 && paceRatio < 2.5) {
              // Scale stabilization by scope length.
              const scale = Math.max(0.15, totalMinutes / 48.0);
              const possMin = 10.0 * scale;
              const possRange = 25.0 * scale;
              const timeMin = 6.0 * scale;
              const timeRange = 18.0 * scale;

              const wPoss = Math.max(0, Math.min(1, (possLive - possMin) / Math.max(1e-6, possRange)));
              const wTime = Math.max(0, Math.min(1, (elapsedMin - timeMin) / Math.max(1e-6, timeRange)));
              const wPace = Math.max(0, Math.min(1, Math.min(wPoss, wTime)));

              const paceRatioShrunk = 1.0 + (paceRatio - 1.0) * wPace;
              const actPpp = actTot / Math.max(1.0, possLive);
              const effDelta = actPpp - expPpp;
              const wEffProj = n(adjCfg && adjCfg.eff_weight_proj);
              const wEff = ((wEffProj != null) ? wEffProj : 0.5) * wPace;
              const effDeltaShrunk = effDelta * wEff;

              const projPossFull = possExpectedFull * paceRatioShrunk;
              const projPpp = expPpp + effDeltaShrunk;
              let possBased = projPossFull * projPpp;

              // Guardrails vs SmartSim median for the scope.
              const maxDevCfg = n(adjCfg && adjCfg.max_dev_points);
              const maxDev = (maxDevCfg != null) ? maxDevCfg : (2.0 + (25.0 * (totalMinutes / 48.0)));
              possBased = Math.max(simFinal - maxDev, Math.min(simFinal + maxDev, possBased));

              // Blend with points-based ladder output to preserve SmartSim ladder prior.
              const alpha = wPace;
              let blended = (1.0 - alpha) * paceFinal + alpha * possBased;
              const maxDeltaCfg = n(adjCfg && adjCfg.max_delta_points);
              const maxDelta = (maxDeltaCfg != null) ? maxDeltaCfg : (2.0 + (15.0 * (totalMinutes / 48.0)));
              blended = Math.max(paceFinal - maxDelta, Math.min(paceFinal + maxDelta, blended));
              paceFinal = blended;
            }
          }
        }
      } catch (_) {
        // ignore
      }

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

      if (outSimAt) outSimAt.textContent = (simAt == null) ? '—' : fmt(simAt, 0);
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

    // Historical/final prefill: if actual totals are embedded on the tile, set min remaining=0
    // and total points=actual so the lens outputs show the final interval state.
    try {
      const aHome = n(el.dataset.actualHome);
      const aAway = n(el.dataset.actualAway);
      const aGameTot = n(el.dataset.actualGameTotal);
      const aH1Tot = n(el.dataset.actualH1Total);
      const aQ1Tot = n(el.dataset.actualQ1Total);
      const aQ3Tot = n(el.dataset.actualQ3Total);

      const isFinal = (aHome != null && aAway != null) || (aGameTot != null);
      if (isFinal) {
        el.dataset.final = '1';

        const st = el.querySelector('.lens-live-status');
        const sc = el.querySelector('.lens-live-score');
        const ph = el.querySelectorAll('.lens-phase');
        if (st) st.textContent = 'FINAL';
        if (sc && aHome != null && aAway != null) sc.textContent = `${fmt(aAway, 0)}–${fmt(aHome, 0)}`;
        try {
          ph.forEach((x) => { x.textContent = 'Live: FINAL'; });
        } catch (_) {
          // ignore
        }

        function setScope(scope, tot) {
          if (tot == null) return;
          const col = el.querySelector(`.lens-col[data-scope="${scope}"]`);
          if (!col) return;
          const minEl = col.querySelector('select.lens-min');
          const totEl = col.querySelector('input.lens-total');
          if (minEl) minEl.value = '0';
          if (totEl) totEl.value = String(Math.round(tot));
          // Trigger recompute via synthetic events
          try {
            if (minEl) minEl.dispatchEvent(new Event('change'));
            if (totEl) totEl.dispatchEvent(new Event('input'));
          } catch (_) {
            // ignore
          }
          try {
            col.dataset.frozen = '1';
          } catch (_) {
            // ignore
          }
        }

        setScope('q1', aQ1Tot);
        setScope('q3', aQ3Tot);
        setScope('half', aH1Tot);
        setScope('game', (aGameTot != null) ? aGameTot : ((aHome != null && aAway != null) ? (aHome + aAway) : null));

        // Summary tiles (Live row)
        const liveQ1Tot = (aQ1Tot != null) ? aQ1Tot : null;
        const liveQ3Tot = (aQ3Tot != null) ? aQ3Tot : null;
        const liveHalfTot = (aH1Tot != null) ? aH1Tot : null;
        const liveGameTot = (aGameTot != null) ? aGameTot : ((aHome != null && aAway != null) ? (aHome + aAway) : null);

        const q1Sum = el.querySelector('.lens-summary[data-scope="q1"]');
        const q3Sum = el.querySelector('.lens-summary[data-scope="q3"]');
        const halfSum = el.querySelector('.lens-summary[data-scope="half"]');
        const gameSum = el.querySelector('.lens-summary[data-scope="game"]');

        if (q1Sum && liveQ1Tot != null) {
          const t = q1Sum.querySelector('.lens-sum-live-total');
          if (t) t.textContent = fmt(liveQ1Tot, 0);
        }
        if (q3Sum && liveQ3Tot != null) {
          const t = q3Sum.querySelector('.lens-sum-live-total');
          if (t) t.textContent = fmt(liveQ3Tot, 0);
        }
        if (halfSum && liveHalfTot != null) {
          const t = halfSum.querySelector('.lens-sum-live-total');
          if (t) t.textContent = fmt(liveHalfTot, 0);
        }
        if (gameSum && liveGameTot != null) {
          const t = gameSum.querySelector('.lens-sum-live-total');
          if (t) t.textContent = fmt(liveGameTot, 0);

          // For full-game, we can also populate winner + ATS from final score.
          try {
            const wEl = gameSum.querySelector('.lens-sum-live-winner');
            const atsEl = gameSum.querySelector('.lens-sum-live-ats');
            if (wEl && aHome != null && aAway != null) wEl.textContent = (aHome >= aAway) ? homeTri : awayTri;

            if (atsEl && aHome != null && aAway != null) {
              // Prefer the pregame home spread chip if present in the MARKET strip.
              // This avoids needing deeper access to odds objects here.
              let homeSpr = null;
              try {
                const lineAts = gameSum.querySelector('.lens-sum-line-ats');
                const txt = lineAts ? String(lineAts.textContent || '') : '';
                const m = txt.match(/\b(-?\d+(?:\.\d+)?)\b/);
                homeSpr = m ? n(m[1]) : null;
              } catch (_) {
                homeSpr = null;
              }

              if (homeSpr != null) {
                const margin = aHome - aAway;
                const coverEdge = margin + homeSpr; // >0 home covers
                if (coverEdge >= 0) atsEl.textContent = `${homeTri} ${fmt(homeSpr, 1)}`;
                else atsEl.textContent = `${awayTri} ${fmt(-homeSpr, 1)}`;
              }
            }
          } catch (_) {
            // ignore
          }

          const s = gameSum.querySelector('.lens-sum-live-score');
          if (s && aHome != null && aAway != null) s.textContent = `${fmt(aAway, 0)}–${fmt(aHome, 0)}`;
        }
      }
    } catch (_) {
      // ignore
    }
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

function _liveLensGameTotalAdjCfg() {
  try {
    const t = (__liveLensTuning && typeof __liveLensTuning === 'object') ? __liveLensTuning : null;
    const a = t && t.adjustments && typeof t.adjustments === 'object' ? t.adjustments : null;
    const g = a && a.game_total && typeof a.game_total === 'object' ? a.game_total : null;
    return g;
  } catch (_) {
    return null;
  }
}

function _liveLensScopeTotalAdjCfg(totalMinutes) {
  // Optional knobs for non-full-game scopes. Falls back to game_total.
  try {
    const t = (__liveLensTuning && typeof __liveLensTuning === 'object') ? __liveLensTuning : null;
    const a = t && t.adjustments && typeof t.adjustments === 'object' ? t.adjustments : null;
    if (!a) return null;

    const tm = Number(totalMinutes);
    if (tm === 12 && a.quarter_total && typeof a.quarter_total === 'object') return a.quarter_total;
    if (tm === 24 && a.half_total && typeof a.half_total === 'object') return a.half_total;
    if (a.scope_total && typeof a.scope_total === 'object') return a.scope_total;
    if (a.game_total && typeof a.game_total === 'object') return a.game_total;
    return null;
  } catch (_) {
    return null;
  }
}

function _liveLensPossLiveAvg(meta, live) {
  try {
    const pbpPoss = (live && live.pbp_possessions) ? live.pbp_possessions : null;
    if (!pbpPoss) return null;
    const aTri = meta && meta.away ? String(meta.away).toUpperCase().trim() : '';
    const hTri = meta && meta.home ? String(meta.home).toUpperCase().trim() : '';
    const ap = pbpPoss && (pbpPoss[aTri] || pbpPoss.away) ? (pbpPoss[aTri] || pbpPoss.away) : null;
    const hp = pbpPoss && (pbpPoss[hTri] || pbpPoss.home) ? (pbpPoss[hTri] || pbpPoss.home) : null;
    const apPoss = ap ? n(ap.poss_est) : null;
    const hpPoss = hp ? n(hp.poss_est) : null;
    if (apPoss == null || hpPoss == null) return null;
    return (apPoss + hpPoss) / 2.0;
  } catch (_) {
    return null;
  }
}

function computePossessionPaceFinalForGame(meta, live, curMinLeft, actTot, pointsBasedLens) {
  const info = computePossessionPaceForGame(meta, live, curMinLeft, actTot, pointsBasedLens);
  return info ? info.pace_final : null;
}

function computePossessionPaceForGame(meta, live, curMinLeft, actTot, pointsBasedLens) {
  // Possession-driven pace estimate for FULL GAME.
  // Returns components so we can log + display what drove the projection.
  const pb = pointsBasedLens && pointsBasedLens.paceFinal != null ? n(pointsBasedLens.paceFinal) : null;
  const simFinal = pointsBasedLens && pointsBasedLens.simFinal != null ? n(pointsBasedLens.simFinal) : null;
  if (actTot == null || pb == null) return null;

  const out = {
    pace_final: pb,
    pace_points: pb,
    pace_poss: null,
    pace_alpha: 0.0,
    poss_live: null,
    poss_expected: null,
    pace_ratio: null,
    elapsed_min: null,
  };

  const adjCfg = _liveLensGameTotalAdjCfg();
  if (adjCfg && adjCfg.enabled === false) return out;

  const cm = n(curMinLeft);
  const elapsedMin = (cm != null) ? (48.0 - cm) : (pointsBasedLens && pointsBasedLens.elapsedMinutes != null ? n(pointsBasedLens.elapsedMinutes) : null);
  out.elapsed_min = elapsedMin;
  if (elapsedMin == null) return out;

  const minElapsed = n(adjCfg && adjCfg.min_elapsed_min);
  if (elapsedMin < ((minElapsed != null) ? minElapsed : 6.0)) return out;

  const possLive = _liveLensPossLiveAvg(meta, live);
  out.poss_live = possLive;
  if (possLive == null || !(possLive > 8.0)) return out;

  const expHomePace = n(meta && meta.home_pace);
  const expAwayPace = n(meta && meta.away_pace);
  const expPace = (expHomePace != null && expAwayPace != null) ? ((expHomePace + expAwayPace) / 2.0) : null;
  const expTotal = n(meta && (meta.total_mean != null ? meta.total_mean : null));
  const expPpp = (expPace != null && expTotal != null && expPace > 1e-6) ? (expTotal / expPace) : null;
  if (expPace == null || expPpp == null) return out;

  const possExpectedSoFar = expPace * (elapsedMin / 48.0);
  out.poss_expected = possExpectedSoFar;
  if (!(possExpectedSoFar > 5.0)) return out;

  const paceRatio = possLive / possExpectedSoFar;
  out.pace_ratio = paceRatio;
  if (!(paceRatio > 0.4) || !(paceRatio < 2.5)) return out;

  // Shrinkage: pace is more repeatable than shooting; both stabilize with elapsed time/possessions.
  const wPoss = Math.max(0, Math.min(1, (possLive - 10.0) / 25.0));
  const wTime = Math.max(0, Math.min(1, (elapsedMin - 6.0) / 18.0));
  const wPace = Math.max(0, Math.min(1, Math.min(wPoss, wTime)));
  const paceRatioShrunk = 1.0 + (paceRatio - 1.0) * wPace;

  const actPpp = actTot / Math.max(1.0, possLive);
  const effDelta = actPpp - expPpp;
  const wEff = 0.5 * wPace; // eff is noisier
  const effDeltaShrunk = effDelta * wEff;

  const projPossFull = expPace * paceRatioShrunk;
  const projPpp = expPpp + effDeltaShrunk;
  let possBased = projPossFull * projPpp;
  out.pace_poss = possBased;

  // Guardrails: keep projection close to SmartSim median when very early / noisy.
  if (simFinal != null) {
    const maxDev = 25.0;
    possBased = Math.max(simFinal - maxDev, Math.min(simFinal + maxDev, possBased));
    out.pace_poss = possBased;
  }

  // Blend with points-based ladder output to preserve SmartSim pacing prior.
  const alpha = wPace;
  out.pace_alpha = alpha;
  let blended = (1.0 - alpha) * pb + alpha * possBased;

  // Final clamp vs points-based to avoid whipsaw.
  const maxDelta = 15.0;
  blended = Math.max(pb - maxDelta, Math.min(pb + maxDelta, blended));
  out.pace_final = blended;
  return out;
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
  const adjCfg = _liveLensGameTotalAdjCfg();
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

function normPlayerName(s) {
  return String(s || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function clamp(x, lo, hi) {
  const v = n(x);
  if (v == null) return null;
  return Math.max(lo, Math.min(hi, v));
}

function computeGameElapsedMinutes(period, secLeftPeriod, isFinal) {
  try {
    if (isFinal) return 48.0;
    const p = (period != null && Number.isFinite(Number(period))) ? Number(period) : null;
    const sec = (secLeftPeriod != null && Number.isFinite(Number(secLeftPeriod))) ? Number(secLeftPeriod) : null;
    if (p == null || sec == null) return null;
    if (p < 1) return null;
    if (p > 4) return 48.0;
    const elapsed = ((p - 1) * 12 * 60) + Math.max(0, (12 * 60 - sec));
    return Math.max(0, Math.min(48.0, elapsed / 60.0));
  } catch (_) {
    return null;
  }
}

function getPlayerActualForMarket(p, market) {
  const mk = String(market || '').toLowerCase().trim();
  const pts = n(p && p.pts);
  const reb = n(p && p.reb);
  const ast = n(p && p.ast);
  const threes = n(p && (p.threes_made != null ? p.threes_made : p.threes));
  if (mk === 'pts' || mk === 'points') return pts;
  if (mk === 'reb' || mk === 'rebounds') return reb;
  if (mk === 'ast' || mk === 'assists') return ast;
  if (mk === 'threes' || mk === '3pm' || mk === '3pt' || mk === 'threes_made') return threes;
  if (mk === 'pra') return (pts != null && reb != null && ast != null) ? (pts + reb + ast) : null;
  if (mk === 'pa') return (pts != null && ast != null) ? (pts + ast) : null;
  if (mk === 'pr') return (pts != null && reb != null) ? (pts + reb) : null;
  if (mk === 'ra') return (reb != null && ast != null) ? (reb + ast) : null;
  return null;
}

function renderPlayerLiveLens(meta, liveLensGame, isFinal) {
  try {
    const rowsIn = liveLensGame && typeof liveLensGame === 'object' ? liveLensGame.rows : null;
    const rows = Array.isArray(rowsIn) ? rowsIn : [];
    if (!rows.length) {
      return '<div class="subtle">No live player rows yet.</div>';
    }

    const tbl = rows.map((r) => {
      const teamTri = String(r && r.team_tri != null ? r.team_tri : '').toUpperCase().trim();
      const player = String(r && r.player != null ? r.player : '').trim();
      const stat = String(r && r.stat != null ? r.stat : '').toLowerCase().trim();
      const mk = marketLabel(stat);
      const mp = n(r && r.mp);
      const act = n(r && r.actual);
      const mu = n(r && r.sim_mu);
      const line = n(r && r.line);
      const pace = n(r && r.pace_proj);
      const dP = n(r && r.pace_vs_line);
      const dS = n(r && r.sim_vs_line);
      const lean = String(r && r.lean != null ? r.lean : '').toUpperCase().trim();
      const leanTxt = lean ? lean : '—';
      const hasLine = (line != null);
      return `
        <tr data-has-line="${hasLine ? '1' : '0'}">
          <td><span class="badge">${esc(teamTri)}</span> ${esc(player)}</td>
          <td>${esc(mk)}</td>
          <td class="num">${mp == null ? '—' : fmt(mp, 1)}</td>
          <td class="num">${act == null ? '—' : fmt(act, 1)}</td>
          <td class="num">${mu == null ? '—' : fmt(mu, 1)}</td>
          <td class="num">${line == null ? '—' : fmt(line, 1)}</td>
          <td class="num">${pace == null ? '—' : fmt(pace, 1)}</td>
          <td class="num">${dP == null ? '—' : fmt(dP, 1)}</td>
          <td class="num">${dS == null ? '—' : fmt(dS, 1)}</td>
          <td>${esc(leanTxt)}</td>
        </tr>
      `;
    }).join('');

    const note = isFinal
      ? 'Final (actuals only).'
      : 'PaceProj uses actual per-minute × expected minutes (from props_predictions roll10_min when available).';

    return `
      <div class="subtle">${esc(note)}</div>
      <div class="table-wrap" style="margin-top:6px;">
        <table class="data-table boxscore-table player-lens-table" style="font-size:12px;">
          <thead>
            <tr>
              <th>Player</th>
              <th>Stat</th>
              <th class="num">MP</th>
              <th class="num">Act</th>
              <th class="num">Sim μ</th>
              <th class="num">Line</th>
              <th class="num">PaceProj</th>
              <th class="num">ΔPace-Line</th>
              <th class="num">ΔSim-Line</th>
              <th>Lean</th>
            </tr>
          </thead>
          <tbody>
            ${tbl || '<tr><td colspan="10" class="subtle">No player rows.</td></tr>'}
          </tbody>
        </table>
      </div>
    `;
  } catch (_) {
    return '<div class="subtle">Player live lens unavailable.</div>';
  }
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
    ml: thr('ml', 0.03, 0.06),
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

  function pickBestTagFromLensEl(lensEl) {
    if (!lensEl) return { klass: 'NONE', text: '' };
    const order = ['.lens-rec-total', '.lens-rec-ats', '.lens-rec-ml', '.lens-rec-half', '.lens-rec-qtr'];
    let best = { klass: 'NONE', text: '' };

    function parseTag(txt) {
      const s = String(txt || '').trim();
      if (!s || /:\s*—\s*$/.test(s)) return null;
      const m = s.match(/\b(BET|WATCH)\b/i);
      if (!m) return null;
      const klass = String(m[1] || '').toUpperCase();
      const text = s.replace(/:\s*/, ' ');
      return { klass, text };
    }

    for (const sel of order) {
      const el = lensEl.querySelector(sel);
      const t = parseTag(el ? el.textContent : '');
      if (!t) continue;
      if (t.klass === 'BET') return t;
      if (t.klass === 'WATCH' && best.klass !== 'WATCH') best = t;
    }
    return best;
  }

  function updateScoreboardStrip(sbById) {
    try {
      const strip = root.querySelector('.scoreboard-strip');
      if (!strip) return;
      const items = strip.querySelectorAll('.s-item[data-game-id]');
      items.forEach((item) => {
        const gid = canonGameId(item.dataset.gameId);
        if (!gid) return;

        const s = sbById ? sbById.get(gid) : null;
        const statusEl = item.querySelector('.s-status');
        const scoreEl = item.querySelector('.s-score');

        if (statusEl) statusEl.textContent = (s && s.status) ? String(s.status) : (statusEl.textContent || '');
        if (scoreEl) {
          const ap = (s && s.away_pts != null) ? s.away_pts : null;
          const hp = (s && s.home_pts != null) ? s.home_pts : null;
          if (ap != null && hp != null) scoreEl.textContent = `${ap}-${hp}`;
        }

        const lensEl = root.querySelector(`.live-lens[data-game-id="${CSS.escape(gid)}"]`);
        const tag = pickBestTagFromLensEl(lensEl);
        const tagEl = item.querySelector('.s-tag');
        if (tagEl) tagEl.textContent = tag.text || '';

        const inProg = !!(s && s.in_progress) || (lensEl && lensEl.dataset.inProgress === '1');
        item.classList.remove('bet', 'watch', 'live', 'neu');
        if (tag.klass === 'BET') item.classList.add('bet');
        else if (tag.klass === 'WATCH') item.classList.add('watch');
        else if (inProg) item.classList.add('live');
        else item.classList.add('neu');
      });
    } catch (_) {
      // ignore
    }
  }

  const byGameId = new Map();
  (games || []).forEach((g) => {
    const gid = canonGameId((g && g.sim && g.sim.game_id != null) ? g.sim.game_id : '');
    if (!gid) return;
    const intervals = (g && g.sim && (g.sim.intervals_1m || g.sim.intervals))
      ? (g.sim.intervals_1m || g.sim.intervals)
      : (g ? (g.intervals_1m || g.intervals) : null);
    const odds0 = (g && g.odds) ? g.odds : null;
    const sim0 = (g && g.sim) ? g.sim : g;
    const score0 = (sim0 && sim0.score) ? sim0.score : (g && g.score ? g.score : null);
    const market0 = (sim0 && sim0.market) ? sim0.market : (g && g.market ? g.market : null);

    const marginMean = n(score0 ? score0.margin_mean : null);
    const pHomeWin = n(score0 ? score0.p_home_win : null);

    const pregameTotal = n(odds0 && odds0.total != null ? odds0.total : (market0 && market0.market_total != null ? market0.market_total : null));
    const pregameHomeSpr = n(odds0 && odds0.home_spread != null ? odds0.home_spread : (market0 && market0.market_home_spread != null ? market0.market_home_spread : null));
    const pregameAwayMl = n(odds0 && odds0.away_ml != null ? odds0.away_ml : null);
    const pregameHomeMl = n(odds0 && odds0.home_ml != null ? odds0.home_ml : null);

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
      prop_recommendations: (g && g.prop_recommendations) ? g.prop_recommendations : null,
      margin_mean: marginMean,
      p_home_win: pHomeWin,
      pregame_total: pregameTotal,
      pregame_home_spread: pregameHomeSpr,
      pregame_away_ml: pregameAwayMl,
      pregame_home_ml: pregameHomeMl,
      home_pace: homePace,
      away_pace: awayPace,
      home_mean: homeMean,
      away_mean: awayMean,
      total_mean: totalMean,
    });
  });

  function possAvgFromPossObj(meta, possObj) {
    try {
      if (!meta || !possObj) return null;
      const aTri = meta && meta.away ? String(meta.away).toUpperCase().trim() : '';
      const hTri = meta && meta.home ? String(meta.home).toUpperCase().trim() : '';
      const ap = possObj && (possObj[aTri] || possObj.away) ? (possObj[aTri] || possObj.away) : null;
      const hp = possObj && (possObj[hTri] || possObj.home) ? (possObj[hTri] || possObj.home) : null;
      const apPoss = ap ? n(ap.poss_est) : null;
      const hpPoss = hp ? n(hp.poss_est) : null;
      if (apPoss == null || hpPoss == null) return null;
      return (apPoss + hpPoss) / 2.0;
    } catch (_) {
      return null;
    }
  }

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

    // Keep the top scoreboard strip in sync with basic status/score.
    updateScoreboardStrip(sbById);

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
    let playerLensMap = new Map();
    if (detailEventIds.length) {
      try {
        const pbpPromise = fetchJsonWithTimeout(`/api/live_pbp_stats?ttl=20&event_ids=${encodeURIComponent(detailEventIds.join(','))}&date=${encodeURIComponent(dateStr)}`, 8000);
        const linesPromise = lineEventIds.length
          ? fetchJsonWithTimeout(`/api/live_lines?ttl=20&date=${encodeURIComponent(dateStr)}&event_ids=${encodeURIComponent(lineEventIds.join(','))}&include_period_totals=1`, 8000)
          : Promise.resolve({ games: [] });
        const playersPromise = fetchJsonWithTimeout(`/api/live_player_lens?ttl=20&date=${encodeURIComponent(dateStr)}&event_ids=${encodeURIComponent(detailEventIds.join(','))}`, 8000);
        const settled = await Promise.allSettled([pbpPromise, linesPromise, playersPromise]);
        const pbp = (settled[0] && settled[0].status === 'fulfilled') ? settled[0].value : null;
        const lines = (settled[1] && settled[1].status === 'fulfilled') ? settled[1].value : null;
        const players = (settled[2] && settled[2].status === 'fulfilled') ? settled[2].value : null;

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

        const playerGames = Array.isArray(players?.games) ? players.games : [];
        playerGames.forEach((gg) => {
          const eid = String(gg && gg.event_id != null ? gg.event_id : '').trim();
          if (!eid) return;
          playerLensMap.set(eid, gg);
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
      const livePlayerLens = eid ? playerLensMap.get(eid) : null;

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

      const elapsedMin = computeGameElapsedMinutes(period, secLeftPeriod, isFinal);

      let lineTotal = n(ln && ln.lines ? ln.lines.total : null);
      let homeSpr = n(ln && ln.lines ? ln.lines.home_spread : null);
      const awayMl = n(ln && ln.lines ? ln.lines.away_ml : null);
      const homeMl = n(ln && ln.lines ? ln.lines.home_ml : null);
      // Treat 0 totals/spreads as missing placeholders.
      if (lineTotal != null && Math.abs(lineTotal) < 0.001) lineTotal = null;
      if (homeSpr != null && Math.abs(homeSpr) < 0.001 && lineTotal == null) homeSpr = null;
      const periodTotals = (ln && ln.lines && ln.lines.period_totals) ? ln.lines.period_totals : null;
      const periodSpreads = (ln && ln.lines && ln.lines.period_spreads) ? ln.lines.period_spreads : null;

      // Fall back to pregame betting lines when live lines are missing.
      const effLineTotal = (lineTotal != null) ? lineTotal : n(meta && meta.pregame_total);
      const effHomeSpr = (homeSpr != null) ? homeSpr : n(meta && meta.pregame_home_spread);
      const effAwayMl = (awayMl != null) ? awayMl : n(meta && meta.pregame_away_ml);
      const effHomeMl = (homeMl != null) ? homeMl : n(meta && meta.pregame_home_ml);

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
          total: effLineTotal,
          home_spread: effHomeSpr,
          period_totals: periodTotals,
        },
        pbp_attempts: pbp && pbp.pbp_attempts ? pbp.pbp_attempts : null,
        pbp_attempts_periods: pbp && pbp.pbp_attempts_periods ? pbp.pbp_attempts_periods : null,
        pbp_possessions: pbp && pbp.pbp_possessions ? pbp.pbp_possessions : null,
        pbp_possessions_periods: pbp && pbp.pbp_possessions_periods ? pbp.pbp_possessions_periods : null,
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
        if (effLineTotal != null) pieces.push(`Tot ${fmt(effLineTotal, 1)}`);
        if (effHomeSpr != null) pieces.push(`Spr ${fmt(effHomeSpr, 1)}`);

        if (effAwayMl != null && effHomeMl != null) pieces.push(`ML ${fmtAmer(effAwayMl)}/${fmtAmer(effHomeMl)}`);

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

      // Player live lens (all players: sim vs line vs live + pacing)
      try {
        const body = el.querySelector('.lens-player-body');
        if (body) {
          body.innerHTML = renderPlayerLiveLens(meta, livePlayerLens, isFinal);
        }
      } catch (_) {
        // ignore
      }

      // MARKET chips (NCAAB-style strip)
      try {
        const mlChip = el.querySelector('.lens-market-ml');
        const atsChip = el.querySelector('.lens-market-ats');
        const totChip = el.querySelector('.lens-market-total');
        const hAtsChip = el.querySelector('.lens-market-1h-ats');
        const hTotChip = el.querySelector('.lens-market-1h-total');

        if (mlChip) {
          if (effAwayMl != null && effHomeMl != null) mlChip.textContent = `ML: ${meta.away} ${fmtAmer(effAwayMl)} / ${meta.home} ${fmtAmer(effHomeMl)}`;
          else mlChip.textContent = 'ML: —';
        }
        if (atsChip) {
          if (effHomeSpr != null) atsChip.textContent = `ATS: ${meta.home} ${fmt(effHomeSpr, 1)}`;
          else atsChip.textContent = 'ATS: —';
        }
        if (totChip) {
          if (effLineTotal != null) totChip.textContent = `Total: ${fmt(effLineTotal, 1)}`;
          else totChip.textContent = 'Total: —';
        }
        if (hAtsChip) hAtsChip.textContent = '1H ATS: —';

        let h1Line = null;
        try {
          const h1LineRaw = periodTotals && periodTotals.h1 != null ? n(periodTotals.h1) : null;
          h1Line = (h1LineRaw != null && Math.abs(h1LineRaw) < 0.001) ? null : h1LineRaw;
        } catch (_) {
          h1Line = null;
        }
        if (hTotChip) {
          if (h1Line != null) hTotChip.textContent = `1H Total: ${fmt(h1Line, 1)}`;
          else hTotChip.textContent = '1H Total: —';
        }
      } catch (_) {
        // ignore
      }

      // Summary tiles (1H / FULL GAME)
      try {
        const sumQ1 = el.querySelector('.lens-summary[data-scope="q1"]');
        const sumQ3 = el.querySelector('.lens-summary[data-scope="q3"]');
        const sumHalf = el.querySelector('.lens-summary[data-scope="half"]');
        const sumGame = el.querySelector('.lens-summary[data-scope="game"]');

        const hpTxt = (homePts != null) ? String(homePts) : '—';
        const apTxt = (awayPts != null) ? String(awayPts) : '—';
        const scoreText = `${meta.away} ${apTxt} – ${hpTxt} ${meta.home}`;

        const pq = (live && live.pbp_quarters) ? live.pbp_quarters : null;
        const qTotals = pq && pq.q_totals ? pq.q_totals : null;
        const cur = pq && pq.current ? pq.current : null;

        const q1Final = n(qTotals ? qTotals.q1 : null);
        const q2Final = n(qTotals ? qTotals.q2 : null);
        const q3Final = n(qTotals ? qTotals.q3 : null);
        const curPer = cur ? Number(cur.period) : null;
        const curQTot = n(cur ? cur.q_total : null);

        const q1LiveTot = (curPer === 1 && curQTot != null) ? curQTot : q1Final;
        const q3LiveTot = (curPer === 3 && curQTot != null) ? curQTot : q3Final;
        let h1LiveTot = null;
        if (period != null && period <= 2 && totalPts != null) {
          h1LiveTot = totalPts;
        } else if (q1Final != null && q2Final != null) {
          h1LiveTot = q1Final + q2Final;
        } else if (curPer === 2 && q1Final != null && curQTot != null) {
          h1LiveTot = q1Final + curQTot;
        }

        function setLineTot(sumEl, tot) {
          if (!sumEl) return;
          const lineTot = sumEl.querySelector('.lens-sum-line-total');
          if (!lineTot) return;
          const v = n(tot);
          if (v != null) lineTot.textContent = fmt(v, 1);
        }

        function setLineAts(sumEl, spr) {
          if (!sumEl) return;
          const lineAts = sumEl.querySelector('.lens-sum-line-ats');
          if (!lineAts) return;
          const v = n(spr);
          lineAts.textContent = (v != null) ? `${meta.home} ${fmt(v, 1)}` : '—';
        }

        function setLineScore(sumEl, tot, spr) {
          if (!sumEl) return;
          const lineScore = sumEl.querySelector('.lens-sum-line-score');
          if (!lineScore) return;
          const imp = impliedScoreFromTotalAndHomeSpread(tot, spr);
          lineScore.textContent = imp ? `${fmt(imp.away, 1)}–${fmt(imp.home, 1)}` : '—';
        }

        function setLiveTot(sumEl, tot) {
          if (!sumEl) return;
          const liveTot = sumEl.querySelector('.lens-sum-live-total');
          if (!liveTot) return;
          const v = n(tot);
          liveTot.textContent = (v != null) ? fmt(v, 1) : '—';
        }

        function setLiveScore(sumEl) {
          if (!sumEl) return;
          const liveScore = sumEl.querySelector('.lens-sum-live-score');
          if (liveScore) liveScore.textContent = scoreText || '—';
        }

        if (sumQ1) {
          try {
            const q1LineRaw = periodTotals && periodTotals.q1 != null ? n(periodTotals.q1) : null;
            const q1Line = (q1LineRaw != null && Math.abs(q1LineRaw) < 0.001) ? null : q1LineRaw;
            setLineTot(sumQ1, q1Line);
            const q1SprRaw = periodSpreads && periodSpreads.q1 != null ? n(periodSpreads.q1) : null;
            const q1Spr = (q1SprRaw != null && Math.abs(q1SprRaw) < 0.001 && q1Line == null) ? null : q1SprRaw;
            setLineAts(sumQ1, q1Spr);
            setLineScore(sumQ1, q1Line, q1Spr);
          } catch (_) {
            // keep prefilled value
          }
          setLiveScore(sumQ1);
          setLiveTot(sumQ1, q1LiveTot);
        }

        if (sumQ3) {
          try {
            const q3LineRaw = periodTotals && periodTotals.q3 != null ? n(periodTotals.q3) : null;
            const q3Line = (q3LineRaw != null && Math.abs(q3LineRaw) < 0.001) ? null : q3LineRaw;
            setLineTot(sumQ3, q3Line);
            const q3SprRaw = periodSpreads && periodSpreads.q3 != null ? n(periodSpreads.q3) : null;
            const q3Spr = (q3SprRaw != null && Math.abs(q3SprRaw) < 0.001 && q3Line == null) ? null : q3SprRaw;
            setLineAts(sumQ3, q3Spr);
            setLineScore(sumQ3, q3Line, q3Spr);
          } catch (_) {
            // keep prefilled value
          }
          setLiveScore(sumQ3);
          setLiveTot(sumQ3, q3LiveTot);
        }

        if (sumHalf) {
          const lineTot = sumHalf.querySelector('.lens-sum-line-total');
          const liveScore = sumHalf.querySelector('.lens-sum-live-score');
          if (lineTot) {
            let h1Line = null;
            try {
              const h1LineRaw = periodTotals && periodTotals.h1 != null ? n(periodTotals.h1) : null;
              h1Line = (h1LineRaw != null && Math.abs(h1LineRaw) < 0.001) ? null : h1LineRaw;
            } catch (_) {
              h1Line = null;
            }
            if (h1Line != null) lineTot.textContent = fmt(h1Line, 1);

            try {
              const h1SprRaw = periodSpreads && periodSpreads.h1 != null ? n(periodSpreads.h1) : null;
              const h1Spr = (h1SprRaw != null && Math.abs(h1SprRaw) < 0.001 && h1Line == null) ? null : h1SprRaw;
              setLineAts(sumHalf, h1Spr);
              setLineScore(sumHalf, h1Line, h1Spr);
            } catch (_) {
              // ignore
            }
          }
          if (liveScore) liveScore.textContent = scoreText || '—';
          setLiveTot(sumHalf, h1LiveTot);
        }

        if (sumGame) {
          const lineAts = sumGame.querySelector('.lens-sum-line-ats');
          const lineTot = sumGame.querySelector('.lens-sum-line-total');
          const lineScore = sumGame.querySelector('.lens-sum-line-score');
          const liveScore = sumGame.querySelector('.lens-sum-live-score');
          if (lineAts) lineAts.textContent = (effHomeSpr != null) ? `${meta.home} ${fmt(effHomeSpr, 1)}` : '—';
          if (lineTot) lineTot.textContent = (effLineTotal != null) ? fmt(effLineTotal, 1) : '—';
          if (lineScore) {
            const imp = impliedScoreFromTotalAndHomeSpread(effLineTotal, effHomeSpr);
            lineScore.textContent = imp ? `${fmt(imp.away, 1)}–${fmt(imp.home, 1)}` : '—';
          }
          if (liveScore) liveScore.textContent = scoreText || '—';
        }
      } catch (_) {
        // ignore
      }

      // Phase chips inside each scope panel
      try {
        const pNow = (period == null) ? null : Number(period);
        const clockTxt = (s && s.clock != null) ? String(s.clock) : '';
        const phaseTxt = (pNow != null && Number.isFinite(pNow) && clockTxt) ? `Live: P${pNow} ${clockTxt}` : 'Live: —';
        const phases = el.querySelectorAll('.lens-phase');
        phases.forEach((ph) => { try { ph.textContent = phaseTxt; } catch (_) { /* ignore */ } });
      } catch (_) {
        // ignore
      }

      // Attempts (team-order)
      function _fmtAttemptsPair(attObj) {
        if (!attObj) return null;
        const a0 = attObj && (attObj[meta.away] || attObj.away) ? (attObj[meta.away] || attObj.away) : null;
        const h0 = attObj && (attObj[meta.home] || attObj.home) ? (attObj[meta.home] || attObj.home) : null;
        if (!a0 || !h0) return null;
        const ft = `FT ${n(a0.ft_made) ?? 0}/${n(a0.ft_att) ?? 0}-${n(h0.ft_made) ?? 0}/${n(h0.ft_att) ?? 0}`;
        const p2 = `2P ${n(a0.fg2_made) ?? 0}/${n(a0.fg2_att) ?? 0}-${n(h0.fg2_made) ?? 0}/${n(h0.fg2_att) ?? 0}`;
        const p3 = `3P ${n(a0.fg3_made) ?? 0}/${n(a0.fg3_att) ?? 0}-${n(h0.fg3_made) ?? 0}/${n(h0.fg3_att) ?? 0}`;
        return `${ft} · ${p2} · ${p3}`;
      }

      function _fmtPossPair(possObj) {
        if (!possObj) return '';
        const ap = possObj && (possObj[meta.away] || possObj.away) ? (possObj[meta.away] || possObj.away) : null;
        const hp = possObj && (possObj[meta.home] || possObj.home) ? (possObj[meta.home] || possObj.home) : null;
        const apPoss = ap ? n(ap.poss_est) : null;
        const hpPoss = hp ? n(hp.poss_est) : null;
        if (apPoss != null && hpPoss != null) return ` · Poss ${fmt(apPoss, 0)}-${fmt(hpPoss, 0)}`;
        return '';
      }

      const attemptsEl = el.querySelector('.lens-live-attempts');
      try {
        const gameAttempts = (live && live.pbp_attempts) ? live.pbp_attempts : null;
        const gamePoss = (live && live.pbp_possessions) ? live.pbp_possessions : null;
        const txt = _fmtAttemptsPair(gameAttempts);
        if (attemptsEl) attemptsEl.textContent = txt ? `Attempts: ${txt}${_fmtPossPair(gamePoss)}` : 'Attempts: —';
      } catch (_) {
        if (attemptsEl) attemptsEl.textContent = 'Attempts: —';
      }

      // Per-scope attempts/possessions (period-exclusive) + freeze once the period completes.
      try {
        const pNow = (period == null) ? null : Number(period);
        const attemptsPeriods = (live && live.pbp_attempts_periods) ? live.pbp_attempts_periods : null;
        const possPeriods = (live && live.pbp_possessions_periods) ? live.pbp_possessions_periods : null;

        function scopeKey(scope) {
          if (!scope) return null;
          if (scope === 'game') return 'game';
          if (scope === 'half') return 'h1';
          if (/^q[1-4]$/.test(scope)) return scope;
          return null;
        }

        function shouldFreeze(scope) {
          if (isFinal) return true;
          const pn = (pNow != null && Number.isFinite(pNow)) ? Math.floor(pNow) : null;
          if (pn == null) return false;
          if (scope === 'half') return pn > 2;
          if (scope === 'game') return false;
          if (/^q[1-4]$/.test(scope)) {
            const qn = Number(scope.replace('q', ''));
            return pn > qn;
          }
          return false;
        }

        const scopeCols = el.querySelectorAll('.lens-col');
        scopeCols.forEach((col) => {
          try {
            const sc = col.dataset.scope;
            if (!sc) return;
            const key = scopeKey(sc);
            if (!key) return;

            const freezeNow = shouldFreeze(sc);
            if (freezeNow && col.dataset.frozen !== '1') col.dataset.frozen = '1';

            const aObj = attemptsPeriods && attemptsPeriods[key] ? attemptsPeriods[key] : (sc === 'game' ? (live && live.pbp_attempts) : null);
            const pObj = possPeriods && possPeriods[key] ? possPeriods[key] : (sc === 'game' ? (live && live.pbp_possessions) : null);

            // Store live possessions for interval-smart pace projections (used by computeScope()).
            try {
              const possAvg = possAvgFromPossObj(meta, pObj);
              col.dataset.possLive = (possAvg == null) ? '' : String(possAvg);
            } catch (_) {
              // ignore
            }

            const base = _fmtAttemptsPair(aObj);
            const possTxt = _fmtPossPair(pObj);
            const scopeTxt = base ? `${base}${possTxt}` : '—';

            const atEl = col.querySelector('.lens-scope-attempts');
            if (atEl) atEl.textContent = scopeTxt;
          } catch (_) {
            // ignore
          }
        });
      } catch (_) {
        // ignore
      }

      // Per-panel score chips
      try {
        const scoreText = scoreEl ? String(scoreEl.textContent || '') : '';
        const scopeScores = el.querySelectorAll('.lens-scope-score');
        scopeScores.forEach((x) => { try { x.textContent = scoreText ? `Score: ${scoreText}` : 'Score: —'; } catch (_) { /* ignore */ } });
      } catch (_) {
        // ignore
      }

      // Per-panel ML/ATS chips
      try {
        const scopeML = el.querySelectorAll('.lens-scope-ml');
        const scopeATS = el.querySelectorAll('.lens-scope-ats');
        const mlTxt = (effAwayMl != null && effHomeMl != null) ? `ML: ${meta.away} ${fmtAmer(effAwayMl)} / ${meta.home} ${fmtAmer(effHomeMl)}` : 'ML: —';
        const atsTxt = (effHomeSpr != null) ? `ATS: ${meta.home} ${fmt(effHomeSpr, 1)}` : 'ATS: —';
        scopeML.forEach((x) => { try { x.textContent = mlTxt; } catch (_) { /* ignore */ } });
        scopeATS.forEach((x) => { try { x.textContent = atsTxt; } catch (_) { /* ignore */ } });
      } catch (_) {
        // ignore
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
          if (tot) tot.value = (totalPts != null) ? String(Math.round(totalPts)) : '';
          if (liv) liv.value = (effLineTotal != null) ? String(effLineTotal) : '';
          if (sel) sel.dispatchEvent(new Event('change'));
          if (tot) tot.dispatchEvent(new Event('input'));
          if (liv) liv.dispatchEvent(new Event('input'));

          // Possession-driven pace (FULL GAME only): override the displayed paceFinal + dataset.
          try {
            const pointsLens = computePaceFinalFromIntervals(meta.intervals, 48, (vMin != null) ? vMin : (minLeftRaw == null ? 48 : Math.round(minLeftRaw)), totalPts);
            const possInfo = computePossessionPaceForGame(meta, live, (vMin != null) ? vMin : null, totalPts, pointsLens);
            const possPaceFinal = possInfo ? n(possInfo.pace_final) : null;
            if (possPaceFinal != null) {
              const outPace = gameCol.querySelector('.lens-pace');
              if (outPace) outPace.textContent = fmt(possPaceFinal, 1);
              try { gameCol.dataset.paceFinal = String(possPaceFinal); } catch (_) { /* ignore */ }
              // Keep Lean consistent with the overridden paceFinal.
              try {
                const liveTot2 = n(liv && liv.value != null && String(liv.value).trim() !== '' ? liv.value : null);
                const outLean = gameCol.querySelector('.lens-lean');
                if (outLean && liveTot2 != null) {
                  const diff2 = possPaceFinal - liveTot2;
                  if (diff2 > 1.0) outLean.textContent = `Over (+${fmt(diff2, 1)})`;
                  else if (diff2 < -1.0) outLean.textContent = `Under (${fmt(diff2, 1)})`;
                  else outLean.textContent = 'No edge';
                }
              } catch (_) {
                // ignore
              }
            }
          } catch (_) {
            // ignore
          }
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

          const pNow = (period == null) ? null : Number(period);
          const freezeNow = !!isFinal || (pNow != null && Number.isFinite(pNow) && pNow > qNum);
          if (freezeNow && qCol.dataset.frozen === '1') continue;

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

          if (pNow != null && Number.isFinite(pNow)) {
            if (pNow < qNum) {
              qMinLeft = 12;
            } else if (pNow === qNum) {
              if (secLeftPeriodRaw != null) qMinLeft = Math.round(secLeftPeriodRaw / 60.0);
            } else if (pNow > qNum) {
              qMinLeft = 0;
            }
          }

          if (qMinLeft != null) qMinLeft = Math.max(0, Math.min(12, qMinLeft));
          if (sel && qMinLeft != null) sel.value = String(qMinLeft);
          if (tot) tot.value = (qAct != null) ? String(Math.round(qAct)) : '';

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

          if (freezeNow) qCol.dataset.frozen = '1';
        }
      } catch (_) {
        // ignore
      }

      // Auto-fill 1H scope only while in 1H
      try {
        const halfCol = el.querySelector('.lens-col[data-scope="half"]');
        if (halfCol) {
          const pn = (period == null) ? null : Number(period);
          const freezeNow = !!isFinal || (pn != null && Number.isFinite(pn) && pn > 2);
          if (freezeNow && halfCol.dataset.frozen === '1') {
            // fully frozen: do not overwrite user inputs
          } else {
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
          if (tot) tot.value = (halfAct != null) ? String(Math.round(halfAct)) : '';

          // Half live total line (OddsAPI) when available (only meaningful in 1H)
          const h1LineRaw = periodTotals && periodTotals.h1 != null ? n(periodTotals.h1) : null;
          const h1Line = (h1LineRaw != null && Math.abs(h1LineRaw) < 0.001) ? null : h1LineRaw;
          if (liv) liv.value = (h1Line != null && (period == null || Number(period) <= 2) && !isFinal) ? String(h1Line) : '';
          if (sel) sel.dispatchEvent(new Event('change'));
          if (tot) tot.dispatchEvent(new Event('input'));
          if (liv) liv.dispatchEvent(new Event('input'));

            if (freezeNow) halfCol.dataset.frozen = '1';
          }
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
          const recMLEl = el.querySelector('.lens-rec-ml');
          if (recTotalEl) recTotalEl.textContent = 'Total: —';
          if (recHalfEl) recHalfEl.textContent = '1H: —';
          if (recQtrEl) recQtrEl.textContent = 'Q: —';
          if (recATSEl) recATSEl.textContent = 'ATS: —';
          if (recMLEl) recMLEl.textContent = 'ML: —';
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
      const recMLEl = el.querySelector('.lens-rec-ml');
      let curMinLeft = (minLeftRaw == null) ? 48 : Math.max(0, Math.min(48, Math.round(minLeftRaw)));
      if (period != null && Number(period) > 4) curMinLeft = 0;
      let lens = computePaceFinalFromIntervals(meta.intervals, 48, curMinLeft, totalPts);
      let possInfoForLog = null;
      try {
        possInfoForLog = computePossessionPaceForGame(meta, live, curMinLeft, totalPts, lens);
        const possPaceFinal = possInfoForLog ? n(possInfoForLog.pace_final) : null;
        if (lens && possPaceFinal != null) lens = { ...lens, paceFinal: possPaceFinal };
      } catch (_) {
        // ignore
      }
      let totalDiffRaw = null;
      let totalDiff = null;
      let totalCtx = null;
      let totalClass = 'NONE';
      let totalSide = null;

      if (lens && effLineTotal != null) {
        totalDiffRaw = lens.paceFinal - effLineTotal;
        totalCtx = adjustGameTotalDiffWithContext(totalDiffRaw, effLineTotal, meta, live, curMinLeft);
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
      if (effHomeSpr != null && meta.margin_mean != null && live && live.score) {
        const curMargin = n(live.score.home_margin);
        const minLeft = n(live.time ? live.time.game_min_left : null);
        const elapsed = (minLeft != null) ? (48 - minLeft) : (lens ? lens.elapsedMinutes : 0);
        const w = Math.max(0, Math.min(1, (elapsed || 0) / 48.0));
        const adjMargin = (1 - w) * meta.margin_mean + w * (curMargin ?? 0);
        // Home covers if margin > -spr (e.g., -4.5 => margin > 4.5)
        const homeEdge = adjMargin + effHomeSpr;
        const awayEdge = -adjMargin - effHomeSpr;
        const pickHome = Math.abs(homeEdge) >= Math.abs(awayEdge);
        atsEdge = pickHome ? homeEdge : awayEdge;
        const side = pickHome ? meta.home : meta.away;
        atsClass = classifyDiff(Math.abs(atsEdge), thr.ats.watch, thr.ats.bet);
        if (atsClass === 'BET') atsText = `ATS: BET ${side} (${fmt(atsEdge, 1)})`;
        else if (atsClass === 'WATCH') atsText = `ATS: WATCH ${side} (${fmt(atsEdge, 1)})`;
      }
      if (recATSEl) recATSEl.textContent = atsText;

      // Compute tags (ML) using sim win prob blended with live score state and betting MLs.
      let mlClass = 'NONE';
      let mlText = 'ML: —';
      try {
        const pPregame = n(meta && meta.p_home_win != null ? meta.p_home_win : null);
        const curMargin = n(live && live.score ? live.score.home_margin : null);
        const minLeft = n(live && live.time ? live.time.game_min_left : null);
        const pHomeImplied = impliedProbFromAmer(effHomeMl);
        const pAwayImplied = impliedProbFromAmer(effAwayMl);

        let pHomeScore = null;
        if (curMargin != null && minLeft != null) {
          const ml = Math.max(0, Math.min(48, minLeft));
          let scale = 6.0 + 0.35 * ml;

          // Possessions-aware confidence: if possessions >> expected so far, margin is more informative;
          // if possessions << expected, margin is noisier. Adjust scale accordingly.
          try {
            const possLive = _liveLensPossLiveAvg(meta, live);
            const expHomePace = n(meta && meta.home_pace);
            const expAwayPace = n(meta && meta.away_pace);
            const expPace = (expHomePace != null && expAwayPace != null) ? ((expHomePace + expAwayPace) / 2.0) : null;
            const elapsedMin = 48.0 - ml;
            const possExpSoFar = (expPace != null) ? (expPace * (elapsedMin / 48.0)) : null;
            if (possLive != null && possExpSoFar != null && possExpSoFar > 5.0) {
              const ratio = possLive / possExpSoFar;
              const ratioClamped = Math.max(0.6, Math.min(1.6, ratio));
              const f = Math.sqrt(ratioClamped);
              scale = scale / f;
            }
          } catch (_) {
            // ignore
          }

          pHomeScore = 1.0 / (1.0 + Math.exp(-(curMargin / scale)));
        }

        let pHomeModel = pPregame;
        if (pHomeScore != null && pPregame != null) {
          const elapsed = (minLeft != null) ? (48 - minLeft) : 0;
          const w = Math.max(0, Math.min(1, (elapsed || 0) / 48.0));
          pHomeModel = (1 - w) * pPregame + w * pHomeScore;
        } else if (pHomeScore != null && pHomeModel == null) {
          pHomeModel = pHomeScore;
        }

        if (pHomeModel != null && pHomeImplied != null && pAwayImplied != null) {
          const edgeHome = pHomeModel - pHomeImplied;
          const edgeAway = (1.0 - pHomeModel) - pAwayImplied;
          const pickHome = Math.abs(edgeHome) >= Math.abs(edgeAway);
          const edge = pickHome ? edgeHome : edgeAway;
          const side = pickHome ? meta.home : meta.away;
          mlClass = classifyDiff(Math.abs(edge), thr.ml.watch, thr.ml.bet);
          if (mlClass === 'BET') mlText = `ML: BET ${side} (${fmt(edge * 100.0, 1)}pp)`;
          else if (mlClass === 'WATCH') mlText = `ML: WATCH ${side} (${fmt(edge * 100.0, 1)}pp)`;
        }
      } catch (_) {
        mlClass = 'NONE';
        mlText = 'ML: —';
      }
      if (recMLEl) recMLEl.textContent = mlText;

      // Card-level highlight (NCAAB parity)
      try {
        const card = el.closest('.card');
        if (card) {
          const anyBet = (totalClass === 'BET') || (halfClass === 'BET') || (qClass === 'BET') || (atsClass === 'BET') || (mlClass === 'BET');
          const anyWatch = (totalClass === 'WATCH') || (halfClass === 'WATCH') || (qClass === 'WATCH') || (atsClass === 'WATCH') || (mlClass === 'WATCH');
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
          pace_components: (possInfoForLog && typeof possInfoForLog === 'object') ? {
            pace_final: possInfoForLog.pace_final,
            pace_points: possInfoForLog.pace_points,
            pace_poss: possInfoForLog.pace_poss,
            alpha: possInfoForLog.alpha,
            poss_live_total: possInfoForLog.poss_live_total,
            poss_expected_total: possInfoForLog.poss_expected_total,
            pace_ratio_poss: possInfoForLog.pace_ratio,
          } : null,
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

    // Re-run strip update after signals/classes may have changed.
    updateScoreboardStrip(sbById);
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

function renderCards(games, reconGameRows, reconQuarterRows, reconPlayerRows, showResults, hideOdds, dateStr) {
  const root = document.getElementById('cards');
  if (!root) return;
  if (!Array.isArray(games) || games.length === 0) {
    root.innerHTML = '<div class="card"><div class="subtle">No SmartSim games found for this date.</div></div>';
    return;
  }

  const isToday = (typeof dateStr === 'string' && isYmd(dateStr) && dateStr === localYMD());

  const reconIndex = buildReconIndex(reconGameRows);
  const reconQIndex = buildReconIndex(reconQuarterRows);
  const reconPIndex = (showResults && Array.isArray(reconPlayerRows) && reconPlayerRows.length)
    ? buildReconPlayersIndex(reconPlayerRows)
    : null;

  const stripHtml = (() => {
    const items = games.map((g) => {
      const gid = canonGameId((g && g.sim && g.sim.game_id != null) ? g.sim.game_id : (g && g.game_id != null ? g.game_id : ''))
        || `${String(g.home_tri || '').toUpperCase().trim()}_${String(g.away_tri || '').toUpperCase().trim()}`;
      const homeTri = String(g.home_tri || '').toUpperCase().trim();
      const awayTri = String(g.away_tri || '').toUpperCase().trim();

      const recon = showResults ? (reconIndex.get(`${homeTri}|${awayTri}`) || reconIndex.get(`${String(g.home_name || '').trim()}|${String(g.away_name || '').trim()}`)) : null;
      const actualHome = recon ? n(recon.home_pts) : null;
      const actualAway = recon ? n(recon.visitor_pts) : null;

      const odds = g.odds || {};
      const timeStr = fmtTime(odds.commence_time);
      const statusTxt = (actualHome != null && actualAway != null) ? 'Final' : (timeStr || '—');
      const scoreTxt = (actualHome != null && actualAway != null) ? `${actualAway}-${actualHome}` : '';

      return `
        <button type="button" class="s-item neu" data-game-id="${esc(gid)}" aria-label="Jump to ${esc(awayTri)} at ${esc(homeTri)}">
          <span class="s-teams">${esc(awayTri)} @ ${esc(homeTri)}</span>
          <span class="s-mid">
            <span class="s-status">${esc(statusTxt)}</span>
            <span class="s-score">${esc(scoreTxt)}</span>
          </span>
          <span class="s-tag"></span>
        </button>
      `;
    }).join('');

    return `
      <div class="scoreboard-strip" role="navigation" aria-label="Games">
        ${items}
      </div>
    `;
  })();

  const html = games.map((g) => {
    const homeTri = String(g.home_tri || '').toUpperCase().trim();
    const awayTri = String(g.away_tri || '').toUpperCase().trim();
    const homeName = String(g.home_name || homeTri).trim();
    const awayName = String(g.away_name || awayTri).trim();
        const gid = canonGameId((g && g.sim && g.sim.game_id != null) ? g.sim.game_id : (g && g.game_id != null ? g.game_id : ''))
          || `${homeTri}_${awayTri}`;
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
      const outBadge = out ? ` ${badgeForOutcome(out)}` : '';

      let lbl = String(x.label || '').trim();
      if (lbl === 'ML Home') lbl = 'ML H';
      else if (lbl === 'ML Away') lbl = 'ML A';
      else if (lbl === 'Spread Home') lbl = 'Spr H';
      else if (lbl === 'Spread Away') lbl = 'Spr A';
      else if (lbl === 'Total Over') lbl = 'Tot O';
      else if (lbl === 'Total Under') lbl = 'Tot U';

      const pTxt = pct(x.p, 0);
      const evTxt = (ev == null) ? '—' : ev.toFixed(3);
      return `<span class="chip model-pick neutral">${esc(lbl)} p${pTxt} EV <span class="ev-badge ${evCls}">${evTxt}</span>${outBadge}</span>`;
    }).join('');

    const warnLines = [];
    if (simErr) warnLines.push(`SmartSim error: ${simErr}`);
    if (Array.isArray(g.warnings) && g.warnings.length) warnLines.push(...g.warnings);
    let boxscoreReconBadge = '';
    try {
      const bs = g && g.boxscore_recon ? g.boxscore_recon : null;
      if (bs && bs.reason === 'missing_boxscore') {
        // For today's slate, missing boxscores are expected pregame/in-progress.
        if (!isToday) {
          warnLines.push(`Boxscore cache missing for game_id ${bs.game_id}`);
          boxscoreReconBadge = '<span class="badge ok">BOX MISSING</span>';
        }
      } else if (bs && bs.ok === false) {
        warnLines.push(
          `Boxscore recon mismatch: ${awayTri} pts(players)=${bs.away_pts_players} vs actual=${bs.away_pts_actual}; ` +
          `${homeTri} pts(players)=${bs.home_pts_players} vs actual=${bs.home_pts_actual}`
        );
        boxscoreReconBadge = '<span class="badge bad">BOX MISMATCH</span>';
      } else if (bs && bs.ok === true) {
        boxscoreReconBadge = '<span class="badge good">BOX OK</span>';
      }
    } catch (_) {
      // ignore
    }
    const warn = warnLines.length
      ? `<div class="alert">${warnLines.map((w) => esc(w)).join('<br/>')}</div>`
      : '';

    const playersHome = (sim.players && sim.players.home) ? sim.players.home : [];
    const playersAway = (sim.players && sim.players.away) ? sim.players.away : [];

    let playerReconBadge = '';
    let homeRecon = null;
    let awayRecon = null;
    try {
      if (reconPIndex) {
        const gid10 = canonNbaGameId10(g && g.sim ? g.sim.game_id : null);
        const byTeam = gid10 && reconPIndex[gid10] ? reconPIndex[gid10] : null;
        homeRecon = byTeam && byTeam[homeTri] ? byTeam[homeTri] : null;
        awayRecon = byTeam && byTeam[awayTri] ? byTeam[awayTri] : null;
        const hs = homeRecon ? reconTeamSummary(homeRecon) : null;
        const as = awayRecon ? reconTeamSummary(awayRecon) : null;
        if (hs || as) {
          playerReconBadge = `<span class="chip neutral" style="margin-left:8px;">Player recon MAE — HOME PTS ${fmt(hs && hs.maePts, 1)} / PRA ${fmt(hs && hs.maePra, 1)} (miss ${esc(hs && hs.missing)}) • AWAY PTS ${fmt(as && as.maePts, 1)} / PRA ${fmt(as && as.maePra, 1)} (miss ${esc(as && as.missing)})</span>`;
        }
      }
    } catch (_) {
      playerReconBadge = '';
      homeRecon = null;
      awayRecon = null;
    }

    const awayP10 = n(score.away_q && score.away_q.p10);
    const awayP90 = n(score.away_q && score.away_q.p90);
    const homeP10 = n(score.home_q && score.home_q.p10);
    const homeP90 = n(score.home_q && score.home_q.p90);
    const quantLine = (awayP10 != null && awayP90 != null && homeP10 != null && homeP90 != null)
      ? `Away p10/p90: ${fmt(awayP10, 0)}/${fmt(awayP90, 0)} • Home p10/p90: ${fmt(homeP10, 0)}/${fmt(homeP90, 0)}`
      : '';

    return `
      <section class="card card-v2" id="game-${esc(gid)}" data-game-id="${esc(gid)}">
        <div class="row head">
          <span class="venue">${esc(timeStr || '')}</span>
          <span class="venue">${esc(odds.bookmaker || odds.bookmaker_odds || 'odds')}</span>
          ${showResults && recon ? `<span class="result-badge">${finalLine}</span>` : ''}
        </div>

        <div class="row matchup">
          <div class="team side">
            <div class="team-line">${logoImg(awayTri)}<div class="name"><span class="team-role">AWAY</span>${esc(awayName)}</div></div>
          </div>
          <div class="score-block">
            <div class="sub">Projected score</div>
            <div class="live-score">${projFinalHtml}</div>
            <div class="proj-score">Home win: <span class="fw-700">${pct(score.p_home_win, 0)}</span></div>
            ${quantLine ? `<div class="proj-score">${esc(quantLine)}</div>` : ''}
          </div>
          <div class="team side" style="justify-self:end;">
            <div class="team-line">${logoImg(homeTri)}<div class="name"><span class="team-role">HOME</span>${esc(homeName)}</div></div>
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

        <div class="market-grid market-grid-top">
          <div class="market-tile market-tile-bets">
            <div class="market-title">Sim-based bet leans (top EV)</div>
            <div class="market-main">${betChips || '<span class="subtle">No EV computed (missing odds/prices).</span>'}</div>
          </div>
          <div class="market-tile market-tile-model">
            <div class="market-title">Model probabilities</div>
            <div class="model-strip">
              <div class="kv"><span class="k">P(HomeWin)</span><span class="v">${pct(bet.p_home_win, 1)}</span></div>
              <div class="kv"><span class="k">P(HomeCover)</span><span class="v">${pct(bet.p_home_cover, 1)}</span></div>
              <div class="kv"><span class="k">P(Over)</span><span class="v">${pct(bet.p_total_over, 1)}</span></div>
              <div class="kv"><span class="k">Sims</span><span class="v">${esc(sim.n_sims ?? '—')}</span></div>
            </div>
          </div>
          <div class="market-tile market-tile-writeup">
            <div class="market-title">Write-up</div>
            <div class="writeup-recap">${esc(g.writeup || '—')}</div>
          </div>
        </div>

        <div class="market-grid">
          ${renderLiveLens(
            intervals,
            `${homeTri}_${awayTri}`,
            g && g.sim ? g.sim.game_id : null,
            showResults ? {
              home_pts: actualHome,
              away_pts: actualAway,
              game_total: actualTotal,
              h1_total: reconQ ? n(reconQ.actual_h1_total) : null,
              q1_total: reconQ ? n(reconQ.actual_q1_total) : null,
              q3_total: reconQ ? n(reconQ.actual_q3_total) : null,
            } : null
          )}
        </div>

        <details class="players-block" open>
          <summary class="players-toggle cursor-pointer">Projected boxscore (aggregated sim means) ${boxscoreReconBadge} ${playerReconBadge}</summary>
          ${renderPlayersTable(`HOME (${homeTri}) players`, playersHome, homeRecon)}
          ${renderInjurySummary(`HOME (${homeTri})`, (g && g.sim && g.sim.injuries && g.sim.injuries.home) ? g.sim.injuries.home : playersHome)}
          <div class="mb-6"></div>
          ${renderPlayersTable(`AWAY (${awayTri}) players`, playersAway, awayRecon)}
          ${renderInjurySummary(`AWAY (${awayTri})`, (g && g.sim && g.sim.injuries && g.sim.injuries.away) ? g.sim.injuries.away : playersAway)}
        </details>

        <details class="writeup-block">
          <summary class="writeup-toggle cursor-pointer">Recommended props (sim vs line)</summary>
          ${renderPropRecommendations(g.prop_recommendations, homeTri, awayTri)}
        </details>
      </section>
    `;
  }).join('\n');

  root.innerHTML = `${stripHtml}\n${html}`;

  // Scoreboard strip click-to-scroll
  try {
    const strip = root.querySelector('.scoreboard-strip');
    if (strip && !strip.dataset.bound) {
      strip.dataset.bound = '1';
      strip.addEventListener('click', (ev) => {
        const btn = ev.target && ev.target.closest ? ev.target.closest('.s-item[data-game-id]') : null;
        if (!btn) return;
        const gid = canonGameId(btn.dataset.gameId);
        if (!gid) return;
        const target = root.querySelector(`.card[data-game-id="${CSS.escape(gid)}"]`) || document.getElementById(`game-${gid}`);
        if (!target) return;
        try { target.scrollIntoView({ behavior: 'smooth', block: 'start' }); } catch (_) { target.scrollIntoView(); }
      });
    }
  } catch (_) {
    // ignore
  }

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
    let reconPlayerRows = [];
    if (showResults) {
      const [csvG, csvQ, csvP] = await Promise.all([
        fetchText(`/api/processed/recon_games?date=${encodeURIComponent(dateStr)}`),
        fetchText(`/api/processed/recon_quarters?date=${encodeURIComponent(dateStr)}`),
        fetchText(`/api/processed/recon_players?date=${encodeURIComponent(dateStr)}`),
      ]);
      reconGameRows = csvG ? csvParse(csvG) : [];
      reconQuarterRows = csvQ ? csvParse(csvQ) : [];
      reconPlayerRows = csvP ? csvParse(csvP) : [];
    }

    renderCards(games, reconGameRows, reconQuarterRows, reconPlayerRows, showResults, hideOdds, dateStr);
  } catch (e) {
    setNote(`Failed to load cards: ${String(e && e.message ? e.message : e)}`);
    renderCards([], [], [], [], false, false, dateStr);
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
