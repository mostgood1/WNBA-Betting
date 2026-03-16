// Cards page renderer (SmartSim-driven).
// Includes: odds, bet leans, quarter projections, projected boxscores, prop targets, and matchup write-up.

function localYMD() {
  const tz = 'America/New_York';
  const cutoffHour = 6; // Treat 12:00am–5:59am ET as the prior NBA slate day.
  try {
    const now = new Date();
    const hourParts = new Intl.DateTimeFormat('en-US', {
      timeZone: tz,
      hour: '2-digit',
      hour12: false,
    }).formatToParts(now);
    const hourStr = (hourParts || []).find((p) => p && p.type === 'hour')?.value;
    const hour = Number(hourStr);
    const base = (Number.isFinite(hour) && hour < cutoffHour)
      ? new Date(now.getTime() - 24 * 60 * 60 * 1000)
      : now;
    const parts = new Intl.DateTimeFormat('en-US', {
      timeZone: tz,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).formatToParts(base);
    const y = (parts || []).find((p) => p && p.type === 'year')?.value;
    const m = (parts || []).find((p) => p && p.type === 'month')?.value;
    const d = (parts || []).find((p) => p && p.type === 'day')?.value;
    if (y && m && d) return `${y}-${m}-${d}`;
    throw new Error('bad date parts');
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

// Surface JS runtime failures directly in the page so it doesn't look like a "blank" load.
(function installGlobalErrorTraps() {
  try {
    if (window.__cardsErrorTrapsInstalled) return;
    window.__cardsErrorTrapsInstalled = true;

    window.addEventListener('error', (e) => {
      try {
        const target = e && e.target;
        if (target && typeof target.tagName === 'string' && target.tagName.toUpperCase() === 'IMG') return;
        const msg = e && e.message ? e.message : 'Unknown error';
        setNote(`JS error: ${msg}`);
      } catch (_) { /* ignore */ }
    });

    window.addEventListener('unhandledrejection', (e) => {
      try {
        const r = e && e.reason;
        const msg = (r && r.message) ? r.message : String(r);
        setNote(`JS error: ${msg}`);
      } catch (_) { /* ignore */ }
    });
  } catch (_) {
    // ignore
  }
})();

function playerHeadshotInitials(name) {
  const parts = String(name || '').trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return '?';
  const initials = parts.slice(0, 2).map((part) => part.charAt(0).toUpperCase()).join('');
  return initials || '?';
}

function nbaHeadshotUrl(playerId) {
  const pid = n(playerId);
  return (pid == null) ? '' : `https://cdn.nba.com/headshots/nba/latest/1040x760/${pid}.png`;
}

function renderPlayerHeadshot(name, opts) {
  const options = (opts && typeof opts === 'object') ? opts : {};
  const player = String(name || '').trim();
  const explicitPhoto = [options.playerPhoto, options.photo]
    .map((value) => String(value || '').trim())
    .find(Boolean) || '';
  const inferredPhoto = nbaHeadshotUrl(options.playerId);
  const src = explicitPhoto || inferredPhoto;
  const fallbackSrc = (inferredPhoto && inferredPhoto !== src) ? inferredPhoto : '';
  const initials = playerHeadshotInitials(player);

  if (!src) {
    return `<span class="prop-callout-photo prop-callout-photo-fallback" aria-hidden="true">${esc(initials)}</span>`;
  }

  return `<img src="${esc(src)}" alt="${esc(player)}" width="46" height="46" class="prop-callout-photo" data-fallback-src="${esc(fallbackSrc)}" data-fallback-initials="${esc(initials)}" loading="lazy" />`;
}

(function installHeadshotFallbackHandler() {
  try {
    if (window.__cardsHeadshotFallbackInstalled) return;
    window.__cardsHeadshotFallbackInstalled = true;

    document.addEventListener('error', (event) => {
      try {
        const target = event && event.target;
        if (!(typeof HTMLImageElement !== 'undefined' && target instanceof HTMLImageElement)) return;
        if (!target.classList.contains('prop-callout-photo')) return;

        const fallbackSrc = String(target.dataset.fallbackSrc || '').trim();
        if (fallbackSrc) {
          target.dataset.fallbackSrc = '';
          target.src = fallbackSrc;
          return;
        }

        const fallback = document.createElement('span');
        fallback.className = 'prop-callout-photo prop-callout-photo-fallback';
        fallback.setAttribute('aria-hidden', 'true');
        fallback.textContent = String(target.dataset.fallbackInitials || '?').trim() || '?';
        target.replaceWith(fallback);
      } catch (_) {
        // ignore image fallback failures
      }
    }, true);
  } catch (_) {
    // ignore
  }
})();

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
    const s = String(iso).trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
    const d = new Date(s);
    if (Number.isNaN(d.getTime())) return String(iso);
    return new Intl.DateTimeFormat(undefined, {
      hour: 'numeric',
      minute: '2-digit',
      hour12: true,
    }).format(d);
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

function isUnavailableSimPlayer(player) {
  if (!player || typeof player !== 'object') return false;
  const st = String(player.injury_status || '').trim().toUpperCase();
  return st === 'OUT' || player.playing_today === false;
}

function renderPlayersTable(title, players, reconByPlayerId) {
  const arr = Array.isArray(players) ? players.filter((player) => !isUnavailableSimPlayer(player)) : [];
  // Sort by minutes first so the table reflects the expected rotation.
  arr.sort((a, b) => {
    const dm = (n(b?.min_mean) ?? -1e9) - (n(a?.min_mean) ?? -1e9);
    if (dm !== 0) return dm;
    return (n(b?.pts_mean) ?? -1e9) - (n(a?.pts_mean) ?? -1e9);
  });
  const top = selectVisibleBoxscorePlayers(arr, 10);

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
    let actStl = '—';
    let actBlk = '—';
    let actTov = '—';
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
          actStl = fmt(rr.actual_stl, 1);
          actBlk = fmt(rr.actual_blk, 1);
          actTov = fmt(rr.actual_tov, 1);
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
        <td class="num">${renderSimBoxscoreStatCell(p, 'pts', p.pts_mean)}</td>
        <td class="num boxscore-line-col">${renderBoxscorePropLineCell(p, 'pts')}</td>
        ${hasRecon ? `<td class="num">${esc(actPts)}</td>` : ''}
        <td class="num">${renderSimBoxscoreStatCell(p, 'reb', p.reb_mean)}</td>
        <td class="num boxscore-line-col">${renderBoxscorePropLineCell(p, 'reb')}</td>
        ${hasRecon ? `<td class="num">${esc(actReb)}</td>` : ''}
        <td class="num">${renderSimBoxscoreStatCell(p, 'ast', p.ast_mean)}</td>
        <td class="num boxscore-line-col">${renderBoxscorePropLineCell(p, 'ast')}</td>
        ${hasRecon ? `<td class="num">${esc(actAst)}</td>` : ''}
        <td class="num">${renderSimBoxscoreStatCell(p, 'threes', p.threes_mean)}</td>
        <td class="num boxscore-line-col">${renderBoxscorePropLineCell(p, 'threes')}</td>
        ${hasRecon ? `<td class="num">${esc(act3pm)}</td>` : ''}
        <td class="num">${renderSimBoxscoreStatCell(p, 'stl', p.stl_mean)}</td>
        <td class="num boxscore-line-col">${renderBoxscorePropLineCell(p, 'stl')}</td>
        ${hasRecon ? `<td class="num">${esc(actStl)}</td>` : ''}
        <td class="num">${renderSimBoxscoreStatCell(p, 'blk', p.blk_mean)}</td>
        <td class="num boxscore-line-col">${renderBoxscorePropLineCell(p, 'blk')}</td>
        ${hasRecon ? `<td class="num">${esc(actBlk)}</td>` : ''}
        <td class="num">${renderSimBoxscoreStatCell(p, 'tov', p.tov_mean)}</td>
        <td class="num boxscore-line-col">${renderBoxscorePropLineCell(p, 'tov')}</td>
        ${hasRecon ? `<td class="num">${esc(actTov)}</td>` : ''}
        <td class="num">${renderSimBoxscoreStatCell(p, 'pra', p.pra_mean)}</td>
        <td class="num boxscore-line-col">${renderBoxscorePropLineCell(p, 'pra')}</td>
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
            <th class="num sortable boxscore-line-col" data-sort="num">PTS LINE</th>
            ${hasRecon ? '<th class="num sortable" data-sort="num">ACT PTS</th>' : ''}
            <th class="num sortable" data-sort="num">REB</th>
            <th class="num sortable boxscore-line-col" data-sort="num">REB LINE</th>
            ${hasRecon ? '<th class="num sortable" data-sort="num">ACT REB</th>' : ''}
            <th class="num sortable" data-sort="num">AST</th>
            <th class="num sortable boxscore-line-col" data-sort="num">AST LINE</th>
            ${hasRecon ? '<th class="num sortable" data-sort="num">ACT AST</th>' : ''}
            <th class="num sortable" data-sort="num">3PM</th>
            <th class="num sortable boxscore-line-col" data-sort="num">3PM LINE</th>
            ${hasRecon ? '<th class="num sortable" data-sort="num">ACT 3PM</th>' : ''}
            <th class="num sortable" data-sort="num">STL</th>
            <th class="num sortable boxscore-line-col" data-sort="num">STL LINE</th>
            ${hasRecon ? '<th class="num sortable" data-sort="num">ACT STL</th>' : ''}
            <th class="num sortable" data-sort="num">BLK</th>
            <th class="num sortable boxscore-line-col" data-sort="num">BLK LINE</th>
            ${hasRecon ? '<th class="num sortable" data-sort="num">ACT BLK</th>' : ''}
            <th class="num sortable" data-sort="num">TOV</th>
            <th class="num sortable boxscore-line-col" data-sort="num">TOV LINE</th>
            ${hasRecon ? '<th class="num sortable" data-sort="num">ACT TOV</th>' : ''}
            <th class="num sortable" data-sort="num">PRA</th>
            <th class="num sortable boxscore-line-col" data-sort="num">PRA LINE</th>
            ${hasRecon ? '<th class="num sortable" data-sort="num">ACT PRA</th><th class="num sortable" data-sort="num">ΔPRA</th>' : ''}
          </tr>
        </thead>
        <tbody>
          ${rows || `<tr><td colspan="${hasRecon ? 28 : 18}" class="subtle">No player projections.</td></tr>`}
        </tbody>
      </table>
    </div>
  `;
}

function normPlayerNameForMerge(s) {
  const raw = normPlayerName(s);
  if (!raw) return '';
  return raw
    .replace(/\b(jr|sr|ii|iii|iv|v)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function playerMergeKeys(name) {
  const full = normPlayerNameForMerge(name);
  if (!full) return { full: '', short: '' };
  const parts = full.split(' ').filter(Boolean);
  const short = (parts.length >= 2)
    ? `${parts[0].charAt(0)} ${parts[parts.length - 1]}`
    : full;
  return { full, short };
}

function simPlayerBoxscoreStats(player) {
  return {
    mp: n(player && player.min_mean),
    pts: n(player && player.pts_mean),
    reb: n(player && player.reb_mean),
    ast: n(player && player.ast_mean),
    threes: n(player && player.threes_mean),
    stl: n(player && player.stl_mean),
    blk: n(player && player.blk_mean),
    tov: n(player && player.tov_mean),
    pra: n(player && player.pra_mean),
  };
}

function livePlayerBoxscoreStats(row) {
  const pts = n(row && row.pts);
  const reb = n(row && row.reb);
  const ast = n(row && row.ast);
  const threes = n(row && (row.threes_made != null ? row.threes_made : row.threes));
  const stl = n(row && (row.stl != null ? row.stl : row.steals));
  const blk = n(row && (row.blk != null ? row.blk : row.blocks));
  const tov = n(row && (row.tov != null ? row.tov : (row.turnovers != null ? row.turnovers : row.to)));
  return {
    mp: n(row && row.mp),
    pts,
    reb,
    ast,
    threes,
    stl,
    blk,
    tov,
    pra: (pts != null && reb != null && ast != null) ? (pts + reb + ast) : null,
  };
}

function reconPlayerBoxscoreStats(row) {
  const pts = n(row && row.actual_pts);
  const reb = n(row && row.actual_reb);
  const ast = n(row && row.actual_ast);
  const threes = n(row && row.actual_3pm);
  const pra = n(row && row.actual_pra);
  const stl = n(row && row.actual_stl);
  const blk = n(row && row.actual_blk);
  const tov = n(row && row.actual_tov);
  return {
    mp: n(row && row.actual_min),
    pts,
    reb,
    ast,
    threes,
    stl,
    blk,
    tov,
    pra: (pra != null)
      ? pra
      : ((pts != null && reb != null && ast != null) ? (pts + reb + ast) : null),
  };
}

function buildSimNameMaps(players) {
  const byFull = new Map();
  const byShort = new Map();
  for (const player of (Array.isArray(players) ? players : [])) {
    const keys = playerMergeKeys(player && player.player_name);
    if (keys.full && !byFull.has(keys.full)) byFull.set(keys.full, player);
    if (keys.short && !byShort.has(keys.short)) byShort.set(keys.short, player);
  }
  return { byFull, byShort };
}

function buildReconNameMaps(reconByPlayerId) {
  const byFull = new Map();
  const byShort = new Map();
  const rows = Object.values(reconByPlayerId || {});
  for (const row of rows) {
    const keys = playerMergeKeys((row && (row.player_name || row.player)) || '');
    if (keys.full && !byFull.has(keys.full)) byFull.set(keys.full, row);
    if (keys.short && !byShort.has(keys.short)) byShort.set(keys.short, row);
  }
  return { byFull, byShort };
}

function liveLensStatRow(player, statKey) {
  try {
    const stats = player && typeof player === 'object' ? player.stats : null;
    return (stats && typeof stats === 'object') ? (stats[statKey] || null) : null;
  } catch (_) {
    return null;
  }
}

function liveLensPrimaryRow(player) {
  const statKeys = ['pts', 'reb', 'ast', 'threes', 'stl', 'blk', 'tov', 'pra'];
  for (const statKey of statKeys) {
    const row = liveLensStatRow(player, statKey);
    if (row) return row;
  }
  try {
    const stats = player && typeof player === 'object' ? player.stats : null;
    for (const row of Object.values(stats || {})) {
      if (row) return row;
    }
  } catch (_) {
    // ignore
  }
  return null;
}

function buildLiveLensNameMaps(rows) {
  const byFull = new Map();
  const byShort = new Map();
  for (const row of (Array.isArray(rows) ? rows : [])) {
    const playerName = String(row && row.player ? row.player : '').trim();
    const statKey = String(row && row.stat ? row.stat : '').toLowerCase().trim();
    if (!playerName || !statKey) continue;

    const keys = playerMergeKeys(playerName);
    let bucket = null;
    if (keys.full && byFull.has(keys.full)) bucket = byFull.get(keys.full) || null;
    if (!bucket && keys.short && byShort.has(keys.short)) bucket = byShort.get(keys.short) || null;
    if (!bucket) bucket = { player: playerName, stats: {}, projMinFinal: null };

    bucket.stats[statKey] = row;
    const projMinFinal = n(row && row.proj_min_final);
    if (projMinFinal != null) bucket.projMinFinal = projMinFinal;

    if (keys.full) byFull.set(keys.full, bucket);
    if (keys.short) byShort.set(keys.short, bucket);
  }
  return { byFull, byShort };
}

function findLiveLensPlayer(maps, name) {
  const keys = playerMergeKeys(name);
  if (keys.full && maps && maps.byFull && maps.byFull.has(keys.full)) return maps.byFull.get(keys.full) || null;
  if (keys.short && maps && maps.byShort && maps.byShort.has(keys.short)) return maps.byShort.get(keys.short) || null;
  return null;
}

function liveLensPlayerActualStats(player) {
  const minuteRow = liveLensPrimaryRow(player);
  const pts = n(liveLensStatRow(player, 'pts') && liveLensStatRow(player, 'pts').actual);
  const reb = n(liveLensStatRow(player, 'reb') && liveLensStatRow(player, 'reb').actual);
  const ast = n(liveLensStatRow(player, 'ast') && liveLensStatRow(player, 'ast').actual);
  const threes = n(liveLensStatRow(player, 'threes') && liveLensStatRow(player, 'threes').actual);
  const stl = n(liveLensStatRow(player, 'stl') && liveLensStatRow(player, 'stl').actual);
  const blk = n(liveLensStatRow(player, 'blk') && liveLensStatRow(player, 'blk').actual);
  const tov = n(liveLensStatRow(player, 'tov') && liveLensStatRow(player, 'tov').actual);
  const pra = n(liveLensStatRow(player, 'pra') && liveLensStatRow(player, 'pra').actual);
  return {
    mp: n(minuteRow && minuteRow.mp),
    pts,
    reb,
    ast,
    threes,
    stl,
    blk,
    tov,
    pra: (pra != null)
      ? pra
      : ((pts != null && reb != null && ast != null) ? (pts + reb + ast) : null),
  };
}

function mergeLiveActualStats(boxscoreRow, liveLensPlayer) {
  const primary = livePlayerBoxscoreStats(boxscoreRow);
  const fallback = liveLensPlayerActualStats(liveLensPlayer);
  const pts = (primary.pts != null) ? primary.pts : fallback.pts;
  const reb = (primary.reb != null) ? primary.reb : fallback.reb;
  const ast = (primary.ast != null) ? primary.ast : fallback.ast;
  return {
    mp: (primary.mp != null) ? primary.mp : fallback.mp,
    pts,
    reb,
    ast,
    threes: (primary.threes != null) ? primary.threes : fallback.threes,
    stl: (primary.stl != null) ? primary.stl : fallback.stl,
    blk: (primary.blk != null) ? primary.blk : fallback.blk,
    tov: (primary.tov != null) ? primary.tov : fallback.tov,
    pra: (primary.pra != null)
      ? primary.pra
      : ((fallback.pra != null)
        ? fallback.pra
        : ((pts != null && reb != null && ast != null) ? (pts + reb + ast) : null)),
  };
}

function liveLensProjectionStat(player, statKey) {
  if (statKey === 'mp') {
    const primary = liveLensPrimaryRow(player);
    return n(player && player.projMinFinal != null ? player.projMinFinal : (primary && primary.proj_min_final));
  }
  const row = liveLensStatRow(player, statKey);
  return n(row && row.pace_proj);
}

function liveLensLineEntry(player, statKey) {
  const row = liveLensStatRow(player, statKey);
  if (!row) return { line: null, source: '' };

  const liveLine = n(row && row.line_live);
  if (liveLine != null) return { line: liveLine, source: 'live' };

  const pregameLine = n(row && row.line_pregame);
  if (pregameLine != null) return { line: pregameLine, source: 'pregame' };

  const source = String(row && row.line_source ? row.line_source : '').toLowerCase().trim();
  const line = n(row && row.line);
  if (line != null && source && source !== 'model') return { line, source };
  return { line: null, source };
}

function buildMergedPlayerBoxscoreRows(simPlayers, actualRows, actualMode, liveLensRows, lineOnlyPlayers) {
  const simArr = Array.isArray(simPlayers)
    ? simPlayers.filter((player) => !isUnavailableSimPlayer(player))
    : [];
  const simMaps = buildSimNameMaps(simArr);
  const liveLensMaps = actualMode === 'live'
    ? buildLiveLensNameMaps(liveLensRows)
    : { byFull: new Map(), byShort: new Map() };
  const seenSim = new Set();
  const rows = [];
  const rowByFull = new Map();
  const rowByShort = new Map();

  const rememberRow = (row) => {
    const keys = playerMergeKeys(row && row.name);
    if (keys.full && !rowByFull.has(keys.full)) rowByFull.set(keys.full, row);
    if (keys.short && !rowByShort.has(keys.short)) rowByShort.set(keys.short, row);
  };

  const addRow = (row) => {
    rows.push(row);
    rememberRow(row);
  };

  if (actualMode === 'live') {
    const liveArr = Array.isArray(actualRows) ? actualRows.slice() : [];
    liveArr.sort((a, b) => {
      const dMp = (n(b && b.mp) ?? -1e9) - (n(a && a.mp) ?? -1e9);
      if (dMp !== 0) return dMp;
      return (n(b && b.pts) ?? -1e9) - (n(a && a.pts) ?? -1e9);
    });
    for (const row of liveArr) {
      const keys = playerMergeKeys(row && row.player);
      let simPlayer = null;
      if (keys.full) simPlayer = simMaps.byFull.get(keys.full) || null;
      if (!simPlayer && keys.short) simPlayer = simMaps.byShort.get(keys.short) || null;
      const liveLensPlayer = findLiveLensPlayer(liveLensMaps, row && row.player);
      addRow({
        name: String((row && row.player) || (simPlayer && simPlayer.player_name) || '').trim() || '—',
        simPlayer,
        propPlayer: simPlayer,
        actual: mergeLiveActualStats(row, liveLensPlayer),
        liveLens: liveLensPlayer,
        lineOnly: false,
      });
      if (simPlayer) seenSim.add(simPlayer);
    }
  } else if (actualMode === 'recon') {
    const reconById = (actualRows && typeof actualRows === 'object') ? actualRows : {};
    const reconMaps = buildReconNameMaps(reconById);
    for (const simPlayer of simArr) {
      const pid = String(simPlayer && simPlayer.player_id != null ? simPlayer.player_id : '').trim();
      let reconRow = pid ? (reconById[pid] || null) : null;
      if (!reconRow) {
        const keys = playerMergeKeys(simPlayer && simPlayer.player_name);
        if (keys.full) reconRow = reconMaps.byFull.get(keys.full) || null;
        if (!reconRow && keys.short) reconRow = reconMaps.byShort.get(keys.short) || null;
      }
      addRow({
        name: String((simPlayer && simPlayer.player_name) || (reconRow && (reconRow.player_name || reconRow.player)) || '').trim() || '—',
        simPlayer,
        propPlayer: simPlayer,
        actual: reconRow ? reconPlayerBoxscoreStats(reconRow) : null,
        liveLens: null,
        lineOnly: false,
      });
      seenSim.add(simPlayer);
    }
  }

  for (const simPlayer of simArr) {
    if (seenSim.has(simPlayer)) continue;
    const liveLensPlayer = actualMode === 'live'
      ? findLiveLensPlayer(liveLensMaps, simPlayer && simPlayer.player_name)
      : null;
    addRow({
      name: String((simPlayer && simPlayer.player_name) || '').trim() || '—',
      simPlayer,
      propPlayer: simPlayer,
      actual: (actualMode === 'live' && liveLensPlayer) ? liveLensPlayerActualStats(liveLensPlayer) : null,
      liveLens: liveLensPlayer,
      lineOnly: false,
    });
  }

  for (const player of (Array.isArray(lineOnlyPlayers) ? lineOnlyPlayers : [])) {
    if (!boxscorePlayerHasVisibleMergedPropLine(player)) continue;
    const keys = playerMergeKeys(player && player.player_name);
    let existingRow = null;
    if (keys.full) existingRow = rowByFull.get(keys.full) || null;
    if (!existingRow && keys.short) existingRow = rowByShort.get(keys.short) || null;
    if (existingRow) {
      existingRow.propPlayer = player;
      if (!existingRow.simPlayer) existingRow.lineOnly = true;
      continue;
    }

    const liveLensPlayer = actualMode === 'live'
      ? findLiveLensPlayer(liveLensMaps, player && player.player_name)
      : null;
    addRow({
      name: String((player && player.player_name) || '').trim() || '—',
      simPlayer: null,
      propPlayer: player,
      actual: (actualMode === 'live' && liveLensPlayer) ? liveLensPlayerActualStats(liveLensPlayer) : null,
      liveLens: liveLensPlayer,
      lineOnly: true,
    });
  }

  rows.sort((a, b) => {
    const aActualMp = n(a && a.actual && a.actual.mp);
    const bActualMp = n(b && b.actual && b.actual.mp);
    if ((bActualMp ?? -1e9) !== (aActualMp ?? -1e9)) return (bActualMp ?? -1e9) - (aActualMp ?? -1e9);

    const aActualPts = n(a && a.actual && a.actual.pts);
    const bActualPts = n(b && b.actual && b.actual.pts);
    if ((bActualPts ?? -1e9) !== (aActualPts ?? -1e9)) return (bActualPts ?? -1e9) - (aActualPts ?? -1e9);

    const aSim = simPlayerBoxscoreStats(a && a.simPlayer ? a.simPlayer : null);
    const bSim = simPlayerBoxscoreStats(b && b.simPlayer ? b.simPlayer : null);
    if ((bSim.mp ?? -1e9) !== (aSim.mp ?? -1e9)) return (bSim.mp ?? -1e9) - (aSim.mp ?? -1e9);
    if ((bSim.pts ?? -1e9) !== (aSim.pts ?? -1e9)) return (bSim.pts ?? -1e9) - (aSim.pts ?? -1e9);
    return String(a && a.name || '').localeCompare(String(b && b.name || ''));
  });

  const top = rows.slice(0, 12);
  const extras = rows.slice(12).filter((row) => boxscorePlayerHasVisibleMergedPropLine(row && (row.propPlayer || row.simPlayer)));
  return top.concat(extras);
}

function sumMergedBoxscoreStat(rows, bucket, statKey) {
  let total = 0;
  let seen = false;
  for (const row of (Array.isArray(rows) ? rows : [])) {
    const stats = (bucket === 'actual')
      ? (row && row.actual ? row.actual : null)
      : simPlayerBoxscoreStats(row && row.simPlayer ? row.simPlayer : null);
    const value = n(stats && stats[statKey]);
    if (value == null) continue;
    total += value;
    seen = true;
  }
  return seen ? total : null;
}

function fmtActualBoxscoreStat(statKey, value) {
  const v = n(value);
  if (v == null) return '—';
  return (statKey === 'mp') ? fmt(v, 1) : fmt(v, 0);
}

function fmtSimBoxscoreStat(_statKey, value) {
  const v = n(value);
  if (v == null) return '—';
  return fmt(v, 1);
}

function boxscorePropSimMean(player, statKey) {
  const sim = simPlayerBoxscoreStats(player);
  return n(sim && sim[statKey]);
}

function boxscorePropLeanForLine(player, statKey, line) {
  const mean = boxscorePropSimMean(player, statKey);
  if (mean == null || line == null) return '';
  return mean >= line ? 'O' : 'U';
}

function boxscorePropLineOptions(player, statKey) {
  try {
    const propLineOptions = player && typeof player.prop_line_options === 'object' ? player.prop_line_options : null;
    const rawOptions = Array.isArray(propLineOptions && propLineOptions[statKey]) ? propLineOptions[statKey] : [];
    const normalized = rawOptions
      .map((option) => {
        const line = n(option && option.line);
        if (line == null) return null;
        const sideRaw = String(option && option.side ? option.side : '').trim().toUpperCase();
        const side = (sideRaw === 'OVER' || sideRaw === 'UNDER') ? sideRaw : '';
        return {
          line,
          side,
          lean: boxscorePropLeanForLine(player, statKey, line),
          books: Array.isArray(option && option.books) ? option.books : [],
          bookCount: Math.max(0, Number(option && option.book_count) || 0),
          bestPrice: n(option && option.best_price),
          recommended: !!(option && option.recommended),
          recommendedPrimary: !!(option && option.recommended_primary),
          recommendationAction: String(option && option.recommendation_action ? option.recommendation_action : '').trim(),
          recommendationSummary: String(option && option.recommendation_summary ? option.recommendation_summary : '').trim(),
          recommendationBook: String(option && option.recommendation_book ? option.recommendation_book : '').trim(),
          recommendationPrice: n(option && option.recommendation_price),
          recommendationPlayToLine: n(option && option.recommendation_play_to_line),
          recommendationEvPct: n(option && option.recommendation_ev_pct),
        };
      })
      .filter(Boolean);
    if (normalized.length) return normalized;
  } catch (_) {
    // Ignore malformed payloads and fall through to the legacy single-line shape.
  }

  const propLines = player && typeof player.prop_lines === 'object' ? player.prop_lines : null;
  const line = n(propLines && propLines[statKey]);
  if (line == null) return [];
  return [{
    line,
    side: '',
    lean: boxscorePropLeanForLine(player, statKey, line),
    books: [],
    bookCount: 0,
    bestPrice: null,
    recommended: false,
    recommendedPrimary: false,
    recommendationAction: '',
    recommendationSummary: '',
    recommendationBook: '',
    recommendationPrice: null,
    recommendationPlayToLine: null,
    recommendationEvPct: null,
  }];
}

function boxscorePropLineValue(player, statKey) {
  try {
    const options = boxscorePropLineOptions(player, statKey);
    if (options.length) return n(options[0] && options[0].line);
    const propLines = player && typeof player.prop_lines === 'object' ? player.prop_lines : null;
    return n(propLines && propLines[statKey]);
  } catch (_) {
    return null;
  }
}

const MERGED_BOXSCORE_HIDDEN_STAT_KEYS = new Set(['stl', 'blk', 'tov']);
const MERGED_BOXSCORE_STAT_COLUMNS = [
  { key: 'pts', label: 'PTS' },
  { key: 'reb', label: 'REB' },
  { key: 'ast', label: 'AST' },
  { key: 'threes', label: '3PM' },
  { key: 'stl', label: 'STL' },
  { key: 'blk', label: 'BLK' },
  { key: 'tov', label: 'TOV' },
  { key: 'pra', label: 'PRA' },
];

function mergedBoxscoreStatColumns() {
  return MERGED_BOXSCORE_STAT_COLUMNS.filter((column) => !MERGED_BOXSCORE_HIDDEN_STAT_KEYS.has(column.key));
}

function mergedBoxscoreActualColumnCount(actualMode) {
  const statCount = mergedBoxscoreStatColumns().length;
  return 1 + statCount;
}

function mergedBoxscoreSimColumnCount() {
  return 1 + mergedBoxscoreStatColumns().length;
}

function boxscorePlayerHasVisibleMergedPropLine(player) {
  return mergedBoxscoreStatColumns().some((column) => boxscorePropLineOptions(player, column.key).length > 0);
}

function boxscorePlayerHasAnyPropLine(player) {
  return ['pts', 'reb', 'ast', 'threes', 'stl', 'blk', 'tov', 'pra'].some((statKey) => boxscorePropLineOptions(player, statKey).length > 0);
}

function selectVisibleBoxscorePlayers(players, baseCount) {
  const arr = Array.isArray(players) ? players : [];
  const limit = Math.max(0, Number(baseCount) || 0);
  if (!limit || arr.length <= limit) return arr;
  const top = arr.slice(0, limit);
  const extras = arr.slice(limit).filter((player) => boxscorePlayerHasAnyPropLine(player));
  return top.concat(extras);
}

function boxscorePropLean(player, statKey) {
  const line = boxscorePropLineValue(player, statKey);
  return boxscorePropLeanForLine(player, statKey, line);
}

function renderSimBoxscoreStatCell(player, statKey, value) {
  return esc(fmtSimBoxscoreStat(statKey, value));
}

function renderLiveBoxscoreStatValueCell(statKey, actualValue, projectionValue) {
  const actual = n(actualValue);
  if (actual == null) return '<span class="subtle">—</span>';

  const actualTxt = fmtActualBoxscoreStat(statKey, actual);
  const projection = n(projectionValue);
  if (projection == null) return esc(actualTxt);

  return `${esc(actualTxt)} <span class="subtle">(${esc(fmtSimBoxscoreStat(statKey, projection))})</span>`;
}

function renderLiveBoxscoreStatCell(actualStats, liveLensPlayer, statKey) {
  return renderLiveBoxscoreStatValueCell(
    statKey,
    actualStats && actualStats[statKey],
    liveLensProjectionStat(liveLensPlayer, statKey),
  );
}

function renderMergedBoxscoreStatCell(primaryHtml, secondaryHtml = '') {
  const primary = String(primaryHtml || '').trim() || '<span class="subtle">—</span>';
  const secondary = String(secondaryHtml || '').trim();
  return `
    <div class="boxscore-stat-stack${secondary ? ' has-lines' : ''}">
      <div class="boxscore-stat-main">${primary}</div>
      ${secondary ? `<div class="boxscore-stat-secondary">${secondary}</div>` : ''}
    </div>
  `;
}

function renderLiveMergedBoxscoreCell(actualStats, liveLensPlayer, statKey) {
  const entry = liveLensLineEntry(liveLensPlayer, statKey);
  const hasLine = n(entry && entry.line) != null;
  return renderMergedBoxscoreStatCell(
    renderLiveBoxscoreStatCell(actualStats, liveLensPlayer, statKey),
    hasLine ? renderLiveBoxscorePropLineCell(liveLensPlayer, statKey) : '',
  );
}

function renderSimMergedBoxscoreCell(player, statKey, value) {
  const hasLine = boxscorePropLineOptions(player, statKey).length > 0;
  return renderMergedBoxscoreStatCell(
    renderSimBoxscoreStatCell(player, statKey, value),
    hasLine ? renderBoxscorePropLineCell(player, statKey) : '',
  );
}

function renderBoxscorePropLineCell(player, statKey) {
  const options = boxscorePropLineOptions(player, statKey);
  if (!options.length) return '<span class="subtle">—</span>';

  return `
    <div class="boxscore-prop-stack">
      ${options.map((option) => {
        const line = n(option && option.line);
        const lean = String(option && option.lean ? option.lean : '').trim().toUpperCase();
        const side = String(option && option.side ? option.side : '').trim().toUpperCase();
        const sideShort = side === 'OVER' ? 'O' : (side === 'UNDER' ? 'U' : '');
        const tone = lean === 'O' ? 'over' : (lean === 'U' ? 'under' : 'neutral');
        const label = sideShort
          ? `${sideShort}${fmt(line, 1)}`
          : `${fmt(line, 1)}${lean ? ` ${lean}` : ''}`;
        const bookCount = Math.max(0, Number(option && option.bookCount) || 0);
        const books = Array.isArray(option && option.books) ? option.books : [];
        const bookNames = books
          .map((book) => String(book && (book.book_title || book.book) ? (book.book_title || book.book) : '').trim())
          .filter(Boolean);
        const titleBits = [];
        titleBits.push(sideShort ? `${side === 'OVER' ? 'Over' : 'Under'} ${fmt(line, 1)}` : `Prop line ${fmt(line, 1)}`);
        if (lean) titleBits.push(`sim leans ${lean === 'O' ? 'over' : 'under'}`);
        if (bookCount) titleBits.push(bookCount === 1 ? '1 book posted' : `${bookCount} books posted`);
        if (n(option && option.bestPrice) != null) titleBits.push(`best price ${fmt(Number(option.bestPrice), 0)}`);
        if (bookNames.length) titleBits.push(`books: ${bookNames.join(', ')}`);
        if (option && option.recommendedPrimary) titleBits.push('primary recommendation');
        else if (option && option.recommended) titleBits.push('recommended option');
        if (option && option.recommendationAction) titleBits.push(`guidance: ${option.recommendationAction}`);
        if (n(option && option.recommendationPlayToLine) != null) titleBits.push(`play to ${fmt(Number(option.recommendationPlayToLine), 1)}`);
        if (n(option && option.recommendationPrice) != null) titleBits.push(`recommended price ${fmt(Number(option.recommendationPrice), 0)}`);
        if (n(option && option.recommendationEvPct) != null) titleBits.push(`EV ${fmt(Number(option.recommendationEvPct), 1)}%`);
        if (option && option.recommendationSummary) titleBits.push(option.recommendationSummary);
        const title = titleBits.join('. ');
        return `
          <span class="boxscore-prop-pill ${tone}${option && option.recommended ? ' recommended' : ''}${option && option.recommendedPrimary ? ' recommended-primary' : ''}" title="${esc(title)}">
            <span class="boxscore-prop-pill-main">${esc(label)}</span>
            ${bookCount > 1 ? `<span class="boxscore-prop-pill-count">${esc(`${bookCount}x`)}</span>` : ''}
            ${option && option.recommendedPrimary ? '<span class="boxscore-prop-pill-tag">BEST</span>' : (option && option.recommended ? '<span class="boxscore-prop-pill-tag">REC</span>' : '')}
          </span>
        `;
      }).join('')}
    </div>
  `;
}

function renderLiveBoxscorePropLineCell(liveLensPlayer, statKey) {
  const entry = liveLensLineEntry(liveLensPlayer, statKey);
  const line = n(entry && entry.line);
  if (line == null) return '<span class="subtle">—</span>';

  const projection = liveLensProjectionStat(liveLensPlayer, statKey);
  const lean = projection == null ? '' : (projection >= line ? 'O' : 'U');
  const tone = lean === 'O' ? 'over' : (lean === 'U' ? 'under' : 'neutral');
  const sourceText = entry && entry.source === 'live'
    ? 'Live line'
    : ((entry && entry.source === 'pregame') ? 'Pregame fallback line' : 'Line');
  const title = lean
    ? `${sourceText} ${fmt(line, 1)} with projection ${lean === 'O' ? 'over' : 'under'} the market.`
    : `${sourceText} ${fmt(line, 1)}.`;
  return `<span class="boxscore-prop-pill ${tone}" title="${esc(title)}">${esc(fmt(line, 1))}${lean ? ` ${esc(lean)}` : ''}</span>`;
}

function sumMergedLiveProjectionStat(rows, statKey) {
  let total = 0;
  let seen = false;
  for (const row of (Array.isArray(rows) ? rows : [])) {
    const value = liveLensProjectionStat(row && row.liveLens ? row.liveLens : null, statKey);
    if (value == null) continue;
    total += value;
    seen = true;
  }
  return seen ? total : null;
}

function buildActualLineScoreRows(periods, actualAway, actualHome) {
  const byQuarter = { 1: {}, 2: {}, 3: {}, 4: {} };
  for (const row of (Array.isArray(periods) ? periods : [])) {
    const quarter = Math.floor(n(row && row.period) ?? 0);
    if (quarter < 1 || quarter > 4) continue;
    byQuarter[quarter] = {
      away: n(row && row.away),
      home: n(row && row.home),
    };
  }

  const sumOrNull = (vals) => {
    const finite = (vals || []).map((v) => n(v)).filter((v) => v != null);
    return finite.length ? finite.reduce((acc, v) => acc + v, 0) : null;
  };

  const away = {
    q1: byQuarter[1].away ?? null,
    q2: byQuarter[2].away ?? null,
    q3: byQuarter[3].away ?? null,
    q4: byQuarter[4].away ?? null,
    total: n(actualAway),
  };
  const home = {
    q1: byQuarter[1].home ?? null,
    q2: byQuarter[2].home ?? null,
    q3: byQuarter[3].home ?? null,
    q4: byQuarter[4].home ?? null,
    total: n(actualHome),
  };

  if (away.total == null) away.total = sumOrNull([away.q1, away.q2, away.q3, away.q4]);
  if (home.total == null) home.total = sumOrNull([home.q1, home.q2, home.q3, home.q4]);

  const hasAny = [away.q1, away.q2, away.q3, away.q4, away.total, home.q1, home.q2, home.q3, home.q4, home.total]
    .some((v) => n(v) != null);
  return { away, home, hasAny };
}

function buildSimLineScoreRows(meta) {
  const periods = meta && meta.sim_periods ? meta.sim_periods : {};
  const away = {
    q1: n(periods && periods.q1 && periods.q1.away_mean),
    q2: n(periods && periods.q2 && periods.q2.away_mean),
    q3: n(periods && periods.q3 && periods.q3.away_mean),
    q4: n(periods && periods.q4 && periods.q4.away_mean),
    total: n(meta && meta.sim_score && meta.sim_score.away_mean),
  };
  const home = {
    q1: n(periods && periods.q1 && periods.q1.home_mean),
    q2: n(periods && periods.q2 && periods.q2.home_mean),
    q3: n(periods && periods.q3 && periods.q3.home_mean),
    q4: n(periods && periods.q4 && periods.q4.home_mean),
    total: n(meta && meta.sim_score && meta.sim_score.home_mean),
  };

  const sumOrNull = (vals) => {
    const finite = (vals || []).map((v) => n(v)).filter((v) => v != null);
    return finite.length ? finite.reduce((acc, v) => acc + v, 0) : null;
  };

  if (away.total == null) away.total = sumOrNull([away.q1, away.q2, away.q3, away.q4]);
  if (home.total == null) home.total = sumOrNull([home.q1, home.q2, home.q3, home.q4]);
  return { away, home };
}

function fmtActualLineScore(value) {
  const v = n(value);
  return v == null ? '—' : fmt(v, 0);
}

function fmtSimLineScore(value) {
  const v = n(value);
  return v == null ? '—' : fmt(v, 1);
}

function renderLineScoreBlock(meta, actualSource) {
  const simRows = buildSimLineScoreRows(meta);
  const actualRows = buildActualLineScoreRows(
    actualSource && actualSource.periods,
    actualSource && actualSource.actualAway,
    actualSource && actualSource.actualHome,
  );
  const label = String((actualSource && actualSource.label) || 'Live').trim() || 'Live';

  return `
    <div class="merged-boxscore-section">
      <div class="merged-boxscore-title">${esc(label)} vs Sim line score</div>
      <div class="table-wrap">
        <table class="data-table merged-linescore-table">
          <thead>
            <tr>
              <th rowspan="2"></th>
              <th colspan="5" class="boxscore-side-header boxscore-side-header-live">${esc(label)}</th>
              <th colspan="5" class="boxscore-side-header boxscore-side-header-sim">Sim</th>
            </tr>
            <tr>
              <th class="num">Q1</th>
              <th class="num">Q2</th>
              <th class="num">Q3</th>
              <th class="num">Q4</th>
              <th class="num">Total</th>
              <th class="num boxscore-divider-start">Q1</th>
              <th class="num">Q2</th>
              <th class="num">Q3</th>
              <th class="num">Q4</th>
              <th class="num">Total</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td class="team">${esc(meta && meta.away ? meta.away : 'Away')}</td>
              <td class="num">${fmtActualLineScore(actualRows.away.q1)}</td>
              <td class="num">${fmtActualLineScore(actualRows.away.q2)}</td>
              <td class="num">${fmtActualLineScore(actualRows.away.q3)}</td>
              <td class="num">${fmtActualLineScore(actualRows.away.q4)}</td>
              <td class="num">${fmtActualLineScore(actualRows.away.total)}</td>
              <td class="num boxscore-divider-start">${fmtSimLineScore(simRows.away.q1)}</td>
              <td class="num">${fmtSimLineScore(simRows.away.q2)}</td>
              <td class="num">${fmtSimLineScore(simRows.away.q3)}</td>
              <td class="num">${fmtSimLineScore(simRows.away.q4)}</td>
              <td class="num">${fmtSimLineScore(simRows.away.total)}</td>
            </tr>
            <tr>
              <td class="team">${esc(meta && meta.home ? meta.home : 'Home')}</td>
              <td class="num">${fmtActualLineScore(actualRows.home.q1)}</td>
              <td class="num">${fmtActualLineScore(actualRows.home.q2)}</td>
              <td class="num">${fmtActualLineScore(actualRows.home.q3)}</td>
              <td class="num">${fmtActualLineScore(actualRows.home.q4)}</td>
              <td class="num">${fmtActualLineScore(actualRows.home.total)}</td>
              <td class="num boxscore-divider-start">${fmtSimLineScore(simRows.home.q1)}</td>
              <td class="num">${fmtSimLineScore(simRows.home.q2)}</td>
              <td class="num">${fmtSimLineScore(simRows.home.q3)}</td>
              <td class="num">${fmtSimLineScore(simRows.home.q4)}</td>
              <td class="num">${fmtSimLineScore(simRows.home.total)}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  `;
}

function renderActualBoxscoreCells(row, actualMode) {
  const actual = row && row.actual ? row.actual : null;
  const liveLensPlayer = row && row.liveLens ? row.liveLens : null;
  const statColumns = mergedBoxscoreStatColumns();
  if (actualMode === 'live') {
    const cells = [
      `<td class="num boxscore-live-stat">${renderLiveBoxscoreStatCell(actual, liveLensPlayer, 'mp')}</td>`,
    ];
    for (const column of statColumns) {
      cells.push(`<td class="num boxscore-combo-cell boxscore-combo-cell-live">${renderLiveMergedBoxscoreCell(actual, liveLensPlayer, column.key)}</td>`);
    }
    return cells.join('');
  }

  const cells = [`<td class="num">${fmtActualBoxscoreStat('mp', actual && actual.mp)}</td>`];
  for (const column of statColumns) {
    cells.push(`<td class="num">${fmtActualBoxscoreStat(column.key, actual && actual[column.key])}</td>`);
  }
  return cells.join('');
}

function renderActualBoxscoreTotalsCells(rows, actualMode) {
  const statColumns = mergedBoxscoreStatColumns();
  if (actualMode === 'live') {
    const cells = [
      `<td class="num boxscore-live-stat">${renderLiveBoxscoreStatValueCell('mp', sumMergedBoxscoreStat(rows, 'actual', 'mp'), sumMergedLiveProjectionStat(rows, 'mp'))}</td>`,
    ];
    for (const column of statColumns) {
      cells.push(`<td class="num boxscore-combo-cell boxscore-combo-cell-live">${renderMergedBoxscoreStatCell(renderLiveBoxscoreStatValueCell(column.key, sumMergedBoxscoreStat(rows, 'actual', column.key), sumMergedLiveProjectionStat(rows, column.key)))}</td>`);
    }
    return cells.join('');
  }

  const cells = [`<td class="num">${fmtActualBoxscoreStat('mp', sumMergedBoxscoreStat(rows, 'actual', 'mp'))}</td>`];
  for (const column of statColumns) {
    cells.push(`<td class="num">${fmtActualBoxscoreStat(column.key, sumMergedBoxscoreStat(rows, 'actual', column.key))}</td>`);
  }
  return cells.join('');
}

function renderComparePlayerBoxscoreTable(title, simPlayers, actualRows, actualMode, actualLabel, liveLensRows, lineOnlyPlayers) {
  const isLiveMode = actualMode === 'live';
  const statColumns = mergedBoxscoreStatColumns();
  const actualHeaderCols = mergedBoxscoreActualColumnCount(actualMode);
  const simHeaderCols = mergedBoxscoreSimColumnCount();
  const totalCols = 1 + actualHeaderCols + simHeaderCols;
  const rows = buildMergedPlayerBoxscoreRows(simPlayers, actualRows, actualMode, liveLensRows, lineOnlyPlayers);
  const body = rows.map((row) => {
    const sim = simPlayerBoxscoreStats(row && row.simPlayer ? row.simPlayer : null);
    const propPlayer = row && (row.propPlayer || row.simPlayer) ? (row.propPlayer || row.simPlayer) : null;
    const simCells = [`<td class="num boxscore-divider-start">${fmtSimBoxscoreStat('mp', sim.mp)}</td>`];
    for (const column of statColumns) {
      simCells.push(`<td class="num boxscore-combo-cell boxscore-combo-cell-sim">${renderSimMergedBoxscoreCell(propPlayer, column.key, sim[column.key])}</td>`);
    }
    return `
      <tr>
        <td>${esc(row && row.name ? row.name : '—')}${row && row.lineOnly ? ' <span class="boxscore-line-only-flag">LINES</span>' : ''}</td>
        ${renderActualBoxscoreCells(row, actualMode)}
        ${simCells.join('')}
      </tr>
    `;
  }).join('');

  const simTotalsCells = [`<td class="num boxscore-divider-start">${fmtSimBoxscoreStat('mp', sumMergedBoxscoreStat(rows, 'sim', 'mp'))}</td>`];
  for (const column of statColumns) {
    simTotalsCells.push(`<td class="num boxscore-combo-cell boxscore-combo-cell-sim">${renderMergedBoxscoreStatCell(fmtSimBoxscoreStat(column.key, sumMergedBoxscoreStat(rows, 'sim', column.key)))}</td>`);
  }

  const totalsRow = `
    <tr>
      <td>TEAM TOTAL</td>
      ${renderActualBoxscoreTotalsCells(rows, actualMode)}
      ${simTotalsCells.join('')}
    </tr>
  `;

  const actualHeaderRow = isLiveMode
    ? [`<th class="num">MIN</th>`]
        .concat(statColumns.map((column) => `<th class="num boxscore-combo-col boxscore-combo-col-live">${column.label}</th>`))
        .join('')
    : [`<th class="num">MIN</th>`]
        .concat(statColumns.map((column) => `<th class="num">${column.label}</th>`))
        .join('');

  const simHeaderRow = [`<th class="num boxscore-divider-start">MIN</th>`]
    .concat(statColumns.map((column) => `<th class="num boxscore-combo-col boxscore-combo-col-sim">${column.label}</th>`))
    .join('');

  return `
    <div class="merged-boxscore-section">
      <div class="merged-boxscore-title">${esc(title)}</div>
      <div class="table-wrap">
        <table class="data-table merged-player-boxscore">
          <thead>
            <tr>
              <th rowspan="2">Player</th>
              <th colspan="${esc(String(actualHeaderCols))}" class="boxscore-side-header boxscore-side-header-live">${esc(actualLabel)}</th>
              <th colspan="${esc(String(simHeaderCols))}" class="boxscore-side-header boxscore-side-header-sim">Sim</th>
            </tr>
            <tr>
              ${actualHeaderRow}
              ${simHeaderRow}
            </tr>
          </thead>
          <tbody>
            ${body || `<tr><td colspan="${totalCols}" class="subtle">No player rows.</td></tr>`}
          </tbody>
          <tfoot>
            ${totalsRow}
          </tfoot>
        </table>
      </div>
    </div>
  `;
}

function renderMissingSimPropPlayersTable(title, players) {
  const arr = Array.isArray(players) ? players : [];
  if (!arr.length) return '';

  const body = arr.map((player) => `
      <tr>
        <td>${esc(player && player.player_name ? player.player_name : '—')}</td>
        <td class="num boxscore-line-col boxscore-line-col-sim">${renderBoxscorePropLineCell(player, 'pts')}</td>
        <td class="num boxscore-line-col boxscore-line-col-sim">${renderBoxscorePropLineCell(player, 'reb')}</td>
        <td class="num boxscore-line-col boxscore-line-col-sim">${renderBoxscorePropLineCell(player, 'ast')}</td>
        <td class="num boxscore-line-col boxscore-line-col-sim">${renderBoxscorePropLineCell(player, 'threes')}</td>
        <td class="num boxscore-line-col boxscore-line-col-sim">${renderBoxscorePropLineCell(player, 'stl')}</td>
        <td class="num boxscore-line-col boxscore-line-col-sim">${renderBoxscorePropLineCell(player, 'blk')}</td>
        <td class="num boxscore-line-col boxscore-line-col-sim">${renderBoxscorePropLineCell(player, 'tov')}</td>
        <td class="num boxscore-line-col boxscore-line-col-sim">${renderBoxscorePropLineCell(player, 'pra')}</td>
      </tr>
    `).join('');

  return `
    <div class="table-wrap">
      <table class="data-table merged-player-boxscore boxscore-missing-table">
        <thead>
          <tr>
            <th>${esc(title)}</th>
            <th class="num boxscore-line-col boxscore-line-col-sim">PTS LINE</th>
            <th class="num boxscore-line-col boxscore-line-col-sim">REB LINE</th>
            <th class="num boxscore-line-col boxscore-line-col-sim">AST LINE</th>
            <th class="num boxscore-line-col boxscore-line-col-sim">3PM LINE</th>
            <th class="num boxscore-line-col boxscore-line-col-sim">STL LINE</th>
            <th class="num boxscore-line-col boxscore-line-col-sim">BLK LINE</th>
            <th class="num boxscore-line-col boxscore-line-col-sim">TOV LINE</th>
            <th class="num boxscore-line-col boxscore-line-col-sim">PRA LINE</th>
          </tr>
        </thead>
        <tbody>
          ${body}
        </tbody>
      </table>
    </div>
  `;
}

function renderMissingSimPropLineAudit(meta) {
  const away = (meta && Array.isArray(meta.missing_prop_players_away) ? meta.missing_prop_players_away : []).filter((player) => boxscorePlayerHasVisibleMergedPropLine(player));
  const home = (meta && Array.isArray(meta.missing_prop_players_home) ? meta.missing_prop_players_home : []).filter((player) => boxscorePlayerHasVisibleMergedPropLine(player));
  if (!away.length && !home.length) return '';

  const parts = [];
  if (away.length) parts.push(`${away.length} away line-only players`);
  if (home.length) parts.push(`${home.length} home line-only players`);

  return `
    <div class="alert boxscore-missing-audit">
      <div class="merged-boxscore-title">SmartSim coverage audit</div>
      <div class="subtle">${esc(parts.join(', '))} are shown directly in the merged player table with blank sim columns. Re-run SmartSim if those players should be projected.</div>
    </div>
  `;
}

function renderMergedBoxscoreSection(meta, actualSource) {
  const source = (actualSource && typeof actualSource === 'object') ? actualSource : {};
  const sourceMode = (source.mode === 'live' || source.mode === 'recon') ? source.mode : 'none';
  const mode = sourceMode === 'recon' ? 'recon' : 'live';
  const label = String(source.label || (mode === 'recon' ? 'Actual' : 'Live')).trim() || (mode === 'recon' ? 'Actual' : 'Live');
  const homeRows = source.homeRows || (mode === 'recon' ? {} : []);
  const awayRows = source.awayRows || (mode === 'recon' ? {} : []);
  const homeLensRows = Array.isArray(source.homeLensRows) ? source.homeLensRows : [];
  const awayLensRows = Array.isArray(source.awayLensRows) ? source.awayLensRows : [];

  let note = 'Live columns are prebuilt here and fill once ESPN summary data is available.';
  if (mode === 'recon') note = 'Actual columns come from saved reconciliation data.';
  if (sourceMode === 'live') note = `${label} columns refresh from ESPN summary data.`;
  note += ' Sim columns include the core posted prop-line groups when markets are available, and recommendation-tagged options are marked inline.';

  return `
    <div class="merged-boxscore-block">
      <div class="merged-boxscore-k">${esc(`Sim vs ${label}`)}</div>
      <div class="subtle">${esc(note)}</div>
      ${renderMissingSimPropLineAudit(meta)}
      ${renderLineScoreBlock(meta, source)}
      ${renderComparePlayerBoxscoreTable(`AWAY (${meta && meta.away ? meta.away : 'Away'})`, meta && meta.sim_players_away ? meta.sim_players_away : [], awayRows, mode, label, awayLensRows, meta && meta.missing_prop_players_away ? meta.missing_prop_players_away : [])}
      ${renderComparePlayerBoxscoreTable(`HOME (${meta && meta.home ? meta.home : 'Home'})`, meta && meta.sim_players_home ? meta.sim_players_home : [], homeRows, mode, label, homeLensRows, meta && meta.missing_prop_players_home ? meta.missing_prop_players_home : [])}
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
    stl: 'STL',
    blk: 'BLK',
    tov: 'TOV',
    pra: 'PRA',
    pa: 'P+A',
    pr: 'P+R',
    ra: 'R+A',
  };
  return map[k] || String(m || '').toUpperCase();
}

function fmtSigned(x, digits = 1) {
  const v = n(x);
  if (v == null) return '—';
  return `${v >= 0 ? '+' : ''}${v.toFixed(digits)}`;
}

function prettyBookName(book) {
  const raw = String(book || '').trim();
  if (!raw) return '';
  return raw
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (ch) => ch.toUpperCase());
}

function pregamePropActionBadgeClass(code) {
  const key = String(code || '').trim().toLowerCase();
  if (key === 'bet_now') return 'good';
  if (key === 'shop') return 'ok';
  if (key === 'pass') return 'bad';
  return '';
}

function pregamePropActionRank(code) {
  const key = String(code || '').trim().toLowerCase();
  if (key === 'bet_now') return 4;
  if (key === 'shop') return 3;
  if (key === 'wait') return 2;
  if (key === 'pass') return 1;
  return 0;
}

function normalizePregameGuidance(guidance) {
  if (!guidance || typeof guidance !== 'object') return null;
  const action = String(guidance.action || '').trim();
  const actionCode = String(guidance.action_code || action.toLowerCase().replace(/\s+/g, '_')).trim().toLowerCase();
  return {
    ...guidance,
    action,
    action_code: actionCode,
    rank: pregamePropActionRank(actionCode),
    tags: Array.isArray(guidance.tags) ? guidance.tags.filter(Boolean) : [],
  };
}

function pregamePropActionBadgeTitle(guidance) {
  const info = normalizePregameGuidance(guidance);
  if (!info) return '';
  const code = String(info.action_code || '').trim().toLowerCase();
  if (code === 'bet_now') return 'Action: bet now. Current number is still inside the playable range.';
  if (code === 'shop') return 'Action: shop. Edge is live, but compare books before entering.';
  if (code === 'wait') {
    if (info.market_view === 'disagree') return 'Action: wait. The signal is live, but the market is leaning the other way and a better entry may develop.';
    return 'Action: wait. Hold for a cleaner entry.';
  }
  if (code === 'pass') return 'Action: pass. Current number is outside the playable range.';
  return String(info.summary || '').trim();
}

function renderPregameActionBadge(guidance) {
  const info = normalizePregameGuidance(guidance);
  if (!info || !info.action) return '';
  const title = pregamePropActionBadgeTitle(info);
  const titleAttr = title ? ` title="${esc(title)}"` : '';
  return `<span class="badge ${pregamePropActionBadgeClass(info.action_code)}"${titleAttr}>${esc(info.action)}</span>`;
}

function renderPregameMarketBadge(guidance) {
  const info = normalizePregameGuidance(guidance);
  if (!info) return '';
  if (info.market_view === 'agree') {
    return '<span class="badge good" title="Market move is supporting this side.">Market agrees</span>';
  }
  if (info.market_view === 'disagree') {
    return '<span class="badge bad" title="Market move is leaning the other way.">Market disagrees</span>';
  }
  return '';
}

function renderLivePropSignalBadge(klass, side) {
  const signal = String(klass || '').toUpperCase().trim();
  const lean = String(side || '').toUpperCase().trim();
  if (signal !== 'BET' && signal !== 'WATCH') return lean ? esc(lean) : '<span class="subtle">—</span>';
  const sideShort = (lean === 'OVER') ? 'O' : ((lean === 'UNDER') ? 'U' : '');
  const label = `${signal} ${sideShort}`.trim();
  const title = (signal === 'BET')
    ? `Signal: stronger live ${lean ? lean.toLowerCase() : ''} lean from the player-prop model.`.trim()
    : `Signal: watchlist live ${lean ? lean.toLowerCase() : ''} lean from the player-prop model.`.trim();
  return `<span class="badge ${signal === 'BET' ? 'good' : 'ok'}" title="${esc(title)}">${esc(label)}</span>`;
}

function renderLivePropLineBadge(kind, inlineStyle = '') {
  const key = String(kind || 'UNK').toUpperCase().trim() || 'UNK';
  const cls = (key === 'LIVE') ? ' good' : '';
  const title = (key === 'LIVE')
    ? 'Using an OddsAPI live line for the current guide.'
    : ((key === 'PRE')
      ? 'No live line is available, so this guide is still anchored to the pregame line.'
      : 'Line source is unknown.');
  const styleAttr = inlineStyle ? ` style="${esc(inlineStyle)}"` : '';
  return `<span class="badge${cls}"${styleAttr} title="${esc(title)}">${esc(key)}</span>`;
}

function renderLivePropSimBadge(simAgree, simDisagree) {
  if (simDisagree) return '<span class="badge bad" title="SmartSim disagrees with the pace-vs-line direction.">SIM≠</span>';
  if (simAgree) return '<span class="badge good" title="SmartSim agrees with the pace-vs-line direction.">SIM✓</span>';
  return '';
}

function renderLivePropWhyBadge(label, cls = '') {
  const key = String(label || '').toUpperCase().trim();
  if (!key) return '';
  let title = '';
  if (key === 'INJ') title = 'Injury status raises live minutes or role risk.';
  else if (key === 'F4') title = 'Four fouls increase rotation and aggression risk.';
  else if (key === 'F5+') title = 'Five or more fouls create major foul-out risk.';
  else if (key === 'LOWMIN') title = 'Projected minutes remaining are low for this player.';
  else if (key === 'BENCH') title = 'Player is in an extended bench stint.';
  const clsAttr = cls ? ` ${cls}` : '';
  const titleAttr = title ? ` title="${esc(title)}"` : '';
  return `<span class="badge${clsAttr}"${titleAttr}>${esc(key)}</span>`;
}

function livePropGuideTagTitle(tag) {
  const txt = String(tag || '').trim();
  if (!txt) return '';
  if (txt === 'LIVE line') return 'Guide context: using a real live line.';
  if (txt === 'PRE fallback') return 'Guide context: still using the pregame line because no live line is available.';
  if (txt === 'No line') return 'Guide context: no market line is attached to this row.';
  if (txt === 'Bettable') return 'Guide context: the live market passed bettable-quality checks.';
  if (txt === 'SIM agrees') return 'Guide context: SmartSim agrees with the pace-vs-line direction.';
  if (txt === 'SIM disagrees') return 'Guide context: SmartSim disagrees with the pace-vs-line direction.';
  if (txt === 'Actionable edge') return 'Guide context: model edge clears the haircut by a useful margin.';
  if (txt === 'Role support') return 'Guide context: rotation and role context support the signal.';
  if (txt === 'Fresh line') return 'Guide context: the live quote is recent enough to trust more.';
  if (txt === 'Stale line') return 'Guide context: the live quote may be old; confirm before acting.';
  if (txt === 'Wide market') return 'Guide context: books are spread out, so line shopping matters more.';
  if (txt === 'Thin quality') return 'Guide context: market quality is thin, so the edge is less trustworthy.';
  if (/^\d+\s+books?$/i.test(txt)) return 'Guide context: number of books contributing to the current live market snapshot.';
  if (txt === '1 book') return 'Guide context: only one book is posting this line right now.';
  return '';
}

function renderLivePropGuideTags(tags, limit = 4) {
  const arr = Array.isArray(tags) ? tags.filter(Boolean) : [];
  if (!arr.length) return '';
  return arr.slice(0, limit).map((tag) => {
    const title = livePropGuideTagTitle(tag);
    const titleAttr = title ? ` title="${esc(title)}"` : '';
    return `<span class="live-prop-guide-tag"${titleAttr}>${esc(tag)}</span>`;
  }).join('');
}

function renderLivePropGuideLegend() {
  return `
    signal badges show the lean; action badges say what to do at the current number;
    ${renderLivePropSignalBadge('BET', 'OVER')}/${renderLivePropSignalBadge('WATCH', 'UNDER')} live signal strength;
    ${renderPregameActionBadge({ action: 'Bet now', action_code: 'bet_now' })} inside the playable range;
    ${renderPregameActionBadge({ action: 'Shop', action_code: 'shop' })} edge is live, but compare books;
    ${renderPregameActionBadge({ action: 'Wait', action_code: 'wait', market_view: 'disagree' })} wait for a cleaner live entry;
    ${renderLivePropLineBadge('LIVE')} using a live OddsAPI line;
    ${renderLivePropLineBadge('PRE')} pregame fallback;
    ${renderLivePropSimBadge(true, false)} SmartSim agrees;
    ${renderLivePropSimBadge(false, true)} SmartSim disagrees;
    <span class="live-prop-guide-tag" title="Guide context: the live quote is recent enough to trust more.">Fresh line</span> recent quote;
    <span class="live-prop-guide-tag" title="Guide context: still using the pregame line because no live line is available.">PRE fallback</span> no live line yet;
    <span class="live-prop-guide-tag" title="Guide context: market quality is thin, so the edge is less trustworthy.">Thin quality</span> lower-confidence market.
  `;
}

function renderLivePropRiskLegend() {
  return `
    why badges surface live-specific risk:
    ${renderLivePropWhyBadge('INJ', 'bad')} injury risk;
    ${renderLivePropWhyBadge('F4', 'ok')}/${renderLivePropWhyBadge('F5+', 'bad')} foul pressure;
    ${renderLivePropWhyBadge('LOWMIN', 'ok')} low projected minutes remaining;
    ${renderLivePropWhyBadge('BENCH', 'ok')} extended bench stint.
  `;
}

function buildPregameGuidance(pick, opts = {}) {
  const existing = normalizePregameGuidance(pick && pick.guidance);
  if (existing) return existing;

  const side = String((pick && pick.side) || opts.side || '').trim().toUpperCase();
  const line = n(pick && pick.line);
  const price = n(pick && pick.price);
  const openLine = n(pick && pick.open_line);
  const openPrice = n(pick && pick.open_price);
  let lineMove = n(pick && pick.line_move);
  if (lineMove == null && openLine != null && line != null) lineMove = line - openLine;
  const impliedMove = n(pick && pick.implied_move);
  const modelLine = n(opts.modelLine != null ? opts.modelLine : (pick && pick.sim_mu));
  let booksCount = n(opts.booksCount);
  if (booksCount == null) {
    const consensus = n(opts.consensus);
    if (consensus != null) booksCount = Math.max(1, Math.round(1 + (consensus * 4.0)));
  }
  const reasons = Array.isArray(opts.reasons) ? opts.reasons.map((x) => String(x || '').trim()).filter(Boolean) : [];
  const bestLine = (opts.bestLine != null)
    ? !!opts.bestLine
    : ((n(opts.lineAdv) != null) ? (n(opts.lineAdv) >= 0.99) : reasons.some((x) => /best line/i.test(x)));
  const bestPrice = (opts.bestPrice != null)
    ? !!opts.bestPrice
    : reasons.some((x) => /best price/i.test(x));

  let entryState = 'flat';
  try {
    if (line != null && openLine != null && Math.abs(line - openLine) >= 0.24) {
      if (side === 'OVER') entryState = (line < openLine) ? 'better' : 'worse';
      else if (side === 'UNDER') entryState = (line > openLine) ? 'better' : 'worse';
    } else if (price != null && openPrice != null && Math.abs(price - openPrice) >= 10.0) {
      entryState = (price > openPrice) ? 'better' : 'worse';
    }
  } catch (_) {
    entryState = 'flat';
  }

  let moveSignal = 0;
  try {
    if (lineMove != null && Math.abs(lineMove) >= 0.24) {
      if (side === 'OVER') moveSignal = (lineMove > 0) ? 1 : -1;
      else if (side === 'UNDER') moveSignal = (lineMove < 0) ? 1 : -1;
    } else if (impliedMove != null && Math.abs(impliedMove) >= 0.015) {
      if (side === 'OVER') moveSignal = (impliedMove > 0) ? 1 : -1;
      else if (side === 'UNDER') moveSignal = (impliedMove < 0) ? 1 : -1;
    }
  } catch (_) {
    moveSignal = 0;
  }
  const marketView = (moveSignal > 0) ? 'agree' : ((moveSignal < 0) ? 'disagree' : 'neutral');

  let fastMove = false;
  try {
    fastMove = !!(
      (lineMove != null && Math.abs(lineMove) >= 1.0)
      || (impliedMove != null && Math.abs(impliedMove) >= 0.05)
    );
  } catch (_) {
    fastMove = false;
  }

  let playToLine = null;
  let withinPlayTo = true;
  let edgeCushion = null;
  try {
    if (modelLine != null && line != null && (side === 'OVER' || side === 'UNDER')) {
      edgeCushion = (side === 'OVER') ? (modelLine - line) : (line - modelLine);
      let haircut = 0.35;
      if (marketView === 'disagree') haircut += 0.15;
      if (entryState === 'worse') haircut += 0.15;
      if ((booksCount != null) && booksCount <= 1) haircut += 0.10;
      if (!bestLine) haircut += 0.10;
      if (fastMove) haircut += 0.10;
      if (edgeCushion > haircut) {
        const target = (side === 'OVER') ? (modelLine - haircut) : (modelLine + haircut);
        if (side === 'OVER') {
          playToLine = Math.floor(target * 2.0) / 2.0;
          withinPlayTo = line <= (playToLine + 1e-9);
        } else {
          playToLine = Math.ceil(target * 2.0) / 2.0;
          withinPlayTo = line >= (playToLine - 1e-9);
        }
      } else {
        withinPlayTo = false;
      }
    }
  } catch (_) {
    playToLine = null;
    withinPlayTo = true;
    edgeCushion = null;
  }

  let action = 'Shop';
  if (!withinPlayTo) {
    action = 'Pass';
  } else if (marketView === 'agree') {
    if (entryState === 'better' || entryState === 'flat') action = (bestLine || (bestPrice && (booksCount || 0) >= 2)) ? 'Bet now' : 'Shop';
    else action = (fastMove && !bestLine) ? 'Pass' : 'Shop';
  } else if (marketView === 'disagree') {
    if (entryState === 'better' || entryState === 'flat') action = 'Wait';
    else action = 'Pass';
  } else if (entryState === 'better') {
    action = bestLine ? 'Bet now' : 'Shop';
  } else if (entryState === 'flat') {
    action = (bestLine && (bestPrice || (booksCount || 0) >= 2)) ? 'Bet now' : 'Shop';
  } else {
    action = (bestLine && !fastMove) ? 'Shop' : 'Pass';
  }

  let summary = 'Little movement so far; use line shopping.';
  if (marketView === 'agree' && entryState === 'worse') summary = `Market agreed with the ${side.toLowerCase()}, but this number is worse than the open.`;
  else if (marketView === 'agree') summary = `Market agreed with the ${side.toLowerCase()} and this entry is still playable.`;
  else if (marketView === 'disagree' && entryState === 'better') summary = 'This number improved from the open, but the market is leaning the other way.';
  else if (marketView === 'disagree') summary = `The market is moving against the ${side.toLowerCase()} without giving a better entry.`;
  else if (entryState === 'better') summary = 'Entry improved from the open, but confirmation is limited.';
  else if (entryState === 'worse') summary = 'Current entry is worse than the open, so price shopping matters.';

  const tags = [];
  if (marketView === 'agree') tags.push('Market agrees');
  else if (marketView === 'disagree') tags.push('Market disagrees');
  if (entryState === 'better') tags.push('Better than open');
  else if (entryState === 'worse') tags.push('Worse than open');
  if ((booksCount || 0) >= 3) tags.push(`${booksCount} books aligned`);
  else if ((booksCount || 0) >= 2) tags.push(`${booksCount} books posted`);
  if (bestLine) tags.push('Best line');
  else if (bestPrice) tags.push('Best price');
  if (fastMove) tags.push('Fast move');

  return normalizePregameGuidance({
    action,
    action_code: action.toLowerCase().replace(/\s+/g, '_'),
    market_view: marketView,
    entry_state: entryState,
    books_count: booksCount,
    best_line: !!bestLine,
    best_price: !!bestPrice,
    fast_move: !!fastMove,
    play_to_line: playToLine,
    within_play_to: !!withinPlayTo,
    edge_cushion: edgeCushion,
    summary,
    tags,
  });
}

function pregamePropPlayLabel(pick) {
  if (!pick || typeof pick !== 'object') return '';
  const mk = marketLabel(pick.market);
  const side = String(pick.side || '').toUpperCase();
  const line = n(pick.line);
  return `${mk} ${side}${line == null ? '' : ` ${fmt(line, 1)}`}`.trim();
}

function pregamePropMoveText(pick) {
  if (!pick || typeof pick !== 'object') return '';
  const openLine = n(pick.open_line);
  const openPrice = n(pick.open_price);
  const curLine = n(pick.line);
  const curPrice = n(pick.price);
  const lineMove = n(pick.line_move);
  const impliedMove = n(pick.implied_move);
  const bits = [];

  if (openLine != null || curLine != null || openPrice != null || curPrice != null) {
    const openBits = [];
    const nowBits = [];
    if (openLine != null) openBits.push(fmt(openLine, 1));
    if (openPrice != null) openBits.push(fmtAmer(openPrice));
    if (curLine != null) nowBits.push(fmt(curLine, 1));
    if (curPrice != null) nowBits.push(fmtAmer(curPrice));
    if (openBits.length || nowBits.length) bits.push(`Open ${openBits.join(' ')} → Now ${nowBits.join(' ')}`);
  }
  if (lineMove != null && Math.abs(lineMove) >= 0.1) bits.push(`ΔLine ${fmtSigned(lineMove, 1)}`);
  if (impliedMove != null && Math.abs(impliedMove) >= 0.005) bits.push(`ΔImp ${pct(impliedMove, 1)}`);
  return bits.join(' · ');
}

function renderPregamePropAltPick(pick) {
  if (!pick || typeof pick !== 'object') return '';
  const label = pregamePropPlayLabel(pick);
  const book = prettyBookName(pick.book);
  const price = n(pick.price);
  const evPct = n(pick.ev_pct);
  return [
    label,
    book ? `@ ${book}` : '',
    price != null ? fmtAmer(price) : '',
    evPct != null ? `EV ${fmt(evPct, 1)}%` : '',
  ].filter(Boolean).join(' ');
}

function renderPropRecommendations(propRecs, homeTri, awayTri) {
  const recs = propRecs && typeof propRecs === 'object' ? propRecs : {};
  const home = Array.isArray(recs.home) ? recs.home : [];
  const away = Array.isArray(recs.away) ? recs.away : [];

  const entries = [...home.map((r) => ({ ...r, side: 'home' })), ...away.map((r) => ({ ...r, side: 'away' }))]
    .map((r) => {
      const sideTri = r.side === 'home' ? homeTri : awayTri;
      const player = String(r.player || '').trim();
      const b = (r && r.best && typeof r.best === 'object') ? r.best : null;
      if (!player || !b) return null;
      const picks = Array.isArray(r.picks) ? r.picks.filter((pp) => pp && typeof pp === 'object') : [b];
      const best = picks[0] || b;
      const guidance = buildPregameGuidance(best, { modelLine: n(best && best.sim_mu) });
      return { sideTri, player, picks, best, guidance };
    })
    .filter(Boolean);

  entries.sort((a, b) => {
    const aRank = (a && a.guidance) ? pregamePropActionRank(a.guidance.action_code) : 0;
    const bRank = (b && b.guidance) ? pregamePropActionRank(b.guidance.action_code) : 0;
    if (bRank !== aRank) return bRank - aRank;
    const aEv = n(a && a.best && a.best.ev_pct) ?? -1e9;
    const bEv = n(b && b.best && b.best.ev_pct) ?? -1e9;
    if (bEv !== aEv) return bEv - aEv;
    return String(a && a.player || '').localeCompare(String(b && b.player || ''));
  });

  const rows = entries
    .map(({ sideTri, player, picks, best, guidance }) => {
      const label = pregamePropPlayLabel(best);
      const book = prettyBookName(best.book);
      const price = n(best.price);
      const evPct = n(best.ev_pct);
      const pwin = n(best.p_win);
      const mu = n(best.sim_mu);
      const metaBits = [
        (pwin != null) ? `p≈${pct(pwin, 0)}` : '',
        (mu != null) ? `μ ${fmt(mu, 1)}` : '',
        (evPct != null) ? `EV ${fmt(evPct, 1)}%` : '',
        book ? `@ ${book}` : '',
        (price != null) ? fmtAmer(price) : '',
      ].filter(Boolean).join(' · ');
      const moveText = pregamePropMoveText(best);
      const playToLine = n(guidance && guidance.play_to_line);
      const side = String(best.side || '').toUpperCase();
      const playToText = (playToLine == null)
        ? ''
        : `${marketLabel(best.market)} ${side} ${fmt(playToLine, 1)} or better`;
      const actionBadge = renderPregameActionBadge(guidance);
      const tagHtml = guidance && Array.isArray(guidance.tags)
        ? guidance.tags.slice(0, 4).map((tag) => `<span class="prop-rec-tag">${esc(tag)}</span>`).join('')
        : '';
      const altLines = picks.slice(1, 3).map(renderPregamePropAltPick).filter(Boolean).join(' • ');

      return `
        <li class="prop-rec-item">
          <div class="prop-rec-head">
            <div class="prop-rec-title">
              <span class="badge">${esc(sideTri)}</span>
              <span class="prop-rec-player">${esc(player)}</span>
            </div>
            ${actionBadge}
          </div>
          <div class="prop-rec-pick">${esc(label)}</div>
          ${metaBits ? `<div class="prop-rec-meta">${esc(metaBits)}</div>` : ''}
          ${guidance && guidance.summary ? `<div class="prop-rec-guidance">${esc(guidance.summary)}</div>` : ''}
          ${playToText ? `<div class="prop-rec-playto"><span class="badge">PLAY TO</span><span>${esc(playToText)}</span></div>` : ''}
          ${moveText ? `<div class="prop-rec-move subtle">${esc(moveText)}</div>` : ''}
          ${tagHtml ? `<div class="prop-rec-tags">${tagHtml}</div>` : ''}
          ${altLines ? `<div class="prop-rec-alts subtle">Other lines: ${esc(altLines)}</div>` : ''}
        </li>
      `;
    })
    .slice(0, 12)
    .join('');

  return `
    <div class="writeup-content">
      <div class="subtle">Recommendations are still model-vs-line. Movement is used as execution guidance so you can see whether to bet now, shop, wait, or pass.</div>
      <ul class="prop-rec-list">
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
  const paceFinal = Math.max(actTot, actTot + (simFinal - simAt));
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
          <span class="chip neutral">Bet proj: <span class="fw-700 lens-proj-bet">—</span></span>
          <span class="chip neutral">${esc(simFinalLabel)}: <span class="fw-700 lens-sim-final">—</span></span>
          <span class="chip neutral">Driver: <span class="fw-700 lens-driver">—</span></span>
          <span class="chip neutral">Lean: <span class="fw-700 lens-lean">—</span></span>
          <span class="chip neutral">Rates: <span class="fw-700 lens-rates">—</span></span>
          <span class="chip neutral">Recent: <span class="fw-700 lens-recent">—</span></span>
          <span class="chip neutral">Adjust: <span class="fw-700 lens-adjust">—</span></span>
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
    <div class="market-tile live-lens" data-lens-id="${esc(id)}" data-game-id="${esc(gid)}" ${dAttrs.join(' ')}>
      <div class="lens-top">
        <div class="market-title">LIVE LENS</div>
        <div class="subtle lens-live-bar">Live: <span class="lens-live-status">—</span> · <span class="lens-live-score">—</span> · <span class="lens-live-lines">Lines —</span> · Updated <span class="lens-live-updated">—</span></div>
      </div>

      <div class="row chips lens-market-row" style="margin-top:4px;">
        <span class="chip neutral lens-market-ml">ML: —</span>
        <span class="chip neutral lens-market-ats">ATS: —</span>
        <span class="chip neutral lens-market-total">Total: —</span>
        <span class="chip neutral lens-market-1h-ats">1H ATS: —</span>
        <span class="chip neutral lens-market-1h-total">1H Total: —</span>
        <span class="chip neutral lens-live-attempts">Attempts: —</span>
      </div>

      <div class="row chips lens-rec-row" style="margin-top:4px;">
        <span class="chip neutral lens-rec-total" title="Live Lens total recommendation">Total: —</span>
        <span class="chip neutral lens-rec-half" title="Live Lens 1H total recommendation">1H: —</span>
        <span class="chip neutral lens-rec-qtr" title="Live Lens quarter total recommendation">Q: —</span>
        <span class="chip neutral lens-rec-ats" title="Live Lens ATS recommendation">ATS: —</span>
        <span class="chip neutral lens-rec-ml" title="Live Lens ML recommendation">ML: —</span>
      </div>

      <div class="row chips lens-total-explain-row" style="margin-top:4px;">
        <span class="chip neutral lens-total-explain-main">Signal: —</span>
        <span class="chip neutral lens-total-explain-build">Build: —</span>
        <span class="chip neutral lens-total-explain-rates">Rates: —</span>
        <span class="chip neutral lens-total-explain-adjust">Adjust: —</span>
      </div>

      <div class="lens-columns" style="margin-top:8px;">
        ${renderSegmentTile('q1', '1Q', q1SimTotal, renderQuarterCol(1))}
        ${renderSegmentTile('half', '1H', halfSimTotal, renderScopeCol('half', '1H interval', 24, '1H'))}
        ${renderSegmentTile('q3', '3Q', q3SimTotal, renderQuarterCol(3))}
        ${renderSegmentTile('game', 'FULL GAME', gameSimTotal, renderScopeCol('game', 'Full game interval', 48, 'G'))}
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
      const isFrozen = !!(scopeEl && scopeEl.dataset && scopeEl.dataset.frozen === '1');
      const minEl = scopeEl.querySelector('select.lens-min');
      const totEl = scopeEl.querySelector('input.lens-total');
      const liveEl = scopeEl.querySelector('input.lens-live');

      const outSimAt = scopeEl.querySelector('.lens-sim-at');
      const outDelta = scopeEl.querySelector('.lens-delta');
      const outPace = scopeEl.querySelector('.lens-pace');
      const outProjBet = scopeEl.querySelector('.lens-proj-bet');
      const outSimFinal = scopeEl.querySelector('.lens-sim-final');
      const outDriver = scopeEl.querySelector('.lens-driver');
      const outLean = scopeEl.querySelector('.lens-lean');
      const outRates = scopeEl.querySelector('.lens-rates');
      const outRecent = scopeEl.querySelector('.lens-recent');
      const outAdjust = scopeEl.querySelector('.lens-adjust');

      const minRem = clampInt(minEl && minEl.value, 0, totalMinutes, totalMinutes);
      const actTot = n(totEl && totEl.value != null ? totEl.value : null);
      const liveTot = n(liveEl && liveEl.value != null && String(liveEl.value).trim() !== '' ? liveEl.value : null);
      const recentWindowSecRaw = n(scopeEl && scopeEl.dataset ? scopeEl.dataset.recentWindowSec : null);
      const recentPossRaw = n(scopeEl && scopeEl.dataset ? scopeEl.dataset.recentPoss : null);
      const recentPtsRaw = n(scopeEl && scopeEl.dataset ? scopeEl.dataset.recentPts : null);

      if (actTot == null) {
        if (outSimAt) outSimAt.textContent = '—';
        if (outDelta) outDelta.textContent = '—';
        if (outPace) outPace.textContent = '—';
        if (outProjBet) outProjBet.textContent = '—';
        if (outSimFinal) outSimFinal.textContent = '—';
        if (outDriver) outDriver.textContent = '—';
        if (outLean) outLean.textContent = '—';
        if (outRates) outRates.textContent = '—';
        if (outRecent) outRecent.textContent = '—';
        if (outAdjust) outAdjust.textContent = '—';
        return;
      }

      const elapsed = totalMinutes - minRem; // minutes elapsed in scope
  const simAt = cumP50AtElapsedMinutes(segsLocal, segMinLocal, elapsed, totalMinutes, finalIdx);
  const ss = Array.isArray(segsLocal) ? segsLocal : [];
  const simFinal = n(ss[finalIdx] && ss[finalIdx].cum_q && ss[finalIdx].cum_q.p50);

      const delta = (simAt == null) ? null : (simAt - actTot); // Sim - Act
      let paceFinal = (simAt != null && simFinal != null) ? (actTot + (simFinal - simAt)) : null;

        // Projection can never be below points already scored in this scope.
        if (paceFinal != null) paceFinal = Math.max(actTot, paceFinal);

      // Optional context for logging/tuning.
      let possCtx = null;

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

        try {
          const actPpp0 = (actTot != null && possLive != null) ? (actTot / Math.max(1.0, possLive)) : null;
          possCtx = {
            poss_live: possLive,
            poss_expected_so_far: possExpectedSoFar,
            poss_expected_full: possExpectedFull,
            exp_ppp: expPpp,
            act_ppp: actPpp0,
            pace_ratio: (possLive != null && possExpectedSoFar != null && possExpectedSoFar > 1e-6) ? (possLive / possExpectedSoFar) : null,
            pace_points: paceFinal,
            pace_poss: null,
            pace_alpha: 0.0,
            pace_blend_delta: 0.0,
            pace_final: paceFinal,
            w_pace: 0.0,
          };
        } catch (_) {
          possCtx = null;
        }

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
              try { if (possCtx && typeof possCtx === 'object') possCtx.w_pace = wPace; } catch (_) { /* ignore */ }

              const paceRatioShrunk = 1.0 + (paceRatio - 1.0) * wPace;
              const actPpp = actTot / Math.max(1.0, possLive);
              const effDelta = actPpp - expPpp;
              const wEffProj = n(adjCfg && adjCfg.eff_weight_proj);
              const wEff = ((wEffProj != null) ? wEffProj : 0.5) * wPace;
              const effDeltaShrunk = effDelta * wEff;

              const projPossFull = possExpectedFull * paceRatioShrunk;
              const projPpp = expPpp + effDeltaShrunk;

              // Possession-based projection anchored on points already scored.
              // This prevents impossible states like projected final < current points.
              const pointsBasedBeforePoss = paceFinal;
              const possRemaining = Math.max(0.0, projPossFull - possLive);
              let possBased = actTot + (possRemaining * projPpp);

              // Guardrails vs SmartSim median for the scope.
              const maxDevCfg = n(adjCfg && adjCfg.max_dev_points);
              const maxDev = (maxDevCfg != null) ? maxDevCfg : (2.0 + (25.0 * (totalMinutes / 48.0)));
              const hi = simFinal + maxDev;
              if (actTot <= hi) {
                possBased = Math.max(simFinal - maxDev, Math.min(hi, possBased));
              }
              possBased = Math.max(actTot, possBased);

              // Blend with points-based ladder output to preserve SmartSim ladder prior.
              const alpha = wPace;
              let blended = (1.0 - alpha) * paceFinal + alpha * possBased;
              const maxDeltaCfg = n(adjCfg && adjCfg.max_delta_points);
              const maxDelta = (maxDeltaCfg != null) ? maxDeltaCfg : (2.0 + (15.0 * (totalMinutes / 48.0)));
              blended = Math.max(paceFinal - maxDelta, Math.min(paceFinal + maxDelta, blended));
              try {
                if (possCtx && typeof possCtx === 'object') {
                  possCtx.pace_points = pointsBasedBeforePoss;
                  possCtx.pace_poss = possBased;
                  possCtx.pace_alpha = alpha;
                  possCtx.pace_blend_delta = blended - pointsBasedBeforePoss;
                  possCtx.proj_poss_full = projPossFull;
                  possCtx.proj_ppp = projPpp;
                  possCtx.eff_delta = effDelta;
                  possCtx.eff_delta_shrunk = effDeltaShrunk;
                  possCtx.poss_remaining = possRemaining;
                }
              } catch (_) {
                // ignore
              }
              paceFinal = blended;
            }
          }
        }

        // Recent-window adjustment: detect fast pace/eff shifts using last-N-seconds window.
        try {
          const t = (__liveLensTuning && typeof __liveLensTuning === 'object') ? __liveLensTuning : null;
          const rwCfg = t && t.recent_window && typeof t.recent_window === 'object' ? t.recent_window : null;
          if (!isFinal && !isFrozen && rwCfg && rwCfg.enabled !== false && paceFinal != null && expPace != null && expPpp != null) {
            const windowSec = recentWindowSecRaw ?? n(rwCfg.window_sec) ?? 180;
            const windowMin = Math.max(0.25, Math.min(10.0, windowSec / 60.0));

            const recentPoss = recentPossRaw;
            const recentPts = recentPtsRaw;

            const expPossWindow = expPace * (windowMin / 48.0);
            const expPossRemaining = expPace * (minRem / 48.0);
            const minPoss = n(rwCfg.min_possessions) ?? 6;
            const minAtt = n(rwCfg.min_attempts) ?? 8;

            // Conservative activation: require reasonable sample + some game state.
            if (recentPoss != null && recentPoss > 0.5 && expPossWindow > 0.25 && expPossRemaining > 0.25) {
              const paceRatioRecent = recentPoss / expPossWindow;
              const wPoss = Math.max(0, Math.min(1, (recentPoss - minPoss) / Math.max(1e-6, minPoss)));
              const wTime = Math.max(0, Math.min(1, (elapsedMin - (windowMin * 0.5)) / Math.max(1.0, windowMin)));
              const w = Math.max(0, Math.min(1, Math.min(wPoss, wTime)));

              if (w > 0) {
                const paceWeight = n(rwCfg.pace_weight) ?? 0.35;
                const paceCap = n(rwCfg.pace_cap_points) ?? 3.0;
                const effWeight = n(rwCfg.eff_weight) ?? 0.20;
                const effCap = n(rwCfg.eff_cap_points) ?? 2.0;

                // Pace nudge: adjust remaining possessions based on recent pace ratio (shrunk).
                const paceRatioShrunk = 1.0 + (paceRatioRecent - 1.0) * w;
                const paceAdjRaw = (expPossRemaining * expPpp) * (paceRatioShrunk - 1.0);
                let paceAdj = paceAdjRaw * paceWeight;
                paceAdj = Math.max(-paceCap, Math.min(paceCap, paceAdj));

                // Efficiency nudge: points-per-possession in recent window vs expected PPP (shrunk).
                let effAdj = 0.0;
                if (recentPts != null && recentPts >= 0) {
                  const recentPpp = recentPts / Math.max(1.0, recentPoss);
                  const effDelta = recentPpp - expPpp;
                  const effAdjRaw = expPossRemaining * (effDelta * w);
                  effAdj = effAdjRaw * effWeight;
                  effAdj = Math.max(-effCap, Math.min(effCap, effAdj));
                }

                paceFinal = paceFinal + paceAdj + effAdj;
                try {
                  if (possCtx && typeof possCtx === 'object') {
                    possCtx.recent_window = {
                      window_sec: windowSec,
                      poss: recentPoss,
                      pts: recentPts,
                      pace_ratio_recent: paceRatioRecent,
                      w_recent: w,
                      pace_adj: paceAdj,
                      eff_adj: effAdj,
                    };
                  }
                } catch (_) {
                  // ignore
                }
              }
            }
          }
        } catch (_) {
          // ignore
        }
      } catch (_) {
        // ignore
      }

      try {
        if (possCtx && typeof possCtx === 'object') possCtx.pace_final = paceFinal;
      } catch (_) {
        // ignore
      }

      let driver = null;
      if (delta != null) {
        if (delta > 3.0) driver = 'Act behind';
        else if (delta < -3.0) driver = 'Act ahead';
        else driver = 'On track';
      }

      // Interval drift correction: adjust projections within known-biased segments.
      // This is a small, tunable nudge (typically end-of-quarter segments).
      try {
        const t = (__liveLensTuning && typeof __liveLensTuning === 'object') ? __liveLensTuning : null;
        const drift = t && t.interval_drift && typeof t.interval_drift === 'object' ? t.interval_drift : null;
        if (!isFinal && drift && drift.enabled !== false && paceFinal != null) {
          const ss0 = Array.isArray(segsLocal) ? segsLocal : [];
          const elapsedMin = totalMinutes - minRem;
          const segMin = Math.max(1, n(segMinLocal) ?? 1);
          const segCount = Math.max(1, Math.min(100, (finalIdx != null ? (Number(finalIdx) + 1) : ss0.length) || ss0.length || 1));
          const localSegIdx = Math.min(segCount, Math.max(1, Math.floor(elapsedMin / segMin) + 1));
          const segObj = ss0[localSegIdx - 1];
          const globalSegIdx = n(segObj && segObj.idx);
          const biasMap = drift.seg_bias_points && typeof drift.seg_bias_points === 'object' ? drift.seg_bias_points : null;
          const bias = (biasMap && globalSegIdx != null) ? n(biasMap[String(Math.round(globalSegIdx))]) : null;
          if (bias != null) {
            const within = elapsedMin - (Math.floor(elapsedMin / segMin) * segMin);
            const rem = Math.max(0, Math.min(segMin, segMin - within));
            const frac = (segMin > 1e-6) ? (rem / segMin) : 0.0;
            const maxAbs = n(drift.max_abs_points);
            let adj = bias * frac;
            if (maxAbs != null && maxAbs > 0) {
              adj = Math.max(-maxAbs, Math.min(maxAbs, adj));
            }
            paceFinal = paceFinal + adj;
            try {
              if (possCtx && typeof possCtx === 'object') {
                possCtx.interval_drift = {
                  global_seg_idx: globalSegIdx,
                  bias_points: bias,
                  rem_frac: frac,
                  adj_points: adj,
                };
              }
            } catch (_) {
              // ignore
            }
          }
        }
      } catch (_) {
        // ignore
      }

      // Endgame close-game correction (G + Q4): late scoring inflation and under-edge
      // reversion are applied consistently with the main game-total signal path.
      try {
        const ef = _liveLensEndgameFoulCfg();
        if (!isFinal && !isFrozen && ef && ef.enabled !== false && paceFinal != null) {
          const pNow = lensRoot && lensRoot.dataset ? n(lensRoot.dataset.period) : null;
          const secLeft = lensRoot && lensRoot.dataset ? n(lensRoot.dataset.secLeftPeriod) : null;
          const marginHome = lensRoot && lensRoot.dataset ? n(lensRoot.dataset.margin) : null;

          const isQ4 = (pNow != null && Math.floor(pNow) === 4);

          const isGame = (totalMinutes === 48 && labelPrefix === 'G');
          const isQ4Scope = (totalMinutes === 12 && /^Q4$/i.test(String(labelPrefix || '')));

          if ((isGame || isQ4Scope) && isQ4 && secLeft != null && marginHome != null) {
            const liveDiff = (liveTot != null && paceFinal != null) ? (paceFinal - liveTot) : null;
            const foulCtx = computeEndgameTotalCorrection(ef, {
              period: pNow,
              sec_left_period: secLeft,
              margin_home: marginHome,
            }, liveDiff);
            if (foulCtx.applies) {
              paceFinal = paceFinal + foulCtx.total_adj;
              try {
                if (possCtx && typeof possCtx === 'object') {
                  possCtx.endgame_foul = {
                    sec_left: foulCtx.sec_left,
                    abs_margin: foulCtx.abs_margin,
                    w: foulCtx.w,
                    base_adj_points: foulCtx.base_adj,
                    under_adj_points: foulCtx.under_adj,
                    adj_points: foulCtx.total_adj,
                  };
                }
              } catch (_) {
                // ignore
              }
            }
          }
        }
      } catch (_) {
        // ignore
      }

      // Final guardrail: projected finish must be >= points already scored.
      // When the scope clock is actually 0:00 (end of quarter / halftime), snap to the settled actual.
      try {
        if (paceFinal != null) paceFinal = Math.max(actTot, paceFinal);
        const pNow = lensRoot && lensRoot.dataset ? n(lensRoot.dataset.period) : null;
        const secLeft = lensRoot && lensRoot.dataset ? n(lensRoot.dataset.secLeftPeriod) : null;
        const pn = (pNow != null && Number.isFinite(Number(pNow))) ? Math.floor(Number(pNow)) : null;
        const lp = String(labelPrefix || '').toUpperCase().trim();
        const scopeEndedNow = (secLeft != null && Number.isFinite(Number(secLeft)) && Number(secLeft) <= 0 && pn != null)
          ? ((/^Q[1-4]$/.test(lp) && pn === Number(lp.replace('Q', ''))) || (lp === '1H' && pn === 2))
          : false;
        if (scopeEndedNow) paceFinal = actTot;
      } catch (_) {
        // ignore
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

      const fmtSignedVal = (val, digits = 1) => {
        if (val == null) return '—';
        return `${val >= 0 ? '+' : ''}${fmt(val, digits)}`;
      };

      let projBet = null;
      if (paceFinal != null && liveTot != null) {
        const diff = paceFinal - liveTot;
        projBet = `${diff >= 0 ? 'O' : 'U'} ${fmt(paceFinal, 1)} vs ${fmt(liveTot, 1)} (${fmtSignedVal(diff, 1)})`;
      } else if (paceFinal != null && simFinal != null) {
        const diff = paceFinal - simFinal;
        projBet = `${fmt(paceFinal, 1)} vs sim ${fmt(simFinal, 1)} (${fmtSignedVal(diff, 1)})`;
      }

      let ratesSummary = null;
      try {
        if (possCtx && typeof possCtx === 'object' && possCtx.poss_live != null && possCtx.poss_expected_so_far != null) {
          const possTxt = `Poss ${fmt(possCtx.poss_live, 1)}/${fmt(possCtx.poss_expected_so_far, 1)}`;
          const pppTxt = (possCtx.act_ppp != null && possCtx.exp_ppp != null)
            ? `PPP ${fmt(possCtx.act_ppp, 2)}/${fmt(possCtx.exp_ppp, 2)}`
            : null;
          const paceTxt = (possCtx.pace_ratio != null) ? `${fmt(possCtx.pace_ratio, 2)}x pace` : null;
          ratesSummary = [possTxt, pppTxt, paceTxt].filter(Boolean).join(' | ');
        }
      } catch (_) {
        ratesSummary = null;
      }

      let recentSummary = null;
      try {
        if (recentPossRaw != null || recentPtsRaw != null) {
          const rwObj = (possCtx && possCtx.recent_window && typeof possCtx.recent_window === 'object') ? possCtx.recent_window : null;
          const rwSec = (rwObj && rwObj.window_sec != null) ? rwObj.window_sec : recentWindowSecRaw;
          const rwLabel = (rwSec != null)
            ? ((rwSec >= 60) ? `${fmt(rwSec / 60.0, 1)}m` : `${fmt(rwSec, 0)}s`)
            : 'recent';
          const paceTxt = (rwObj && rwObj.pace_ratio_recent != null) ? `${fmt(rwObj.pace_ratio_recent, 2)}x pace` : null;
          recentSummary = `${rwLabel} ${recentPtsRaw != null ? `${fmt(recentPtsRaw, 0)} pts` : ''}${recentPtsRaw != null && recentPossRaw != null ? ', ' : ''}${recentPossRaw != null ? `${fmt(recentPossRaw, 1)} poss` : ''}${paceTxt ? ` | ${paceTxt}` : ''}`.trim();
        }
      } catch (_) {
        recentSummary = null;
      }

      let adjustSummary = null;
      try {
        const parts = [];
        if (possCtx && typeof possCtx === 'object') {
          if (possCtx.pace_blend_delta != null && Math.abs(possCtx.pace_blend_delta) >= 0.05) parts.push(`poss ${fmtSignedVal(possCtx.pace_blend_delta, 1)}`);
          if (possCtx.recent_window && Math.abs((n(possCtx.recent_window.pace_adj) ?? 0) + (n(possCtx.recent_window.eff_adj) ?? 0)) >= 0.05) {
            parts.push(`recent ${fmtSignedVal((n(possCtx.recent_window.pace_adj) ?? 0) + (n(possCtx.recent_window.eff_adj) ?? 0), 1)}`);
          }
          if (possCtx.interval_drift && possCtx.interval_drift.adj_points != null && Math.abs(possCtx.interval_drift.adj_points) >= 0.05) parts.push(`drift ${fmtSignedVal(possCtx.interval_drift.adj_points, 1)}`);
          if (possCtx.endgame_foul && possCtx.endgame_foul.adj_points != null && Math.abs(possCtx.endgame_foul.adj_points) >= 0.05) parts.push(`foul ${fmtSignedVal(possCtx.endgame_foul.adj_points, 1)}`);
          if (!parts.length && possCtx.pace_alpha != null && possCtx.pace_alpha > 0.01) parts.push(`poss α ${fmt(possCtx.pace_alpha, 2)}`);
        }
        adjustSummary = parts.length ? parts.join(' | ') : 'No adj';
      } catch (_) {
        adjustSummary = null;
      }

      if (outSimAt) outSimAt.textContent = (simAt == null) ? '—' : fmt(simAt, 0);
      if (outDelta) outDelta.textContent = (delta == null) ? '—' : fmt(delta, 1);
      if (outPace) outPace.textContent = (paceFinal == null) ? '—' : fmt(paceFinal, 1);
      if (outProjBet) outProjBet.textContent = (projBet == null) ? '—' : projBet;
      if (outSimFinal) outSimFinal.textContent = (simFinal == null) ? '—' : fmt(simFinal, 1);
      if (outDriver) outDriver.textContent = (driver == null) ? '—' : driver;
      if (outLean) outLean.textContent = (isFinal || lean == null) ? '—' : lean;
      if (outRates) outRates.textContent = (ratesSummary == null) ? '—' : ratesSummary;
      if (outRecent) outRecent.textContent = (recentSummary == null || recentSummary === '') ? '—' : recentSummary;
      if (outAdjust) outAdjust.textContent = (adjustSummary == null) ? '—' : adjustSummary;

      // Persist for polling-driven chips
      try {
        scopeEl.dataset.simAt = (simAt == null) ? '' : String(simAt);
        scopeEl.dataset.simFinal = (simFinal == null) ? '' : String(simFinal);
        scopeEl.dataset.paceFinal = (paceFinal == null) ? '' : String(paceFinal);
        scopeEl.dataset.pacePoints = (possCtx && possCtx.pace_points != null) ? String(possCtx.pace_points) : '';
        scopeEl.dataset.pacePoss = (possCtx && possCtx.pace_poss != null) ? String(possCtx.pace_poss) : '';
        scopeEl.dataset.paceAlpha = (possCtx && possCtx.pace_alpha != null) ? String(possCtx.pace_alpha) : '';
        scopeEl.dataset.paceBlendDelta = (possCtx && possCtx.pace_blend_delta != null) ? String(possCtx.pace_blend_delta) : '';
        scopeEl.dataset.deltaSimMinusAct = (delta == null) ? '' : String(delta);
        scopeEl.dataset.liveTotal = (liveTot == null) ? '' : String(liveTot);
        scopeEl.dataset.betProjection = (paceFinal == null) ? '' : String(paceFinal);

        // Optional logging context (best-effort)
        scopeEl.dataset.possExpectedSoFar = (possCtx && possCtx.poss_expected_so_far != null) ? String(possCtx.poss_expected_so_far) : '';
        scopeEl.dataset.possExpectedFull = (possCtx && possCtx.poss_expected_full != null) ? String(possCtx.poss_expected_full) : '';
        scopeEl.dataset.expPpp = (possCtx && possCtx.exp_ppp != null) ? String(possCtx.exp_ppp) : '';
        scopeEl.dataset.actPpp = (possCtx && possCtx.act_ppp != null) ? String(possCtx.act_ppp) : '';
        scopeEl.dataset.paceRatio = (possCtx && possCtx.pace_ratio != null) ? String(possCtx.pace_ratio) : '';
        scopeEl.dataset.wPace = (possCtx && possCtx.w_pace != null) ? String(possCtx.w_pace) : '';
        // Optional interval drift context (if correction applied)
        scopeEl.dataset.intervalDriftAdj = (possCtx && possCtx.interval_drift && possCtx.interval_drift.adj_points != null) ? String(possCtx.interval_drift.adj_points) : '';
        scopeEl.dataset.intervalDriftSegIdx = (possCtx && possCtx.interval_drift && possCtx.interval_drift.global_seg_idx != null) ? String(possCtx.interval_drift.global_seg_idx) : '';
        scopeEl.dataset.intervalDriftBias = (possCtx && possCtx.interval_drift && possCtx.interval_drift.bias_points != null) ? String(possCtx.interval_drift.bias_points) : '';
        scopeEl.dataset.intervalDriftRemFrac = (possCtx && possCtx.interval_drift && possCtx.interval_drift.rem_frac != null) ? String(possCtx.interval_drift.rem_frac) : '';

        // Optional recent-window context (if applied)
        scopeEl.dataset.recentWindowSec = (possCtx && possCtx.recent_window && possCtx.recent_window.window_sec != null) ? String(possCtx.recent_window.window_sec) : (scopeEl.dataset.recentWindowSec || '');
        scopeEl.dataset.recentWindowPoss = (possCtx && possCtx.recent_window && possCtx.recent_window.poss != null) ? String(possCtx.recent_window.poss) : '';
        scopeEl.dataset.recentWindowPts = (possCtx && possCtx.recent_window && possCtx.recent_window.pts != null) ? String(possCtx.recent_window.pts) : '';
        scopeEl.dataset.recentWindowPaceRatio = (possCtx && possCtx.recent_window && possCtx.recent_window.pace_ratio_recent != null) ? String(possCtx.recent_window.pace_ratio_recent) : '';
        scopeEl.dataset.recentWindowW = (possCtx && possCtx.recent_window && possCtx.recent_window.w_recent != null) ? String(possCtx.recent_window.w_recent) : '';
        scopeEl.dataset.recentWindowPaceAdj = (possCtx && possCtx.recent_window && possCtx.recent_window.pace_adj != null) ? String(possCtx.recent_window.pace_adj) : '';
        scopeEl.dataset.recentWindowEffAdj = (possCtx && possCtx.recent_window && possCtx.recent_window.eff_adj != null) ? String(possCtx.recent_window.eff_adj) : '';

        // Optional endgame foul context (if applied)
        scopeEl.dataset.endgameFoulAdj = (possCtx && possCtx.endgame_foul && possCtx.endgame_foul.adj_points != null) ? String(possCtx.endgame_foul.adj_points) : '';
        scopeEl.dataset.endgameFoulW = (possCtx && possCtx.endgame_foul && possCtx.endgame_foul.w != null) ? String(possCtx.endgame_foul.w) : '';
        scopeEl.dataset.endgameFoulSecLeft = (possCtx && possCtx.endgame_foul && possCtx.endgame_foul.sec_left != null) ? String(possCtx.endgame_foul.sec_left) : '';
        scopeEl.dataset.endgameFoulAbsMargin = (possCtx && possCtx.endgame_foul && possCtx.endgame_foul.abs_margin != null) ? String(possCtx.endgame_foul.abs_margin) : '';
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
  const paceFinal = Math.max(actTot, actTot + (simFinal - simAt));
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

function _liveLensEndgameFoulCfg() {
  try {
    const t = (__liveLensTuning && typeof __liveLensTuning === 'object') ? __liveLensTuning : null;
    const ef = t && t.endgame_foul && typeof t.endgame_foul === 'object' ? t.endgame_foul : null;
    return ef;
  } catch (_) {
    return null;
  }
}

function computeEndgameTotalCorrection(cfg, gameState, diffValue) {
  const out = {
    applies: false,
    sec_left: null,
    abs_margin: null,
    w: 0.0,
    base_adj: 0.0,
    under_adj: 0.0,
    total_adj: 0.0,
  };

  if (!cfg || cfg.enabled === false) return out;

  const periodNow = n(gameState && gameState.period);
  const secLeft = n(gameState && gameState.sec_left_period);
  const marginHome = n(gameState && gameState.margin_home);
  const absMargin = (marginHome != null) ? Math.abs(marginHome) : null;
  out.sec_left = secLeft;
  out.abs_margin = absMargin;

  if (periodNow == null || Math.floor(periodNow) !== 4 || secLeft == null || absMargin == null) return out;

  const minSec = Math.max(1.0, n(cfg.min_sec_left) ?? 180.0);
  const minM = Math.max(0.0, n(cfg.min_abs_margin) ?? 1.0);
  const maxM = Math.max(minM, n(cfg.max_abs_margin) ?? 12.0);
  if (!(secLeft <= minSec) || !(absMargin >= minM) || !(absMargin <= maxM)) return out;

  const wTime = Math.max(0, Math.min(1, (minSec - secLeft) / Math.max(1e-6, minSec)));
  const wClose = (maxM <= minM)
    ? 1.0
    : Math.max(0, Math.min(1, (maxM - absMargin) / Math.max(1e-6, (maxM - minM))));
  const w = Math.max(0, Math.min(1, Math.min(wTime, wClose)));
  out.w = w;
  if (!(w > 0)) return out;

  const fullPts = Math.max(0.0, n(cfg.points_at_full_intensity) ?? 6.0);
  const maxAbs = Math.max(0.0, n(cfg.max_abs_points) ?? 6.0);
  out.base_adj = clampNum(fullPts * w, 0, maxAbs) ?? 0.0;

  const diff = n(diffValue);
  const underFrac = Math.max(0.0, n(cfg.under_edge_reversion_frac) ?? 0.50);
  const underCap = Math.max(0.0, n(cfg.under_edge_reversion_cap_points) ?? 4.0);
  if (diff != null && diff < -0.5 && underFrac > 0 && underCap > 0) {
    out.under_adj = Math.max(0.0, Math.min(underCap, Math.abs(diff) * underFrac * w));
  }

  out.total_adj = (out.base_adj || 0.0) + (out.under_adj || 0.0);
  out.applies = out.total_adj > 1e-9;
  return out;
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

  // Anchor on points already scored; prevent projected final < current points.
  const possRemaining = Math.max(0.0, projPossFull - possLive);
  let possBased = actTot + (possRemaining * projPpp);

  // Guardrails: keep projection close to SmartSim median when very early / noisy.
  if (simFinal != null) {
    const maxDev = 25.0;
    const hi = simFinal + maxDev;
    if (actTot <= hi) {
      possBased = Math.max(simFinal - maxDev, Math.min(hi, possBased));
    }
    possBased = Math.max(actTot, possBased);
  }
  out.pace_poss = possBased;

  // Blend with points-based ladder output to preserve SmartSim pacing prior.
  const alpha = wPace;
  out.pace_alpha = alpha;
  let blended = (1.0 - alpha) * pb + alpha * possBased;

  // Final clamp vs points-based to avoid whipsaw.
  const maxDelta = 15.0;
  blended = Math.max(pb - maxDelta, Math.min(pb + maxDelta, blended));
  blended = Math.max(actTot, blended);
  out.pace_final = blended;
  return out;
}

function classifyDiff(absDiff, watchThresh, betThresh) {
  if (absDiff == null) return 'NONE';
  if (absDiff >= betThresh) return 'BET';
  if (absDiff >= watchThresh) return 'WATCH';
  return 'NONE';
}

function _normKlass(k) {
  const s = String(k || '').toUpperCase().trim();
  return (s === 'BET' || s === 'WATCH' || s === 'NONE') ? s : '';
}

function _playerPropSide(r) {
  try {
    const lean = (r && r.lean != null && String(r.lean).trim()) ? String(r.lean).trim().toUpperCase() : '';
    if (lean === 'OVER' || lean === 'UNDER') return lean;
    const dP = n(r && r.pace_vs_line);
    if (dP != null) return (dP > 0) ? 'OVER' : ((dP < 0) ? 'UNDER' : '');
    const evSide = (r && r.ev_side != null && String(r.ev_side).trim()) ? String(r.ev_side).trim().toUpperCase() : '';
    if (evSide === 'OVER' || evSide === 'UNDER') return evSide;
    return '';
  } catch (_) {
    return '';
  }
}

function adjustPlayerPropSignal(r, strength, thr) {
  // Conservative policy:
  // - Start from base klass derived from pace_vs_line strength.
  // - Downgrade when other live data suggests the edge is fragile (injury, long bench time,
  //   foul trouble, little remaining projected minutes).
  // - Never upgrade beyond base; EV is only a light tie-breaker.
  // - If SmartSim strongly disagrees with the live pace edge (opposite sign), downgrade.
  try {
    const watch = (thr && thr.watch != null) ? Number(thr.watch) : 2.0;
    const bet = (thr && thr.bet != null) ? Number(thr.bet) : 4.0;

    const baseKlass = _normKlass(r && r.klass) || classifyDiff(strength, watch, bet);
    let klass = baseKlass;

    const mp = n(r && r.mp);
    const pf = n(r && r.pf);
    const projMin = n(r && (r.proj_min_final != null ? r.proj_min_final : r.exp_min_eff));
    const remMin = (mp != null && projMin != null) ? (projMin - mp) : null;

    const injuryFlag = !!(r && r.injury_flag);
    const rotOn = (r && r.rot_on_court != null) ? !!r.rot_on_court : null;
    const offSec = n(r && r.rot_cur_off_sec);

    const paceMult = n(r && r.pace_mult);
    const roleMult = n(r && r.role_mult);
    const hotMult = n(r && r.hot_cold_mult);
    const ev = n(r && r.ev);
    const dP = n(r && r.pace_vs_line);
    const dS = n(r && r.sim_vs_line);

    let risk = 0.0;
    if (injuryFlag) risk += 3.0;
    if (pf != null) {
      if (pf >= 5) risk += 2.0;
      else if (pf >= 4) risk += 1.0;
    }
    if (remMin != null) {
      if (remMin < 1.5) risk += 2.0;
      else if (remMin < 3.5) risk += 1.0;
    }
    if (rotOn === false && offSec != null) {
      if (offSec >= 720) risk += 2.0;
      else if (offSec >= 480) risk += 1.0;
    }
    if (mp != null && mp < 3.0) risk += 1.0;

    // SmartSim disagreement penalty: if sim_vs_line and pace_vs_line are opposite sign and both
    // meaningfully large, treat the live edge as fragile/noisy.
    // (This is particularly important early in games where pace extrapolation can be unstable.)
    let simDisagree = false;
    let simAgree = false;
    if (dP != null && dS != null && dP !== 0 && dS !== 0) {
      const opp = (dP * dS) < 0;
      const paceAbs = Math.abs(dP);
      const simAbs = Math.abs(dS);
      simDisagree = opp && (paceAbs >= watch) && (simAbs >= watch);
      simAgree = (!opp) && (paceAbs >= watch) && (simAbs >= watch);

      if (opp) {
        if (paceAbs >= bet && simAbs >= bet) risk += 3.0;
        else if (paceAbs >= bet && simAbs >= watch) risk += 2.0;
        else risk += 1.0;
      }
    }

    // Support signals (kept small): role/pace/hot-cold and EV.
    let support = 0.0;
    if (roleMult != null) {
      if (roleMult >= 1.10) support += 0.8;
      else if (roleMult <= 0.90) support -= 0.4;
    }
    if (paceMult != null) {
      if (paceMult >= 1.07) support += 0.4;
      else if (paceMult <= 0.93) support -= 0.2;
    }
    if (hotMult != null) {
      if (hotMult >= 1.03) support += 0.2;
      else if (hotMult <= 0.97) support -= 0.2;
    }
    // EV is deliberately low priority.
    if (ev != null) {
      if (ev >= 0.03) support += 0.15;
      else if (ev <= -0.03) support -= 0.15;
    }

    // When SmartSim agrees (same sign), allow a small support bump (still cannot upgrade klass).
    if (simAgree) support += 0.35;
    // When SmartSim disagrees (opposite sign), apply a small additional drag.
    if (simDisagree) support -= 0.25;

    // Downgrade rules.
    if (klass === 'BET') {
      if (risk >= 4.0) klass = 'NONE';
      else if (risk >= 2.0) klass = 'WATCH';
      else if (support <= -0.8) klass = 'WATCH';
    } else if (klass === 'WATCH') {
      if (risk >= 3.0) klass = 'NONE';
      else if (support <= -1.2) klass = 'NONE';
    }

    const side = _playerPropSide(r);
    const rank = (klass === 'BET') ? 2 : ((klass === 'WATCH') ? 1 : 0);
    const score = (Number.isFinite(Number(strength)) ? Number(strength) : 0)
      + 0.25 * support
      - 0.35 * risk;

    return {
      klass,
      side,
      rank,
      score,
      risk,
      support,
      sim_disagree: simDisagree,
      sim_agree: simAgree,
    };
  } catch (_) {
    const side = _playerPropSide(r);
    const k0 = _normKlass(r && r.klass) || 'NONE';
    return { klass: k0, side, rank: (k0 === 'BET') ? 2 : ((k0 === 'WATCH') ? 1 : 0), score: 0, risk: null, support: null };
  }
}

function livePropPrimaryPrice(r, side) {
  try {
    const sd = String(side || '').toUpperCase().trim();
    if (sd === 'OVER') return n(r && r.price_over);
    if (sd === 'UNDER') return n(r && r.price_under);
  } catch (_) {
    // ignore
  }
  return null;
}

function fmtAgeShort(sec) {
  const v = n(sec);
  if (v == null) return '';
  if (v < 90) return `${Math.round(v)}s old`;
  if (v < 5400) return `${Math.round(v / 60)}m old`;
  return `${Math.round(v / 3600)}h old`;
}

function livePropLineContextText(r) {
  try {
    const bits = [];
    const liveLine = n(r && r.line_live);
    const preLine = n(r && r.line_pregame);
    if (liveLine != null && preLine != null) bits.push(`Live ${fmt(liveLine, 1)} vs pre ${fmt(preLine, 1)} (${fmtSigned(liveLine - preLine, 1)})`);
    else if (liveLine != null) bits.push(`Live ${fmt(liveLine, 1)} line`);
    else if (preLine != null) bits.push(`Pregame ${fmt(preLine, 1)} fallback`);

    const ageTxt = fmtAgeShort(r && r.line_live_age_sec);
    if (ageTxt) bits.push(ageTxt);

    const lineN = n(r && r.line_live_n);
    if (lineN != null) bits.push(`${Math.max(1, Math.round(lineN))} ${lineN >= 2 ? 'books' : 'book'}`);

    const span = n(r && r.line_live_span);
    if (span != null && span >= 1.0) bits.push(`span ${fmt(span, 1)}`);

    return bits.join(' · ');
  } catch (_) {
    return '';
  }
}

function buildLivePropGuidance(r, adj, thr) {
  const safeAdj = (adj && typeof adj === 'object') ? adj : {};
  const side = String((safeAdj && safeAdj.side) || _playerPropSide(r) || '').toUpperCase().trim();
  const klass = String((safeAdj && safeAdj.klass) || (r && r.klass) || '').toUpperCase().trim();
  const stat = String((r && r.stat) || '').toLowerCase().trim();
  const line = n(r && r.line);
  const liveLine = n(r && r.line_live);
  const preLine = n(r && r.line_pregame);
  const paceProj = n(r && r.pace_proj);
  const simMu = n(r && r.sim_mu);
  const dP = n(r && r.pace_vs_line);
  const dS = n(r && r.sim_vs_line);
  const lineSource = String((r && r.line_source) || '').toLowerCase().trim();
  const hasLiveLine = lineSource === 'oddsapi' && liveLine != null;
  const ageSec = n(r && r.line_live_age_sec);
  const lineSpan = n(r && r.line_live_span);
  const lineN = n(r && r.line_live_n);
  const bettableScore = n(r && r.bettable_score);
  const bettable = (r && r.bettable != null) ? !!r.bettable : null;
  const injuryFlag = !!(r && r.injury_flag);
  const edgeSigma = n(r && r.edge_sigma);
  const price = livePropPrimaryPrice(r, side);
  const watchThresh = (thr && thr.watch != null) ? Number(thr.watch) : 2.0;

  let fairLine = null;
  if (paceProj != null && simMu != null) {
    const sameSign = (dP != null && dS != null && dP !== 0 && dS !== 0 && (dP * dS) > 0);
    const oppSign = (dP != null && dS != null && dP !== 0 && dS !== 0 && (dP * dS) < 0);
    if (sameSign) fairLine = (0.75 * paceProj) + (0.25 * simMu);
    else if (oppSign) fairLine = (0.60 * paceProj) + (0.40 * simMu);
    else fairLine = (0.70 * paceProj) + (0.30 * simMu);
  } else if (paceProj != null) {
    fairLine = paceProj;
  } else if (simMu != null) {
    fairLine = simMu;
  }

  let haircut = ({ pts: 0.9, pra: 0.9, reb: 0.7, ast: 0.7, threes: 0.55, stl: 0.45, blk: 0.45, tov: 0.5 }[stat] || 0.65);
  try {
    const risk = n(safeAdj && safeAdj.risk);
    const support = n(safeAdj && safeAdj.support);
    if (risk != null) haircut += Math.max(0, risk) * 0.15;
    if (support != null && support < 0) haircut += 0.10;
    else if (support != null && support > 0.75) haircut -= 0.10;
    if (safeAdj && safeAdj.sim_disagree) haircut += 0.25;
    else if (safeAdj && safeAdj.sim_agree) haircut -= 0.05;
  } catch (_) {
    // ignore
  }
  if (!hasLiveLine) haircut += 0.15;
  if (bettable === false) haircut += 0.15;
  if (hasLiveLine && ageSec == null) haircut += 0.05;
  if (ageSec != null && ageSec > 600) haircut += 0.15;
  if (lineSpan != null && lineSpan >= 1.0) haircut += 0.10;
  if (lineN != null && lineN <= 1) haircut += 0.10;
  if (injuryFlag) haircut += 0.20;
  haircut = clampNum(haircut, 0.35, 2.0);

  let playToLine = null;
  let withinPlayTo = true;
  let edgeCushion = null;
  try {
    if (fairLine != null && line != null && side) {
      edgeCushion = (side === 'OVER') ? (fairLine - line) : (line - fairLine);
      if (edgeCushion > (haircut || 0)) {
        const target = (side === 'OVER') ? (fairLine - haircut) : (fairLine + haircut);
        if (side === 'OVER') {
          playToLine = Math.floor(target * 2.0) / 2.0;
          withinPlayTo = line <= (playToLine + 1e-9);
        } else if (side === 'UNDER') {
          playToLine = Math.ceil(target * 2.0) / 2.0;
          withinPlayTo = line >= (playToLine - 1e-9);
        }
      } else {
        withinPlayTo = false;
      }
    }
  } catch (_) {
    playToLine = null;
    withinPlayTo = true;
    edgeCushion = null;
  }

  const edgeSurplus = (edgeCushion != null && haircut != null) ? (edgeCushion - haircut) : null;
  const marketBuffer = (lineSpan != null) ? (lineSpan / 2.0) : 0.0;
  const strongEdge = (edgeSurplus != null) && (edgeSurplus >= 0.55);
  const enoughBuffer = (edgeSurplus != null) && (edgeSurplus >= Math.max(0.35, 0.15 * marketBuffer));

  let freshnessScore = 0.0;
  if (hasLiveLine) {
    if (ageSec == null) freshnessScore = 0.30;
    else if (ageSec <= 30) freshnessScore = 1.00;
    else if (ageSec <= 90) freshnessScore = 0.95;
    else if (ageSec <= 180) freshnessScore = 0.85;
    else if (ageSec <= 300) freshnessScore = 0.70;
    else if (ageSec <= 600) freshnessScore = 0.55;
    else freshnessScore = 0.25;
  }

  const supportive = !!(safeAdj && safeAdj.sim_agree)
    && ((n(safeAdj && safeAdj.support) == null) || (n(safeAdj && safeAdj.support) >= 0.15))
    && (edgeSigma == null || edgeSigma >= 0.65);
  const qualityReady = hasLiveLine
    && freshnessScore >= 0.70
    && (bettable !== false)
    && (bettableScore == null || bettableScore >= 0.72)
    && (lineN == null || lineN >= 3)
    && ((n(safeAdj && safeAdj.risk) == null) || (n(safeAdj && safeAdj.risk) < 2));

  const freshEnough = !hasLiveLine || ageSec == null || ageSec <= 600;
  const trusted = (bettable !== false) && (bettableScore == null || bettableScore >= 0.65)
    && !injuryFlag && !(safeAdj && safeAdj.sim_disagree)
    && ((n(safeAdj && safeAdj.risk) == null) || (n(safeAdj && safeAdj.risk) < 3));

  let action = 'Shop';
  if (!side || line == null || fairLine == null) {
    action = 'Pass';
  } else if (!withinPlayTo) {
    action = 'Pass';
  } else if (klass === 'BET') {
    if (qualityReady && strongEdge && enoughBuffer && supportive) action = 'Bet now';
    else if (hasLiveLine && (bettableScore == null || bettableScore >= 0.45)) action = 'Shop';
    else action = 'Wait';
  } else if (klass === 'WATCH') {
    if (!hasLiveLine && preLine != null) action = 'Wait';
    else if (hasLiveLine && (bettableScore == null || bettableScore >= 0.55) && !injuryFlag && !(safeAdj && safeAdj.sim_disagree)) action = 'Shop';
    else action = 'Wait';
  } else if (!hasLiveLine && preLine != null && Math.abs(dP || 0) >= (watchThresh * 0.8)) {
    action = 'Wait';
  } else {
    action = 'Pass';
  }

  if (bettableScore != null && bettableScore < 0.35) action = withinPlayTo ? 'Pass' : action;
  if (injuryFlag && (n(safeAdj && safeAdj.risk) != null) && n(safeAdj && safeAdj.risk) >= 3) action = 'Pass';

  let summary = 'Live edge is present, but line shopping still matters.';
  if (action === 'Bet now') {
    if (lineSpan != null && lineSpan >= 4.0) summary = 'Edge is strong enough to clear the current live market spread.';
    else if (hasLiveLine && freshEnough) summary = 'Fresh live line and playable edge.';
    else summary = 'Live edge is still inside the playable range.';
  } else if (action === 'Shop') {
    if (!hasLiveLine && preLine != null) summary = 'Signal exists, but this row is still anchored to the pregame line.';
    else if (strongEdge && !qualityReady) summary = 'Edge is real, but the live market still needs cleaner confirmation.';
    else if (strongEdge && !enoughBuffer) summary = 'Edge is there, but not by enough to clear market dispersion yet.';
    else if (lineSpan != null && lineSpan >= 1.0) summary = 'Live market is playable, but books are spread out.';
    else summary = 'Edge is live, but market quality is mixed.';
  } else if (action === 'Wait') {
    if (!hasLiveLine && preLine != null) summary = 'Wait for a real live line before acting.';
    else if (safeAdj && safeAdj.sim_disagree) summary = 'Wait for live pace and SmartSim to point the same way.';
    else summary = 'Need a cleaner live number before acting.';
  } else if (!withinPlayTo) {
    summary = 'Current line has moved past the playable range.';
  } else if (injuryFlag) {
    summary = 'Minutes risk is too high for a live bet.';
  } else if ((n(safeAdj && safeAdj.risk) != null) && n(safeAdj && safeAdj.risk) >= 3) {
    summary = 'Risk is too high for a live bet right now.';
  } else {
    summary = 'Edge is too fragile right now.';
  }

  const tags = [];
  tags.push(hasLiveLine ? 'LIVE line' : (preLine != null ? 'PRE fallback' : 'No line'));
  if (bettable === true) tags.push('Bettable');
  if (safeAdj && safeAdj.sim_agree) tags.push('SIM agrees');
  else if (safeAdj && safeAdj.sim_disagree) tags.push('SIM disagrees');
  if (edgeSurplus != null && edgeSurplus >= 0.55) tags.push('Actionable edge');
  if ((n(safeAdj && safeAdj.support) != null) && (n(safeAdj && safeAdj.support) >= 0.35)) tags.push('Role support');
  if (injuryFlag) tags.push('INJ risk');
  if (ageSec != null && ageSec <= 180) tags.push('Fresh line');
  else if (ageSec != null && ageSec > 600) tags.push('Stale line');
  if (lineN != null && lineN <= 1) tags.push('1 book');
  else if (lineN != null && lineN >= 2) tags.push(`${Math.round(lineN)} books`);
  if (lineSpan != null && lineSpan >= 1.0) tags.push('Wide market');
  if (bettableScore != null && bettableScore < 0.50) tags.push('Thin quality');

  return normalizePregameGuidance({
    action,
    action_code: action.toLowerCase().replace(/\s+/g, '_'),
    summary,
    tags,
    play_to_line: playToLine,
    within_play_to: !!withinPlayTo,
    edge_cushion: edgeCushion,
    market_text: livePropLineContextText(r),
    price_text: (price != null) ? fmtAmer(price) : '',
  });
}

function clampNum(x, lo, hi) {
  const v = n(x);
  if (v == null) return null;
  return Math.max(lo, Math.min(hi, v));
}

function _liveLensEdgeShrink(rawDiff, possLive, elapsedMin, totalMinutes) {
  // Confidence shrinkage for edges early in a scope.
  // Returns lambda in [0,1] where 0 => fully shrunk to 0.
  const rd = n(rawDiff);
  if (rd == null) return { diff_shrunk: rawDiff, lambda: null, lambda_poss: null, lambda_time: null };

  const tm = Number(totalMinutes);
  const cfg = _liveLensScopeTotalAdjCfg(tm);
  let shrinkCfg = null;
  try {
    shrinkCfg = cfg && cfg.edge_shrink && typeof cfg.edge_shrink === 'object' ? cfg.edge_shrink : null;
  } catch (_) {
    shrinkCfg = null;
  }

  if (shrinkCfg && shrinkCfg.enabled === false) {
    return { diff_shrunk: rd, lambda: 1.0, lambda_poss: 1.0, lambda_time: 1.0 };
  }

  const scale = Math.max(0.15, (tm && Number.isFinite(tm) && tm > 0) ? (tm / 48.0) : 1.0);
  const possMin = n(shrinkCfg && shrinkCfg.poss_min) ?? (10.0 * scale);
  const possRange = n(shrinkCfg && shrinkCfg.poss_range) ?? (25.0 * scale);
  const timeMin = n(shrinkCfg && shrinkCfg.time_min) ?? (6.0 * scale);
  const timeRange = n(shrinkCfg && shrinkCfg.time_range) ?? (18.0 * scale);

  const p = n(possLive);
  const t = n(elapsedMin);
  const wPoss = (p == null) ? 1.0 : Math.max(0, Math.min(1, (p - possMin) / Math.max(1e-6, possRange)));
  const wTime = (t == null) ? 1.0 : Math.max(0, Math.min(1, (t - timeMin) / Math.max(1e-6, timeRange)));
  const lambda = Math.max(0, Math.min(1, Math.min(wPoss, wTime)));
  return { diff_shrunk: rd * lambda, lambda, lambda_poss: wPoss, lambda_time: wTime };
}

function adjustGameTotalDiffWithContext(rawDiff, lineTotal, meta, live, curMinLeft, gameState) {
  const out = {
    diff_adj: rawDiff,
    diff_raw: rawDiff,
    pace_ratio: null,
    eff_ppp_delta: null,
    poss_live: null,
    poss_expected: null,
    elapsed_min: null,
    under_edge_factor: null,
    under_edge_reversion_points: 0.0,
    endgame_foul_adj: 0.0,
    endgame_foul_base_adj: 0.0,
    endgame_foul_under_adj: 0.0,
    endgame_foul_w: 0.0,
    endgame_foul_sec_left: null,
    endgame_foul_abs_margin: null,
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

  const underEdgeFactorRaw = n(adjCfg && adjCfg.under_edge_factor);
  const underEdgeFactor = (underEdgeFactorRaw != null && underEdgeFactorRaw > 0 && underEdgeFactorRaw < 1.0)
    ? underEdgeFactorRaw
    : 1.0;
  if (adj < -0.5 && underEdgeFactor < 0.999) {
    const adjBefore = adj;
    adj = adj * underEdgeFactor;
    out.under_edge_factor = underEdgeFactor;
    out.under_edge_reversion_points = adj - adjBefore;
  }

  try {
    const efCfg = _liveLensEndgameFoulCfg();
    const foulCtx = computeEndgameTotalCorrection(efCfg, gameState, adj);
    out.endgame_foul_w = foulCtx.w;
    out.endgame_foul_sec_left = foulCtx.sec_left;
    out.endgame_foul_abs_margin = foulCtx.abs_margin;
    out.endgame_foul_base_adj = foulCtx.base_adj;
    out.endgame_foul_under_adj = foulCtx.under_adj;
    out.endgame_foul_adj = foulCtx.total_adj;
    if (foulCtx.applies) adj = adj + foulCtx.total_adj;
  } catch (_) {
    // ignore
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
const __liveLensLastProjLogged = new Map();
let __liveLensTuning = null;
let __liveLensTuningAt = 0;
const LIVE_PROPS_POLL_INTERVAL_MS = 20 * 1000;
const LIVE_PROPS_ENDPOINT_TTL_SEC = 20;

// UI filters (client-side only)
const __pregameGamePillsSelected = new Set(); // game_id (canonGameId), empty => all
const __pregamePropFilters = { stats: new Set(), sides: new Set() }; // empty => all
const __gamePillsSelected = new Set(); // live game_id (canonGameId), empty => all
const __playerLensGlobalFilters = { stats: new Set(), signals: new Set(), sides: new Set(), liveLineOnly: false }; // empty => all

function chipSetActive(el, active) {
  try {
    if (!el || !el.classList) return;
    el.classList.remove('neutral', 'positive');
    el.classList.add(active ? 'positive' : 'neutral');
    try { el.setAttribute('aria-pressed', active ? 'true' : 'false'); } catch (_) { /* ignore */ }
  } catch (_) {
    // ignore
  }
}

function applyPregamePropFilters(root) {
  try {
    if (!root) return;
    const gameSet = __pregameGamePillsSelected;
    const statSet = __pregamePropFilters && __pregamePropFilters.stats ? __pregamePropFilters.stats : new Set();
    const sideSet = __pregamePropFilters && __pregamePropFilters.sides ? __pregamePropFilters.sides : new Set();

    const anyGame = !!(gameSet && gameSet.size);
    const statsAny = !!(statSet && statSet.size);
    const sideAny = !!(sideSet && sideSet.size);

    const allGameBtn = root.querySelector('#pregame-game-filter-pills button[data-pregame-game-pill-all]');
    chipSetActive(allGameBtn, !anyGame);
    const gameBtns = root.querySelectorAll('#pregame-game-filter-pills button.pregame-game-pill[data-game-id]');
    gameBtns.forEach((btn) => {
      const gid = canonGameId(btn.dataset.gameId);
      chipSetActive(btn, anyGame ? gameSet.has(gid) : false);
    });

    const allPropBtn = root.querySelector('#pregame-player-prop-filter-pills button[data-pregame-player-prop-pill-all]');
    chipSetActive(allPropBtn, !(statsAny || sideAny));
    const statBtns = root.querySelectorAll('#pregame-player-prop-filter-pills button.pregame-player-prop-pill[data-kind="stat"][data-key]');
    statBtns.forEach((btn) => {
      const key = String(btn.dataset.key || '').toLowerCase().trim();
      chipSetActive(btn, statsAny ? statSet.has(key) : false);
    });
    const sideBtns = root.querySelectorAll('#pregame-player-prop-filter-pills button.pregame-player-prop-pill[data-kind="side"][data-key]');
    sideBtns.forEach((btn) => {
      const key = String(btn.dataset.key || '').toUpperCase().trim();
      chipSetActive(btn, sideAny ? sideSet.has(key) : false);
    });

    const items = root.querySelectorAll('#pregame-prop-callouts button.pregame-prop-callout[data-game-id][data-stat]');
    items.forEach((btn) => {
      const gid = canonGameId(btn.dataset.gameId);
      const stat = String(btn.dataset.stat || '').toLowerCase().trim();
      const side = String(btn.dataset.side || '').toUpperCase().trim();
      const okGame = anyGame ? gameSet.has(gid) : true;
      const okStat = statsAny ? statSet.has(stat) : true;
      const okSide = sideAny ? sideSet.has(side) : true;
      btn.classList.toggle('hidden', !(okGame && okStat && okSide));
    });
  } catch (_) {
    // ignore
  }
}

function applyGamePillsFilter(root) {
  try {
    if (!root) return;
    const activeSet = __gamePillsSelected;
    const any = !!(activeSet && activeSet.size);

    const allBtn = root.querySelector('#live-game-filter-pills button[data-live-game-pill-all]');
    chipSetActive(allBtn, !any);

    const pills = root.querySelectorAll('#live-game-filter-pills button.live-game-pill[data-game-id]');
    pills.forEach((btn) => {
      const gid = canonGameId(btn.dataset.gameId);
      const on = any ? activeSet.has(gid) : false;
      chipSetActive(btn, on);
    });

    // Filter player props (not game cards):
    // 1) Live prop callouts list
    const callouts = root.querySelectorAll('#live-prop-callouts button.live-prop-callout[data-game-id]');
    callouts.forEach((btn) => {
      const gid = canonGameId(btn.dataset.gameId);
      const okGame = any ? activeSet.has(gid) : true;
      btn.classList.toggle('hidden', !okGame);
    });

    // Re-apply stat/signal filters after game-level filtering.
    applyPlayerLensFiltersAll(root);
  } catch (_) {
    // ignore
  }
}

function applyPlayerLensGlobalPills(root) {
  try {
    if (!root) return;
    const statsSel = __playerLensGlobalFilters && __playerLensGlobalFilters.stats ? __playerLensGlobalFilters.stats : new Set();
    const sigSel = __playerLensGlobalFilters && __playerLensGlobalFilters.signals ? __playerLensGlobalFilters.signals : new Set();
    const sideSel = __playerLensGlobalFilters && __playerLensGlobalFilters.sides ? __playerLensGlobalFilters.sides : new Set();
    const liveLineOnly = !!(__playerLensGlobalFilters && __playerLensGlobalFilters.liveLineOnly);
    const statsAny = !!(statsSel && statsSel.size);
    const sigAny = !!(sigSel && sigSel.size);
    const sideAny = !!(sideSel && sideSel.size);

    const allBtn = root.querySelector('#live-player-prop-filter-pills button[data-live-player-prop-pill-all]');
    chipSetActive(allBtn, !(statsAny || sigAny || sideAny || liveLineOnly));

    const statBtns = root.querySelectorAll('#live-player-prop-filter-pills button.live-player-prop-pill[data-kind="stat"][data-key]');
    statBtns.forEach((b) => {
      const key = String(b.dataset.key || '').toLowerCase().trim();
      chipSetActive(b, statsAny ? statsSel.has(key) : false);
    });
    const sideBtns = root.querySelectorAll('#live-player-prop-filter-pills button.live-player-prop-pill[data-kind="side"][data-key]');
    sideBtns.forEach((b) => {
      const key = String(b.dataset.key || '').toUpperCase().trim();
      chipSetActive(b, sideAny ? sideSel.has(key) : false);
    });
    const sigBtns = root.querySelectorAll('#live-player-prop-filter-pills button.live-player-prop-pill[data-kind="sig"][data-key]');
    sigBtns.forEach((b) => {
      const key = String(b.dataset.key || '').toUpperCase().trim();
      chipSetActive(b, sigAny ? sigSel.has(key) : false);
    });

    const lineBtns = root.querySelectorAll('#live-player-prop-filter-pills button.live-player-prop-pill[data-kind="line"][data-key]');
    lineBtns.forEach((b) => {
      chipSetActive(b, liveLineOnly);
    });
  } catch (_) {
    // ignore
  }
}
function applyPlayerPropCalloutsFilter(root) {
  try {
    if (!root) return;
    const statsSel = __playerLensGlobalFilters && __playerLensGlobalFilters.stats ? __playerLensGlobalFilters.stats : new Set();
    const sigSel = __playerLensGlobalFilters && __playerLensGlobalFilters.signals ? __playerLensGlobalFilters.signals : new Set();
    const sideSel = __playerLensGlobalFilters && __playerLensGlobalFilters.sides ? __playerLensGlobalFilters.sides : new Set();
    const liveLineOnly = !!(__playerLensGlobalFilters && __playerLensGlobalFilters.liveLineOnly);
    const statsAny = !!(statsSel && statsSel.size);
    const sigAny = !!(sigSel && sigSel.size);
    const sideAny = !!(sideSel && sideSel.size);

    const activeSet = __gamePillsSelected;
    const anyGame = !!(activeSet && activeSet.size);

    const items = root.querySelectorAll('#live-prop-callouts button.live-prop-callout[data-stat][data-sig]');
    items.forEach((btn) => {
      const stat = String(btn.dataset.stat || '').toLowerCase().trim();
      const sig = String(btn.dataset.sig || '').toUpperCase().trim();
      const side = String(btn.dataset.side || '').toUpperCase().trim();
      const okStat = statsAny ? statsSel.has(stat) : true;
      const okSig = sigAny ? sigSel.has(sig) : true;
      const okSide = sideAny ? sideSel.has(side) : true;

      const hasLiveLine = String(btn.dataset.liveLine || '').trim() === '1';
      const okLiveLine = liveLineOnly ? hasLiveLine : true;

      const gid = canonGameId(btn.dataset.gameId);
      const okGame = anyGame ? activeSet.has(gid) : true;

      btn.classList.toggle('hidden', !(okGame && okStat && okSig && okSide && okLiveLine));
    });
  } catch (_) {
    // ignore
  }
}

function applyPlayerLensFiltersAll(root) {
  try {
    if (!root) return;
    applyPlayerLensGlobalPills(root);
    applyPlayerPropCalloutsFilter(root);
  } catch (_) {
    // ignore
  }
}

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
  let t = String(s || '');
  try {
    t = t.normalize('NFKD').replace(/[\u0300-\u036f]/g, '');
  } catch (_) {
    // ignore browsers without unicode normalization support
  }
  return t
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
  const stl = n(p && (p.stl != null ? p.stl : p.steals));
  const blk = n(p && (p.blk != null ? p.blk : p.blocks));
  const tov = n(p && (p.tov != null ? p.tov : (p.turnovers != null ? p.turnovers : p.to)));
  if (mk === 'pts' || mk === 'points') return pts;
  if (mk === 'reb' || mk === 'rebounds') return reb;
  if (mk === 'ast' || mk === 'assists') return ast;
  if (mk === 'threes' || mk === '3pm' || mk === '3pt' || mk === 'threes_made') return threes;
  if (mk === 'stl' || mk === 'steals' || mk === 'steal') return stl;
  if (mk === 'blk' || mk === 'blocks' || mk === 'block') return blk;
  if (mk === 'tov' || mk === 'turnovers' || mk === 'turnover' || mk === 'to') return tov;
  if (mk === 'pra') return (pts != null && reb != null && ast != null) ? (pts + reb + ast) : null;
  if (mk === 'pa') return (pts != null && ast != null) ? (pts + ast) : null;
  if (mk === 'pr') return (pts != null && reb != null) ? (pts + reb) : null;
  if (mk === 'ra') return (reb != null && ast != null) ? (reb + ast) : null;
  return null;
}

function renderLivePropCallouts(callouts) {
  try {
    const arr0 = Array.isArray(callouts) ? callouts : [];
    const arr = arr0.map((x) => {
      const guidance = normalizePregameGuidance((x && x.guidance) || {
        action: x && x.action,
        action_code: x && x.action_code,
        summary: x && x.summary,
        tags: x && x.tags,
        play_to_line: x && x.play_to_line,
        market_text: x && x.market_text,
        price_text: x && x.price_text,
      });
      return { ...x, guidance };
    }).filter((x) => x && (x.klass === 'BET' || x.klass === 'WATCH') && x.gid && (!x.guidance || x.guidance.action_code !== 'pass'));
    if (!arr.length) return '';

    const items = arr.map((x) => {
      const klass = String(x.klass || '').toUpperCase().trim();
      const side = String(x.side || '').toUpperCase().trim();
      const badge = renderLivePropSignalBadge(klass, side);
      const guidance = x && x.guidance ? x.guidance : null;
      const actionBadge = renderPregameActionBadge(guidance);
      const liveLineBadge = renderLivePropLineBadge((x && x.is_live_line) ? 'LIVE' : 'PRE');

      const game = `${String(x.away || '').toUpperCase().trim()} @ ${String(x.home || '').toUpperCase().trim()}`.trim();
      const stat = marketLabel(String(x.stat || '').toLowerCase().trim());
      const player = String(x.player || '').trim();
      const pid = n(x.player_id);
      const img = renderPlayerHeadshot(player, {
        playerPhoto: x.player_photo,
        playerId: pid,
      });
      const preLine = n(x.line_pregame);
      const liveLine = n(x.line_live);
      const lineUsed = n(x.line);
      const act = n(x.actual);
      const proj = n(x.pace_proj);
      const dp = n(x.dP);
      const ds = n(x.dS);
      const playToLine = n(guidance && guidance.play_to_line);
      const priceTxt = (guidance && guidance.price_text) ? String(guidance.price_text) : '';

      const lineTxt = (lineUsed == null) ? '—' : fmt(lineUsed, 1);
      const preTxt = (preLine == null) ? '—' : fmt(preLine, 1);
      const liveTxt = (liveLine == null) ? '—' : fmt(liveLine, 1);
      const actTxt = (act == null) ? '—' : fmt(act, 1);
      const projTxt = (proj == null) ? '—' : fmt(proj, 1);
      const dpTxt = (dp == null) ? '—' : fmt(dp, 1);
      const dsTxt = (ds == null) ? '—' : fmt(ds, 1);
      const playToTxt = (playToLine == null || !side)
        ? ''
        : `${side === 'OVER' ? 'Over' : 'Under'} to ${fmt(playToLine, 1)}${priceTxt ? ` @ ${priceTxt}` : ''}`;
      const guideTags = renderLivePropGuideTags(
        guidance && Array.isArray(guidance.tags)
          ? guidance.tags.filter((tag) => !['LIVE line', 'PRE fallback', 'No line', 'SIM agrees', 'SIM disagrees'].includes(String(tag || '').trim()))
          : [],
        3,
      );

      const why = (() => {
        try {
          const tags = [];
          if (x.injury_flag) tags.push(renderLivePropWhyBadge('INJ', 'bad'));
          const pf = n(x.pf);
          if (pf != null && pf >= 5) tags.push(renderLivePropWhyBadge('F5+', 'bad'));
          else if (pf != null && pf >= 4) tags.push(renderLivePropWhyBadge('F4', 'ok'));
          const rem = n(x.rem_min);
          if (rem != null && rem < 1.5) tags.push(renderLivePropWhyBadge('LOWMIN', 'bad'));
          else if (rem != null && rem < 3.5) tags.push(renderLivePropWhyBadge('LOWMIN', 'ok'));
          if (x.bench_long) tags.push(renderLivePropWhyBadge('BENCH', 'ok'));
          if (!tags.length) return '';
          return tags.join('');
        } catch (_) {
          return '';
        }
      })();

      const simFlag = renderLivePropSimBadge(x.simAgree, x.simDisagree);

      return `
        <button
          type="button"
          class="chip neutral prop-callout live-prop-callout"
          data-game-id="${esc(String(x.gid))}"
          data-stat="${esc(String(x.stat || '').toLowerCase().trim())}"
          data-sig="${esc(String(klass || '').toUpperCase().trim())}"
          data-side="${esc(String(side || '').toUpperCase().trim())}"
          data-live-line="${x && x.is_live_line ? '1' : '0'}"
          aria-label="Jump to ${esc(player)} ${esc(stat)} ${esc(klass)}"
        >
          <div class="prop-callout-head">
            ${actionBadge}
            ${badge}
            ${simFlag}
            ${liveLineBadge}
            <span class="badge">${esc(String(x.team || ''))}</span>
            ${why}
          </div>
          <div class="prop-callout-body">
            ${img}
            <div class="prop-callout-copy">
              <div class="fw-700 prop-callout-title">
                ${esc(player)} <span class="subtle">${esc(stat)}</span> <span class="subtle">L${esc(lineTxt)}</span>
              </div>
              <div class="prop-callout-line live-prop-guide-copy">
                ${esc((guidance && guidance.summary) || 'Live edge is present.')}
              </div>
              ${playToTxt ? `<div class="subtle prop-callout-line">${esc(playToTxt)}</div>` : ''}
              <div class="subtle prop-callout-line">
                ${esc(game)} · Act ${esc(actTxt)} · Proj ${esc(projTxt)}
              </div>
              <div class="subtle prop-callout-line">
                ${esc((guidance && guidance.market_text) || `Pre ${preTxt} · Live ${liveTxt}`)} · ΔP ${esc(dpTxt)} · ΔS ${esc(dsTxt)}
              </div>
              ${guideTags ? `<div class="live-prop-guide-tags">${guideTags}</div>` : ''}
            </div>
          </div>
        </button>
      `;
    }).join('');

    const liveGuideLegend = renderLivePropGuideLegend();
    const liveRiskLegend = renderLivePropRiskLegend();

    return `
      <div class="subtle" style="margin-top:8px;">Live props signal + action guide — click to jump:</div>
      <div class="row chips prop-callouts-rail">
        ${items}
      </div>
      <div class="subtle" style="margin-top:6px;">
        <span class="fw-700">Notes:</span>
        ${liveGuideLegend}
        ${liveRiskLegend}
      </div>
    `;
  } catch (_) {
    return '';
  }
}

function renderPregamePropCallouts(callouts) {
  try {
    const arr0 = Array.isArray(callouts) ? callouts : [];
    const arr = arr0.filter((x) => x && x.gid && x.player && x.stat);
    if (!arr.length) return '';

    const items = arr.map((x) => {
      const guidance = buildPregameGuidance(x, {
        modelLine: x.model_line,
        booksCount: x.books_count,
        consensus: x.consensus,
        lineAdv: x.line_adv,
        reasons: x.reasons,
      });
      const tier = String(x.tier || '').trim();
      const tierU = tier.toUpperCase();
      const tierShort = (tierU === 'MEDIUM') ? 'MED' : tierU;
      const tierCls = (tierU === 'HIGH') ? 'good' : ((tierU === 'MEDIUM') ? 'ok' : '');

      const side = String(x.side || '').toUpperCase().trim();
      const sideShort = (side === 'OVER') ? 'O' : ((side === 'UNDER') ? 'U' : '');
      const sideWord = (side === 'OVER') ? 'over' : ((side === 'UNDER') ? 'under' : '');
      const tierWord = (tierU === 'HIGH') ? 'fast movement' : ((tierU === 'MEDIUM') ? 'meaningful movement' : 'movement');
      const headTxt = `${tierShort} ${sideShort}`.trim();
      const badgeTitle = [tierU || 'Movement', sideWord ? `${sideWord} look` : '', tierWord].filter(Boolean).join(' • ');
      const badge = headTxt ? `<span class="badge ${tierCls}" title="${esc(badgeTitle)}">${esc(headTxt)}</span>` : '';
      const actionBadge = renderPregameActionBadge(guidance);
      const marketBadge = renderPregameMarketBadge(guidance);

      const game = `${String(x.away || '').toUpperCase().trim()} @ ${String(x.home || '').toUpperCase().trim()}`.trim();
      const stat = marketLabel(String(x.stat || '').toLowerCase().trim());
      const player = String(x.player || '').trim();
      const team = String(x.team || '').toUpperCase().trim();

      const img = renderPlayerHeadshot(player, {
        playerPhoto: x.player_photo,
        photo: x.photo,
        playerId: x.player_id,
      });

      const openLine = n(x.open_line);
      const curLine = n(x.line);
      let lineMove = n(x.line_move);
      if (lineMove == null && openLine != null && curLine != null) lineMove = curLine - openLine;
      const impliedMove = n(x.implied_move);

      const openPrice = n(x.open_price);
      const curPrice = n(x.price);

      const openLineTxt = (openLine == null) ? '—' : fmt(openLine, 1);
      const curLineTxt = (curLine == null) ? '—' : fmt(curLine, 1);
      const curPriceTxt = (curPrice == null) ? '—' : fmtAmer(curPrice);

      const evp = n(x.ev_pct);
      const evTxt = (evp == null) ? '' : `EV ${evp.toFixed(1)}%`;
      const playToLine = n(guidance && guidance.play_to_line);
      const playToText = (playToLine == null) ? '' : `Play to ${fmt(playToLine, 1)}`;
      const moveText = pregamePropMoveText(x);
      const tagText = (guidance && Array.isArray(guidance.tags))
        ? guidance.tags.filter((tag) => !['Market agrees', 'Market disagrees', 'Better than open', 'Worse than open'].includes(String(tag || '').trim())).slice(0, 2).join(' · ')
        : '';

      return `
        <button
          type="button"
          class="chip neutral prop-callout pregame-prop-callout"
          data-game-id="${esc(String(x.gid))}"
          data-stat="${esc(String(x.stat || '').toLowerCase().trim())}"
          data-side="${esc(String(side || '').toUpperCase().trim())}"
          aria-label="Jump to ${esc(player)} ${esc(stat)} pregame movement"
        >
          <div class="prop-callout-head">
            ${actionBadge}
            ${marketBadge}
            ${badge}
            <span class="badge">${esc(stat)}</span>
            ${team ? `<span class="badge">${esc(team)}</span>` : ''}
            ${evTxt ? `<span class="badge">${esc(evTxt)}</span>` : ''}
          </div>
          <div class="prop-callout-body">
            ${img}
            <div class="prop-callout-copy">
              <div class="fw-700 prop-callout-title">
                ${esc(player)} <span class="subtle">${esc(stat)}</span> <span class="subtle">L${esc(curLineTxt)}</span> <span class="subtle">${esc(curPriceTxt)}</span>
              </div>
              <div class="subtle prop-callout-line">
                ${esc(game)}${playToText ? ` · ${esc(playToText)}` : ''}
              </div>
              <div class="subtle prop-callout-line">
                ${esc((guidance && guidance.summary) || 'Movement context unavailable.')}
              </div>
              <div class="subtle prop-callout-line">
                ${esc(moveText || `Open ${openLineTxt} → Now ${curLineTxt}`)}${tagText ? ` · ${esc(tagText)}` : ''}
              </div>
            </div>
          </div>
        </button>
      `;
    }).join('');

    return `
      <div class="subtle" style="margin-top:8px;">Pregame prop movement signal + action guide — click to jump:</div>
      <div class="row chips prop-callouts-rail">
        ${items}
      </div>
      <div class="subtle" style="margin-top:6px;">
        <span class="fw-700">Notes:</span>
        Signal badges show movement; action badges show what to do at the current number;
        <span class="badge good">Market agrees</span> move supports the side;
        <span class="badge bad">Market disagrees</span> move leans the other way;
        <span class="badge good">HIGH O</span>/<span class="badge good">HIGH U</span> fast move on the over/under;
        <span class="badge ok">MED O</span>/<span class="badge ok">MED U</span> meaningful move, but not the fastest tier;
        <span class="badge good">Bet now</span> still inside the playable range;
        <span class="badge ok">Shop</span> edge is live, but compare books;
        <span class="badge">Wait</span> hold for a better entry;
        <span class="badge bad">Pass</span> current number is no longer playable.
      </div>
    `;
  } catch (_) {
    return '';
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
    player_prop: thr('player_prop', 2.0, 4.0),
    roundHalf: !!(t && t.round_live_line_to_half),
    adjustments: (t && t.adjustments && typeof t.adjustments === 'object') ? t.adjustments : null,
    logging: (t && t.logging && typeof t.logging === 'object') ? t.logging : null,
  };
}

async function loadPregamePropCallouts(root, games, dateStr) {
  const el = root && root.querySelector ? root.querySelector('#pregame-prop-callouts') : null;
  if (!el) return;

  const ds = (typeof dateStr === 'string' && isYmd(dateStr)) ? dateStr : localYMD();

  // Guard against out-of-order async updates when the user changes dates quickly.
  const reqId = `${Date.now()}_${Math.random().toString(16).slice(2)}`;
  try { el.dataset.reqId = reqId; } catch (_) { /* ignore */ }

  // Map matchup -> card gid so click-to-scroll works even when cards use numeric game_id.
  const gidByMatchup = new Map();
  try {
    (Array.isArray(games) ? games : []).forEach((g) => {
      const homeTri = String(g && g.home_tri != null ? g.home_tri : '').toUpperCase().trim();
      const awayTri = String(g && g.away_tri != null ? g.away_tri : '').toUpperCase().trim();
      if (!homeTri || !awayTri) return;
      const gid = canonGameId((g && g.sim && g.sim.game_id != null) ? g.sim.game_id : (g && g.game_id != null ? g.game_id : ''))
        || `${homeTri}_${awayTri}`;
      gidByMatchup.set(`${homeTri}|${awayTri}`, gid);
    });
  } catch (_) {
    // ignore
  }

  try {
    const url = `/api/props/movement-callouts?date=${encodeURIComponent(ds)}`
      + `&onlyEV=1&minEV=1.0`
      + `&markets=pts,reb,ast,threes,stl,blk,tov,pra`;

    const payload = await fetchJsonWithTimeout(url, 12000);
    try {
      if (el.dataset.reqId !== reqId) return;
    } catch (_) {
      // ignore
    }

    const data = Array.isArray(payload && payload.data) ? payload.data : [];
    const callouts = [];

    for (const c of data) {
      if (!c || typeof c !== 'object') continue;
      const home = String(c.home_tricode || '').toUpperCase().trim();
      const away = String(c.away_tricode || '').toUpperCase().trim();
      if (!home || !away) continue;

      const gid = gidByMatchup.get(`${home}|${away}`) || `${home}_${away}`;

      const openLine = n(c.open_line);
      const curLine = n(c.line);
      let lineMove = n(c.line_move);
      if (lineMove == null) lineMove = curLine - openLine;

      const impliedMove = n(c.implied_move);
      const absLine = (lineMove == null) ? 0 : Math.abs(lineMove);
      const absImp = (impliedMove == null) ? 0 : Math.abs(impliedMove);
      if (!(absLine >= 0.5 || absImp >= 0.02)) continue;

      callouts.push({
        gid,
        home,
        away,
        team: String(c.team_tricode || c.team || '').toUpperCase().trim(),
        player: String(c.player || '').trim(),
        player_id: n(c.player_id),
        photo: String(c.photo || '').trim(),
        tier: c.tier,
        stat: String(c.market || c.stat || '').toLowerCase().trim(),
        side: String(c.side || '').toUpperCase().trim(),
        open_line: c.open_line,
        line: c.line,
        open_price: c.open_price,
        price: c.price,
        line_move: c.line_move,
        implied_move: c.implied_move,
        ev_pct: c.ev_pct,
        model_line: c.model_line,
        books_count: c.books_count,
        consensus: c.consensus,
        line_adv: c.line_adv,
        reasons: Array.isArray(c.reasons) ? c.reasons : [],
      });
    }

    callouts.sort((a, b) => {
      const aRank = pregamePropActionRank((buildPregameGuidance(a, {
        modelLine: a.model_line,
        booksCount: a.books_count,
        consensus: a.consensus,
        lineAdv: a.line_adv,
        reasons: a.reasons,
      }) || {}).action_code);
      const bRank = pregamePropActionRank((buildPregameGuidance(b, {
        modelLine: b.model_line,
        booksCount: b.books_count,
        consensus: b.consensus,
        lineAdv: b.line_adv,
        reasons: b.reasons,
      }) || {}).action_code);
      if (bRank !== aRank) return bRank - aRank;
      const aLm = Math.abs(n(a.line_move) ?? ((n(a.line) != null && n(a.open_line) != null) ? (n(a.line) - n(a.open_line)) : 0));
      const bLm = Math.abs(n(b.line_move) ?? ((n(b.line) != null && n(b.open_line) != null) ? (n(b.line) - n(b.open_line)) : 0));
      if (bLm !== aLm) return bLm - aLm;
      const aIm = Math.abs(n(a.implied_move) ?? 0);
      const bIm = Math.abs(n(b.implied_move) ?? 0);
      if (bIm !== aIm) return bIm - aIm;
      const aEv = n(a.ev_pct) ?? -1e9;
      const bEv = n(b.ev_pct) ?? -1e9;
      return bEv - aEv;
    });

    const html = renderPregamePropCallouts(callouts.slice(0, 21));
    if (html) {
      el.innerHTML = html;
      el.classList.remove('hidden');
      try { applyPregamePropFilters(root); } catch (_) { /* ignore */ }
    } else {
      el.innerHTML = '';
      el.classList.add('hidden');
      try { applyPregamePropFilters(root); } catch (_) { /* ignore */ }
    }
  } catch (_) {
    try {
      if (el.dataset.reqId !== reqId) return;
    } catch (_) {
      // ignore
    }
    try {
      el.innerHTML = '<div class="subtle" style="margin-top:8px;">Pregame movement callouts are unavailable right now.</div>';
      el.classList.remove('hidden');
      try { applyPregamePropFilters(root); } catch (_) { /* ignore */ }
    } catch (_) {
      // ignore
    }
  }
}

function startLiveLensPolling(root, games, dateStr) {
  if (__liveLensTimer != null) {
    try { clearInterval(__liveLensTimer); } catch (_) { /* ignore */ }
    __liveLensTimer = null;
  }

  // Live prop callouts stability: avoid flicker when a poll temporarily returns
  // no rows (e.g., transient API failure / brief empty payload).
  let __calloutsLastHtml = '';
  let __calloutsLastNonEmptyAt = 0;
  let __calloutsEmptyStreak = 0;

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

  function pickCurrentPeriodTotalTagFromLensEl(lensEl, scoreboardRow) {
    // Goal: show the *current bettable total scope* for the game.
    // Mapping (simple, matches existing lens tags available in UI):
    // - Q1/Q3: quarter total tag
    // - Q2: 1H total tag
    // - Q4+: full game total tag
    if (!lensEl) return { klass: 'NONE', text: '' };

    const p0 = (scoreboardRow && scoreboardRow.period != null && Number.isFinite(Number(scoreboardRow.period)))
      ? Math.floor(Number(scoreboardRow.period))
      : null;

    let sel = '.lens-rec-total';
    let label = 'G';
    let scopeKey = 'game';
    if (p0 === 1 || p0 === 3) {
      sel = '.lens-rec-qtr';
      label = `Q${p0}`;
      scopeKey = `q${p0}`;
    } else if (p0 === 2) {
      sel = '.lens-rec-half';
      label = '1H';
      scopeKey = 'half';
    } else {
      sel = '.lens-rec-total';
      label = 'G';
      scopeKey = 'game';
    }

    const el = lensEl.querySelector(sel);
    const raw = el ? String(el.textContent || '').trim() : '';
    let whyTitle = '';
    try { whyTitle = el && el.title ? String(el.title) : ''; } catch (_) { whyTitle = ''; }
    const t = (() => {
      const out = (raw ? raw : '');
      return out ? out : '';
    })();

    const inProg = !!(scoreboardRow && scoreboardRow.in_progress);

    let proj = null;
    let line = null;
    try {
      // Prefer values from the scope column (always updated by polling auto-fill).
      const scopeCol = lensEl ? lensEl.querySelector(`.lens-col[data-scope="${CSS.escape(String(scopeKey))}"]`) : null;
      proj = scopeCol ? n(scopeCol.dataset.paceFinal) : null;
      line = scopeCol ? n(scopeCol.dataset.liveTotal) : null;

      // Fallback to values attached to the rec element (if present).
      if (proj == null && el) proj = n(el.dataset.projTotal);
      if (line == null && el) line = n(el.dataset.lineTotal);
    } catch (_) {
      proj = null;
      line = null;
    }
    const extra = (proj != null && line != null)
      ? `P${fmt(proj, 1)}/${fmt(line, 1)}`
      : ((proj != null) ? `P${fmt(proj, 1)}` : ((line != null) ? `L${fmt(line, 1)}` : ''));

    // Parse klass + side from the tag text.
    // Examples:
    // - "Q3: BET Over (+4.2)"
    // - "1H: WATCH Under (-2.1)"
    // - "Total: BET Over (+6.0)"
    const m = String(t).match(/\b(BET|WATCH)\b(?:\s+(Over|Under))?\b/i);
    if (!m) {
      if (inProg && extra) return { klass: 'NONE', text: `${label} ${extra}`.trim(), scopeKey, title: whyTitle };
      return { klass: 'NONE', text: inProg ? `${label} —` : '', scopeKey, title: whyTitle };
    }
    const klass = String(m[1] || '').toUpperCase();
    const side = String(m[2] || '').toUpperCase();
    const sideShort = (side === 'OVER') ? 'O' : ((side === 'UNDER') ? 'U' : '');
    // Keep this compact: tile color communicates BET/WATCH.
    const txt = `${label} ${extra} ${sideShort}`.trim();
    return { klass, text: txt, scopeKey, title: whyTitle };
  }

  function updateScoreboardStrip(sbById, opts) {
    try {
      const skipTags = !!(opts && opts.skipTags);
      const strip = root.querySelector('.scoreboard-strip');
      if (!strip) return;
      const items = strip.querySelectorAll('.s-item[data-game-id]');
      items.forEach((item) => {
        const gid = canonGameId(item.dataset.gameId);
        if (!gid) return;

        const s = sbById ? sbById.get(gid) : null;
        const statusEl = item.querySelector('.s-status');
        const scoreEl = item.querySelector('.s-score');
        const awayScoreEl = item.querySelector('.s-score-away');
        const homeScoreEl = item.querySelector('.s-score-home');

        if (statusEl) statusEl.textContent = (s && s.status) ? String(s.status) : (statusEl.textContent || '');
        {
          const ap = (s && s.away_pts != null) ? s.away_pts : null;
          const hp = (s && s.home_pts != null) ? s.home_pts : null;
          if (awayScoreEl) awayScoreEl.textContent = (ap != null) ? String(ap) : '';
          if (homeScoreEl) homeScoreEl.textContent = (hp != null) ? String(hp) : '';
          if (scoreEl && ap != null && hp != null) scoreEl.textContent = `${ap}-${hp}`;
        }

        const lensEl = root.querySelector(`.live-lens[data-game-id="${CSS.escape(gid)}"]`);
        const tag = skipTags
          ? { klass: 'NONE', text: '' }
          : (pickCurrentPeriodTotalTagFromLensEl(lensEl, s) || { klass: 'NONE', text: '' });
        const tagEl = item.querySelector('.s-tag');
        if (tagEl && !skipTags) {
          tagEl.textContent = tag.text || '';
          try { tagEl.title = tag.title || ''; } catch (_) { /* ignore */ }
        }

        const inProg = !!(s && s.in_progress) || (lensEl && lensEl.dataset.inProgress === '1');
        item.classList.remove('bet', 'watch', 'live', 'neu');
        if (!skipTags && tag.klass === 'BET') item.classList.add('bet');
        else if (!skipTags && tag.klass === 'WATCH') item.classList.add('watch');
        else if (inProg) item.classList.add('live');
        else item.classList.add('neu');
      });
    } catch (_) {
      // ignore
    }
  }

  function updateCompactCardTags(sbById, opts) {
    try {
      const skipTags = !!(opts && opts.skipTags);
      const cards = root.querySelectorAll('.card.card-v2[data-game-id]');
      cards.forEach((card) => {
        try {
          const gid = canonGameId(card.dataset.gameId);
          if (!gid) return;
          const s = sbById ? sbById.get(gid) : null;
          const lensEl = root.querySelector(`.live-lens[data-game-id="${CSS.escape(gid)}"]`);
          const tag = skipTags
            ? { klass: 'NONE', text: '', title: '' }
            : (pickCurrentPeriodTotalTagFromLensEl(lensEl, s) || { klass: 'NONE', text: '', title: '' });
          const tagEl = card.querySelector('.lens-card-tag');
          if (!tagEl) return;
          const whyEl = card.querySelector('.lens-why-badges');

          if (skipTags) return;

          function _whyBadgesFromTitle(title, klass) {
            try {
              const t = String(title || '');
              if (!t.trim()) return '';

              const has = (needle) => t.indexOf(needle) >= 0;
              const out = [];
              if (has('edge ')) out.push('EDGE');
              if (has('ctx ')) out.push('CTX');
              if (has('pace×')) out.push('PACE');
              if (has('pppΔ')) out.push('PPP');
              if (has('poss ')) out.push('POSS');
              if (has('shrunk ')) out.push('SHR');
              if (has('λ')) out.push('λ');
              if (has('pScore ')) out.push('LIVE');
              if (has('impHome ')) out.push('ODDS');
              if (has('pHome ')) out.push('PROB');

              const priority = ['EDGE', 'CTX', 'PACE', 'PPP', 'POSS', 'LIVE', 'ODDS', 'PROB', 'SHR', 'λ'];
              const uniq = Array.from(new Set(out));
              uniq.sort((a, b) => priority.indexOf(a) - priority.indexOf(b));
              const chosen = uniq.slice(0, 3);
              if (!chosen.length) return '';

              const badgeClass = (klass === 'BET') ? 'good' : ((klass === 'WATCH') ? 'ok' : '');
              const cls = badgeClass ? `badge ${badgeClass}` : 'badge';
              return chosen.map((x) => `<span class="${cls}">${esc(x)}</span>`).join('');
            } catch (_) {
              return '';
            }
          }

          tagEl.textContent = tag.text || '';
          try { tagEl.title = tag.title || ''; } catch (_) { /* ignore */ }
          tagEl.classList.remove('bet', 'watch');
          if (tag.klass === 'BET') tagEl.classList.add('bet');
          else if (tag.klass === 'WATCH') tagEl.classList.add('watch');

          try {
            if (whyEl) {
              whyEl.innerHTML = _whyBadgesFromTitle(tag.title || '', tag.klass || '');
              whyEl.classList.toggle('bet', tag.klass === 'BET');
              whyEl.classList.toggle('watch', tag.klass === 'WATCH');
              try { whyEl.title = tag.title || ''; } catch (_) { /* ignore */ }
            }
          } catch (_) {
            // ignore
          }
        } catch (_) {
          // ignore
        }
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
      sim_score: score0,
      sim_periods: (sim0 && sim0.periods) ? sim0.periods : (g && g.periods ? g.periods : null),
      sim_players_home: (sim0 && sim0.players && Array.isArray(sim0.players.home)) ? sim0.players.home : [],
      sim_players_away: (sim0 && sim0.players && Array.isArray(sim0.players.away)) ? sim0.players.away : [],
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
      sb = await fetchJson(`/api/live_state?date=${encodeURIComponent(dateStr)}&ttl=${encodeURIComponent(String(LIVE_PROPS_ENDPOINT_TTL_SEC))}`);
    } catch (_) {
      // Back-compat: older servers only have /api/live/scoreboard
      try {
        const legacy = await fetchJson(`/api/live/scoreboard?date=${encodeURIComponent(dateStr)}`);
        const legacyGames = Array.isArray(legacy?.games) ? legacy.games : [];
        sb = {
          date: legacy?.date,
          ttl: LIVE_PROPS_ENDPOINT_TTL_SEC,
          source: legacy?.source,
          games: legacyGames.map((g) => ({
            game_id: g?.game_id,
            event_id: (g?.event_id ?? g?.espn_event_id ?? g?.eventId),
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
            periods: Array.isArray(g?.periods) ? g.periods : [],
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
      const eid = String((x && (x.event_id ?? x.espn_event_id ?? x.eventId)) ?? '').trim();
      if (eid) sbEventByGid.set(gid, eid);
    });

    // Keep the top scoreboard strip in sync with basic status/score.
    updateScoreboardStrip(sbById, { skipTags: true });
    updateCompactCardTags(sbById, { skipTags: true });

    const detailIds = [];
    const detailEventIds = [];
    const inProgEventIds = [];
    const lineEventIds = [];
    byGameId.forEach((meta, gid) => {
      const s = sbById.get(gid);
      if (s && (s.in_progress || s.final)) {
        detailIds.push(gid);
        const eid = sbEventByGid.get(gid);
        if (eid) detailEventIds.push(eid);
        if (s.in_progress && !s.final && eid) {
          lineEventIds.push(eid);
          inProgEventIds.push(eid);
        }
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
    let playerBoxscoreMap = new Map();
    if (detailEventIds.length) {
      try {
        // Allow server-driven recent-window size for pbp_recent feature set.
        let recentWindowSec = 180;
        try {
          const t = (__liveLensTuning && typeof __liveLensTuning === 'object') ? __liveLensTuning : null;
          const rw = t && t.recent_window && typeof t.recent_window === 'object' ? t.recent_window : null;
          const w = n(rw && rw.window_sec);
          if (rw && rw.enabled !== false && w != null && w >= 10 && w <= 600) recentWindowSec = Math.round(w);
        } catch (_) {
          // ignore
        }

        // Keep the heavier live endpoints limited to in-progress games for latency.
        // Final games still need player boxscores for the merged sim-vs-live tables,
        // so fetch raw boxscores for all detail ids even when live games exist.
        const pbpIds = (inProgEventIds && inProgEventIds.length) ? inProgEventIds : detailEventIds;
        const playerLensIds = (inProgEventIds && inProgEventIds.length) ? inProgEventIds : detailEventIds;
        const playerBoxscoreIds = detailEventIds;

        const _ts = Date.now();
        const liveTtlSec = LIVE_PROPS_ENDPOINT_TTL_SEC;

        const pbpPromise = fetchJsonWithTimeout(
          `/api/live_pbp_stats?ttl=${encodeURIComponent(String(liveTtlSec))}&recent_window_sec=${encodeURIComponent(String(recentWindowSec))}`
          + `&event_ids=${encodeURIComponent(pbpIds.join(','))}`
          + `&date=${encodeURIComponent(dateStr)}`
          + `&_ts=${encodeURIComponent(String(_ts))}`,
          8000,
        );
        const linesPromise = lineEventIds.length
          ? fetchJsonWithTimeout(`/api/live_lines?ttl=${encodeURIComponent(String(liveTtlSec))}&date=${encodeURIComponent(dateStr)}&event_ids=${encodeURIComponent(lineEventIds.join(','))}&include_period_totals=1&_ts=${encodeURIComponent(String(_ts))}`, 8000)
          : Promise.resolve({ games: [] });
        // Keep prop live lens aligned with the same recent-window size used by game live adjustments.
        let recentWindowSec2 = 180;
        try {
          const t2 = (__liveLensTuning && typeof __liveLensTuning === 'object') ? __liveLensTuning : null;
          const rw2 = t2 && t2.recent_window && typeof t2.recent_window === 'object' ? t2.recent_window : null;
          const w2 = n(rw2 && rw2.window_sec);
          if (rw2 && rw2.enabled !== false && w2 != null && w2 >= 10 && w2 <= 600) recentWindowSec2 = Math.round(w2);
        } catch (_) {
          // ignore
        }
        const playersPromise = fetchJsonWithTimeout(`/api/live_player_lens?ttl=${encodeURIComponent(String(liveTtlSec))}&recent_window_sec=${encodeURIComponent(String(recentWindowSec2))}&date=${encodeURIComponent(dateStr)}&event_ids=${encodeURIComponent(playerLensIds.join(','))}&_ts=${encodeURIComponent(String(_ts))}`, 8000);
        const boxscorePromise = fetchJsonWithTimeout(`/api/live_player_boxscore?ttl=${encodeURIComponent(String(liveTtlSec))}&event_ids=${encodeURIComponent(playerBoxscoreIds.join(','))}&_ts=${encodeURIComponent(String(_ts))}`, 8000);
        const settled = await Promise.allSettled([pbpPromise, linesPromise, playersPromise, boxscorePromise]);
        const pbp = (settled[0] && settled[0].status === 'fulfilled') ? settled[0].value : null;
        const lines = (settled[1] && settled[1].status === 'fulfilled') ? settled[1].value : null;
        const players = (settled[2] && settled[2].status === 'fulfilled') ? settled[2].value : null;
        const playerBoxscores = (settled[3] && settled[3].status === 'fulfilled') ? settled[3].value : null;

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
        const playerBoxscoreGames = Array.isArray(playerBoxscores?.games) ? playerBoxscores.games : [];
        playerBoxscoreGames.forEach((gg) => {
          const eid = String(gg && gg.event_id != null ? gg.event_id : '').trim();
          if (!eid) return;
          playerBoxscoreMap.set(eid, gg);
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
      const livePlayerBox = eid ? playerBoxscoreMap.get(eid) : null;

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
        pbp_recent: pbp && pbp.pbp_recent ? pbp.pbp_recent : null,
        _event_id: eid || null,
      };

      const el = root.querySelector(`.live-lens[data-game-id="${CSS.escape(gid)}"]`);
      if (!el) return;

      // Expose status for computeScope() so it can suppress Lean/Signal on finals.
      try {
        el.dataset.final = isFinal ? '1' : '0';
        el.dataset.inProgress = isInProgress ? '1' : '0';
        el.dataset.period = (period != null && Number.isFinite(Number(period))) ? String(Math.floor(Number(period))) : '';
        el.dataset.secLeftPeriod = (secLeftPeriod != null && Number.isFinite(Number(secLeftPeriod))) ? String(Math.max(0, Math.round(Number(secLeftPeriod)))) : '';
        el.dataset.margin = (homeMargin != null && Number.isFinite(Number(homeMargin))) ? String(Math.round(Number(homeMargin))) : '';
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

      try {
        const mergedBody = root.querySelector(`.merged-boxscore-body[data-game-id="${CSS.escape(gid)}"]`);
        if (mergedBody) {
          const currentSource = String(mergedBody.dataset.actualSource || '').trim().toLowerCase();
          const livePeriods = Array.isArray(s && s.periods) ? s.periods : [];
          const livePlayersAll = (livePlayerBox && Array.isArray(livePlayerBox.players)) ? livePlayerBox.players : [];
          const liveLensAll = (livePlayerLens && Array.isArray(livePlayerLens.rows)) ? livePlayerLens.rows : [];
          const hasLivePlayers = livePlayersAll.length > 0;
          const hasLiveLensRows = liveLensAll.length > 0;
          const hasLivePeriods = livePeriods.length > 0;
          if (hasLivePlayers || hasLiveLensRows || (hasLivePeriods && currentSource !== 'recon')) {
            const homeRows = livePlayersAll.filter((row) => String((row && row.team_tri) || '').toUpperCase().trim() === meta.home);
            const awayRows = livePlayersAll.filter((row) => String((row && row.team_tri) || '').toUpperCase().trim() === meta.away);
            const homeLensRows = liveLensAll.filter((row) => String((row && row.team_tri) || '').toUpperCase().trim() === meta.home);
            const awayLensRows = liveLensAll.filter((row) => String((row && row.team_tri) || '').toUpperCase().trim() === meta.away);
            mergedBody.innerHTML = renderMergedBoxscoreSection(meta, {
              mode: 'live',
              label: isFinal ? 'Final' : 'Live',
              periods: livePeriods,
              actualHome: homePts,
              actualAway: awayPts,
              homeRows,
              awayRows,
              homeLensRows,
              awayLensRows,
            });
            mergedBody.dataset.actualSource = 'live';
          }
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

        function clearLineTot(sumEl) {
          if (!sumEl) return;
          const lineTot = sumEl.querySelector('.lens-sum-line-total');
          if (!lineTot) return;
          lineTot.textContent = '—';
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
            // If game is in Q1 and we don't have a live Q1 line, don't keep prefill.
            if (isInProgress && period === 1 && q1Line == null) clearLineTot(sumQ1);
            else setLineTot(sumQ1, q1Line);
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
            // If game is in Q3 and we don't have a live Q3 line, don't keep prefill.
            if (isInProgress && period === 3 && q3Line == null) clearLineTot(sumQ3);
            else setLineTot(sumQ3, q3Line);
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

            // Store recent-window stats (used by computeScope() for responsive pace/eff adjustments).
            try {
              const rw = (live && live.pbp_recent && typeof live.pbp_recent === 'object') ? live.pbp_recent : null;
              const rwSec = n(rw && rw.window_sec);
              const rwPts = n(rw && rw.points_total);
              const rwPossAvg = rw && rw.possessions ? possAvgFromPossObj(meta, rw.possessions) : null;
              col.dataset.recentWindowSec = (rwSec == null) ? '' : String(Math.round(rwSec));
              col.dataset.recentPts = (rwPts == null) ? '' : String(rwPts);
              col.dataset.recentPoss = (rwPossAvg == null) ? '' : String(rwPossAvg);
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
          const totalExplainMainEl = el.querySelector('.lens-total-explain-main');
          const totalExplainBuildEl = el.querySelector('.lens-total-explain-build');
          const totalExplainRatesEl = el.querySelector('.lens-total-explain-rates');
          const totalExplainAdjustEl = el.querySelector('.lens-total-explain-adjust');
          if (recTotalEl) recTotalEl.textContent = 'Total: —';
          if (recHalfEl) recHalfEl.textContent = '1H: —';
          if (recQtrEl) recQtrEl.textContent = 'Q: —';
          if (recATSEl) recATSEl.textContent = 'ATS: —';
          if (recMLEl) recMLEl.textContent = 'ML: —';
          if (totalExplainMainEl) totalExplainMainEl.textContent = 'Signal: —';
          if (totalExplainBuildEl) totalExplainBuildEl.textContent = 'Build: —';
          if (totalExplainRatesEl) totalExplainRatesEl.textContent = 'Rates: —';
          if (totalExplainAdjustEl) totalExplainAdjustEl.textContent = 'Adjust: —';
          try {
            [recTotalEl, recHalfEl, recQtrEl, recATSEl, recMLEl, totalExplainMainEl].forEach((x) => {
              if (!x) return;
              x.classList.remove('bet', 'watch');
              x.title = '';
            });
          } catch (_) {
            // ignore
          }
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
      const totalExplainMainEl = el.querySelector('.lens-total-explain-main');
      const totalExplainBuildEl = el.querySelector('.lens-total-explain-build');
      const totalExplainRatesEl = el.querySelector('.lens-total-explain-rates');
      const totalExplainAdjustEl = el.querySelector('.lens-total-explain-adjust');
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
      let totalShrink = null;
      let totalPred = null;
      let totalClass = 'NONE';
      let totalSide = null;
      let totalGateMinElapsed = null;
      let totalGateActive = false;

      if (lens && effLineTotal != null) {
        totalDiffRaw = lens.paceFinal - effLineTotal;
        totalCtx = adjustGameTotalDiffWithContext(totalDiffRaw, effLineTotal, meta, live, curMinLeft, {
          period,
          sec_left_period: secLeftPeriodRaw,
          margin_home: (live && live.score) ? n(live.score.home_margin) : null,
        });
        totalDiff = (totalCtx && totalCtx.diff_adj != null) ? n(totalCtx.diff_adj) : totalDiffRaw;

        // Optional bias correction (server-tunable): compensates systematic under/over in the
        // SmartSim-based projection vs market totals. Ramp in with elapsed time.
        try {
          const adjCfg = _liveLensGameTotalAdjCfg();
          const b = n(adjCfg && adjCfg.bias_points);
          const cap = n(adjCfg && adjCfg.bias_cap_points);
          if (b != null && totalDiff != null && Math.abs(b) > 1e-9) {
            const elapsedMin = 48.0 - curMinLeft;
            const frac = Math.max(0, Math.min(1, elapsedMin / 48.0));
            let bEff = b * frac;
            if (cap != null && cap > 0) bEff = Math.max(-cap, Math.min(cap, bEff));
            totalDiff = totalDiff + bEff;
            if (totalCtx && typeof totalCtx === 'object') {
              totalCtx.bias_points = b;
              totalCtx.bias_eff = bEff;
              totalCtx.bias_frac = frac;
            }
          }
        } catch (_) {
          // ignore
        }

        // Possessions/time-based confidence shrinkage (reduces early-game overconfidence).
        try {
          const totalDiffUnshrunk = totalDiff;
          const possLive = n(totalCtx && totalCtx.poss_live);
          const elapsedMin = 48.0 - curMinLeft;
          totalShrink = _liveLensEdgeShrink(totalDiff, possLive, elapsedMin, 48);
          if (totalShrink && totalShrink.diff_shrunk != null) totalDiff = n(totalShrink.diff_shrunk);
          if (totalCtx && typeof totalCtx === 'object') {
            totalCtx.edge_adj_unshrunk = totalDiffUnshrunk;
            totalCtx.edge_shrink_lambda = totalShrink ? totalShrink.lambda : null;
            totalCtx.edge_shrink_lambda_poss = totalShrink ? totalShrink.lambda_poss : null;
            totalCtx.edge_shrink_lambda_time = totalShrink ? totalShrink.lambda_time : null;
          }
        } catch (_) {
          // ignore
        }

        // Suppress noisy early-game tags (server-tunable).
        try {
          const adjCfg = _liveLensGameTotalAdjCfg();
          const minElapsed = n(adjCfg && adjCfg.min_elapsed_min);
          const elapsedMin = 48.0 - curMinLeft;
          totalGateMinElapsed = minElapsed;
          if (minElapsed != null && elapsedMin < minElapsed) {
            totalClass = 'NONE';
            totalGateActive = true;
          } else {
            totalClass = classifyDiff(Math.abs(totalDiff), thr.total.watch, thr.total.bet);
          }
        } catch (_) {
          totalClass = classifyDiff(Math.abs(totalDiff), thr.total.watch, thr.total.bet);
        }
        if (totalDiff > 1.0) totalSide = 'Over';
        else if (totalDiff < -1.0) totalSide = 'Under';
        else totalSide = 'No edge';

        try {
          totalPred = (effLineTotal != null && totalDiff != null) ? (effLineTotal + totalDiff) : null;
        } catch (_) {
          totalPred = null;
        }
      }
      if (recTotalEl) {
        if (totalClass === 'BET') recTotalEl.textContent = `Total: BET ${totalSide} (${fmt(totalDiff, 1)})`;
        else if (totalClass === 'WATCH') recTotalEl.textContent = `Total: WATCH ${totalSide} (${fmt(totalDiff, 1)})`;
        else recTotalEl.textContent = 'Total: —';

        try {
          recTotalEl.classList.remove('bet', 'watch');
          if (totalClass === 'BET') recTotalEl.classList.add('bet');
          else if (totalClass === 'WATCH') recTotalEl.classList.add('watch');

          const why = [];
          if (totalDiffRaw != null) why.push(`raw ${fmt(totalDiffRaw, 1)}`);
          if (totalCtx && totalCtx.diff_adj != null) why.push(`ctx ${fmt(totalCtx.diff_adj, 1)}`);
          if (totalDiff != null && totalDiffRaw != null && Math.abs(totalDiff - totalDiffRaw) > 1e-6) why.push(`shrunk ${fmt(totalDiff, 1)}`);
          if (totalShrink && totalShrink.lambda != null) why.push(`λ ${fmt(totalShrink.lambda, 2)}`);
          if (totalCtx && totalCtx.poss_live != null) why.push(`poss ${fmt(totalCtx.poss_live, 1)}`);
          if (totalCtx && totalCtx.pace_ratio != null) why.push(`pace× ${fmt(totalCtx.pace_ratio, 2)}`);
          if (totalCtx && totalCtx.eff_ppp_delta != null) why.push(`pppΔ ${fmt(totalCtx.eff_ppp_delta, 3)}`);
          if (totalCtx && totalCtx.under_edge_reversion_points != null && Math.abs(totalCtx.under_edge_reversion_points) > 1e-6) why.push(`uRev ${fmt(totalCtx.under_edge_reversion_points, 1)}`);
          if (totalCtx && totalCtx.endgame_foul_adj != null && Math.abs(totalCtx.endgame_foul_adj) > 1e-6) why.push(`late ${fmt(totalCtx.endgame_foul_adj, 1)}`);
          why.push(`thr ${fmt(thr.total.watch, 1)}/${fmt(thr.total.bet, 1)}`);
          recTotalEl.title = why.filter(Boolean).join(' · ');
        } catch (_) {
          // ignore
        }

        try {
          if (totalPred != null) recTotalEl.dataset.projTotal = String(totalPred);
          else recTotalEl.removeAttribute('data-proj-total');
          if (effLineTotal != null) recTotalEl.dataset.lineTotal = String(effLineTotal);
          else recTotalEl.removeAttribute('data-line-total');
        } catch (_) {
          // ignore
        }
      }

      try {
        const signalParts = [];
        if (totalGateActive && totalGateMinElapsed != null) signalParts.push(`Waiting for ${fmt(totalGateMinElapsed, 0)}m gate`);
        if (totalPred != null && effLineTotal != null) signalParts.push(`Proj ${fmt(totalPred, 1)} vs ${fmt(effLineTotal, 1)}`);
        if (totalSide && totalSide !== 'No edge' && totalDiff != null) signalParts.push(`${totalSide} ${fmtSigned(totalDiff, 1)}`);
        else if (totalDiff != null) signalParts.push(`Edge ${fmtSigned(totalDiff, 1)}`);

        const buildParts = [];
        if (totalDiffRaw != null) buildParts.push(`raw ${fmtSigned(totalDiffRaw, 1)}`);
        if (totalCtx && totalCtx.diff_adj != null && (totalDiffRaw == null || Math.abs(totalCtx.diff_adj - totalDiffRaw) > 1e-6)) buildParts.push(`ctx ${fmtSigned(totalCtx.diff_adj, 1)}`);
        if (totalDiff != null && (totalCtx == null || totalCtx.diff_adj == null || Math.abs(totalDiff - totalCtx.diff_adj) > 1e-6)) buildParts.push(`final ${fmtSigned(totalDiff, 1)}`);

        const rateParts = [];
        if (totalCtx && totalCtx.poss_live != null) rateParts.push(`poss ${fmt(totalCtx.poss_live, 1)}`);
        if (totalCtx && totalCtx.pace_ratio != null) rateParts.push(`pace× ${fmt(totalCtx.pace_ratio, 2)}`);
        if (totalCtx && totalCtx.eff_ppp_delta != null) rateParts.push(`pppΔ ${fmt(totalCtx.eff_ppp_delta, 3)}`);

        const adjustParts = [];
        if (totalCtx && totalCtx.under_edge_reversion_points != null && Math.abs(totalCtx.under_edge_reversion_points) > 1e-6) adjustParts.push(`under ${fmtSigned(totalCtx.under_edge_reversion_points, 1)}`);
        if (totalCtx && totalCtx.endgame_foul_adj != null && Math.abs(totalCtx.endgame_foul_adj) > 1e-6) adjustParts.push(`late ${fmtSigned(totalCtx.endgame_foul_adj, 1)}`);
        if (totalCtx && totalCtx.bias_eff != null && Math.abs(totalCtx.bias_eff) > 1e-6) adjustParts.push(`bias ${fmtSigned(totalCtx.bias_eff, 1)}`);
        if (totalShrink && totalShrink.lambda != null && totalShrink.lambda < 0.999) adjustParts.push(`shrink λ ${fmt(totalShrink.lambda, 2)}`);
        if (!adjustParts.length && totalGateActive && totalGateMinElapsed != null) adjustParts.push(`gate ${fmt(totalGateMinElapsed, 0)}m`);

        if (totalExplainMainEl) {
          totalExplainMainEl.textContent = signalParts.length ? `Signal: ${signalParts.join(' · ')}` : 'Signal: —';
          totalExplainMainEl.classList.remove('bet', 'watch');
          if (totalClass === 'BET') totalExplainMainEl.classList.add('bet');
          else if (totalClass === 'WATCH') totalExplainMainEl.classList.add('watch');
        }
        if (totalExplainBuildEl) totalExplainBuildEl.textContent = buildParts.length ? `Build: ${buildParts.join(' -> ')}` : 'Build: —';
        if (totalExplainRatesEl) totalExplainRatesEl.textContent = rateParts.length ? `Rates: ${rateParts.join(' | ')}` : 'Rates: —';
        if (totalExplainAdjustEl) totalExplainAdjustEl.textContent = adjustParts.length ? `Adjust: ${adjustParts.join(' | ')}` : 'Adjust: none';
      } catch (_) {
        if (totalExplainMainEl) totalExplainMainEl.textContent = 'Signal: —';
        if (totalExplainBuildEl) totalExplainBuildEl.textContent = 'Build: —';
        if (totalExplainRatesEl) totalExplainRatesEl.textContent = 'Rates: —';
        if (totalExplainAdjustEl) totalExplainAdjustEl.textContent = 'Adjust: —';
      }

      // Half-level signal (vs pregame half baseline) during 1H only
      let halfClass = 'NONE';
      let halfSide = null;
      let halfDiff = null;
      let halfDiffRaw = null;
      let halfShrink = null;
      let halfPred = null;
      let halfLine = null;
      try {
        if (recHalfEl && (period == null || Number(period) <= 2)) {
          const halfCol = el.querySelector('.lens-col[data-scope="half"]');
          const pf = halfCol ? n(halfCol.dataset.paceFinal) : null;
          const sf = halfCol ? n(halfCol.dataset.simFinal) : null;
          const hl = halfCol ? n(halfCol.dataset.liveTotal) : null;
          halfLine = hl;

          // Suppress very-early 1H tags (tunable; scaled to scope length).
          let allowHalf = true;
          try {
            const adjCfg = _liveLensScopeTotalAdjCfg(24);
            const minElapsedFull = n(adjCfg && adjCfg.min_elapsed_min);
            const minElapsedHalf = (minElapsedFull != null) ? Math.max(1.0, minElapsedFull * (24.0 / 48.0)) : null;
            const rem = (halfMinLeftRaw != null) ? Math.max(0, Math.min(24, Math.round(halfMinLeftRaw))) : null;
            const elapsed = (rem != null) ? (24.0 - rem) : null;
            if (minElapsedHalf != null && elapsed != null && elapsed < minElapsedHalf) allowHalf = false;
          } catch (_) {
            allowHalf = true;
          }

          if (pf != null && hl != null) {
            halfDiffRaw = pf - hl;
            halfDiff = halfDiffRaw;
            halfPred = pf;

            // Confidence shrink based on possessions/time within the half.
            try {
              const possLive = halfCol ? n(halfCol.dataset.possLive) : null;
              const rem = (halfMinLeftRaw != null) ? Math.max(0, Math.min(24, Math.round(halfMinLeftRaw))) : null;
              const elapsed = (rem != null) ? (24.0 - rem) : null;
              halfShrink = _liveLensEdgeShrink(halfDiff, possLive, elapsed, 24);
              if (halfShrink && halfShrink.diff_shrunk != null) halfDiff = n(halfShrink.diff_shrunk);
            } catch (_) {
              // ignore
            }

            halfClass = allowHalf ? classifyDiff(Math.abs(halfDiff), thr.half_total.watch, thr.half_total.bet) : 'NONE';
            if (halfDiff > 1.0) halfSide = 'Over';
            else if (halfDiff < -1.0) halfSide = 'Under';
            else halfSide = 'No edge';
          } else if (pf != null && sf != null) {
            // Fallback: vs pregame half baseline when no live half line.
            halfDiffRaw = pf - sf;
            halfDiff = halfDiffRaw;
            halfPred = pf;

            // Confidence shrink based on possessions/time within the half.
            try {
              const possLive = halfCol ? n(halfCol.dataset.possLive) : null;
              const rem = (halfMinLeftRaw != null) ? Math.max(0, Math.min(24, Math.round(halfMinLeftRaw))) : null;
              const elapsed = (rem != null) ? (24.0 - rem) : null;
              halfShrink = _liveLensEdgeShrink(halfDiff, possLive, elapsed, 24);
              if (halfShrink && halfShrink.diff_shrunk != null) halfDiff = n(halfShrink.diff_shrunk);
            } catch (_) {
              // ignore
            }

            halfClass = allowHalf ? classifyDiff(Math.abs(halfDiff), thr.half_total.watch, thr.half_total.bet) : 'NONE';
            if (halfDiff > 1.0) halfSide = 'Over';
            else if (halfDiff < -1.0) halfSide = 'Under';
            else halfSide = 'No edge';
          }
          if (halfClass === 'BET') recHalfEl.textContent = `1H: BET ${halfSide} (${fmt(halfDiff, 1)})`;
          else if (halfClass === 'WATCH') recHalfEl.textContent = `1H: WATCH ${halfSide} (${fmt(halfDiff, 1)})`;
          else recHalfEl.textContent = '1H: —';

          try {
            recHalfEl.classList.remove('bet', 'watch');
            if (halfClass === 'BET') recHalfEl.classList.add('bet');
            else if (halfClass === 'WATCH') recHalfEl.classList.add('watch');

            const why = [];
            if (halfDiffRaw != null) why.push(`raw ${fmt(halfDiffRaw, 1)}`);
            if (halfDiff != null && halfDiffRaw != null && Math.abs(halfDiff - halfDiffRaw) > 1e-6) why.push(`shrunk ${fmt(halfDiff, 1)}`);
            if (halfShrink && halfShrink.lambda != null) why.push(`λ ${fmt(halfShrink.lambda, 2)}`);
            if (halfLine != null) why.push(`L ${fmt(halfLine, 1)}`);
            why.push(`thr ${fmt(thr.half_total.watch, 1)}/${fmt(thr.half_total.bet, 1)}`);
            recHalfEl.title = why.filter(Boolean).join(' · ');
          } catch (_) {
            // ignore
          }

          try {
            const halfProjOut = (halfLine != null && halfDiff != null) ? (halfLine + halfDiff) : halfPred;
            if (halfProjOut != null) recHalfEl.dataset.projTotal = String(halfProjOut);
            else recHalfEl.removeAttribute('data-proj-total');
            if (halfLine != null) recHalfEl.dataset.lineTotal = String(halfLine);
            else recHalfEl.removeAttribute('data-line-total');
          } catch (_) {
            // ignore
          }
        } else if (recHalfEl) {
          recHalfEl.textContent = '1H: —';
          try {
            recHalfEl.classList.remove('bet', 'watch');
            recHalfEl.title = '';
          } catch (_) {
            // ignore
          }
          try {
            recHalfEl.removeAttribute('data-proj-total');
            recHalfEl.removeAttribute('data-line-total');
          } catch (_) {
            // ignore
          }
        }
      } catch (_) {
        if (recHalfEl) recHalfEl.textContent = '1H: —';
        try {
          if (recHalfEl) {
            recHalfEl.classList.remove('bet', 'watch');
            recHalfEl.title = '';
          }
        } catch (_) {
          // ignore
        }
        try {
          recHalfEl.removeAttribute('data-proj-total');
          recHalfEl.removeAttribute('data-line-total');
        } catch (_) {
          // ignore
        }
      }

      // Quarter-level signal for the current regulation quarter (vs pregame quarter baseline)
      let qClass = 'NONE';
      let qSide = null;
      let qDiff = null;
      let qDiffRaw = null;
      let qShrink = null;
      let qPred = null;
      let qLabel = 'Q';
      let qLine = null;
      try {
        const pNow = (period == null) ? null : Number(period);
        if (recQtrEl && pNow != null && Number.isFinite(pNow) && pNow >= 1 && pNow <= 4) {
          const qNum = Math.floor(pNow);
          qLabel = `Q${qNum}`;
          const qCol = el.querySelector(`.lens-col[data-scope="q${qNum}"]`);
          const pf = qCol ? n(qCol.dataset.paceFinal) : null;
          const sf = qCol ? n(qCol.dataset.simFinal) : null;
          const ql = qCol ? n(qCol.dataset.liveTotal) : null;
          qLine = ql;

          // Suppress very-early quarter tags (tunable; scaled to scope length).
          let allowQ = true;
          try {
            const adjCfg = _liveLensScopeTotalAdjCfg(12);
            const minElapsedFull = n(adjCfg && adjCfg.min_elapsed_min);
            const minElapsedQ = (minElapsedFull != null) ? Math.max(0.0, minElapsedFull * (12.0 / 48.0)) : null;
            const elapsed = (secLeftPeriodRaw != null) ? (12.0 - Math.max(0, Math.min(12, Math.round((secLeftPeriodRaw / 60.0))))) : null;
            if (minElapsedQ != null && elapsed != null && elapsed < minElapsedQ) allowQ = false;
          } catch (_) {
            allowQ = true;
          }

          if (pf != null && ql != null) {
            qDiffRaw = pf - ql;
            qDiff = qDiffRaw;
            qPred = pf;

            // Confidence shrink based on possessions/time within the quarter.
            try {
              const possLive = qCol ? n(qCol.dataset.possLive) : null;
              const rem = (secLeftPeriodRaw != null) ? Math.max(0, Math.min(12, Math.round((secLeftPeriodRaw / 60.0)))) : null;
              const elapsed = (rem != null) ? (12.0 - rem) : null;
              qShrink = _liveLensEdgeShrink(qDiff, possLive, elapsed, 12);
              if (qShrink && qShrink.diff_shrunk != null) qDiff = n(qShrink.diff_shrunk);
            } catch (_) {
              // ignore
            }

            qClass = allowQ ? classifyDiff(Math.abs(qDiff), thr.quarter_total.watch, thr.quarter_total.bet) : 'NONE';
            if (qDiff > 1.0) qSide = 'Over';
            else if (qDiff < -1.0) qSide = 'Under';
            else qSide = 'No edge';
          } else if (pf != null && sf != null) {
            // Fallback: vs pregame quarter baseline when no live quarter line.
            qDiffRaw = pf - sf;
            qDiff = qDiffRaw;
            qPred = pf;

            // Confidence shrink based on possessions/time within the quarter.
            try {
              const possLive = qCol ? n(qCol.dataset.possLive) : null;
              const rem = (secLeftPeriodRaw != null) ? Math.max(0, Math.min(12, Math.round((secLeftPeriodRaw / 60.0)))) : null;
              const elapsed = (rem != null) ? (12.0 - rem) : null;
              qShrink = _liveLensEdgeShrink(qDiff, possLive, elapsed, 12);
              if (qShrink && qShrink.diff_shrunk != null) qDiff = n(qShrink.diff_shrunk);
            } catch (_) {
              // ignore
            }

            qClass = allowQ ? classifyDiff(Math.abs(qDiff), thr.quarter_total.watch, thr.quarter_total.bet) : 'NONE';
            if (qDiff > 1.0) qSide = 'Over';
            else if (qDiff < -1.0) qSide = 'Under';
            else qSide = 'No edge';
          }
          if (qClass === 'BET') recQtrEl.textContent = `${qLabel}: BET ${qSide} (${fmt(qDiff, 1)})`;
          else if (qClass === 'WATCH') recQtrEl.textContent = `${qLabel}: WATCH ${qSide} (${fmt(qDiff, 1)})`;
          else recQtrEl.textContent = `${qLabel}: —`;

          try {
            recQtrEl.classList.remove('bet', 'watch');
            if (qClass === 'BET') recQtrEl.classList.add('bet');
            else if (qClass === 'WATCH') recQtrEl.classList.add('watch');

            const why = [];
            if (qDiffRaw != null) why.push(`raw ${fmt(qDiffRaw, 1)}`);
            if (qDiff != null && qDiffRaw != null && Math.abs(qDiff - qDiffRaw) > 1e-6) why.push(`shrunk ${fmt(qDiff, 1)}`);
            if (qShrink && qShrink.lambda != null) why.push(`λ ${fmt(qShrink.lambda, 2)}`);
            if (qLine != null) why.push(`L ${fmt(qLine, 1)}`);
            why.push(`thr ${fmt(thr.quarter_total.watch, 1)}/${fmt(thr.quarter_total.bet, 1)}`);
            recQtrEl.title = why.filter(Boolean).join(' · ');
          } catch (_) {
            // ignore
          }

          try {
            const qProjOut = (qLine != null && qDiff != null) ? (qLine + qDiff) : qPred;
            if (qProjOut != null) recQtrEl.dataset.projTotal = String(qProjOut);
            else recQtrEl.removeAttribute('data-proj-total');
            if (qLine != null) recQtrEl.dataset.lineTotal = String(qLine);
            else recQtrEl.removeAttribute('data-line-total');
          } catch (_) {
            // ignore
          }
        } else if (recQtrEl) {
          recQtrEl.textContent = 'Q: —';
          try {
            recQtrEl.classList.remove('bet', 'watch');
            recQtrEl.title = '';
          } catch (_) {
            // ignore
          }
          try {
            recQtrEl.removeAttribute('data-proj-total');
            recQtrEl.removeAttribute('data-line-total');
          } catch (_) {
            // ignore
          }
        }
      } catch (_) {
        if (recQtrEl) recQtrEl.textContent = 'Q: —';
        try {
          if (recQtrEl) {
            recQtrEl.classList.remove('bet', 'watch');
            recQtrEl.title = '';
          }
        } catch (_) {
          // ignore
        }
        try {
          recQtrEl.removeAttribute('data-proj-total');
          recQtrEl.removeAttribute('data-line-total');
        } catch (_) {
          // ignore
        }
      }

      // Compute tags (ATS) using blended margin (pregame -> live)
      let atsClass = 'NONE';
      let atsText = 'ATS: —';
      let atsEdge = null;
      let atsPickHome = null;
      let atsSideKey = null;
      let atsLinePicked = null;
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
        atsPickHome = pickHome;
        atsEdge = pickHome ? homeEdge : awayEdge;
        const side = pickHome ? meta.home : meta.away;
        atsSideKey = side;
        atsLinePicked = pickHome ? effHomeSpr : (-effHomeSpr);
        atsClass = classifyDiff(Math.abs(atsEdge), thr.ats.watch, thr.ats.bet);
        if (atsClass === 'BET') atsText = `ATS: BET ${side} (${fmt(atsEdge, 1)})`;
        else if (atsClass === 'WATCH') atsText = `ATS: WATCH ${side} (${fmt(atsEdge, 1)})`;
      }
      if (recATSEl) recATSEl.textContent = atsText;
      try {
        if (recATSEl) {
          recATSEl.classList.remove('bet', 'watch');
          if (atsClass === 'BET') recATSEl.classList.add('bet');
          else if (atsClass === 'WATCH') recATSEl.classList.add('watch');
          const why = [];
          if (atsEdge != null) why.push(`edge ${fmt(atsEdge, 1)}`);
          if (effHomeSpr != null) why.push(`spr ${fmt(effHomeSpr, 1)}`);
          why.push(`thr ${fmt(thr.ats.watch, 1)}/${fmt(thr.ats.bet, 1)}`);
          recATSEl.title = why.filter(Boolean).join(' · ');
        }
      } catch (_) {
        // ignore
      }

      // Compute tags (ML) using sim win prob blended with live score state and betting MLs.
      let mlClass = 'NONE';
      let mlText = 'ML: —';
      let mlEdge = null;
      let mlSide = '';
      let mlPHomeModel = null;
      let mlPHomeImplied = null;
      let mlPAwayImplied = null;
      let mlCurMargin = null;
      let mlMinLeft = null;
      let mlPHomeScore = null;
      let mlScale = null;
      try {
        const pPregame = n(meta && meta.p_home_win != null ? meta.p_home_win : null);
        const curMargin = n(live && live.score ? live.score.home_margin : null);
        const minLeft = n(live && live.time ? live.time.game_min_left : null);
        const pHomeImplied = impliedProbFromAmer(effHomeMl);
        const pAwayImplied = impliedProbFromAmer(effAwayMl);

        mlCurMargin = curMargin;
        mlMinLeft = minLeft;
        mlPHomeImplied = pHomeImplied;
        mlPAwayImplied = pAwayImplied;

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
          mlPHomeScore = pHomeScore;
          mlScale = scale;
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
          mlPHomeModel = pHomeModel;
          const edgeHome = pHomeModel - pHomeImplied;
          const edgeAway = (1.0 - pHomeModel) - pAwayImplied;
          const pickHome = Math.abs(edgeHome) >= Math.abs(edgeAway);
          const edge = pickHome ? edgeHome : edgeAway;
          const side = pickHome ? meta.home : meta.away;
          mlEdge = edge;
          mlSide = side;
          mlClass = classifyDiff(Math.abs(edge), thr.ml.watch, thr.ml.bet);
          if (mlClass === 'BET') mlText = `ML: BET ${side} (${fmt(edge * 100.0, 1)}pp)`;
          else if (mlClass === 'WATCH') mlText = `ML: WATCH ${side} (${fmt(edge * 100.0, 1)}pp)`;
        }
      } catch (_) {
        mlClass = 'NONE';
        mlText = 'ML: —';
      }
      if (recMLEl) recMLEl.textContent = mlText;
      try {
        if (recMLEl) {
          recMLEl.classList.remove('bet', 'watch');
          if (mlClass === 'BET') recMLEl.classList.add('bet');
          else if (mlClass === 'WATCH') recMLEl.classList.add('watch');
          const why = [];
          if (mlEdge != null) why.push(`edge ${fmt(mlEdge * 100.0, 1)}pp`);
          if (mlPHomeModel != null) why.push(`pHome ${fmt(mlPHomeModel * 100.0, 1)}%`);
          if (mlPHomeImplied != null) why.push(`impHome ${fmt(mlPHomeImplied * 100.0, 1)}%`);
          if (mlCurMargin != null) why.push(`mgn ${fmt(mlCurMargin, 0)}`);
          if (mlMinLeft != null) why.push(`minLeft ${fmt(mlMinLeft, 1)}`);
          if (mlPHomeScore != null) why.push(`pScore ${fmt(mlPHomeScore * 100.0, 1)}%`);
          if (mlScale != null) why.push(`scale ${fmt(mlScale, 1)}`);
          why.push(`thr ${fmt(thr.ml.watch, 1)}/${fmt(thr.ml.bet, 1)}`);
          recMLEl.title = why.filter(Boolean).join(' · ');
        }
      } catch (_) {
        // ignore
      }

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
      // Player props tend to produce fewer "BET" classifications; default to logging WATCH+BET for player props unless overridden.
      const logModeProps = (logCfg && typeof logCfg.player_props_mode === 'string' && logCfg.player_props_mode.trim())
        ? logCfg.player_props_mode.trim().toLowerCase()
        : 'watch';
      const minIntervalSecRaw = n(logCfg && logCfg.min_interval_sec);
      const minIntervalSec = (minIntervalSecRaw != null && minIntervalSecRaw >= 5) ? minIntervalSecRaw : 60;
      const bucketKey = String(Math.floor((Date.now() / 1000.0) / minIntervalSec));

      async function maybeLog(market, klass, payload) {
        const isPlayerProp = (typeof market === 'string') && market.startsWith('player_prop:');
        const mode = isPlayerProp ? logModeProps : logMode;
        // Default parity: BET only. Optional modes for tuning.
        const allow = (mode === 'bet')
          ? (klass === 'BET')
          : (mode === 'watch')
            ? (klass === 'WATCH' || klass === 'BET')
            : (mode === 'all')
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

      async function maybeLogProj(key, klass, payload) {
        const isPlayerProp = (typeof key === 'string') && key.startsWith('player_prop:');
        const mode = isPlayerProp ? logModeProps : logMode;
        const allow = (mode === 'bet')
          ? (klass === 'BET')
          : (mode === 'watch')
            ? (klass === 'WATCH' || klass === 'BET')
            : (mode === 'all')
              ? (klass === 'NONE' || klass === 'WATCH' || klass === 'BET')
              : (klass === 'BET');
        if (!allow) return;
        const k = `${gid}:${key}`;
        const last = __liveLensLastProjLogged.get(k);
        if (last === bucketKey) return;
        __liveLensLastProjLogged.set(k, bucketKey);
        try {
          await postJson('/api/live_lens_projection', payload);
        } catch (_) {
          // ignore projection logging failures
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
        period: period,
        sec_left_period: secLeftPeriodRaw,
        margin_home: (live && live.score) ? n(live.score.home_margin) : null,
        tuning_source: 'api',
        schema_version: 2,
      };

      function getScopeLogContext(scopeName) {
        try {
          const col = el.querySelector(`.lens-col[data-scope="${scopeName}"]`);
          if (!col || !col.dataset) return null;
          const totEl = col.querySelector('input.lens-total');
          const liveEl = col.querySelector('input.lens-live');
          const minEl = col.querySelector('select.lens-min');
          return {
            scope_present: 1,
            scope: scopeName,
            scope_total_points: n(totEl && totEl.value != null ? totEl.value : null),
            scope_live_line: n(liveEl && liveEl.value != null && String(liveEl.value).trim() !== '' ? liveEl.value : null),
            scope_min_remaining: n(minEl && minEl.value != null ? minEl.value : null),
            sim_at: n(col.dataset.simAt),
            sim_final: n(col.dataset.simFinal),
            pace_final: n(col.dataset.paceFinal),
            pace_points: n(col.dataset.pacePoints),
            pace_poss: n(col.dataset.pacePoss),
            pace_alpha: n(col.dataset.paceAlpha),
            pace_blend_delta: n(col.dataset.paceBlendDelta),
            bet_projection: n(col.dataset.betProjection),
            delta_sim_minus_act: n(col.dataset.deltaSimMinusAct),
            poss_live: n(col.dataset.possLive),
            poss_expected_so_far: n(col.dataset.possExpectedSoFar),
            poss_expected_full: n(col.dataset.possExpectedFull),
            exp_ppp: n(col.dataset.expPpp),
            act_ppp: n(col.dataset.actPpp),
            pace_ratio: n(col.dataset.paceRatio),
            w_pace: n(col.dataset.wPace),
            interval_drift_adj: n(col.dataset.intervalDriftAdj),
            interval_drift_seg_idx: n(col.dataset.intervalDriftSegIdx),
            interval_drift_bias_points: n(col.dataset.intervalDriftBias),
            interval_drift_rem_frac: n(col.dataset.intervalDriftRemFrac),
            recent_window_sec: n(col.dataset.recentWindowSec),
            recent_window_poss: n(col.dataset.recentWindowPoss),
            recent_window_pts: n(col.dataset.recentWindowPts),
            recent_window_pace_ratio: n(col.dataset.recentWindowPaceRatio),
            recent_window_w: n(col.dataset.recentWindowW),
            recent_window_pace_adj: n(col.dataset.recentWindowPaceAdj),
            recent_window_eff_adj: n(col.dataset.recentWindowEffAdj),
            endgame_foul_adj: n(col.dataset.endgameFoulAdj),
            endgame_foul_w: n(col.dataset.endgameFoulW),
            endgame_foul_sec_left: n(col.dataset.endgameFoulSecLeft),
            endgame_foul_abs_margin: n(col.dataset.endgameFoulAbsMargin),
          };
        } catch (_) {
          return null;
        }
      }

      function buildScopeAdjustments(scopeCtx) {
        if (!scopeCtx || typeof scopeCtx !== 'object') return null;
        return {
          interval_drift_adj: scopeCtx.interval_drift_adj,
          interval_drift_seg_idx: scopeCtx.interval_drift_seg_idx,
          interval_drift_bias_points: scopeCtx.interval_drift_bias_points,
          interval_drift_rem_frac: scopeCtx.interval_drift_rem_frac,
          recent_window_sec: scopeCtx.recent_window_sec,
          recent_window_poss: scopeCtx.recent_window_poss,
          recent_window_pts: scopeCtx.recent_window_pts,
          recent_window_pace_ratio: scopeCtx.recent_window_pace_ratio,
          recent_window_w: scopeCtx.recent_window_w,
          recent_window_pace_adj: scopeCtx.recent_window_pace_adj,
          recent_window_eff_adj: scopeCtx.recent_window_eff_adj,
          endgame_foul_adj: scopeCtx.endgame_foul_adj,
          endgame_foul_w: scopeCtx.endgame_foul_w,
          endgame_foul_sec_left: scopeCtx.endgame_foul_sec_left,
          endgame_foul_abs_margin: scopeCtx.endgame_foul_abs_margin,
        };
      }

      const gameScopeLog = getScopeLogContext('game');
      const halfScopeLog = getScopeLogContext('half');

      // Player props (top edges; throttled)
      try {
        const pr = (livePlayerLens && typeof livePlayerLens === 'object') ? livePlayerLens.rows : null;
        const rows = Array.isArray(pr) ? pr : [];
        if (!isFinal && rows.length) {
          const scored = rows
            .filter((r) => r && r.player && r.stat && r.line != null && r.pace_vs_line != null)
            .map((r) => {
              const strength = Math.abs(n(r.pace_vs_line) ?? 0);
              const adj = adjustPlayerPropSignal(r, strength, thr.player_prop);
              return { r, strength, adj };
            })
            .filter((x) => x.strength != null && x.strength >= (thr.player_prop ? thr.player_prop.watch : 2.0))
            .sort((a, b) => {
              const ar = n(a && a.adj && a.adj.rank) ?? 0;
              const br = n(b && b.adj && b.adj.rank) ?? 0;
              if (ar !== br) return br - ar;
              const as = n(a && a.adj && a.adj.score) ?? 0;
              const bs = n(b && b.adj && b.adj.score) ?? 0;
              if (as !== bs) return bs - as;
              return (b.strength - a.strength);
            })
            .slice(0, 8);

          for (const it of scored) {
            const r = it.r;
            const strength = it.strength;
            const adj = it.adj;
            const klass = String(adj && adj.klass != null ? adj.klass : '').toUpperCase().trim() || classifyDiff(strength, thr.player_prop.watch, thr.player_prop.bet);
            const nameKey = normPlayerName((r.name_key != null) ? String(r.name_key) : String(r.player || ''));
            const statKey = String(r.stat || '').toLowerCase().trim();
            const throttleKey = `player_prop:${nameKey}:${statKey}`;
            const side = String(adj && adj.side != null ? adj.side : '').toUpperCase().trim()
              || ((r.lean != null && String(r.lean).trim()) ? String(r.lean).trim().toUpperCase() : ((n(r.pace_vs_line) > 0) ? 'OVER' : ((n(r.pace_vs_line) < 0) ? 'UNDER' : null)));

            await maybeLog(throttleKey, klass, {
              ...baseLog,
              klass,
              horizon: 'live',
              market: 'player_prop',
              elapsed: (curMinLeft != null) ? (48 - curMinLeft) : null,
              remaining: curMinLeft,
              signal_key: throttleKey,
              player: r.player,
              team_tri: r.team_tri,
              name_key: nameKey,
              stat: statKey,
              side,
              mp: n(r.mp),
              pf: n(r.pf),
              starter: (r.starter != null) ? !!r.starter : null,
              actual: n(r.actual),
              line: n(r.line),
              line_source: (r.line_source != null) ? String(r.line_source) : null,
              line_live: n(r.line_live),
              line_pregame: n(r.line_pregame),
              pace_proj: n(r.pace_proj),
              sim_mu: n(r.sim_mu),
              edge: n(r.pace_vs_line),
              edge_raw: n(r.pace_vs_line),
              edge_adj: n(r.pace_vs_line),
              strength,
              context: {
                sim_vs_line: n(r.sim_vs_line),
                sim_sd: n(r.sim_sd),
                exp_min: n(r.exp_min),
                exp_min_eff: n(r.exp_min_eff),
                exp_min_rot: n(r.exp_min_rot),
                proj_min_final: n(r.proj_min_final),
                rot_w: n(r.rot_w),
                rot_on_court: (r.rot_on_court != null) ? !!r.rot_on_court : null,
                rot_cur_on_sec: n(r.rot_cur_on_sec),
                rot_cur_off_sec: n(r.rot_cur_off_sec),
                rot_avg_stint_sec: n(r.rot_avg_stint_sec),
                rot_avg_rest_sec: n(r.rot_avg_rest_sec),
                injury_flag: (r.injury_flag != null) ? !!r.injury_flag : null,
                injury_gap: n(r.injury_gap),
                usage_window_sec: n(r._usage_window_sec),
                pace_mult: n(r.pace_mult),
                role_mult: n(r.role_mult),
                foul_mult: n(r.foul_mult),
                hot_cold_mult: n(r.hot_cold_mult),
                hot_ppp_recent: n(r.hot_ppp_recent),
                hot_ppp_game: n(r.hot_ppp_game),
                hot_p3_recent: n(r.hot_p3_recent),
                hot_p3_game: n(r.hot_p3_game),
                usg_recent: n(r.usg_recent),
                usg_game: n(r.usg_game),
                team_usg_recent: n(r.team_usg_recent),
                team_usg_game: n(r.team_usg_game),
                fg3a_recent: n(r.fg3a_recent),
                fg3a_game: n(r.fg3a_game),
                team_3a_recent: n(r.team_3a_recent),
                team_3a_game: n(r.team_3a_game),
                price_over: n(r.price_over),
                price_under: n(r.price_under),
                win_prob_over: n(r.win_prob_over),
                win_prob_under: n(r.win_prob_under),
                implied_prob_over: n(r.implied_prob_over),
                implied_prob_under: n(r.implied_prob_under),
                ev_side: (r.ev_side != null) ? String(r.ev_side) : null,
                win_prob: n(r.win_prob),
                implied_prob: n(r.implied_prob),
                ev: n(r.ev),
                adj_risk: n(adj && adj.risk),
                adj_support: n(adj && adj.support),
                adj_score: n(adj && adj.score),
              },
            });

            // Projection log (separate artifact; mirrors NCAAB signal/projection streams)
            await maybeLogProj(throttleKey, klass, {
              ...baseLog,
              klass,
              horizon: 'live',
              market: 'player_prop',
              elapsed: (curMinLeft != null) ? (48 - curMinLeft) : null,
              remaining: curMinLeft,
              proj_key: throttleKey,
              player: r.player,
              team_tri: r.team_tri,
              name_key: nameKey,
              stat: statKey,
              side,
              line: n(r.line),
              line_source: (r.line_source != null) ? String(r.line_source) : null,
              line_live: n(r.line_live),
              line_pregame: n(r.line_pregame),
              proj: n(r.pace_proj),
              sim_mu: n(r.sim_mu),
              win_prob_over: n(r.win_prob_over),
              win_prob_under: n(r.win_prob_under),
              implied_prob_over: n(r.implied_prob_over),
              implied_prob_under: n(r.implied_prob_under),
              price_over: n(r.price_over),
              price_under: n(r.price_under),
              strength,
            });
          }
        }
      } catch (_) {
        // ignore
      }

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
        pred: totalPred,
        strength: (totalDiff != null) ? Math.abs(totalDiff) : null,
        context: (totalCtx && typeof totalCtx === 'object') ? {
          thr_watch: (thr && thr.total) ? thr.total.watch : null,
          thr_bet: (thr && thr.total) ? thr.total.bet : null,
          pace_ratio: totalCtx.pace_ratio,
          eff_ppp_delta: totalCtx.eff_ppp_delta,
          poss_live: totalCtx.poss_live,
          poss_expected: totalCtx.poss_expected,
          elapsed_min: totalCtx.elapsed_min,
          under_edge_factor: totalCtx.under_edge_factor,
          under_edge_reversion_points: totalCtx.under_edge_reversion_points,
          endgame_foul_adj: totalCtx.endgame_foul_adj,
          endgame_foul_base_adj: totalCtx.endgame_foul_base_adj,
          endgame_foul_under_adj: totalCtx.endgame_foul_under_adj,
          endgame_foul_w: totalCtx.endgame_foul_w,
          endgame_foul_sec_left: totalCtx.endgame_foul_sec_left,
          endgame_foul_abs_margin: totalCtx.endgame_foul_abs_margin,
          bias_points: totalCtx.bias_points,
          bias_eff: totalCtx.bias_eff,
          bias_frac: totalCtx.bias_frac,
          edge_adj_unshrunk: totalCtx.edge_adj_unshrunk,
          edge_shrink_lambda: totalCtx.edge_shrink_lambda,
          edge_shrink_lambda_poss: totalCtx.edge_shrink_lambda_poss,
          edge_shrink_lambda_time: totalCtx.edge_shrink_lambda_time,
          exp_home_pace: meta.home_pace,
          exp_away_pace: meta.away_pace,
          exp_total_mean: meta.total_mean,
          scope_adjustments: buildScopeAdjustments(gameScopeLog),
          scope_context: gameScopeLog,
          pace_components: (possInfoForLog && typeof possInfoForLog === 'object') ? {
            pace_final: possInfoForLog.pace_final,
            pace_points: possInfoForLog.pace_points,
            pace_poss: possInfoForLog.pace_poss,
            pace_alpha: possInfoForLog.pace_alpha,
            poss_live: possInfoForLog.poss_live,
            poss_expected: possInfoForLog.poss_expected,
            pace_ratio: possInfoForLog.pace_ratio,
            elapsed_min: possInfoForLog.elapsed_min,
          } : (gameScopeLog ? {
            pace_final: gameScopeLog.pace_final,
            pace_points: gameScopeLog.pace_points,
            pace_poss: gameScopeLog.pace_poss,
            pace_alpha: gameScopeLog.pace_alpha,
            poss_live: gameScopeLog.poss_live,
            poss_expected: gameScopeLog.poss_expected_so_far,
            pace_ratio: gameScopeLog.pace_ratio,
          } : null),
        } : null,
      });

      if (totalPred != null) {
        await maybeLogProj('game_total', totalClass, {
          ...baseLog,
          klass: totalClass,
          horizon: 'game',
          market: 'total',
          proj_key: 'game_total',
          elapsed: (curMinLeft != null) ? (48 - curMinLeft) : null,
          remaining: curMinLeft,
          total_points: totalPts,
          line: shouldRound ? roundHalf(safeLineTotal) : safeLineTotal,
          live_line: shouldRound ? roundHalf(safeLineTotal) : safeLineTotal,
          side: totalSide,
          proj: totalPred,
          sim_mu: gameScopeLog ? gameScopeLog.sim_final : null,
          proj_points: gameScopeLog ? gameScopeLog.pace_points : null,
          proj_poss: gameScopeLog ? gameScopeLog.pace_poss : null,
          proj_alpha: gameScopeLog ? gameScopeLog.pace_alpha : null,
          strength: (totalDiff != null) ? Math.abs(totalDiff) : null,
          context: {
            exp_home_pace: meta.home_pace,
            exp_away_pace: meta.away_pace,
            exp_total_mean: meta.total_mean,
            scope_context: gameScopeLog,
            pace_components: (possInfoForLog && typeof possInfoForLog === 'object') ? {
              pace_final: possInfoForLog.pace_final,
              pace_points: possInfoForLog.pace_points,
              pace_poss: possInfoForLog.pace_poss,
              pace_alpha: possInfoForLog.pace_alpha,
              poss_live: possInfoForLog.poss_live,
              poss_expected: possInfoForLog.poss_expected,
              pace_ratio: possInfoForLog.pace_ratio,
              elapsed_min: possInfoForLog.elapsed_min,
            } : null,
          },
        });
      }

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
        edge_raw: halfDiffRaw,
        edge_adj: halfDiff,
        pred: halfPred,
        strength: (halfDiff != null) ? Math.abs(halfDiff) : null,
        context: (() => {
          const base = {
            thr_watch: (thr && thr.half_total) ? thr.half_total.watch : null,
            thr_bet: (thr && thr.half_total) ? thr.half_total.bet : null,
            edge_shrink_lambda: halfShrink ? halfShrink.lambda : null,
            edge_shrink_lambda_poss: halfShrink ? halfShrink.lambda_poss : null,
            edge_shrink_lambda_time: halfShrink ? halfShrink.lambda_time : null,
          };
          return halfScopeLog ? { ...base, ...halfScopeLog } : base;
        })(),
      });

      if (halfPred != null) {
        await maybeLogProj('h1_total', halfClass, {
          ...baseLog,
          klass: halfClass,
          horizon: 'h1',
          market: 'half_total',
          proj_key: 'h1_total',
          elapsed: (halfMinLeftRaw != null) ? (24 - Math.max(0, Math.min(24, Math.round(halfMinLeftRaw)))) : null,
          remaining: (halfMinLeftRaw != null) ? Math.max(0, Math.min(24, Math.round(halfMinLeftRaw))) : null,
          total_points: halfScopeLog ? halfScopeLog.scope_total_points : null,
          line: shouldRound
            ? roundHalf(sanitizeTotalLine(periodTotals && periodTotals.h1 != null ? n(periodTotals.h1) : null))
            : sanitizeTotalLine(periodTotals && periodTotals.h1 != null ? n(periodTotals.h1) : null),
          live_line: shouldRound
            ? roundHalf(sanitizeTotalLine(periodTotals && periodTotals.h1 != null ? n(periodTotals.h1) : null))
            : sanitizeTotalLine(periodTotals && periodTotals.h1 != null ? n(periodTotals.h1) : null),
          side: halfSide,
          proj: halfPred,
          sim_mu: halfScopeLog ? halfScopeLog.sim_final : null,
          proj_points: halfScopeLog ? halfScopeLog.pace_points : null,
          proj_poss: halfScopeLog ? halfScopeLog.pace_poss : null,
          proj_alpha: halfScopeLog ? halfScopeLog.pace_alpha : null,
          strength: (halfDiff != null) ? Math.abs(halfDiff) : null,
          context: halfScopeLog ? { scope_context: halfScopeLog } : null,
        });
      }

      // Current quarter total
      try {
        const pNow = (period == null) ? null : Number(period);
        const qNum = (pNow != null && Number.isFinite(pNow)) ? Math.floor(pNow) : null;
        if (qNum != null && qNum >= 1 && qNum <= 4) {
          const qKey = `q${qNum}`;
          const qScopeLog = getScopeLogContext(qKey);
          await maybeLog('q_total', qClass, {
            ...baseLog,
            klass: qClass,
            horizon: qKey,
            market: 'quarter_total',
            elapsed: (secLeftPeriodRaw != null) ? (12 - Math.max(0, Math.min(12, Math.round((secLeftPeriodRaw / 60.0))))) : null,
            remaining: (secLeftPeriodRaw != null) ? Math.max(0, Math.min(12, Math.round((secLeftPeriodRaw / 60.0)))) : null,
            total_points: null,
            live_line: shouldRound
              ? roundHalf(sanitizeTotalLine(periodTotals && periodTotals[qKey] != null ? n(periodTotals[qKey]) : null))
              : sanitizeTotalLine(periodTotals && periodTotals[qKey] != null ? n(periodTotals[qKey]) : null),
            side: qSide,
            edge: qDiff,
            edge_raw: qDiffRaw,
            edge_adj: qDiff,
            pred: qPred,
            strength: (qDiff != null) ? Math.abs(qDiff) : null,
            context: (() => {
              const base = {
                scope_present: qScopeLog ? 1 : 0,
                q_num: qNum,
                thr_watch: (thr && thr.quarter_total) ? thr.quarter_total.watch : null,
                thr_bet: (thr && thr.quarter_total) ? thr.quarter_total.bet : null,
                edge_shrink_lambda: qShrink ? qShrink.lambda : null,
                edge_shrink_lambda_poss: qShrink ? qShrink.lambda_poss : null,
                edge_shrink_lambda_time: qShrink ? qShrink.lambda_time : null,
              };
              return qScopeLog ? { ...base, ...qScopeLog } : base;
            })(),
          });

          if (qPred != null) {
            await maybeLogProj('q_total', qClass, {
              ...baseLog,
              klass: qClass,
              horizon: qKey,
              market: 'quarter_total',
              proj_key: `q_total:${qKey}`,
              elapsed: (secLeftPeriodRaw != null) ? (12 - Math.max(0, Math.min(12, Math.round((secLeftPeriodRaw / 60.0))))) : null,
              remaining: (secLeftPeriodRaw != null) ? Math.max(0, Math.min(12, Math.round((secLeftPeriodRaw / 60.0)))) : null,
              total_points: qScopeLog ? qScopeLog.scope_total_points : null,
              line: shouldRound
                ? roundHalf(sanitizeTotalLine(periodTotals && periodTotals[qKey] != null ? n(periodTotals[qKey]) : null))
                : sanitizeTotalLine(periodTotals && periodTotals[qKey] != null ? n(periodTotals[qKey]) : null),
              live_line: shouldRound
                ? roundHalf(sanitizeTotalLine(periodTotals && periodTotals[qKey] != null ? n(periodTotals[qKey]) : null))
                : sanitizeTotalLine(periodTotals && periodTotals[qKey] != null ? n(periodTotals[qKey]) : null),
              side: qSide,
              proj: qPred,
              sim_mu: qScopeLog ? qScopeLog.sim_final : null,
              proj_points: qScopeLog ? qScopeLog.pace_points : null,
              proj_poss: qScopeLog ? qScopeLog.pace_poss : null,
              proj_alpha: qScopeLog ? qScopeLog.pace_alpha : null,
              strength: (qDiff != null) ? Math.abs(qDiff) : null,
              context: qScopeLog ? { q_num: qNum, scope_context: qScopeLog } : { q_num: qNum },
            });
          }
        }
      } catch (_) {
        // ignore
      }

      // ATS
      const atsCtxForLog = (() => {
        try {
          const curMargin = n(live && live.score ? live.score.home_margin : null);
          const minLeft = n(live && live.time ? live.time.game_min_left : null);
          const elapsed = (minLeft != null) ? (48 - minLeft) : (lens ? lens.elapsedMinutes : 0);
          const w = Math.max(0, Math.min(1, (elapsed || 0) / 48.0));
          const adjMargin = (1 - w) * meta.margin_mean + w * (curMargin ?? 0);
          const homeEdge = adjMargin + effHomeSpr;
          const awayEdge = -adjMargin - effHomeSpr;
          const pickHome = Math.abs(homeEdge) >= Math.abs(awayEdge);
          return {
            thr_watch: (thr && thr.ats) ? thr.ats.watch : null,
            thr_bet: (thr && thr.ats) ? thr.ats.bet : null,
            spr_home: effHomeSpr,
            spr_home_raw: homeSpr,
            pregame_margin_mean: meta.margin_mean,
            cur_margin_home: curMargin,
            elapsed_min: elapsed,
            blend_w: w,
            adj_margin_home: adjMargin,
            edge_home: homeEdge,
            edge_away: awayEdge,
            pick_home: pickHome ? 1 : 0,
          };
        } catch (_) {
          return {
            thr_watch: (thr && thr.ats) ? thr.ats.watch : null,
            thr_bet: (thr && thr.ats) ? thr.ats.bet : null,
          };
        }
      })();

      await maybeLog('game_ats', atsClass, {
        ...baseLog,
        klass: atsClass,
        horizon: 'game',
        market: 'ats',
        elapsed: (curMinLeft != null) ? (48 - curMinLeft) : null,
        remaining: curMinLeft,
        total_points: totalPts,
        live_line: shouldRound
          ? roundHalf((atsLinePicked != null) ? atsLinePicked : homeSpr)
          : ((atsLinePicked != null) ? atsLinePicked : homeSpr),
        side: (atsClass === 'BET' || atsClass === 'WATCH') ? atsSideKey : null,
        edge: atsEdge,
        edge_raw: atsEdge,
        edge_adj: atsEdge,
        strength: (atsEdge != null) ? Math.abs(atsEdge) : null,
        context: atsCtxForLog,
      });

      if (atsEdge != null) {
        await maybeLogProj('game_ats', atsClass, {
          ...baseLog,
          klass: atsClass,
          horizon: 'game',
          market: 'ats',
          proj_key: 'game_ats',
          elapsed: (curMinLeft != null) ? (48 - curMinLeft) : null,
          remaining: curMinLeft,
          total_points: totalPts,
          line: shouldRound
            ? roundHalf((atsLinePicked != null) ? atsLinePicked : homeSpr)
            : ((atsLinePicked != null) ? atsLinePicked : homeSpr),
          live_line: shouldRound
            ? roundHalf((atsLinePicked != null) ? atsLinePicked : homeSpr)
            : ((atsLinePicked != null) ? atsLinePicked : homeSpr),
          side: (atsClass === 'BET' || atsClass === 'WATCH') ? atsSideKey : null,
          proj: (atsCtxForLog && atsCtxForLog.adj_margin_home != null) ? atsCtxForLog.adj_margin_home : null,
          sim_mu: meta.margin_mean,
          strength: Math.abs(atsEdge),
          context: atsCtxForLog,
        });
      }

      // ML
      const mlCtxForLog = {
        thr_watch: (thr && thr.ml) ? thr.ml.watch : null,
        thr_bet: (thr && thr.ml) ? thr.ml.bet : null,
        p_home_model: mlPHomeModel,
        p_home_implied: mlPHomeImplied,
        p_away_implied: mlPAwayImplied,
        cur_margin_home: mlCurMargin,
        game_min_left: mlMinLeft,
        p_home_score: mlPHomeScore,
        scale: mlScale,
      };

      await maybeLog('game_ml', mlClass, {
        ...baseLog,
        klass: mlClass,
        horizon: 'game',
        market: 'ml',
        elapsed: (curMinLeft != null) ? (48 - curMinLeft) : null,
        remaining: curMinLeft,
        total_points: totalPts,
        live_line: (mlSide === meta.home) ? effHomeMl : ((mlSide === meta.away) ? effAwayMl : null),
        side: (mlClass === 'BET' || mlClass === 'WATCH') ? mlSide : null,
        edge: mlEdge,
        edge_raw: mlEdge,
        edge_adj: mlEdge,
        strength: (mlEdge != null) ? Math.abs(mlEdge) : null,
        context: mlCtxForLog,
      });

      if (mlPHomeModel != null) {
        await maybeLogProj('game_ml', mlClass, {
          ...baseLog,
          klass: mlClass,
          horizon: 'game',
          market: 'ml',
          proj_key: 'game_ml',
          elapsed: (curMinLeft != null) ? (48 - curMinLeft) : null,
          remaining: curMinLeft,
          total_points: totalPts,
          line: (mlSide === meta.home) ? effHomeMl : ((mlSide === meta.away) ? effAwayMl : null),
          live_line: (mlSide === meta.home) ? effHomeMl : ((mlSide === meta.away) ? effAwayMl : null),
          side: (mlClass === 'BET' || mlClass === 'WATCH') ? mlSide : null,
          proj: mlPHomeModel,
          sim_mu: meta.p_home_win,
          strength: (mlEdge != null) ? Math.abs(mlEdge) : null,
          context: mlCtxForLog,
        });
      }
    }));

    // Live prop callouts: aggregate BET/WATCH player-prop signals across live games.
    try {
      const calloutsEl = root.querySelector('#live-prop-callouts');
      if (calloutsEl) {
        const hasAnyLiveGame = (() => {
          try {
            for (const s of (sbById && sbById.values ? sbById.values() : [])) {
              if (s && s.in_progress && !s.final) return true;
            }
            return false;
          } catch (_) {
            return false;
          }
        })();

        const invEventToGid = new Map();
        sbEventByGid.forEach((eid, gid) => {
          if (eid) invEventToGid.set(String(eid), String(gid));
        });

        const thrAll = getTuningThresholds();
        const thrPp = thrAll && thrAll.player_prop ? thrAll.player_prop : { watch: 2.0, bet: 4.0 };

        const cands = [];
        const seen = new Set();

        for (const [eid, gg] of (playerLensMap && playerLensMap.entries ? playerLensMap.entries() : [])) {
          const gid = invEventToGid.get(String(eid));
          if (!gid) continue;
          const s = sbById.get(gid);
          if (!s || !s.in_progress || s.final) continue;
          const meta = byGameId.get(gid);
          if (!meta) continue;

          const asof = (gg && gg.live_prop_lines && gg.live_prop_lines.generated_at)
            ? String(gg.live_prop_lines.generated_at)
            : '';

          const rows = Array.isArray(gg && gg.rows) ? gg.rows : [];
          for (const r of rows) {
            try {
              if (!r) continue;

              // Prefer live line when available; fall back to pregame line so the
              // callouts don't disappear when OddsAPI live prop lines are missing.
              const lineLive = n(r.line_live);
              const lineBase = n((r.line != null) ? r.line : r.line_pregame);
              const lineUsed = (lineLive != null) ? lineLive : lineBase;
              if (lineUsed == null) continue;

              const paceProj = n(r.pace_proj);
              const dP0 = n(r.pace_vs_line);
              const dP = (paceProj != null) ? (paceProj - lineUsed) : dP0;
              if (dP == null) continue;

              const strength = Math.abs(dP);
              const rForAdj = (() => {
                try {
                  const isLiveLine = (lineLive != null);
                  const src = isLiveLine
                    ? 'oddsapi'
                    : ((r && r.line_source != null && String(r.line_source).trim()) ? String(r.line_source).trim() : 'pregame');
                  return {
                    ...r,
                    line: lineUsed,
                    line_source: src,
                    pace_vs_line: dP,
                    is_live_line: isLiveLine,
                    line_live: lineLive,
                  };
                } catch (_) {
                  return r;
                }
              })();
              const adj = adjustPlayerPropSignal(rForAdj, strength, thrPp);
              const guidance = buildLivePropGuidance(rForAdj, adj, thrPp);
              const klass = String(adj && adj.klass != null ? adj.klass : '').toUpperCase().trim();
              if (klass !== 'BET' && klass !== 'WATCH') continue;
              if (guidance && guidance.action_code === 'pass') continue;

              const player = String(r.player || '').trim();
              const stat = String(r.stat || '').toLowerCase().trim();
              const key = `${gid}|${player}|${stat}`;
              if (seen.has(key)) continue;
              seen.add(key);

              const mp = n(r.mp);
              const projMin = n(r.proj_min_final != null ? r.proj_min_final : (r.exp_min_eff != null ? r.exp_min_eff : r.exp_min));
              const remMin = (mp != null && projMin != null) ? (projMin - mp) : null;
              const rotOn = (r && r.rot_on_court != null) ? !!r.rot_on_court : null;
              const offSec = n(r.rot_cur_off_sec);
              const benchLong = (rotOn === false && offSec != null && offSec >= 480);

              const simMu = n(r.sim_mu);
              const dS = (simMu != null) ? (simMu - lineUsed) : null;

              cands.push({
                gid,
                home: meta.home,
                away: meta.away,
                team: String(r.team_tri || '').toUpperCase().trim(),
                player,
                player_id: n(r.player_id),
                player_photo: (r && r.player_photo) ? String(r.player_photo) : '',
                stat,
                line: lineUsed,
                line_live: lineLive,
                line_pregame: n(r.line_pregame),
                is_live_line: (lineLive != null),
                actual: n(r.actual),
                pace_proj: (paceProj != null) ? paceProj : null,
                dP,
                dS,
                klass,
                side: String(adj && adj.side != null ? adj.side : ''),
                rank: n(adj && adj.rank) ?? 0,
                score: n(adj && adj.score) ?? 0,
                guidance,
                simDisagree: !!(adj && adj.sim_disagree),
                simAgree: !!(adj && adj.sim_agree),
                asof,
                injury_flag: !!(r && r.injury_flag),
                pf: n(r.pf),
                rem_min: remMin,
                bench_long: benchLong,
              });
            } catch (_) {
              // ignore
            }
          }
        }

        cands.sort((a, b) => {
          const ag = n(a && a.guidance && a.guidance.rank) ?? 0;
          const bg = n(b && b.guidance && b.guidance.rank) ?? 0;
          if (ag !== bg) return bg - ag;
          const ar = n(a.rank) ?? 0;
          const br = n(b.rank) ?? 0;
          if (ar !== br) return br - ar;
          const as = n(a.score) ?? 0;
          const bs = n(b.score) ?? 0;
          if (as !== bs) return bs - as;
          const ae = Math.abs(n(a.dP) ?? 0);
          const be = Math.abs(n(b.dP) ?? 0);
          if (ae !== be) return be - ae;
          return 0;
        });

        const html = renderLivePropCallouts(cands);

        if (html) {
          __calloutsEmptyStreak = 0;
          __calloutsLastHtml = html;
          __calloutsLastNonEmptyAt = Date.now();
          calloutsEl.innerHTML = html;
          calloutsEl.classList.remove('hidden');
          try { applyPlayerLensFiltersAll(root); } catch (_) { /* ignore */ }
        } else {
          __calloutsEmptyStreak += 1;
          const ageMs = (__calloutsLastNonEmptyAt > 0) ? (Date.now() - __calloutsLastNonEmptyAt) : 1e18;

          // Keep last non-empty render for a short time to avoid flicker.
          // Hide only after a few consecutive empties or when the last non-empty is old.
          const allowKeep = (__calloutsLastHtml && ageMs < 45000 && __calloutsEmptyStreak < 3);
          if (allowKeep) {
            calloutsEl.innerHTML = __calloutsLastHtml;
            calloutsEl.classList.remove('hidden');
            try { applyPlayerLensFiltersAll(root); } catch (_) { /* ignore */ }
          } else {
            __calloutsLastHtml = '';
            if (hasAnyLiveGame) {
              calloutsEl.innerHTML = '<div class="subtle" style="margin-top:8px;">Live props signal + action guide: no signals yet (or player props not loaded).</div>';
              calloutsEl.classList.remove('hidden');
            } else {
              calloutsEl.innerHTML = '';
              calloutsEl.classList.add('hidden');
            }
          }
        }
      }
    } catch (_) {
      // ignore
    }

    // Re-run strip update after signals/classes may have changed.
    updateScoreboardStrip(sbById);
    updateCompactCardTags(sbById);
  }

  // Kick off immediately, then every 20s to match the live props target cadence.
  pollOnce();
  __liveLensTimer = setInterval(pollOnce, LIVE_PROPS_POLL_INTERVAL_MS);
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

      const homeName = String(g.home_name || homeTri).trim();
      const awayName = String(g.away_name || awayTri).trim();

      const recon = showResults ? (reconIndex.get(`${homeTri}|${awayTri}`) || reconIndex.get(`${String(g.home_name || '').trim()}|${String(g.away_name || '').trim()}`)) : null;
      const actualHome = recon ? n(recon.home_pts) : null;
      const actualAway = recon ? n(recon.visitor_pts) : null;

      const odds = g.odds || {};
      const timeStr = fmtTime(odds.commence_time);
      const statusTxt = (actualHome != null && actualAway != null) ? 'Final' : (timeStr || '—');
      const awayScoreTxt = (actualHome != null && actualAway != null) ? String(actualAway) : '';
      const homeScoreTxt = (actualHome != null && actualAway != null) ? String(actualHome) : '';

      return `
        <button type="button" class="s-item neu" data-game-id="${esc(gid)}" aria-label="Jump to ${esc(awayTri)} at ${esc(homeTri)}">
          <span class="s-top">
            <span class="s-status">${esc(statusTxt)}</span>
            <span class="s-tag"></span>
          </span>

          <span class="s-rows">
            <span class="s-row">
              <span class="s-team away">${logoImg(awayTri)}<span class="s-name" title="${esc(awayName)}">${esc(awayName)}</span></span>
              <span class="s-score-away" aria-label="Away score">${esc(awayScoreTxt)}</span>
            </span>
            <span class="s-row">
              <span class="s-team home">${logoImg(homeTri)}<span class="s-name" title="${esc(homeName)}">${esc(homeName)}</span></span>
              <span class="s-score-home" aria-label="Home score">${esc(homeScoreTxt)}</span>
            </span>
          </span>

          <span class="s-teams" style="display:none;">${esc(awayTri)} @ ${esc(homeTri)}</span>
          <span class="s-mid" style="display:none;"><span class="s-score">${esc(awayScoreTxt && homeScoreTxt ? `${awayScoreTxt}-${homeScoreTxt}` : '')}</span></span>
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

    const hasReconActual = !!(
      (actualHome != null && actualAway != null)
      || (homeRecon && Object.keys(homeRecon).length)
      || (awayRecon && Object.keys(awayRecon).length)
    );
    const mergedBoxscoreMeta = {
      home: homeTri,
      away: awayTri,
      sim_score: score,
      sim_periods: periods,
      sim_players_home: playersHome,
      sim_players_away: playersAway,
      missing_prop_players_home: (sim && sim.missing_prop_players && Array.isArray(sim.missing_prop_players.home)) ? sim.missing_prop_players.home : [],
      missing_prop_players_away: (sim && sim.missing_prop_players && Array.isArray(sim.missing_prop_players.away)) ? sim.missing_prop_players.away : [],
    };
    const initialBoxscoreSource = hasReconActual
      ? {
          mode: 'recon',
          label: 'Actual',
          periods: [],
          actualHome,
          actualAway,
          homeRows: homeRecon || {},
          awayRows: awayRecon || {},
        }
      : {
          mode: 'none',
          label: '',
          periods: [],
          actualHome,
          actualAway,
          homeRows: null,
          awayRows: null,
        };

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
          <span class="venue lens-card-tag" title="Live Lens (current scope)"></span>
          <span class="lens-why-badges" title="Live Lens why (current scope)"></span>
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
          <summary class="players-toggle cursor-pointer">Boxscore (sim vs live/final) ${boxscoreReconBadge} ${playerReconBadge}</summary>
          <div class="merged-boxscore-body" data-game-id="${esc(gid)}" data-actual-source="${esc(initialBoxscoreSource.mode)}">
            ${renderMergedBoxscoreSection(mergedBoxscoreMeta, initialBoxscoreSource)}
          </div>
          ${renderInjurySummary(`HOME (${homeTri})`, (g && g.sim && g.sim.injuries && g.sim.injuries.home) ? g.sim.injuries.home : playersHome)}
          <div class="mb-6"></div>
          ${renderInjurySummary(`AWAY (${awayTri})`, (g && g.sim && g.sim.injuries && g.sim.injuries.away) ? g.sim.injuries.away : playersAway)}
        </details>

        <details class="writeup-block">
          <summary class="writeup-toggle cursor-pointer">Recommended props (sim vs line)</summary>
          ${renderPropRecommendations(g.prop_recommendations, homeTri, awayTri)}
        </details>
      </section>
    `;
  }).join('\n');

  const gamePillItemsHtml = (() => {
    try {
      return games.map((g) => {
        const gid = canonGameId((g && g.sim && g.sim.game_id != null) ? g.sim.game_id : (g && g.game_id != null ? g.game_id : ''))
          || `${String(g.home_tri || '').toUpperCase().trim()}_${String(g.away_tri || '').toUpperCase().trim()}`;
        const homeTri = String(g.home_tri || '').toUpperCase().trim();
        const awayTri = String(g.away_tri || '').toUpperCase().trim();
        const txt = `${awayTri} @ ${homeTri}`.trim();
        return `<button type="button" class="chip neutral" data-game-id="${esc(gid)}" aria-pressed="false">${esc(txt)}</button>`;
      }).join('');
    } catch (_) {
      return '';
    }
  })();

  const pregameGamePillsHtml = (() => {
    try {
      return `
        <div class="row chips" id="pregame-game-filter-pills" style="margin-top:8px; flex-wrap:wrap;">
          <span class="chip title">Pregame Games</span>
          <button type="button" class="chip neutral" data-pregame-game-pill-all="1" aria-pressed="false">ALL</button>
          ${gamePillItemsHtml.replaceAll('class="chip neutral"', 'class="chip neutral pregame-game-pill"')}
        </div>
      `;
    } catch (_) {
      return '';
    }
  })();

  const pregamePlayerPropPillsHtml = (() => {
    try {
      return `
        <div class="row chips" id="pregame-player-prop-filter-pills" style="margin-top:8px; flex-wrap:wrap;">
          <span class="chip title">Pregame Props</span>
          <button type="button" class="chip neutral" data-pregame-player-prop-pill-all="1" aria-pressed="false">ALL</button>
          <button type="button" class="chip neutral pregame-player-prop-pill" data-kind="stat" data-key="pts" aria-pressed="false">PTS</button>
          <button type="button" class="chip neutral pregame-player-prop-pill" data-kind="stat" data-key="reb" aria-pressed="false">REB</button>
          <button type="button" class="chip neutral pregame-player-prop-pill" data-kind="stat" data-key="ast" aria-pressed="false">AST</button>
          <button type="button" class="chip neutral pregame-player-prop-pill" data-kind="stat" data-key="threes" aria-pressed="false">3PM</button>
          <button type="button" class="chip neutral pregame-player-prop-pill" data-kind="stat" data-key="stl" aria-pressed="false">STL</button>
          <button type="button" class="chip neutral pregame-player-prop-pill" data-kind="stat" data-key="blk" aria-pressed="false">BLK</button>
          <button type="button" class="chip neutral pregame-player-prop-pill" data-kind="stat" data-key="tov" aria-pressed="false">TOV</button>
          <button type="button" class="chip neutral pregame-player-prop-pill" data-kind="stat" data-key="pra" aria-pressed="false">PRA</button>
          <span class="chip title" style="margin-left:6px;">Side</span>
          <button type="button" class="chip neutral pregame-player-prop-pill" data-kind="side" data-key="OVER" aria-pressed="false">OVER</button>
          <button type="button" class="chip neutral pregame-player-prop-pill" data-kind="side" data-key="UNDER" aria-pressed="false">UNDER</button>
        </div>
      `;
    } catch (_) {
      return '';
    }
  })();

  const liveGamePillsHtml = (() => {
    try {
      return `
        <div class="row chips" id="live-game-filter-pills" style="margin-top:10px; flex-wrap:wrap;">
          <span class="chip title">Live Games</span>
          <button type="button" class="chip neutral" data-live-game-pill-all="1" aria-pressed="false">ALL</button>
          ${gamePillItemsHtml.replaceAll('class="chip neutral"', 'class="chip neutral live-game-pill"')}
        </div>
      `;
    } catch (_) {
      return '';
    }
  })();

  const livePlayerPropPillsHtml = (() => {
    try {
      return `
        <div class="row chips" id="live-player-prop-filter-pills" style="margin-top:8px; flex-wrap:wrap;">
          <span class="chip title">Live Props</span>
          <button type="button" class="chip neutral" data-live-player-prop-pill-all="1" aria-pressed="false">ALL</button>
          <button type="button" class="chip neutral live-player-prop-pill" data-kind="stat" data-key="pts" aria-pressed="false">PTS</button>
          <button type="button" class="chip neutral live-player-prop-pill" data-kind="stat" data-key="reb" aria-pressed="false">REB</button>
          <button type="button" class="chip neutral live-player-prop-pill" data-kind="stat" data-key="ast" aria-pressed="false">AST</button>
          <button type="button" class="chip neutral live-player-prop-pill" data-kind="stat" data-key="threes" aria-pressed="false">3PM</button>
          <button type="button" class="chip neutral live-player-prop-pill" data-kind="stat" data-key="stl" aria-pressed="false">STL</button>
          <button type="button" class="chip neutral live-player-prop-pill" data-kind="stat" data-key="blk" aria-pressed="false">BLK</button>
          <button type="button" class="chip neutral live-player-prop-pill" data-kind="stat" data-key="tov" aria-pressed="false">TOV</button>
          <button type="button" class="chip neutral live-player-prop-pill" data-kind="stat" data-key="pra" aria-pressed="false">PRA</button>
          <span class="chip title" style="margin-left:6px;">Side</span>
          <button type="button" class="chip neutral live-player-prop-pill" data-kind="side" data-key="OVER" aria-pressed="false">OVER</button>
          <button type="button" class="chip neutral live-player-prop-pill" data-kind="side" data-key="UNDER" aria-pressed="false">UNDER</button>
          <span class="chip title" style="margin-left:6px;">Line</span>
          <button type="button" class="chip neutral live-player-prop-pill" data-kind="line" data-key="live" aria-pressed="false" title="OddsAPI live line exists">Live line exists</button>
          <span class="chip title" style="margin-left:6px;">Signal</span>
          <button type="button" class="chip neutral live-player-prop-pill" data-kind="sig" data-key="BET" aria-pressed="false">BET</button>
          <button type="button" class="chip neutral live-player-prop-pill" data-kind="sig" data-key="WATCH" aria-pressed="false">WATCH</button>
        </div>
      `;
    } catch (_) {
      return '';
    }
  })();

  root.innerHTML = `${stripHtml}\n${pregameGamePillsHtml}\n${pregamePlayerPropPillsHtml}\n<div id="pregame-prop-callouts" class="hidden"></div>\n${liveGamePillsHtml}\n${livePlayerPropPillsHtml}\n<div id="live-prop-callouts" class="hidden"></div>\n${html}`;

  try {
    if (root && root.dataset && root.dataset.pregameGamePillsBound !== '1') {
      root.dataset.pregameGamePillsBound = '1';
      root.addEventListener('click', (ev) => {
        try {
          const allBtn = ev.target && ev.target.closest ? ev.target.closest('button[data-pregame-game-pill-all]') : null;
          if (allBtn) {
            __pregameGamePillsSelected.clear();
            applyPregamePropFilters(root);
            return;
          }
          const btn = ev.target && ev.target.closest ? ev.target.closest('button.pregame-game-pill[data-game-id]') : null;
          if (!btn) return;
          const gid = canonGameId(btn.dataset.gameId);
          if (!gid) return;
          if (__pregameGamePillsSelected.has(gid)) __pregameGamePillsSelected.delete(gid);
          else __pregameGamePillsSelected.add(gid);
          applyPregamePropFilters(root);
        } catch (_) {
          // ignore
        }
      });
    }
  } catch (_) {
    // ignore
  }

  try {
    if (root && root.dataset && root.dataset.pregamePropPillsBound !== '1') {
      root.dataset.pregamePropPillsBound = '1';
      root.addEventListener('click', (ev) => {
        try {
          const allBtn = ev.target && ev.target.closest ? ev.target.closest('button[data-pregame-player-prop-pill-all]') : null;
          if (allBtn) {
            try { __pregamePropFilters.stats.clear(); } catch (_) { /* ignore */ }
            try { __pregamePropFilters.sides.clear(); } catch (_) { /* ignore */ }
            applyPregamePropFilters(root);
            return;
          }

          const btn = ev.target && ev.target.closest
            ? ev.target.closest('button.pregame-player-prop-pill[data-kind][data-key]')
            : null;
          if (!btn) return;
          const kind = String(btn.dataset.kind || '').toLowerCase().trim();
          const rawKey = String(btn.dataset.key || '').trim();
          if (!rawKey) return;

          if (kind === 'stat') {
            const key = rawKey.toLowerCase();
            if (__pregamePropFilters.stats.has(key)) __pregamePropFilters.stats.delete(key);
            else __pregamePropFilters.stats.add(key);
          } else if (kind === 'side') {
            const key = rawKey.toUpperCase();
            if (__pregamePropFilters.sides.has(key)) __pregamePropFilters.sides.delete(key);
            else __pregamePropFilters.sides.add(key);
          } else {
            return;
          }

          applyPregamePropFilters(root);
        } catch (_) {
          // ignore
        }
      });
    }
    applyPregamePropFilters(root);
  } catch (_) {
    // ignore
  }

  // Live game pills click handler
  try {
    if (root && root.dataset && root.dataset.gamePillsBound !== '1') {
      root.dataset.gamePillsBound = '1';
      root.addEventListener('click', (ev) => {
        try {
          const allBtn = ev.target && ev.target.closest ? ev.target.closest('button[data-live-game-pill-all]') : null;
          if (allBtn) {
            __gamePillsSelected.clear();
            applyGamePillsFilter(root);
            return;
          }
          const btn = ev.target && ev.target.closest ? ev.target.closest('button.live-game-pill[data-game-id]') : null;
          if (!btn) return;
          const gid = canonGameId(btn.dataset.gameId);
          if (!gid) return;
          if (__gamePillsSelected.has(gid)) __gamePillsSelected.delete(gid);
          else __gamePillsSelected.add(gid);
          applyGamePillsFilter(root);
        } catch (_) {
          // ignore
        }
      });
    }
    applyGamePillsFilter(root);
  } catch (_) {
    // ignore
  }

  // Live player prop pills click handler
  try {
    if (root && root.dataset && root.dataset.playerPropPillsBound !== '1') {
      root.dataset.playerPropPillsBound = '1';
      root.addEventListener('click', (ev) => {
        try {
          const allBtn = ev.target && ev.target.closest ? ev.target.closest('button[data-live-player-prop-pill-all]') : null;
          if (allBtn) {
            try { __playerLensGlobalFilters.stats.clear(); } catch (_) { /* ignore */ }
            try { __playerLensGlobalFilters.signals.clear(); } catch (_) { /* ignore */ }
            try { __playerLensGlobalFilters.sides.clear(); } catch (_) { /* ignore */ }
            try { __playerLensGlobalFilters.liveLineOnly = false; } catch (_) { /* ignore */ }
            applyPlayerLensFiltersAll(root);
            return;
          }

          const btn = ev.target && ev.target.closest
            ? ev.target.closest('button.live-player-prop-pill[data-kind][data-key]')
            : null;
          if (!btn) return;
          const kind = String(btn.dataset.kind || '').toLowerCase().trim();
          const rawKey = String(btn.dataset.key || '').trim();
          if (!rawKey) return;

          if (kind === 'stat') {
            const key = rawKey.toLowerCase();
            if (__playerLensGlobalFilters.stats.has(key)) __playerLensGlobalFilters.stats.delete(key);
            else __playerLensGlobalFilters.stats.add(key);
          } else if (kind === 'side') {
            const key = rawKey.toUpperCase();
            if (__playerLensGlobalFilters.sides.has(key)) __playerLensGlobalFilters.sides.delete(key);
            else __playerLensGlobalFilters.sides.add(key);
          } else if (kind === 'sig') {
            const key = rawKey.toUpperCase();
            if (__playerLensGlobalFilters.signals.has(key)) __playerLensGlobalFilters.signals.delete(key);
            else __playerLensGlobalFilters.signals.add(key);
          } else if (kind === 'line' && rawKey.toLowerCase() === 'live') {
            __playerLensGlobalFilters.liveLineOnly = !__playerLensGlobalFilters.liveLineOnly;
          } else {
            return;
          }

          applyPlayerLensFiltersAll(root);
        } catch (_) {
          // ignore
        }
      });
    }
    applyPlayerLensFiltersAll(root);
  } catch (_) {
    // ignore
  }

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

  // Pregame prop movement callouts click-to-scroll
  try {
    const callouts = root.querySelector('#pregame-prop-callouts');
    if (callouts && !callouts.dataset.bound) {
      callouts.dataset.bound = '1';
      callouts.addEventListener('click', (ev) => {
        const btn = ev.target && ev.target.closest ? ev.target.closest('.prop-callout[data-game-id]') : null;
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

  // Live prop callouts click-to-scroll
  try {
    const callouts = root.querySelector('#live-prop-callouts');
    if (callouts && !callouts.dataset.bound) {
      callouts.dataset.bound = '1';
      callouts.addEventListener('click', (ev) => {
        const btn = ev.target && ev.target.closest ? ev.target.closest('.prop-callout[data-game-id]') : null;
        if (!btn) return;
        const gid = canonGameId(btn.dataset.gameId);
        if (!gid) return;
        const target = root.querySelector(`.card[data-game-id="${CSS.escape(gid)}"]`) || document.getElementById(`game-${gid}`);
        if (!target) return;
        try { target.scrollIntoView({ behavior: 'smooth', block: 'start' }); } catch (_) { target.scrollIntoView(); }
        try {
          const det = target.querySelector('details.players-block');
          if (det) det.open = true;
        } catch (_) {
          // ignore
        }
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

  // Pregame movement callouts (one-shot load)
  try {
    Promise.resolve(loadPregamePropCallouts(root, games, dateStr)).catch(() => { /* ignore */ });
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
  setNote('Loading cards…');
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
    const resolvedDate = isYmd(payload?.date) ? payload.date : dateStr;
    const usingLookAhead = resolvedDate !== dateStr;
    const games = Array.isArray(payload?.games) ? payload.games : [];
    if (usingLookAhead) {
      try {
        setDatePickerYmd(document.getElementById('datePicker'), resolvedDate);
        setUrlDate(resolvedDate);
        const q = encodeURIComponent(resolvedDate);
        const marketAccuracyBtn = document.getElementById('marketAccuracyBtn');
        const liveGameLensAccuracyBtn = document.getElementById('liveGameLensAccuracyBtn');
        const livePlayerPropsLensAccuracyBtn = document.getElementById('livePlayerPropsLensAccuracyBtn');
        if (marketAccuracyBtn) marketAccuracyBtn.setAttribute('href', `/accuracy-market?date=${q}`);
        if (liveGameLensAccuracyBtn) liveGameLensAccuracyBtn.setAttribute('href', `/live-game-lens-accuracy?date=${q}`);
        if (livePlayerPropsLensAccuracyBtn) livePlayerPropsLensAccuracyBtn.setAttribute('href', `/live-player-props-lens-accuracy?date=${q}`);
      } catch (_) {
        // ignore
      }
    }
    setNote(usingLookAhead ? `Rendering ${games.length} games for ${resolvedDate}…` : `Rendering ${games.length} games…`);

    let reconGameRows = [];
    let reconQuarterRows = [];
    let reconPlayerRows = [];
    if (showResults) {
      const [csvG, csvQ, csvP] = await Promise.all([
        fetchText(`/api/processed/recon_games?date=${encodeURIComponent(resolvedDate)}`),
        fetchText(`/api/processed/recon_quarters?date=${encodeURIComponent(resolvedDate)}`),
        fetchText(`/api/processed/recon_players?date=${encodeURIComponent(resolvedDate)}`),
      ]);
      reconGameRows = csvG ? csvParse(csvG) : [];
      reconQuarterRows = csvQ ? csvParse(csvQ) : [];
      reconPlayerRows = csvP ? csvParse(csvP) : [];
    }

    renderCards(games, reconGameRows, reconQuarterRows, reconPlayerRows, showResults, hideOdds, resolvedDate);
    setNote(usingLookAhead ? `Showing ${resolvedDate} look-ahead slate.` : '');
  } catch (e) {
    setNote(`Failed to load cards: ${String(e && e.message ? e.message : e)}`);
    try {
      renderCards([], [], [], [], false, false, dateStr);
    } catch (_) {
      // ignore
    }
  }
}

function setUrlDate(dateStr) {
  const u = new URL(window.location.href);
  u.searchParams.set('date', dateStr);
  window.history.replaceState({}, '', u.toString());
}

function setDatePickerYmd(datePicker, ymd) {
  if (!datePicker) return;
  datePicker.value = ymd;
  if (datePicker.value) return;
  try {
    const parts = String(ymd || '').split('-').map((x) => Number(x));
    const y = parts[0], m = parts[1], d = parts[2];
    if (Number.isFinite(y) && Number.isFinite(m) && Number.isFinite(d)) {
      datePicker.valueAsDate = new Date(y, m - 1, d);
    }
  } catch (_) {
    // ignore
  }
}

window.addEventListener('DOMContentLoaded', () => {
  const datePicker = document.getElementById('datePicker');
  const applyBtn = document.getElementById('applyBtn');
  const todayBtn = document.getElementById('todayBtn');
  const resultsToggle = document.getElementById('resultsToggle');
  const hideOddsToggle = document.getElementById('hideOdds');
  const marketAccuracyBtn = document.getElementById('marketAccuracyBtn');
  const liveGameLensAccuracyBtn = document.getElementById('liveGameLensAccuracyBtn');
  const livePlayerPropsLensAccuracyBtn = document.getElementById('livePlayerPropsLensAccuracyBtn');

  const u = new URL(window.location.href);
  const qd = u.searchParams.get('date');
  const d0 = isYmd(qd) ? qd : localYMD();
  setDatePickerYmd(datePicker, d0);

  function updateAccuracyLinks(d) {
    try {
      const ds = (isYmd(d) ? d : (datePicker && datePicker.value) ? datePicker.value : localYMD());
      const q = encodeURIComponent(ds);
      if (marketAccuracyBtn) marketAccuracyBtn.setAttribute('href', `/accuracy-market?date=${q}`);
      if (liveGameLensAccuracyBtn) liveGameLensAccuracyBtn.setAttribute('href', `/live-game-lens-accuracy?date=${q}`);
      if (livePlayerPropsLensAccuracyBtn) livePlayerPropsLensAccuracyBtn.setAttribute('href', `/live-player-props-lens-accuracy?date=${q}`);
    } catch (_) {
      // ignore
    }
  }
  updateAccuracyLinks(d0);

  function apply() {
    const d = (datePicker && datePicker.value) ? datePicker.value : localYMD();
    updateAccuracyLinks(d);
    setUrlDate(d);
    setNote('Loading…');
    Promise.resolve(load(d)).catch((e) => {
      try {
        setNote(`Failed to load cards: ${String(e && e.message ? e.message : e)}`);
      } catch (_) {
        setNote('Failed to load cards');
      }
    });
  }

  if (applyBtn) applyBtn.addEventListener('click', apply);
  if (todayBtn) todayBtn.addEventListener('click', () => {
    setDatePickerYmd(datePicker, localYMD());
    apply();
  });
  if (resultsToggle) resultsToggle.addEventListener('change', apply);
  if (hideOddsToggle) hideOddsToggle.addEventListener('change', apply);

  apply();
});
