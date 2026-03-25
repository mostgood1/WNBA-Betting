(function () {
  const root = document.getElementById('cards');
  if (!root) {
    return;
  }

  const mode = document.body?.dataset?.pageMode === 'live' ? 'live' : 'pregame';
  const datePicker = document.getElementById('datePicker');
  const applyBtn = document.getElementById('applyBtn');
  const todayBtn = document.getElementById('todayBtn');
  const prevDateLink = document.getElementById('cardsPrevDateLink');
  const nextDateLink = document.getElementById('cardsNextDateLink');
  const headerMeta = document.getElementById('cardsHeaderMeta');
  const dateBadge = document.getElementById('cardsDateBadge');
  const sourceMeta = document.getElementById('cardsSourceMeta');
  const filtersEl = document.getElementById('cardsFilters');
  const resultsToggle = document.getElementById('resultsToggle');
  const hideOddsToggle = document.getElementById('hideOdds');
  const note = document.getElementById('note');
  const pollIntervalMs = 30000;

  const state = {
    activeTabs: new Map(),
    date: '',
    filter: 'all',
    hideOdds: false,
    payload: null,
    pollHandle: null,
    propDetails: new Map(),
    showResults: false,
  };

  function getLocalDateISO() {
    const now = new Date();
    const offsetMs = now.getTimezoneOffset() * 60000;
    return new Date(now.getTime() - offsetMs).toISOString().slice(0, 10);
  }

  function escapeHtml(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function fmtNumber(value, digits = 1) {
    const number = Number(value);
    return Number.isFinite(number) ? number.toFixed(digits) : '--';
  }

  function fmtInteger(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number.toFixed(0) : '--';
  }

  function fmtSigned(value, digits = 1) {
    const number = Number(value);
    if (!Number.isFinite(number)) {
      return '--';
    }
    return `${number > 0 ? '+' : ''}${number.toFixed(digits)}`;
  }

  function fmtPercent(value, digits = 0) {
    const number = Number(value);
    return Number.isFinite(number) ? `${(number * 100).toFixed(digits)}%` : '--';
  }

  function fmtPercentValue(value, digits = 1) {
    const number = Number(value);
    return Number.isFinite(number) ? `${number.toFixed(digits)}%` : '--';
  }

  function fmtAmerican(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) {
      return '--';
    }
    return number > 0 ? `+${Math.round(number)}` : `${Math.round(number)}`;
  }

  function fmtTime(value) {
    if (!value) {
      return 'Time TBD';
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return 'Time TBD';
    }
    return new Intl.DateTimeFormat(undefined, {
      hour: 'numeric',
      minute: '2-digit',
      month: 'short',
      day: 'numeric',
    }).format(date);
  }

  function shiftISODate(value, days) {
    const date = new Date(`${String(value || getLocalDateISO())}T12:00:00`);
    if (Number.isNaN(date.getTime())) {
      return getLocalDateISO();
    }
    date.setDate(date.getDate() + Number(days || 0));
    const offsetMs = date.getTimezoneOffset() * 60000;
    return new Date(date.getTime() - offsetMs).toISOString().slice(0, 10);
  }

  function titleCase(value) {
    const raw = String(value || '').trim();
    if (!raw) {
      return '';
    }
    return raw
      .split(/[_\s]+/)
      .map((part) => part ? part.charAt(0).toUpperCase() + part.slice(1).toLowerCase() : '')
      .join(' ');
  }

  function marketLabel(value) {
    const key = String(value || '').trim().toLowerCase();
    return {
      pts: 'Points',
      reb: 'Rebounds',
      ast: 'Assists',
      threes: '3PM',
      stl: 'Steals',
      blk: 'Blocks',
      tov: 'Turnovers',
      pra: 'PRA',
      pr: 'PR',
      pa: 'PA',
      ra: 'RA',
    }[key] || titleCase(key);
  }

  function logoForTri(tri) {
    const key = String(tri || '').trim().toUpperCase();
    return key ? `/web/assets/logos/${encodeURIComponent(key)}.svg` : '';
  }

  function cardId(game) {
    return String(game?.sim?.game_id || `${game?.away_tri || 'AWAY'}@${game?.home_tri || 'HOME'}`);
  }

  function statusClass(game) {
    const warnings = Array.isArray(game?.warnings) ? game.warnings : [];
    if (mode === 'live') {
      return 'is-live';
    }
    if (warnings.length) {
      return 'is-warn';
    }
    return 'is-soft';
  }

  function statusText(game) {
    if (mode === 'live') {
      return 'Live';
    }
    return 'Scheduled';
  }

  function showNote(message, kind) {
    if (!note) {
      return;
    }
    const text = String(message || '').trim();
    note.textContent = text;
    note.classList.toggle('hidden', !text);
    note.dataset.kind = kind || 'info';
  }

  function setLoading() {
    root.classList.add('parity-root');
    root.innerHTML = '<div class="cards-empty">Loading slate...</div>';
  }

  function safeArray(value) {
    return Array.isArray(value) ? value : [];
  }

  function recCount(game) {
    return safeArray(game?.prop_recommendations?.home).length + safeArray(game?.prop_recommendations?.away).length;
  }

  function hasAnyPlayerLines(game) {
    const players = safeArray(game?.sim?.players?.home)
      .concat(safeArray(game?.sim?.players?.away))
      .concat(safeArray(game?.sim?.missing_prop_players?.home))
      .concat(safeArray(game?.sim?.missing_prop_players?.away));
    return players.some((player) => player && player.prop_line_options && Object.keys(player.prop_line_options).length);
  }

  function cardStatus(game) {
    if (mode === 'live') {
      return 'live';
    }
    return 'scheduled';
  }

  function hasOfficialPlays(game) {
    return recCount(game) > 0;
  }

  function hasProps(game) {
    return hasOfficialPlays(game) || hasAnyPlayerLines(game);
  }

  function matchesFilter(game, filterKey) {
    const key = String(filterKey || 'all');
    if (key === 'official') {
      return hasOfficialPlays(game);
    }
    if (key === 'props') {
      return hasProps(game);
    }
    if (key === 'live') {
      return cardStatus(game) === 'live';
    }
    if (key === 'final') {
      return cardStatus(game) === 'final';
    }
    return true;
  }

  function slateCounts(games) {
    const liveCount = games.filter((game) => cardStatus(game) === 'live').length;
    const finalCount = games.filter((game) => cardStatus(game) === 'final').length;
    const officialCount = games.filter(hasOfficialPlays).length;
    const propsCount = games.filter(hasProps).length;
    const upcomingCount = Math.max(games.length - liveCount - finalCount, 0);
    return {
      liveCount,
      finalCount,
      officialCount,
      propsCount,
      upcomingCount,
    };
  }

  function sourceMetaPill(label, variant) {
    return `<span class="cards-source-meta-pill${variant ? ` is-${escapeHtml(variant)}` : ''}">${escapeHtml(label)}</span>`;
  }

  function buildFilters(games) {
    const counts = slateCounts(games);
    return [
      { key: 'all', label: `All ${games.length}` },
      { key: 'official', label: `Official ${counts.officialCount}` },
      { key: 'props', label: `Props ${counts.propsCount}` },
      { key: 'live', label: `Live ${counts.liveCount}` },
      { key: 'final', label: `Final ${counts.finalCount}` },
    ];
  }

  function renderHeaderMeta() {
    const games = safeArray(state.payload?.games);
    const counts = slateCounts(games);
    if (dateBadge) {
      dateBadge.textContent = state.payload?.date || state.date || getLocalDateISO();
    }
    if (headerMeta) {
      headerMeta.textContent = `${games.length} games on the slate | ${counts.officialCount} with official plays`;
    }
  }

  function renderSourceMeta() {
    if (!sourceMeta) {
      return;
    }
    const games = safeArray(state.payload?.games);
    const counts = slateCounts(games);
    const pills = [
      sourceMetaPill(`${games.length} games`),
      sourceMetaPill(`${counts.upcomingCount} upcoming`),
      sourceMetaPill(`${counts.officialCount} with official plays`),
      sourceMetaPill(mode === 'live' ? '30s refresh' : 'Pregame board', mode === 'live' ? 'live' : 'soft'),
      sourceMetaPill(`${counts.propsCount} with props`, counts.propsCount ? 'accent' : 'soft'),
    ];
    if (state.payload?.lookahead_applied) {
      pills.push(sourceMetaPill('Showing next available slate', 'warn'));
    }
    sourceMeta.innerHTML = pills.join('');
  }

  function renderFilters() {
    if (!filtersEl) {
      return;
    }
    const games = safeArray(state.payload?.games);
    filtersEl.innerHTML = buildFilters(games)
      .map((filter) => `
        <button type="button" class="cards-filter-pill ${filter.key === state.filter ? 'is-active' : ''}" data-filter-key="${escapeHtml(filter.key)}">
          ${escapeHtml(filter.label)}
        </button>
      `)
      .join('');
  }

  function updateDateControls() {
    const basePath = mode === 'live' ? '/live' : '/pregame';
    const previousDate = shiftISODate(state.date, -1);
    const nextDate = shiftISODate(state.date, 1);
    if (prevDateLink) {
      prevDateLink.href = `${basePath}?date=${encodeURIComponent(previousDate)}`;
    }
    if (nextDateLink) {
      nextDateLink.href = `${basePath}?date=${encodeURIComponent(nextDate)}`;
    }
  }

  function bestMarketPick(game, marketKey) {
    const betting = game?.betting || {};
    const home = game?.home_tri || 'HOME';
    const away = game?.away_tri || 'AWAY';
    if (marketKey === 'moneyline') {
      const candidates = [
        { detail: `${home} ML ${fmtAmerican(betting.home_ml)}`, ev: Number(betting.home_ml_ev), probability: betting.p_home_win, tabTarget: 'game' },
        { detail: `${away} ML ${fmtAmerican(betting.away_ml)}`, ev: Number(betting.away_ml_ev), probability: betting.p_away_win, tabTarget: 'game' },
      ].filter((item) => Number.isFinite(item.ev));
      return candidates.sort((a, b) => b.ev - a.ev)[0] || null;
    }
    if (marketKey === 'spread') {
      const spread = Number(betting.home_spread);
      const candidates = [
        { detail: `${home} ${Number.isFinite(spread) ? fmtSigned(spread) : '--'}`, ev: Number(betting.home_spread_ev), probability: betting.p_home_cover, tabTarget: 'game' },
        { detail: `${away} ${Number.isFinite(spread) ? fmtSigned(-spread) : '--'}`, ev: Number(betting.away_spread_ev), probability: betting.p_away_cover, tabTarget: 'game' },
      ].filter((item) => Number.isFinite(item.ev));
      return candidates.sort((a, b) => b.ev - a.ev)[0] || null;
    }
    if (marketKey === 'total') {
      const total = Number(betting.total);
      const candidates = [
        { detail: `Over ${Number.isFinite(total) ? fmtNumber(total, 1) : '--'}`, ev: Number(betting.over_ev), probability: betting.p_total_over, tabTarget: 'game' },
        { detail: `Under ${Number.isFinite(total) ? fmtNumber(total, 1) : '--'}`, ev: Number(betting.under_ev), probability: betting.p_total_under, tabTarget: 'game' },
      ].filter((item) => Number.isFinite(item.ev));
      return candidates.sort((a, b) => b.ev - a.ev)[0] || null;
    }
    return null;
  }

  function officialCardRows(game) {
    const picks = [bestMarketPick(game, 'moneyline'), bestMarketPick(game, 'spread'), bestMarketPick(game, 'total')].filter(Boolean);
    return picks.map((pick, index) => {
      const labels = ['Moneyline', 'Spread', 'Total'];
      return `
        <li class="cards-callout-item">
          <div>
            <div class="cards-callout-label">${escapeHtml(labels[index] || 'Market')}</div>
            <div class="cards-callout-main">${escapeHtml(pick.detail)}</div>
          </div>
          <div class="cards-callout-meta">
            <span class="cards-chip cards-chip--accent">EV ${fmtPercentValue(pick.ev)}</span>
            <span class="cards-chip">${fmtPercent(pick.probability, 0)}</span>
          </div>
        </li>
      `;
    }).join('');
  }

  function probabilityRows(game) {
    const betting = game?.betting || {};
    const rows = [
      {
        label: 'Moneyline win split',
        away: Number(betting.p_away_win),
        home: Number(betting.p_home_win),
        meta: `${game.away_tri} ${fmtPercent(betting.p_away_win, 0)} · ${game.home_tri} ${fmtPercent(betting.p_home_win, 0)}`,
      },
      {
        label: 'Spread cover split',
        away: Number(betting.p_away_cover),
        home: Number(betting.p_home_cover),
        meta: `${game.away_tri} ${fmtPercent(betting.p_away_cover, 0)} · ${game.home_tri} ${fmtPercent(betting.p_home_cover, 0)}`,
      },
      {
        label: 'Total split',
        away: Number(betting.p_total_under),
        home: Number(betting.p_total_over),
        meta: `Under ${fmtPercent(betting.p_total_under, 0)} · Over ${fmtPercent(betting.p_total_over, 0)}`,
      },
    ];
    return rows.map((entry) => {
      const away = Number.isFinite(entry.away) ? entry.away : 0.5;
      const home = Number.isFinite(entry.home) ? entry.home : 0.5;
      return `
        <div class="cards-prob-row">
          <div class="cards-prob-label">${escapeHtml(entry.label)}</div>
          <div class="cards-prob-bar" style="--away-pct:${Math.max(10, away * 100).toFixed(1)}%; --home-pct:${Math.max(10, home * 100).toFixed(1)}%;">
            <div class="cards-prob-away"></div>
            <div class="cards-prob-home"></div>
          </div>
          <div class="cards-mini-copy">${escapeHtml(entry.meta)}</div>
        </div>
      `;
    }).join('');
  }

  function miniMetrics(game) {
    const context = game?.sim?.context || {};
    const counts = {
      home: safeArray(game?.prop_recommendations?.home).length,
      away: safeArray(game?.prop_recommendations?.away).length,
    };
    return [
      { label: `${game.away_tri} pace`, value: fmtNumber(context.away_pace, 1), sub: 'expected possessions' },
      { label: `${game.home_tri} pace`, value: fmtNumber(context.home_pace, 1), sub: 'expected possessions' },
      { label: 'Official props', value: String(counts.home + counts.away), sub: `${counts.away} away · ${counts.home} home` },
    ].map((entry) => `
      <div class="cards-mini-metric">
        <span class="cards-section-label">${escapeHtml(entry.label)}</span>
        <strong>${escapeHtml(entry.value)}</strong>
        <div class="cards-mini-copy">${escapeHtml(entry.sub)}</div>
      </div>
    `).join('');
  }

  function marketCountSummary(game) {
    const parts = [];
    if (bestMarketPick(game, 'moneyline')) {
      parts.push('ML');
    }
    if (bestMarketPick(game, 'total')) {
      parts.push('Tot');
    }
    if (bestMarketPick(game, 'spread')) {
      parts.push('Spr');
    }
    const playable = recCount(game);
    if (playable) {
      parts.push(`+${playable} playable`);
    }
    return parts.join(' · ') || 'No market snapshot';
  }

  function stripLogoMarkup(teamTri) {
    const logo = logoForTri(teamTri);
    return logo
      ? `<img class="cards-strip-logo" src="${escapeHtml(logo)}" alt="${escapeHtml(teamTri)} logo" />`
      : '';
  }

  function renderScoreboardItem(game) {
    const id = cardId(game);
    const betting = game?.betting || {};
    const score = game?.sim?.score || {};
    return `
      <button class="cards-strip-card" type="button" data-jump-card="${escapeHtml(id)}">
        <div class="cards-strip-head">
          <span>${escapeHtml(statusText(game))}</span>
          <span class="cards-start-time">${escapeHtml(fmtTime(game?.odds?.commence_time))}</span>
        </div>
        <div class="cards-linescore is-compact is-strip">
          <div class="cards-linescore-head">
            <span class="cards-linescore-team-label">Team</span>
            <span class="cards-linescore-stat-head">PTS</span>
            <span class="cards-linescore-stat-head">WIN</span>
            <span class="cards-linescore-stat-head">COV</span>
          </div>
          <div class="cards-linescore-row">
            <div class="cards-linescore-team">
              ${stripLogoMarkup(game.away_tri)}
              <strong>${escapeHtml(game.away_tri || 'AWY')}</strong>
            </div>
            <span class="cards-linescore-stat">${fmtInteger(score.away_mean)}</span>
            <span class="cards-linescore-stat">${fmtPercent(betting.p_away_win, 0)}</span>
            <span class="cards-linescore-stat">${fmtPercent(betting.p_away_cover, 0)}</span>
          </div>
          <div class="cards-linescore-row">
            <div class="cards-linescore-team">
              ${stripLogoMarkup(game.home_tri)}
              <strong>${escapeHtml(game.home_tri || 'HME')}</strong>
            </div>
            <span class="cards-linescore-stat">${fmtInteger(score.home_mean)}</span>
            <span class="cards-linescore-stat">${fmtPercent(betting.p_home_win, 0)}</span>
            <span class="cards-linescore-stat">${fmtPercent(betting.p_home_cover, 0)}</span>
          </div>
        </div>
        <div class="cards-strip-meta">${escapeHtml(marketCountSummary(game))}</div>
      </button>
    `;
  }

  function renderMarketTile(title, pick, auxLine, noteText, cardIdValue) {
    return `
      <button class="cards-market-tile" type="button" ${pick?.tabTarget ? `data-market-tab-target="${escapeHtml(pick.tabTarget)}" data-card-target="${escapeHtml(cardIdValue)}"` : ''}>
        <div class="cards-market-kicker">${escapeHtml(title)}</div>
        <div class="cards-market-main">${escapeHtml(pick?.detail || 'No playable edge')}</div>
        <div class="cards-mini-copy">${pick ? `Win ${fmtPercent(pick.probability, 0)} · EV ${fmtPercentValue(pick.ev)}` : 'Off card'}</div>
        <div class="cards-market-sub" data-odds-only="true">${escapeHtml(auxLine)}</div>
        <div class="cards-market-sub">${escapeHtml(noteText)}</div>
      </button>
    `;
  }

  function renderPeriodTiles(periods) {
    const order = ['q1', 'q2', 'q3', 'q4'];
    const items = order
      .map((key) => [key, periods?.[key]])
      .filter(([, value]) => value && typeof value === 'object')
      .map(([key, value]) => `
        <section class="cards-period-tile">
          <div class="cards-table-title">${escapeHtml(String(key).toUpperCase())}</div>
          <div class="cards-mini-copy">Total ${fmtNumber(value.total_mean, 1)}</div>
          <div class="cards-mini-copy">Margin ${fmtSigned(value.margin_mean, 1)}</div>
          <div class="cards-mini-copy">Home win ${fmtPercent(value.p_home_win, 0)}</div>
        </section>
      `)
      .join('');
    return items || '<div class="cards-empty">No quarter-level outlook available.</div>';
  }

  function renderContextBadges(game) {
    const context = game?.sim?.context || {};
    const items = [
      Number(context.home_b2b) ? `${game.home_tri} B2B` : '',
      Number(context.away_b2b) ? `${game.away_tri} B2B` : '',
      context.roster_mode ? `Roster ${context.roster_mode}` : '',
      context.pbp_used ? 'PBP priors loaded' : '',
      Number.isFinite(Number(context.home_injuries_out)) ? `${game.home_tri} outs ${fmtInteger(context.home_injuries_out)}` : '',
      Number.isFinite(Number(context.away_injuries_out)) ? `${game.away_tri} outs ${fmtInteger(context.away_injuries_out)}` : '',
    ].filter(Boolean);
    return items.length
      ? items.map((item) => `<span class="cards-source-meta-pill is-soft">${escapeHtml(item)}</span>`).join('')
      : '<div class="cards-empty">No extra context flags on this matchup.</div>';
  }

  function renderGamePanel(game) {
    const betting = game?.betting || {};
    const score = game?.sim?.score || {};
    const market = game?.sim?.market || {};
    const warnings = safeArray(game?.warnings);
    const id = cardId(game);
    return `
      <div class="cards-overview-grid">
        <div class="cards-panel-card">
          <div class="cards-box-head">
            <div class="cards-table-title"><strong>Game lens</strong></div>
            <span class="cards-overview-badge ${statusClass(game)}">${escapeHtml(mode === 'live' ? 'Live model' : 'Pregame model')}</span>
          </div>
          <div class="cards-prob-grid">${probabilityRows(game)}</div>
          <div class="cards-mini-metrics">${miniMetrics(game)}</div>
          <div class="cards-market-row cards-market-row--tiles">
            ${renderMarketTile('Moneyline', bestMarketPick(game, 'moneyline'), `${game.home_tri} ${fmtAmerican(betting.home_ml)} / ${game.away_tri} ${fmtAmerican(betting.away_ml)}`, `Home win ${fmtPercent(betting.p_home_win, 0)} · Away win ${fmtPercent(betting.p_away_win, 0)}`, id)}
            ${renderMarketTile('Spread', bestMarketPick(game, 'spread'), `${game.home_tri} ${fmtSigned(betting.home_spread)} · ${game.away_tri} ${fmtSigned(-Number(betting.home_spread))}`, `Model margin ${fmtSigned(score.margin_mean, 1)} · Market ${fmtSigned(-Number(market.market_home_spread), 1)}`, id)}
            ${renderMarketTile('Total', bestMarketPick(game, 'total'), `Total ${fmtNumber(betting.total, 1)}`, `Model total ${fmtNumber(score.total_mean, 1)} · Over ${fmtPercent(betting.p_total_over, 0)}`, id)}
            ${renderMarketTile('Props', { detail: `${safeArray(game?.prop_recommendations?.home).length + safeArray(game?.prop_recommendations?.away).length} playable props`, probability: null, ev: null, tabTarget: 'props' }, 'Jump to props board', 'Filter by team side and stat type in the Props tab.', id)}
          </div>
        </div>
        <div class="cards-panel-card">
          <div class="cards-box-head">
            <div class="cards-table-title"><strong>Official card</strong></div>
            <span class="cards-chip">3 core markets</span>
          </div>
          <ul class="cards-callout-list">${officialCardRows(game)}</ul>
          <div class="cards-panel-copy">${escapeHtml(game?.writeup || 'No matchup writeup generated for this game.')}</div>
          ${warnings.length ? `<div class="cards-warning-list">${warnings.map((warning) => `<span class="cards-warning">${escapeHtml(warning)}</span>`).join('')}</div>` : ''}
          <div class="cards-source-meta">${renderContextBadges(game)}</div>
          <div>
            <div class="cards-table-title"><strong>Quarter outlook</strong></div>
            <div class="cards-period-grid">${renderPeriodTiles(game?.sim?.periods || {})}</div>
          </div>
        </div>
      </div>
    `;
  }

  function teamSimTotals(players) {
    const rows = safeArray(players);
    return {
      pts: rows.reduce((sum, player) => sum + (Number(player?.pts_mean) || 0), 0),
      reb: rows.reduce((sum, player) => sum + (Number(player?.reb_mean) || 0), 0),
      ast: rows.reduce((sum, player) => sum + (Number(player?.ast_mean) || 0), 0),
      pra: rows.reduce((sum, player) => sum + (Number(player?.pra_mean) || 0), 0),
    };
  }

  function renderLinescoreSummary(teamTri, totals) {
    return `
      <div class="cards-linescore is-compact">
        <div class="cards-linescore-head">
          <span class="cards-linescore-team-label">Team</span>
          <span class="cards-linescore-stat-head">PTS</span>
          <span class="cards-linescore-stat-head">REB</span>
          <span class="cards-linescore-stat-head">AST</span>
          <span class="cards-linescore-stat-head">PRA</span>
        </div>
        <div class="cards-linescore-row">
          <div class="cards-linescore-team"><strong>${escapeHtml(teamTri)}</strong></div>
          <span class="cards-linescore-stat">${fmtInteger(totals.pts)}</span>
          <span class="cards-linescore-stat">${fmtInteger(totals.reb)}</span>
          <span class="cards-linescore-stat">${fmtInteger(totals.ast)}</span>
          <span class="cards-linescore-stat">${fmtInteger(totals.pra)}</span>
        </div>
      </div>
    `;
  }

  function renderBoxTableRows(players) {
    const sorted = [...safeArray(players)].sort((a, b) => Number(b.min_mean || 0) - Number(a.min_mean || 0));
    return sorted.map((player) => {
      const actual = player.actual_props || {};
      const actualBits = state.showResults
        ? [
            Number.isFinite(Number(actual.pts)) ? `PTS ${fmtInteger(actual.pts)}` : '',
            Number.isFinite(Number(actual.reb)) ? `REB ${fmtInteger(actual.reb)}` : '',
            Number.isFinite(Number(actual.ast)) ? `AST ${fmtInteger(actual.ast)}` : '',
            Number.isFinite(Number(actual.pra)) ? `PRA ${fmtInteger(actual.pra)}` : '',
          ].filter(Boolean).join(' · ')
        : '';
      return `
        <tr>
          <td>
            <div class="box-player-cell">
              <strong>${escapeHtml(player.player_name || 'Player')}</strong>
              ${actualBits ? `<span class="box-player-actual">Actual ${escapeHtml(actualBits)}</span>` : ''}
            </div>
          </td>
          <td>${fmtNumber(player.min_mean, 1)}</td>
          <td>${fmtNumber(player.pts_mean, 1)}</td>
          <td>${fmtNumber(player.reb_mean, 1)}</td>
          <td>${fmtNumber(player.ast_mean, 1)}</td>
          <td>${fmtNumber(player.threes_mean, 1)}</td>
          <td>${fmtNumber(player.pra_mean, 1)}</td>
        </tr>
      `;
    }).join('');
  }

  function renderBoxSection(teamTri, teamName, players, injuries, missingPlayers) {
    const logo = logoForTri(teamTri);
    const totals = teamSimTotals(players);
    const injuryBits = safeArray(injuries).map((item) => `<span class="box-injury">${escapeHtml(item.player_name || 'Player')} ${escapeHtml(item.injury_status || 'OUT')}</span>`).join('');
    const missingBits = safeArray(missingPlayers).slice(0, 6).map((item) => `<span class="box-missing">Prop-only ${escapeHtml(item.player_name || 'Player')}</span>`).join('');
    const tableMarkup = safeArray(players).length
      ? `
        <div class="cards-table-wrap">
          <table class="cards-table box-table">
            <thead>
              <tr>
                <th>Player</th>
                <th>Min</th>
                <th>Pts</th>
                <th>Reb</th>
                <th>Ast</th>
                <th>3PM</th>
                <th>PRA</th>
              </tr>
            </thead>
            <tbody>${renderBoxTableRows(players)}</tbody>
          </table>
        </div>
      `
      : '<div class="box-empty">No simulated box score rows available.</div>';
    return `
      <div class="cards-panel-card cards-box-panel">
        <div class="cards-box-head">
          <div class="cards-table-title"><strong>${escapeHtml(teamTri)} sim box</strong></div>
          <span class="cards-chip">${safeArray(players).length} rows</span>
        </div>
        <div class="cards-box-team-head">
          ${logo ? `<img class="cards-box-team__logo" src="${escapeHtml(logo)}" alt="${escapeHtml(teamTri)} logo" />` : ''}
          <div>
            <div class="cards-box-team-title">${escapeHtml(teamName || teamTri)}</div>
            <div class="cards-mini-copy">${escapeHtml(teamTri)} projected player box</div>
          </div>
        </div>
        <div class="cards-box-totals">${renderLinescoreSummary(teamTri, totals)}</div>
        ${injuryBits ? `<div class="cards-source-meta">${injuryBits}</div>` : ''}
        ${missingBits ? `<div class="cards-source-meta">${missingBits}</div>` : ''}
        ${tableMarkup}
      </div>
    `;
  }

  function ensurePropDetail(game) {
    const id = cardId(game);
    if (!state.propDetails.has(id)) {
      state.propDetails.set(id, {
        selectedKey: null,
        side: 'all',
        type: 'all',
      });
    }
    return state.propDetails.get(id);
  }

  function propKey(row) {
    return [row.teamTri, row.player, row.market, row.side, row.line, row.rank || ''].join('|');
  }

  function allPropRows(game) {
    const rows = [];
    [['away', game.away_tri], ['home', game.home_tri]].forEach(([sideKey, teamTri]) => {
      safeArray(game?.prop_recommendations?.[sideKey]).forEach((row) => {
        const picks = safeArray(row?.picks);
        const basePicks = picks.length ? picks : (row?.best ? [row.best] : []);
        basePicks.forEach((pick, index) => {
          if (!pick || !pick.market) {
            return;
          }
          rows.push({
            key: '',
            cardId: cardId(game),
            teamTri,
            sideKey,
            player: row.player,
            playerPhoto: row.player_photo || row.photo,
            market: String(pick.market || '').toLowerCase(),
            marketLabel: marketLabel(pick.market),
            side: String(pick.side || '').toUpperCase(),
            line: Number(pick.line),
            price: pick.price,
            book: pick.book,
            evPct: pick.ev_pct,
            pWin: pick.p_win,
            simMu: pick.sim_mu,
            simSd: pick.sim_sd,
            summary: row.basketball_summary || pick.basketball_summary || row.display_pick || '',
            reasons: safeArray(pick.reasons).length ? safeArray(pick.reasons) : safeArray(row.top_play_reasons),
            matchup: row.matchup,
            rank: index + 1,
            primary: index === 0,
          });
        });
      });
    });
    rows.forEach((row) => {
      row.key = propKey(row);
    });
    return rows.sort((left, right) => {
      if (left.primary !== right.primary) {
        return left.primary ? -1 : 1;
      }
      return Number(right.evPct || 0) - Number(left.evPct || 0);
    });
  }

  function filteredPropRows(game) {
    const detail = ensurePropDetail(game);
    const rows = allPropRows(game);
    return rows.filter((row) => {
      if (detail.side !== 'all' && row.sideKey !== detail.side) {
        return false;
      }
      if (detail.type !== 'all' && row.market !== detail.type) {
        return false;
      }
      return true;
    });
  }

  function selectedPropRow(game) {
    const detail = ensurePropDetail(game);
    const filtered = filteredPropRows(game);
    const allRows = allPropRows(game);
    let selected = filtered.find((row) => row.key === detail.selectedKey) || null;
    if (!selected) {
      selected = filtered[0] || allRows[0] || null;
      detail.selectedKey = selected ? selected.key : null;
    }
    return selected;
  }

  function renderPropFilters(game) {
    const detail = ensurePropDetail(game);
    const rows = allPropRows(game);
    const markets = Array.from(new Set(rows.map((row) => row.market))).sort();
    const sidePills = [
      { key: 'all', label: 'All sides' },
      { key: 'away', label: `${game.away_tri}` },
      { key: 'home', label: `${game.home_tri}` },
    ];
    const typePills = [{ key: 'all', label: 'All props' }].concat(markets.map((market) => ({ key: market, label: marketLabel(market) })));
    return `
      <div class="cards-filters">
        ${sidePills.map((pill) => `<button class="cards-filter-pill ${detail.side === pill.key ? 'is-active' : ''}" type="button" data-prop-filter-side="${escapeHtml(pill.key)}" data-card-target="${escapeHtml(cardId(game))}">${escapeHtml(pill.label)}</button>`).join('')}
      </div>
      <div class="cards-filters">
        ${typePills.map((pill) => `<button class="cards-filter-pill ${detail.type === pill.key ? 'is-active' : ''}" type="button" data-prop-filter-type="${escapeHtml(pill.key)}" data-card-target="${escapeHtml(cardId(game))}">${escapeHtml(pill.label)}</button>`).join('')}
      </div>
    `;
  }

  function renderPropGroups(game) {
    const rows = filteredPropRows(game);
    if (!rows.length) {
      return '<div class="cards-empty">No official or playable props matched the current side and prop-type filters for this game.</div>';
    }
    return `
      <div class="cards-prop-list">
        ${rows.map((row) => `
          <button class="cards-prop-button ${selectedPropRow(game)?.key === row.key ? 'is-active' : ''}" type="button" data-prop-select="${escapeHtml(row.key)}" data-card-target="${escapeHtml(cardId(game))}">
            <div class="cards-prop-button-head">
              <strong>${escapeHtml(row.player || 'Player')}</strong>
              <span class="cards-chip ${row.primary ? 'cards-chip--accent' : ''}">${row.primary ? 'Top play' : row.marketLabel}</span>
            </div>
            <div class="cards-prop-button-main">${escapeHtml(row.marketLabel)} ${escapeHtml(row.side)} ${fmtNumber(row.line, 1)}</div>
            <div class="cards-mini-copy">${escapeHtml(row.teamTri)} · EV ${fmtPercentValue(row.evPct)} · ${fmtAmerican(row.price)} ${escapeHtml(row.book || '')}</div>
          </button>
        `).join('')}
      </div>
    `;
  }

  function playerSimRow(game, selected) {
    if (!selected) {
      return null;
    }
    const sideBucket = selected.sideKey === 'away' ? 'away' : 'home';
    const players = safeArray(game?.sim?.players?.[sideBucket]).concat(safeArray(game?.sim?.missing_prop_players?.[sideBucket]));
    return players.find((player) => String(player.player_name || '').trim().toLowerCase() === String(selected.player || '').trim().toLowerCase()) || null;
  }

  function renderLensDetailPairs(selected, simRow) {
    const metricValue = simRow ? simRow[`${selected.market}_mean`] : null;
    const pairs = [
      { label: 'Team side', value: selected.teamTri },
      { label: 'Selection', value: `${selected.side} ${fmtNumber(selected.line, 1)}` },
      { label: 'Odds', value: `${fmtAmerican(selected.price)} ${selected.book || ''}`.trim() },
      { label: 'EV', value: fmtPercentValue(selected.evPct) },
      { label: 'Win prob', value: fmtPercent(selected.pWin, 0) },
      { label: 'Model mean', value: fmtNumber(metricValue ?? selected.simMu, 1) },
    ];
    return pairs.map((pair) => `
      <div class="cards-data-pair">
        <span>${escapeHtml(pair.label)}</span>
        <strong>${escapeHtml(pair.value)}</strong>
      </div>
    `).join('');
  }

  function renderPlayerRowTable(simRow) {
    if (!simRow) {
      return '<div class="cards-empty">No sim row matched this player.</div>';
    }
    return `
      <div class="cards-table-wrap">
        <table class="cards-table">
          <thead>
            <tr>
              <th>Player</th>
              <th>Min</th>
              <th>Pts</th>
              <th>Reb</th>
              <th>Ast</th>
              <th>3PM</th>
              <th>PRA</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>${escapeHtml(simRow.player_name || 'Player')}</td>
              <td>${fmtNumber(simRow.min_mean, 1)}</td>
              <td>${fmtNumber(simRow.pts_mean, 1)}</td>
              <td>${fmtNumber(simRow.reb_mean, 1)}</td>
              <td>${fmtNumber(simRow.ast_mean, 1)}</td>
              <td>${fmtNumber(simRow.threes_mean, 1)}</td>
              <td>${fmtNumber(simRow.pra_mean, 1)}</td>
            </tr>
          </tbody>
        </table>
      </div>
    `;
  }

  function renderPropLens(game) {
    const selected = selectedPropRow(game);
    if (!selected) {
      return `
        <div class="cards-lens-head">
          <div>
            <div class="cards-lens-label">Prop lens</div>
            <div class="cards-lens-main">No filtered prop selected</div>
          </div>
          <span class="cards-lens-badge is-live">Refine filters</span>
        </div>
        <div class="cards-callout-copy">No official or playable props matched the current side and prop-type filters for this game.</div>
      `;
    }
    const simRow = playerSimRow(game, selected);
    const reasons = safeArray(selected.reasons).slice(0, 6);
    return `
      <div class="cards-lens-head">
        <div>
          <div class="cards-lens-label">Prop lens</div>
          <div class="cards-lens-main">${escapeHtml(selected.player)} - ${escapeHtml(selected.marketLabel)}</div>
          <div class="cards-subcopy">${escapeHtml(selected.teamTri)} · ${escapeHtml(selected.side)} ${fmtNumber(selected.line, 1)} · ${escapeHtml(selected.matchup || `${game.away_tri} at ${game.home_tri}`)}</div>
        </div>
        <span class="cards-lens-badge ${selected.primary ? '' : 'is-live'}">${selected.primary ? 'Top play' : 'Filtered'}</span>
      </div>
      <div class="cards-detail-grid">${renderLensDetailPairs(selected, simRow)}</div>
      <div class="cards-callout-copy">${escapeHtml(selected.summary || 'No prop summary available.')}</div>
      ${reasons.length ? `<div class="cards-source-meta">${reasons.map((reason) => `<span class="cards-source-meta-pill">${escapeHtml(reason)}</span>`).join('')}</div>` : ''}
      <div class="cards-box-grid">
        <div class="cards-panel-card cards-prop-stack">
          <div class="cards-table-title">Sim player row</div>
          ${renderPlayerRowTable(simRow)}
        </div>
        <div class="cards-panel-card cards-prop-stack">
          <div class="cards-table-title">Ladders</div>
          <a class="cards-game-link" href="/prop-ladders?date=${encodeURIComponent(state.date)}&team=${encodeURIComponent(selected.teamTri)}&player=${encodeURIComponent(selected.player)}&market=${encodeURIComponent(selected.market)}">Open full ${escapeHtml(selected.marketLabel)} ladders board</a>
          <div class="cards-callout-copy">Use the ladders board to inspect alternate rungs and over/under dispersion for this player and stat.</div>
        </div>
      </div>
    `;
  }

  function renderPropsPanel(game) {
    const rows = allPropRows(game);
    return `
      <div class="cards-props-grid">
        <div class="cards-panel-card">
          <div class="cards-box-head">
            <div class="cards-table-title"><strong>Props board</strong></div>
            <span class="cards-chip">${rows.length} playable rows</span>
          </div>
          <div class="cards-prop-filter-shell">${renderPropFilters(game)}</div>
          <div class="cards-prop-groups">${renderPropGroups(game)}</div>
        </div>
        <div class="cards-lens-shell">${renderPropLens(game)}</div>
      </div>
    `;
  }

  function renderGameCard(game) {
    const id = cardId(game);
    const activeTab = state.activeTabs.get(id) || 'game';
    const score = game?.sim?.score || {};
    const awayLogo = logoForTri(game.away_tri);
    const homeLogo = logoForTri(game.home_tri);
    return `
      <article class="cards-game-card" data-card-id="${escapeHtml(id)}" id="game-card-${escapeHtml(id)}">
        <div class="cards-strip-head">
          <div class="cards-head-left">
            <div class="cards-team-code-shell">
              ${awayLogo ? `<img class="cards-team-logo" src="${escapeHtml(awayLogo)}" alt="${escapeHtml(game.away_tri)} logo" />` : ''}
              <span class="cards-team-code">${escapeHtml(game.away_tri)}</span>
            </div>
            <span class="cards-score-divider">@</span>
            <div class="cards-team-code-shell">
              ${homeLogo ? `<img class="cards-team-logo" src="${escapeHtml(homeLogo)}" alt="${escapeHtml(game.home_tri)} logo" />` : ''}
              <span class="cards-team-code">${escapeHtml(game.home_tri)}</span>
            </div>
          </div>
          <div class="cards-status-cluster">
            <span class="cards-status-badge ${statusClass(game)}">${escapeHtml(statusText(game))}</span>
            <div class="cards-start-time">${escapeHtml(fmtTime(game?.odds?.commence_time))}</div>
            <a class="cards-game-link" href="#game-card-${encodeURIComponent(id)}">Card view</a>
          </div>
        </div>

        <div class="cards-score-ribbon">
          <div class="cards-score-side">
            <div class="cards-score-label">Away</div>
            <div class="cards-score-number">${fmtNumber(score.away_mean, 1)}</div>
            <strong>${escapeHtml(game.away_tri)}</strong>
          </div>
          <div class="cards-score-divider">at</div>
          <div class="cards-score-side">
            <div class="cards-score-label">Home</div>
            <div class="cards-score-number">${fmtNumber(score.home_mean, 1)}</div>
            <strong>${escapeHtml(game.home_tri)}</strong>
          </div>
          <div class="cards-score-meta">
            <div class="cards-live-line">${escapeHtml(mode === 'live' ? 'Live board shell active.' : 'Pregame matchup card.')}</div>
            <div class="cards-sim-line">Model total ${fmtNumber(score.total_mean, 1)} · margin ${fmtSigned(score.margin_mean, 1)}</div>
            <div class="cards-mini-copy">${escapeHtml(game.away_name || game.away_tri)} at ${escapeHtml(game.home_name || game.home_tri)}</div>
          </div>
        </div>

        <div class="cards-card__header">
          <div class="cards-tabs">
            <button class="cards-tab ${activeTab === 'game' ? 'is-active' : ''}" type="button" data-card-tab="game" data-card-target="${escapeHtml(id)}">Game</button>
            <button class="cards-tab ${activeTab === 'box' ? 'is-active' : ''}" type="button" data-card-tab="box" data-card-target="${escapeHtml(id)}">Box Score</button>
            <button class="cards-tab ${activeTab === 'props' ? 'is-active' : ''}" type="button" data-card-tab="props" data-card-target="${escapeHtml(id)}">Props</button>
          </div>
        </div>

        <div class="cards-card__body">
          <section class="cards-panel ${activeTab === 'game' ? 'is-active' : ''}" data-panel-id="game">${renderGamePanel(game)}</section>
          <section class="cards-panel ${activeTab === 'box' ? 'is-active' : ''}" data-panel-id="box">
            <div class="cards-box-grid">
              ${renderBoxSection(game.away_tri, game.away_name, game?.sim?.players?.away || [], game?.sim?.injuries?.away || [], game?.sim?.missing_prop_players?.away || [])}
              ${renderBoxSection(game.home_tri, game.home_name, game?.sim?.players?.home || [], game?.sim?.injuries?.home || [], game?.sim?.missing_prop_players?.home || [])}
            </div>
          </section>
          <section class="cards-panel ${activeTab === 'props' ? 'is-active' : ''}" data-panel-id="props">${renderPropsPanel(game)}</section>
        </div>
      </article>
    `;
  }

  function renderBoard() {
    const games = safeArray(state.payload?.games);
    const filteredGames = games.filter((game) => matchesFilter(game, state.filter));
    root.classList.add('parity-root');
    root.classList.toggle('hide-odds', state.hideOdds);
    if (!games.length) {
      root.innerHTML = '<div class="cards-empty">No game cards available for this date.</div>';
      return;
    }
    if (!filteredGames.length) {
      root.innerHTML = '<div class="cards-empty">No games matched the selected slate filter.</div>';
      return;
    }
    root.innerHTML = `
      <section class="cards-scoreboard">${filteredGames.map(renderScoreboardItem).join('')}</section>
      <section class="cards-grid">${filteredGames.map(renderGameCard).join('')}</section>
    `;
  }

  async function loadBoard() {
    setLoading();
    try {
      const response = await fetch(`/api/cards?date=${encodeURIComponent(state.date)}`, { cache: 'no-store' });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload?.error || 'Failed to load game cards.');
      }
      state.payload = payload;
      updateDateControls();
      renderHeaderMeta();
      renderSourceMeta();
      renderFilters();
      if (payload.lookahead_applied && payload.date && payload.requested_date && payload.date !== payload.requested_date) {
        showNote(`No slate for ${payload.requested_date}. Showing next available cards from ${payload.date}.`, 'warning');
      } else {
        showNote('', 'info');
      }
      renderBoard();
    } catch (error) {
      state.payload = null;
      if (headerMeta) {
        headerMeta.textContent = 'Failed to load slate.';
      }
      root.innerHTML = `<div class="cards-empty">${escapeHtml(error?.message || 'Failed to load slate.')}</div>`;
      showNote(error?.message || 'Failed to load slate.', 'warning');
    }
  }

  function syncFromControls() {
    state.date = datePicker?.value || getLocalDateISO();
    state.showResults = Boolean(resultsToggle?.checked);
    state.hideOdds = Boolean(hideOddsToggle?.checked);
  }

  function applyAndLoad() {
    syncFromControls();
    const url = new URL(window.location.href);
    url.searchParams.set('date', state.date);
    window.history.replaceState({}, '', url);
    loadBoard();
  }

  function setupPolling() {
    if (mode !== 'live') {
      return;
    }
    if (state.pollHandle) {
      window.clearInterval(state.pollHandle);
    }
    state.pollHandle = window.setInterval(() => {
      syncFromControls();
      loadBoard();
    }, pollIntervalMs);
  }

  root.addEventListener('click', (event) => {
    const tabButton = event.target.closest('[data-card-tab]');
    if (tabButton) {
      const cardTarget = tabButton.getAttribute('data-card-target') || '';
      const tabKey = tabButton.getAttribute('data-card-tab') || 'game';
      state.activeTabs.set(cardTarget, tabKey);
      renderBoard();
      return;
    }

    const jumpButton = event.target.closest('[data-jump-card]');
    if (jumpButton) {
      const cardTarget = jumpButton.getAttribute('data-jump-card') || '';
      const card = root.querySelector(`[data-card-id="${CSS.escape(cardTarget)}"]`);
      if (card) {
        card.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
      return;
    }

    const marketTab = event.target.closest('[data-market-tab-target]');
    if (marketTab) {
      const cardTarget = marketTab.getAttribute('data-card-target') || '';
      const tabKey = marketTab.getAttribute('data-market-tab-target') || 'game';
      state.activeTabs.set(cardTarget, tabKey);
      renderBoard();
      return;
    }

    const propSide = event.target.closest('[data-prop-filter-side]');
    if (propSide) {
      const cardTarget = propSide.getAttribute('data-card-target') || '';
      const detail = state.propDetails.get(cardTarget);
      if (detail) {
        detail.side = propSide.getAttribute('data-prop-filter-side') || 'all';
        detail.selectedKey = null;
        renderBoard();
      }
      return;
    }

    const propType = event.target.closest('[data-prop-filter-type]');
    if (propType) {
      const cardTarget = propType.getAttribute('data-card-target') || '';
      const detail = state.propDetails.get(cardTarget);
      if (detail) {
        detail.type = propType.getAttribute('data-prop-filter-type') || 'all';
        detail.selectedKey = null;
        renderBoard();
      }
      return;
    }

    const propSelect = event.target.closest('[data-prop-select]');
    if (propSelect) {
      const cardTarget = propSelect.getAttribute('data-card-target') || '';
      const detail = state.propDetails.get(cardTarget);
      if (detail) {
        detail.selectedKey = propSelect.getAttribute('data-prop-select') || null;
        renderBoard();
      }
    }
  });

  filtersEl?.addEventListener('click', (event) => {
    const button = event.target.closest('[data-filter-key]');
    if (!button) {
      return;
    }
    state.filter = button.getAttribute('data-filter-key') || 'all';
    renderFilters();
    renderBoard();
  });

  applyBtn?.addEventListener('click', applyAndLoad);
  todayBtn?.addEventListener('click', () => {
    const today = getLocalDateISO();
    if (datePicker) {
      datePicker.value = today;
    }
    applyAndLoad();
  });
  resultsToggle?.addEventListener('change', () => {
    syncFromControls();
    renderBoard();
  });
  hideOddsToggle?.addEventListener('change', () => {
    syncFromControls();
    renderBoard();
  });

  const initialDate = new URLSearchParams(window.location.search).get('date') || getLocalDateISO();
  state.date = initialDate;
  if (datePicker) {
    datePicker.value = initialDate;
  }
  syncFromControls();
  setupPolling();
  loadBoard();
})();