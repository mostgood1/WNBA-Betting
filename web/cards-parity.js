(function () {
  const scoreboardRoot = document.getElementById('cardsScoreboard');
  const gridRoot = document.getElementById('cardsGrid');
  const boardShell = document.querySelector('.cards-page-shell') || document.body;
  if (!scoreboardRoot || !gridRoot) {
    return;
  }

  const mode = document.body?.dataset?.pageMode === 'live' ? 'live' : 'pregame';
  const datePicker = document.getElementById('datePicker');
  const applyBtn = document.getElementById('applyBtn');
  const prevDateLink = document.getElementById('cardsPrevDateLink');
  const nextDateLink = document.getElementById('cardsNextDateLink');
  const seasonBettingCardLink = document.getElementById('cardsSeasonBettingCardLink');
  const headerMeta = document.getElementById('cardsHeaderMeta');
  const sourceMeta = document.getElementById('cardsSourceMeta');
  const filtersEl = document.getElementById('cardsFilters');
  const propsStripEl = document.getElementById('cardsPropsStrip');
  const note = document.getElementById('note');
  const pollIntervalMs = 15000;

  const state = {
    activeTabs: new Map(),
    boardInitialized: false,
    date: '',
    filter: 'all',
    liveDataLoading: false,
    liveGameLens: new Map(),
    livePlayerBoxscores: new Map(),
    liveStates: new Map(),
    payload: null,
    pollHandle: null,
    propDetails: new Map(),
    simDetailCache: new Map(),
    simDetailLoading: new Set(),
    propsStripPayload: null,
    propsStripDefaultCount: mode === 'live' ? 18 : 12,
    propsStripVisibleCount: mode === 'live' ? 18 : 12,
    propsStripFilters: {
      game: 'all',
      market: 'all',
      side: 'all',
    },
    propsStripSort: mode === 'live' ? 'best' : 'default',
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

  function extractApiErrorText(text, fallbackMessage) {
    const raw = String(text || '').trim();
    if (!raw) {
      return fallbackMessage;
    }
    if (raw.startsWith('<')) {
      return `${fallbackMessage} Server returned HTML instead of JSON.`;
    }
    if (raw.startsWith('{') || raw.startsWith('[')) {
      return fallbackMessage;
    }
    return raw.slice(0, 240);
  }

  async function readApiJson(response, fallbackMessage) {
    const rawText = await response.text();
    let payload = null;
    if (rawText) {
      try {
        payload = JSON.parse(rawText);
      } catch (_error) {
        payload = null;
      }
    }

    if (!response.ok) {
      const payloadMessage = payload && typeof payload === 'object'
        ? (payload.error || payload.message || payload.detail)
        : null;
      throw new Error(payloadMessage || extractApiErrorText(rawText, fallbackMessage));
    }

    if (payload !== null) {
      return payload;
    }

    throw new Error(extractApiErrorText(rawText, fallbackMessage));
  }

  function waitFor(ms) {
    return new Promise((resolve) => window.setTimeout(resolve, ms));
  }

  function isRetryableApiError(error) {
    const message = String(error?.message || '').toLowerCase();
    if (!message) {
      return false;
    }
    return (
      message.includes('server returned html instead of json')
      || message.includes('failed to fetch')
      || message.includes('networkerror')
      || message.includes('load failed')
      || message.includes('network request failed')
      || message.includes('timeout')
    );
  }

  async function fetchApiJson(url, fallbackMessage, options = {}) {
    const retries = Math.max(0, Number(options?.retries) || 0);
    const retryDelayMs = Math.max(100, Number(options?.retryDelayMs) || 700);
    let lastError = null;

    for (let attempt = 0; attempt <= retries; attempt += 1) {
      try {
        const response = await fetch(url, { cache: 'no-store' });
        return await readApiJson(response, fallbackMessage);
      } catch (error) {
        lastError = error;
        if (attempt >= retries || !isRetryableApiError(error)) {
          throw error;
        }
        await waitFor(retryDelayMs * (attempt + 1));
      }
    }

    throw lastError || new Error(fallbackMessage);
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

  function fmtMinutesPlayed(value) {
    const raw = String(value ?? '').trim();
    if (!raw) {
      return '--';
    }
    if (/^\d{1,2}:\d{2}$/.test(raw)) {
      return raw;
    }
    const number = Number(raw);
    return Number.isFinite(number) ? number.toFixed(1) : raw;
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

  function currentBoardDate() {
    return String(state.payload?.date || state.date || getLocalDateISO());
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

  function matchupKey(awayTri, homeTri) {
    const away = String(awayTri || '').trim().toUpperCase();
    const home = String(homeTri || '').trim().toUpperCase();
    return away && home ? `${away}@${home}` : '';
  }

  function gameMatchupKey(game) {
    return matchupKey(game?.away_tri, game?.home_tri);
  }

  function findGameCardElement(cardTarget) {
    const rawTarget = String(cardTarget || '').trim();
    if (!rawTarget || !gridRoot) {
      return null;
    }
    const byId = gridRoot.querySelector(`[data-card-id="${CSS.escape(rawTarget)}"]`);
    if (byId) {
      return byId;
    }
    return gridRoot.querySelector(`[data-matchup-key="${CSS.escape(rawTarget)}"]`);
  }

  function findScoreboardItemElement(cardTarget) {
    const rawTarget = String(cardTarget || '').trim();
    if (!rawTarget || !scoreboardRoot) {
      return null;
    }
    const byId = scoreboardRoot.querySelector(`[data-card-id="${CSS.escape(rawTarget)}"]`);
    if (byId) {
      return byId;
    }
    return scoreboardRoot.querySelector(`[data-matchup-key="${CSS.escape(rawTarget)}"]`);
  }

  function findGameByCardId(cardTarget) {
    const target = String(cardTarget || '').trim();
    if (!target) {
      return null;
    }
    return safeArray(state.payload?.games).find((game) => cardId(game) === target) || null;
  }

  function hasLoadedSimDetail(game) {
    return Boolean(game?.sim?.players_loaded);
  }

  function sameSlate(nextGames, currentGames) {
    const left = safeArray(currentGames);
    const right = safeArray(nextGames);
    if (left.length !== right.length) {
      return false;
    }
    for (let index = 0; index < left.length; index += 1) {
      if (cardId(left[index]) !== cardId(right[index])) {
        return false;
      }
    }
    return true;
  }

  function mergeSimDetail(cardTarget, detailGame) {
    const target = String(cardTarget || '').trim();
    if (!target || !detailGame || !Array.isArray(state.payload?.games)) {
      return;
    }
    const index = state.payload.games.findIndex((game) => cardId(game) === target);
    if (index < 0) {
      return;
    }
    const currentGame = state.payload.games[index] || {};
    state.payload.games[index] = {
      ...currentGame,
      ...detailGame,
      sim: {
        ...(currentGame.sim || {}),
        ...(detailGame.sim || {}),
      },
    };
  }

  function reapplyCachedSimDetails() {
    state.simDetailCache.forEach((detailGame, target) => {
      mergeSimDetail(target, detailGame);
    });
  }

  async function ensureSimDetail(cardTarget) {
    const target = String(cardTarget || '').trim();
    if (!target) {
      return;
    }
    const cached = state.simDetailCache.get(target);
    if (cached) {
      mergeSimDetail(target, cached);
      renderGameCardByTarget(target);
      return;
    }
    const game = findGameByCardId(target);
    if (!game || hasLoadedSimDetail(game) || state.simDetailLoading.has(target)) {
      return;
    }
    const away = String(game?.away_tri || '').trim().toUpperCase();
    const home = String(game?.home_tri || '').trim().toUpperCase();
    const dateValue = String(state.payload?.date || state.date || '').trim();
    if (!away || !home || !dateValue) {
      return;
    }
    state.simDetailLoading.add(target);
    renderGameCardByTarget(target);
    try {
      const params = new URLSearchParams({
        date: dateValue,
        away,
        home,
      });
      const response = await fetch(`/api/cards/sim-detail?${params.toString()}`, { cache: 'no-store' });
      const payload = await readApiJson(response, 'Failed to load game sim details.');
      const detailGame = safeArray(payload?.games)[0] || null;
      if (detailGame) {
        state.simDetailCache.set(target, detailGame);
        mergeSimDetail(target, detailGame);
      }
    } catch (_error) {
    } finally {
      state.simDetailLoading.delete(target);
      renderGameCardByTarget(target);
    }
  }

  function resolveStripCardTarget(item) {
    const matchup = stripCardTarget(item);
    if (!matchup) {
      return '';
    }
    const matchedGame = safeArray(state.payload?.games).find((game) => gameMatchupKey(game) === matchup);
    return matchedGame ? cardId(matchedGame) : matchup;
  }

  function clampNumber(value, min, max) {
    return Math.min(max, Math.max(min, value));
  }

  function parseClockToMinutes(clockText) {
    const value = String(clockText || '').trim();
    if (!value) {
      return null;
    }
    const match = value.match(/^(\d{1,2}):(\d{2})$/);
    if (!match) {
      return null;
    }
    const minutes = Number(match[1]);
    const seconds = Number(match[2]);
    if (!Number.isFinite(minutes) || !Number.isFinite(seconds)) {
      return null;
    }
    return minutes + (seconds / 60);
  }

  function liveElapsedMinutes(status) {
    if (!status) {
      return null;
    }
    if (status.final) {
      return 48;
    }
    if (!status.in_progress) {
      return 0;
    }
    const period = Number(status.period);
    const clockMinutes = parseClockToMinutes(status.clock);
    if (!Number.isFinite(period)) {
      return null;
    }
    if (period <= 4) {
      const remaining = Number.isFinite(clockMinutes) ? clampNumber(clockMinutes, 0, 12) : 0;
      return clampNumber(((period - 1) * 12) + (12 - remaining), 0, 48);
    }
    const overtimePeriod = period - 5;
    const remaining = Number.isFinite(clockMinutes) ? clampNumber(clockMinutes, 0, 5) : 0;
    return 48 + (overtimePeriod * 5) + (5 - remaining);
  }

  function impliedProbFromAmerican(value) {
    const odds = Number(value);
    if (!Number.isFinite(odds) || odds === 0) {
      return null;
    }
    if (odds > 0) {
      return 100 / (odds + 100);
    }
    return Math.abs(odds) / (Math.abs(odds) + 100);
  }

  function classifyLens(absEdge, watchThreshold, betThreshold) {
    const edge = Number(absEdge);
    if (!Number.isFinite(edge)) {
      return 'NONE';
    }
    if (edge >= Number(betThreshold)) {
      return 'BET';
    }
    if (edge >= Number(watchThreshold)) {
      return 'WATCH';
    }
    return 'NONE';
  }

  function liveLensThresholds(tuning) {
    const markets = tuning?.markets || {};
    function marketThreshold(key, watchDefault, betDefault) {
      const value = markets?.[key] || {};
      const watch = Number(value.watch);
      const bet = Number(value.bet);
      return {
        watch: Number.isFinite(watch) ? watch : watchDefault,
        bet: Number.isFinite(bet) ? bet : betDefault,
      };
    }
    return {
      total: marketThreshold('total', 3.0, 6.0),
      half_total: marketThreshold('half_total', 3.0, 6.0),
      quarter_total: marketThreshold('quarter_total', 2.0, 4.0),
      ats: marketThreshold('ats', 2.0, 4.0),
      ml: marketThreshold('ml', 0.03, 0.06),
      adjustments: tuning?.adjustments || {},
      recentWindow: tuning?.recent_window || {},
    };
  }

  function simPeriodMean(game, periodKey) {
    const periods = game?.sim?.periods || {};
    const direct = Number(periods?.[periodKey]?.total_mean);
    if (Number.isFinite(direct)) {
      return direct;
    }
    if (periodKey === 'h1') {
      const q1 = Number(periods?.q1?.total_mean);
      const q2 = Number(periods?.q2?.total_mean);
      if (Number.isFinite(q1) && Number.isFinite(q2)) {
        return q1 + q2;
      }
    }
    return null;
  }

  function livePeriodTotalFromLinescore(liveState, quarterNumber) {
    const periods = safeArray(liveState?.periods);
    const match = periods.find((entry) => Number(entry?.period) === Number(quarterNumber));
    if (!match) {
      return null;
    }
    const home = Number(match?.home);
    const away = Number(match?.away);
    return Number.isFinite(home) && Number.isFinite(away) ? home + away : null;
  }

  function halfTotalSoFar(liveState, pbpStats) {
    const currentPeriod = Number(liveState?.period);
    if (!Number.isFinite(currentPeriod) || currentPeriod > 2) {
      return null;
    }
    const q1 = livePeriodTotalFromLinescore(liveState, 1);
    const q2Linescore = livePeriodTotalFromLinescore(liveState, 2);
    const currentQuarter = Number(pbpStats?.pbp_quarters?.current?.q_total);
    if (currentPeriod === 1) {
      return Number(currentQuarter);
    }
    if (currentPeriod === 2) {
      const secondQuarter = Number.isFinite(currentQuarter) ? currentQuarter : q2Linescore;
      return Number.isFinite(q1) && Number.isFinite(secondQuarter) ? q1 + secondQuarter : null;
    }
    return null;
  }

  function signalPriority(signal) {
    if (!signal) {
      return 0;
    }
    if (signal.klass === 'BET') {
      return 2;
    }
    if (signal.klass === 'WATCH') {
      return 1;
    }
    return 0;
  }

  function signalScore(absEdge, betThreshold) {
    const edge = Number(absEdge);
    const threshold = Number(betThreshold);
    if (!Number.isFinite(edge) || !Number.isFinite(threshold) || threshold <= 0) {
      return 0;
    }
    return edge / threshold;
  }

  function rankSignals(signals) {
    return [...safeArray(signals)].sort((left, right) => (signalPriority(right) - signalPriority(left)) || ((Number(right.score) || 0) - (Number(left.score) || 0)) || (Math.abs(Number(right.edge) || 0) - Math.abs(Number(left.edge) || 0)));
  }

  function compactSignalSelection(signals) {
    const ranked = rankSignals(signals).filter((signal) => signal?.klass === 'BET' || signal?.klass === 'WATCH');
    if (!ranked.length) {
      return [];
    }
    const totals = ranked.filter((signal) => ['quarter_total', 'half_total', 'total'].includes(String(signal?.key || '')));
    const selected = [];
    if (totals.length) {
      selected.push(totals[0]);
    }
    ranked.forEach((signal) => {
      if (selected.length >= 2) {
        return;
      }
      if (!selected.some((existing) => existing?.key === signal?.key)) {
        selected.push(signal);
      }
    });
    return selected;
  }

  function signalLaneKey(signal) {
    const key = String(signal?.key || '').toLowerCase();
    if (key.includes('ml')) {
      return 'ml';
    }
    if (key.includes('ats')) {
      return 'ats';
    }
    if (key.includes('total')) {
      return 'total';
    }
    return 'other';
  }

  function featuredSignalSelection(signals) {
    const ranked = rankSignals(signals).filter((signal) => signal?.klass === 'BET' || signal?.klass === 'WATCH');
    if (!ranked.length) {
      return [null, null, null, null, null, null];
    }

    const selected = [];
    const selectedKeys = new Set();
    const requiredLanes = ['ml', 'ats', 'total'];

    requiredLanes.forEach((lane) => {
      const signal = ranked.find((entry) => signalLaneKey(entry) === lane && !selectedKeys.has(entry?.key));
      if (signal) {
        selected.push(signal);
        selectedKeys.add(signal.key);
      } else {
        selected.push(null);
      }
    });

    while (selected.length < 6) {
      const wildcard = ranked.find((entry) => !selectedKeys.has(entry?.key));
      if (!wildcard) {
        selected.push(null);
        continue;
      }
      selected.push(wildcard);
      selectedKeys.add(wildcard.key);
    }

    return selected;
  }

  function completedMarginBeforePeriod(liveState, beforePeriod) {
    const periods = safeArray(liveState?.periods);
    return periods
      .filter((entry) => Number(entry?.period) < Number(beforePeriod))
      .reduce((sum, entry) => {
        const home = Number(entry?.home);
        const away = Number(entry?.away);
        return Number.isFinite(home) && Number.isFinite(away) ? sum + (home - away) : sum;
      }, 0);
  }

  function currentQuarterMargin(liveState, currentMargin, quarterKey) {
    if (!Number.isFinite(currentMargin)) {
      return null;
    }
    const quarterNumber = Number(String(quarterKey || '').replace('q', ''));
    if (!Number.isFinite(quarterNumber)) {
      return null;
    }
    return currentMargin - completedMarginBeforePeriod(liveState, quarterNumber);
  }

  function simPeriodMargin(game, periodKey) {
    const periods = game?.sim?.periods || {};
    const direct = Number(periods?.[periodKey]?.margin_mean);
    if (Number.isFinite(direct)) {
      return direct;
    }
    if (periodKey === 'h1') {
      const q1 = Number(periods?.q1?.margin_mean);
      const q2 = Number(periods?.q2?.margin_mean);
      if (Number.isFinite(q1) && Number.isFinite(q2)) {
        return q1 + q2;
      }
    }
    return null;
  }

  function derivedPeriodMlThresholds(scopeKey) {
    if (scopeKey === 'h1') {
      return { watch: 0.08, bet: 0.14 };
    }
    return { watch: 0.1, bet: 0.18 };
  }

  function periodMlScale(minutesRemaining) {
    const remaining = clampNumber(Number(minutesRemaining) || 0, 0, 24);
    return 1.75 + (0.18 * remaining);
  }

  function projectedWinProbFromMargin(margin, minutesRemaining) {
    const marginValue = Number(margin);
    if (!Number.isFinite(marginValue)) {
      return null;
    }
    const scale = periodMlScale(minutesRemaining);
    return 1 / (1 + Math.exp(-(marginValue / scale)));
  }

  function finiteFirst(...values) {
    for (const value of values) {
      const number = Number(value);
      if (Number.isFinite(number)) {
        return number;
      }
    }
    return null;
  }

  function getLiveState(game) {
    return state.liveStates.get(gameMatchupKey(game)) || null;
  }

  function getLivePlayerBoxscore(game) {
    return state.livePlayerBoxscores.get(gameMatchupKey(game)) || null;
  }

  function getLiveLens(game) {
    return state.liveGameLens.get(gameMatchupKey(game)) || null;
  }

  function computeLiveGameLens(game, liveState, liveLines, pbpStats, tuning) {
    if (!liveState) {
      return null;
    }

    const score = game?.sim?.score || {};
    const betting = game?.betting || {};
    const thresholds = liveLensThresholds(tuning);
    const homePts = Number(liveState.home_pts);
    const awayPts = Number(liveState.away_pts);
    const currentTotal = Number.isFinite(homePts) && Number.isFinite(awayPts) ? homePts + awayPts : null;
    const currentMargin = Number.isFinite(homePts) && Number.isFinite(awayPts) ? homePts - awayPts : null;
    const elapsedMinutesRaw = liveElapsedMinutes(liveState);
    const elapsedMinutes = Number.isFinite(elapsedMinutesRaw) ? elapsedMinutesRaw : null;
    const remainingMinutes = Number.isFinite(elapsedMinutes) ? Math.max(0, 48 - Math.min(48, elapsedMinutes)) : null;
    const pregamePrior = game?.sim?.context?.pregame_prior || {};
    const pregameTotal = finiteFirst(pregamePrior?.pred_total_adjusted, pregamePrior?.pred_total, score.total_mean);
    const pregameMargin = finiteFirst(pregamePrior?.pred_margin_adjusted, pregamePrior?.pred_margin, score.margin_mean);
    const pregameHomeWin = finiteFirst(pregamePrior?.home_win_prob_adjusted, pregamePrior?.home_win_prob, betting.p_home_win);
    const lineTotal = Number(liveLines?.lines?.total);
    const homeSpread = Number(liveLines?.lines?.home_spread);
    const homeMl = Number(liveLines?.lines?.home_ml);
    const awayMl = Number(liveLines?.lines?.away_ml);
    const totalGate = Number(thresholds.adjustments?.game_total?.min_elapsed_min);
    const recentWindow = pbpStats?.pbp_recent || {};
    const currentPeriod = Number(liveState?.period);
    const periodTotals = liveLines?.lines?.period_totals || {};
    const currentQuarterKey = Number.isFinite(currentPeriod) && currentPeriod >= 1 && currentPeriod <= 4
      ? `q${Math.floor(currentPeriod)}`
      : null;
    const pregameContext = game?.sim?.context || {};

    function bucketForTeam(source, teamTri, sideKey) {
      const buckets = source && typeof source === 'object' ? source : {};
      const teamKey = String(teamTri || '').trim().toUpperCase();
      const direct = buckets?.[teamKey];
      if (direct && typeof direct === 'object') {
        return direct;
      }
      const sideValue = buckets?.[sideKey];
      if (sideValue && typeof sideValue === 'object') {
        return sideValue;
      }
      return null;
    }

    function possessionEstimateForTeam(teamTri, sideKey) {
      const bucket = bucketForTeam(pbpStats?.pbp_possessions, teamTri, sideKey);
      const direct = Number(bucket?.poss_est);
      if (Number.isFinite(direct)) {
        return direct;
      }
      return null;
    }

    function livePaceProjection(teamTri, sideKey, pregamePace) {
      if (!Number.isFinite(elapsedMinutes) || elapsedMinutes <= 0) {
        return Number.isFinite(Number(pregamePace)) ? Number(pregamePace) : null;
      }
      const possEst = possessionEstimateForTeam(teamTri, sideKey);
      const pregame = Number(pregamePace);
      if (!Number.isFinite(possEst)) {
        return Number.isFinite(pregame) ? pregame : null;
      }
      const projected = (possEst / Math.max(elapsedMinutes, 1)) * 48;
      if (!Number.isFinite(projected)) {
        return Number.isFinite(pregame) ? pregame : null;
      }
      if (!Number.isFinite(pregame)) {
        return projected;
      }
      const blendWeight = clampNumber(elapsedMinutes / 48, 0.18, 1);
      return ((1 - blendWeight) * pregame) + (blendWeight * projected);
    }

    function buildSignal(key, label, klass, side, edge, line, projection, extraDetail) {
      const sideLabel = side ? `${side} ` : '';
      const edgeValue = Number(edge);
      const projectionValue = Number(projection);
      const lineValue = Number(line);
      const parts = [];
      if (Number.isFinite(projectionValue) && Number.isFinite(lineValue)) {
        parts.push(`Proj ${fmtNumber(projectionValue, 1)} vs ${fmtNumber(lineValue, 1)}`);
      }
      if (extraDetail) {
        parts.push(extraDetail);
      }
      return {
        key,
        label,
        klass,
        side,
        edge: Number.isFinite(edgeValue) ? edgeValue : null,
        line: Number.isFinite(lineValue) ? lineValue : null,
        projection: Number.isFinite(projectionValue) ? projectionValue : null,
        score: 0,
        detail: parts.join(' · '),
        compactLabel: klass === 'NONE' ? `${label} —` : `${label} ${klass} ${sideLabel}${key === 'ml' ? fmtSigned(edgeValue * 100, 1) + 'pp' : fmtSigned(edgeValue, 1)}`.trim(),
      };
    }

    let totalSignal = null;
    if (liveState.in_progress && Number.isFinite(currentTotal) && Number.isFinite(elapsedMinutes) && Number.isFinite(lineTotal)) {
      const elapsedForRate = Math.max(elapsedMinutes, 1);
      const liveRate = currentTotal / elapsedForRate;
      const paceRaw = currentTotal + (liveRate * Math.max(0, remainingMinutes || 0));
      const blendWeight = clampNumber(elapsedForRate / 48, 0.12, 1);
      let projection = Number.isFinite(pregameTotal)
        ? ((1 - blendWeight) * pregameTotal) + (blendWeight * paceRaw)
        : paceRaw;

      const recentPoints = Number(recentWindow.points_total);
      const recentWindowSec = Number(recentWindow.window_sec);
      if (Number.isFinite(recentPoints) && Number.isFinite(recentWindowSec) && recentWindowSec > 0 && Number.isFinite(remainingMinutes) && elapsedForRate >= 6) {
        const recentRate = recentPoints / (recentWindowSec / 60);
        const gameRate = currentTotal / elapsedForRate;
        const paceCap = Number(thresholds.recentWindow?.pace_cap_points);
        const maxRecentAdj = Number.isFinite(paceCap) ? paceCap : 3;
        const recentAdj = clampNumber((recentRate - gameRate) * Math.min(remainingMinutes, 12) * 0.2, -maxRecentAdj, maxRecentAdj);
        projection += recentAdj;
      }

      const edge = projection - lineTotal;
      const side = edge > 1 ? 'Over' : (edge < -1 ? 'Under' : 'No edge');
      const klass = Number.isFinite(totalGate) && elapsedForRate < totalGate
        ? 'WAIT'
        : classifyLens(Math.abs(edge), thresholds.total.watch, thresholds.total.bet);
      const priorDetail = Number.isFinite(pregameTotal) ? `Prior ${fmtNumber(pregameTotal, 1)}` : null;
      totalSignal = buildSignal('total', 'G', klass, side, edge, lineTotal, projection, [priorDetail, `Total ${fmtInteger(currentTotal)}`].filter(Boolean).join(' · '));
      totalSignal.score = signalScore(Math.abs(edge), thresholds.total.bet);
    }

    let halfSignal = null;
    if (liveState.in_progress && Number.isFinite(currentPeriod) && currentPeriod <= 2) {
      const halfLine = Number(periodTotals?.h1);
      const halfActual = halfTotalSoFar(liveState, pbpStats);
      const halfSim = simPeriodMean(game, 'h1');
      const halfMinutesElapsed = Number.isFinite(elapsedMinutes) ? elapsedMinutes : null;
      const halfMinutesRemaining = Number.isFinite(halfMinutesElapsed) ? Math.max(0, 24 - Math.min(24, halfMinutesElapsed)) : null;
      if (Number.isFinite(halfLine) && Number.isFinite(halfActual) && Number.isFinite(halfMinutesElapsed) && Number(halfMinutesRemaining) > 0) {
        const elapsedForRate = Math.max(halfMinutesElapsed, 1);
        const liveRate = halfActual / elapsedForRate;
        const paceRaw = halfActual + (liveRate * Math.max(0, halfMinutesRemaining || 0));
        const blendWeight = clampNumber(elapsedForRate / 24, 0.15, 1);
        const projection = Number.isFinite(halfSim)
          ? ((1 - blendWeight) * halfSim) + (blendWeight * paceRaw)
          : paceRaw;
        const edge = projection - halfLine;
        const side = edge > 1 ? 'Over' : (edge < -1 ? 'Under' : 'No edge');
        const klass = classifyLens(Math.abs(edge), thresholds.half_total.watch, thresholds.half_total.bet);
        halfSignal = buildSignal('half_total', '1H', klass, side, edge, halfLine, projection, `Total ${fmtInteger(halfActual)}`);
        halfSignal.score = signalScore(Math.abs(edge), thresholds.half_total.bet);
      }
    }

    let quarterSignal = null;
    if (liveState.in_progress && currentQuarterKey) {
      const quarterLine = Number(periodTotals?.[currentQuarterKey]);
      const quarterActual = Number(pbpStats?.pbp_quarters?.current?.q_total);
      const quarterSim = simPeriodMean(game, currentQuarterKey);
      const quarterMinutesElapsed = Number.isFinite(currentPeriod) && Number.isFinite(elapsedMinutes)
        ? Math.max(0, elapsedMinutes - ((Math.floor(currentPeriod) - 1) * 12))
        : null;
      const quarterMinutesRemaining = Number.isFinite(quarterMinutesElapsed) ? Math.max(0, 12 - Math.min(12, quarterMinutesElapsed)) : null;
      if (Number.isFinite(quarterLine) && Number.isFinite(quarterActual) && Number.isFinite(quarterMinutesElapsed) && Number(quarterMinutesRemaining) > 0) {
        const elapsedForRate = Math.max(quarterMinutesElapsed, 1);
        const liveRate = quarterActual / elapsedForRate;
        const paceRaw = quarterActual + (liveRate * Math.max(0, quarterMinutesRemaining || 0));
        const blendWeight = clampNumber(elapsedForRate / 12, 0.18, 1);
        const projection = Number.isFinite(quarterSim)
          ? ((1 - blendWeight) * quarterSim) + (blendWeight * paceRaw)
          : paceRaw;
        const edge = projection - quarterLine;
        const side = edge > 1 ? 'Over' : (edge < -1 ? 'Under' : 'No edge');
        const klass = classifyLens(Math.abs(edge), thresholds.quarter_total.watch, thresholds.quarter_total.bet);
        quarterSignal = buildSignal('quarter_total', String(currentQuarterKey).toUpperCase(), klass, side, edge, quarterLine, projection, `Total ${fmtInteger(quarterActual)}`);
        quarterSignal.score = signalScore(Math.abs(edge), thresholds.quarter_total.bet);
      }
    }

    let halfAtsSignal = null;
    let halfMlSignal = null;
    if (liveState.in_progress && Number.isFinite(currentPeriod) && currentPeriod <= 2) {
      const halfSpread = Number(liveLines?.lines?.period_spreads?.h1);
      const actualHalfMargin = Number.isFinite(currentMargin) ? currentMargin : null;
      const simHalfMargin = simPeriodMargin(game, 'h1');
      const halfMinutesElapsed = Number.isFinite(elapsedMinutes) ? elapsedMinutes : null;
      const halfMinutesRemaining = Number.isFinite(halfMinutesElapsed) ? Math.max(0, 24 - Math.min(24, halfMinutesElapsed)) : null;
      if (Number.isFinite(actualHalfMargin) && Number.isFinite(halfMinutesElapsed) && Number(halfMinutesRemaining) > 0) {
        const blendWeight = clampNumber(halfMinutesElapsed / 24, 0.18, 1);
        const projectedHalfMargin = Number.isFinite(simHalfMargin)
          ? ((1 - blendWeight) * simHalfMargin) + (blendWeight * actualHalfMargin)
          : actualHalfMargin;

        if (Number.isFinite(halfSpread)) {
          const homeEdge = projectedHalfMargin + halfSpread;
          const awayEdge = -projectedHalfMargin - halfSpread;
          const pickHome = Math.abs(homeEdge) >= Math.abs(awayEdge);
          const edge = pickHome ? homeEdge : awayEdge;
          const side = pickHome ? game?.home_tri : game?.away_tri;
          const line = pickHome ? halfSpread : -halfSpread;
          const projection = pickHome ? projectedHalfMargin : -projectedHalfMargin;
          const klass = classifyLens(Math.abs(edge), thresholds.ats.watch, thresholds.ats.bet);
          halfAtsSignal = buildSignal('half_ats', '1H ATS', klass, side, edge, line, projection, `Margin ${fmtSigned(projectedHalfMargin, 1)}`);
          halfAtsSignal.score = signalScore(Math.abs(edge), thresholds.ats.bet);
        }

        const halfMlThresholds = derivedPeriodMlThresholds('h1');
        const pHomeHalf = projectedWinProbFromMargin(projectedHalfMargin, halfMinutesRemaining);
        if (Number.isFinite(pHomeHalf)) {
          const homeEdge = pHomeHalf - 0.5;
          const awayEdge = (1 - pHomeHalf) - 0.5;
          const pickHome = Math.abs(homeEdge) >= Math.abs(awayEdge);
          const edge = pickHome ? homeEdge : awayEdge;
          const side = pickHome ? game?.home_tri : game?.away_tri;
          const projection = pickHome ? pHomeHalf : (1 - pHomeHalf);
          const klass = classifyLens(Math.abs(edge), halfMlThresholds.watch, halfMlThresholds.bet);
          halfMlSignal = buildSignal('half_ml', '1H ML', klass, side, edge, 0.5, projection, `Model ${fmtPercent(projection, 0)}`);
          halfMlSignal.score = signalScore(Math.abs(edge), halfMlThresholds.bet);
        }
      }
    }

    let quarterAtsSignal = null;
    let quarterMlSignal = null;
    if (liveState.in_progress && currentQuarterKey) {
      const quarterSpread = Number(liveLines?.lines?.period_spreads?.[currentQuarterKey]);
      const actualQuarterMargin = currentQuarterMargin(liveState, currentMargin, currentQuarterKey);
      const simQuarterMargin = simPeriodMargin(game, currentQuarterKey);
      const quarterMinutesElapsed = Number.isFinite(currentPeriod) && Number.isFinite(elapsedMinutes)
        ? Math.max(0, elapsedMinutes - ((Math.floor(currentPeriod) - 1) * 12))
        : null;
      const quarterMinutesRemaining = Number.isFinite(quarterMinutesElapsed) ? Math.max(0, 12 - Math.min(12, quarterMinutesElapsed)) : null;
      if (Number.isFinite(actualQuarterMargin) && Number.isFinite(quarterMinutesElapsed) && Number(quarterMinutesRemaining) > 0) {
        const blendWeight = clampNumber(quarterMinutesElapsed / 12, 0.22, 1);
        const projectedQuarterMargin = Number.isFinite(simQuarterMargin)
          ? ((1 - blendWeight) * simQuarterMargin) + (blendWeight * actualQuarterMargin)
          : actualQuarterMargin;

        if (Number.isFinite(quarterSpread)) {
          const homeEdge = projectedQuarterMargin + quarterSpread;
          const awayEdge = -projectedQuarterMargin - quarterSpread;
          const pickHome = Math.abs(homeEdge) >= Math.abs(awayEdge);
          const edge = pickHome ? homeEdge : awayEdge;
          const side = pickHome ? game?.home_tri : game?.away_tri;
          const line = pickHome ? quarterSpread : -quarterSpread;
          const projection = pickHome ? projectedQuarterMargin : -projectedQuarterMargin;
          const klass = classifyLens(Math.abs(edge), thresholds.ats.watch, thresholds.ats.bet);
          quarterAtsSignal = buildSignal('quarter_ats', `${String(currentQuarterKey).toUpperCase()} ATS`, klass, side, edge, line, projection, `Margin ${fmtSigned(projectedQuarterMargin, 1)}`);
          quarterAtsSignal.score = signalScore(Math.abs(edge), thresholds.ats.bet);
        }

        const quarterMlThresholds = derivedPeriodMlThresholds(currentQuarterKey);
        const pHomeQuarter = projectedWinProbFromMargin(projectedQuarterMargin, quarterMinutesRemaining);
        if (Number.isFinite(pHomeQuarter)) {
          const homeEdge = pHomeQuarter - 0.5;
          const awayEdge = (1 - pHomeQuarter) - 0.5;
          const pickHome = Math.abs(homeEdge) >= Math.abs(awayEdge);
          const edge = pickHome ? homeEdge : awayEdge;
          const side = pickHome ? game?.home_tri : game?.away_tri;
          const projection = pickHome ? pHomeQuarter : (1 - pHomeQuarter);
          const klass = classifyLens(Math.abs(edge), quarterMlThresholds.watch, quarterMlThresholds.bet);
          quarterMlSignal = buildSignal('quarter_ml', `${String(currentQuarterKey).toUpperCase()} ML`, klass, side, edge, 0.5, projection, `Model ${fmtPercent(projection, 0)}`);
          quarterMlSignal.score = signalScore(Math.abs(edge), quarterMlThresholds.bet);
        }
      }
    }

    let atsSignal = null;
    if (liveState.in_progress && Number.isFinite(currentMargin) && Number.isFinite(homeSpread) && Number.isFinite(elapsedMinutes)) {
      const blendWeight = clampNumber(Math.max(elapsedMinutes, 0) / 48, 0, 1);
      const projectedMargin = Number.isFinite(pregameMargin)
        ? ((1 - blendWeight) * pregameMargin) + (blendWeight * currentMargin)
        : currentMargin;
      const homeEdge = projectedMargin + homeSpread;
      const awayEdge = -projectedMargin - homeSpread;
      const pickHome = Math.abs(homeEdge) >= Math.abs(awayEdge);
      const edge = pickHome ? homeEdge : awayEdge;
      const side = pickHome ? game?.home_tri : game?.away_tri;
      const projection = pickHome ? projectedMargin : -projectedMargin;
      const line = pickHome ? homeSpread : -homeSpread;
      const klass = classifyLens(Math.abs(edge), thresholds.ats.watch, thresholds.ats.bet);
      const priorDetail = Number.isFinite(pregameMargin) ? `Prior ${fmtSigned(pregameMargin, 1)}` : null;
      atsSignal = buildSignal('ats', 'ATS', klass, side, edge, line, projection, [priorDetail, `Margin ${fmtSigned(projectedMargin, 1)}`].filter(Boolean).join(' · '));
      atsSignal.score = signalScore(Math.abs(edge), thresholds.ats.bet);
    }

    let mlSignal = null;
    if (liveState.in_progress && Number.isFinite(currentMargin) && Number.isFinite(elapsedMinutes) && Number.isFinite(homeMl) && Number.isFinite(awayMl)) {
      const minLeft = Number.isFinite(remainingMinutes) ? remainingMinutes : 48;
      const scale = 6 + (0.35 * minLeft);
      const scoreProb = 1 / (1 + Math.exp(-(currentMargin / scale)));
      const blendWeight = clampNumber(Math.max(elapsedMinutes, 0) / 48, 0, 1);
      const pHomeModel = Number.isFinite(pregameHomeWin)
        ? ((1 - blendWeight) * pregameHomeWin) + (blendWeight * scoreProb)
        : scoreProb;
      const pHomeImplied = impliedProbFromAmerican(homeMl);
      const pAwayImplied = impliedProbFromAmerican(awayMl);
      if (Number.isFinite(pHomeImplied) && Number.isFinite(pAwayImplied)) {
        const homeEdge = pHomeModel - pHomeImplied;
        const awayEdge = (1 - pHomeModel) - pAwayImplied;
        const pickHome = Math.abs(homeEdge) >= Math.abs(awayEdge);
        const edge = pickHome ? homeEdge : awayEdge;
        const side = pickHome ? game?.home_tri : game?.away_tri;
        const line = pickHome ? homeMl : awayMl;
        const projection = pickHome ? pHomeModel : (1 - pHomeModel);
        const klass = classifyLens(Math.abs(edge), thresholds.ml.watch, thresholds.ml.bet);
        const priorDetail = Number.isFinite(pregameHomeWin) ? `Prior ${fmtPercent(pickHome ? pregameHomeWin : (1 - pregameHomeWin), 0)}` : null;
        mlSignal = buildSignal('ml', 'ML', klass, side, edge, line, projection, [priorDetail, `Model ${fmtPercent(projection, 0)}`].filter(Boolean).join(' · '));
        mlSignal.score = signalScore(Math.abs(edge), thresholds.ml.bet);
      }
    }

    const signals = [quarterSignal, halfSignal, totalSignal, quarterAtsSignal, halfAtsSignal, atsSignal, quarterMlSignal, halfMlSignal, mlSignal].filter(Boolean);
    const rankedSignals = rankSignals(signals).filter((signal) => signal.klass === 'BET' || signal.klass === 'WATCH');
    const topSignals = featuredSignalSelection(signals);
    const compactSignals = compactSignalSelection(signals);

    const overallClass = rankedSignals[0]?.klass || 'NONE';
    const scoreLabel = hasStartedGame(liveState) && Number.isFinite(awayPts) && Number.isFinite(homePts)
      ? `${game?.away_tri || 'AWY'} ${fmtInteger(awayPts)} - ${fmtInteger(homePts)} ${game?.home_tri || 'HME'}`
      : `${game?.away_tri || 'AWY'} at ${game?.home_tri || 'HME'}`;
    const statusLabel = liveState.final
      ? 'Final'
      : (liveState.in_progress ? String(liveState.status || `Q${liveState.period || ''} ${liveState.clock || ''}`).trim() : 'Scheduled');
    const awayPace = livePaceProjection(game?.away_tri, 'away', pregameContext.away_pace);
    const homePace = livePaceProjection(game?.home_tri, 'home', pregameContext.home_pace);
    const awayAttempts = bucketForTeam(pbpStats?.pbp_attempts, game?.away_tri, 'away');
    const homeAttempts = bucketForTeam(pbpStats?.pbp_attempts, game?.home_tri, 'home');
    const awayPossessions = possessionEstimateForTeam(game?.away_tri, 'away');
    const homePossessions = possessionEstimateForTeam(game?.home_tri, 'home');

    return {
      statusLabel,
      scoreLabel,
      currentTotal,
      currentMargin,
      elapsedMinutes,
      awayPace,
      homePace,
      awayPossessions,
      homePossessions,
      awayAttempts,
      homeAttempts,
      signals: {
        quarter_total: quarterSignal,
        half_total: halfSignal,
        total: totalSignal,
        quarter_ats: quarterAtsSignal,
        half_ats: halfAtsSignal,
        ats: atsSignal,
        quarter_ml: quarterMlSignal,
        half_ml: halfMlSignal,
        ml: mlSignal,
      },
      compactSignals,
      topSignals,
      overallClass,
    };
  }

  async function loadLiveGameLens(dateValue, games, options = {}) {
    const silent = Boolean(options?.silent);
    state.liveDataLoading = true;
    if (mode !== 'live') {
      state.liveDataLoading = false;
      return;
    }
    const nextLiveGameLens = new Map();
    const nextLivePlayerBoxscores = new Map();
    const nextLiveStates = new Map();
    try {
      const liveStatePayload = await fetchApiJson(
        `/api/live_state?date=${encodeURIComponent(dateValue)}`,
        'Failed to load live state.',
        { retries: silent ? 2 : 1 }
      );

      const payloadGames = safeArray(games);
      const liveStateMap = new Map();
      safeArray(liveStatePayload?.games).forEach((item) => {
        const key = matchupKey(item?.away, item?.home);
        if (key) {
          liveStateMap.set(key, item);
          nextLiveStates.set(key, item);
        }
      });

      const matchedStates = payloadGames
        .map((game) => ({ game, liveState: liveStateMap.get(gameMatchupKey(game)) || null }))
        .filter((entry) => entry.liveState);
      const cardsByMatchup = new Map(matchedStates.map((entry) => [gameMatchupKey(entry.game), entry.game]));
      const eventIds = matchedStates
        .map((entry) => String(entry.liveState?.event_id || '').trim())
        .filter(Boolean);
      const eventIdToMatchup = new Map(
        matchedStates
          .map((entry) => [String(entry.liveState?.event_id || '').trim(), gameMatchupKey(entry.game)])
          .filter(([eventId, matchup]) => eventId && matchup)
      );

      let liveLinesMap = new Map();
      let pbpMap = new Map();
      let tuning = null;
      let liveBoxscorePayload = null;

      if (eventIds.length) {
        const [linesPayload, pbpPayload, tuningPayload, boxscorePayload] = await Promise.all([
          fetchApiJson(
            `/api/live_lines?date=${encodeURIComponent(dateValue)}&event_ids=${encodeURIComponent(eventIds.join(','))}&include_period_totals=1`,
            'Failed to load live lines.',
            { retries: silent ? 2 : 1 }
          ),
          fetchApiJson(
            `/api/live_pbp_stats?date=${encodeURIComponent(dateValue)}&event_ids=${encodeURIComponent(eventIds.join(','))}`,
            'Failed to load live PBP stats.',
            { retries: silent ? 2 : 1 }
          ),
          fetchApiJson('/api/live_lens_tuning?ttl=300', 'Failed to load live lens tuning.', { retries: 1 }),
          fetchApiJson(
            `/api/live_player_boxscore?event_ids=${encodeURIComponent(eventIds.join(','))}`,
            'Failed to load live player boxscore.',
            { retries: silent ? 2 : 1 }
          ),
        ]);
        tuning = tuningPayload || null;
        liveBoxscorePayload = boxscorePayload || null;

        safeArray(linesPayload?.games).forEach((item) => {
          const eventId = String(item?.event_id || '').trim();
          if (eventId) {
            liveLinesMap.set(eventId, item);
          }
        });
        safeArray(pbpPayload?.games).forEach((item) => {
          const eventId = String(item?.event_id || '').trim();
          if (eventId) {
            pbpMap.set(eventId, item);
          }
        });

        safeArray(liveBoxscorePayload?.games).forEach((entry) => {
          const eventId = String(entry?.event_id || '').trim();
          const matchup = eventIdToMatchup.get(eventId);
          const game = matchup ? cardsByMatchup.get(matchup) : null;
          if (!matchup || !game) {
            return;
          }
          const awayTri = String(game?.away_tri || '').trim().toUpperCase();
          const homeTri = String(game?.home_tri || '').trim().toUpperCase();
          const grouped = { away: [], home: [] };
          safeArray(entry?.players).forEach((row) => {
            const teamTri = String(row?.team_tri || '').trim().toUpperCase();
            if (teamTri === awayTri) {
              grouped.away.push(row);
            } else if (teamTri === homeTri) {
              grouped.home.push(row);
            }
          });
          grouped.away.sort((left, right) => (Number(right?.mp || 0) - Number(left?.mp || 0)) || (Number(right?.pts || 0) - Number(left?.pts || 0)));
          grouped.home.sort((left, right) => (Number(right?.mp || 0) - Number(left?.mp || 0)) || (Number(right?.pts || 0) - Number(left?.pts || 0)));
          nextLivePlayerBoxscores.set(matchup, grouped);
        });
      }

      matchedStates.forEach(({ game, liveState }) => {
        const eventId = String(liveState?.event_id || '').trim();
        const liveLines = eventId ? liveLinesMap.get(eventId) : null;
        const pbpStats = eventId ? pbpMap.get(eventId) : null;
        const lens = computeLiveGameLens(game, liveState, liveLines, pbpStats, tuning);
        const key = gameMatchupKey(game);
        if (key && lens) {
          nextLiveGameLens.set(key, lens);
        }
      });
      state.liveGameLens = nextLiveGameLens;
      state.livePlayerBoxscores = nextLivePlayerBoxscores;
      state.liveStates = nextLiveStates;
    } catch (error) {
      if (!silent) {
        state.liveGameLens = new Map();
        state.livePlayerBoxscores = new Map();
        state.liveStates = new Map();
      }
      showNote(error?.message || 'Failed to refresh live signals.', 'warning');
    } finally {
      state.liveDataLoading = false;
    }
  }

  function statusClass(game) {
    const warnings = Array.isArray(game?.warnings) ? game.warnings : [];
    if (mode === 'live') {
      const liveState = getLiveState(game);
      if (liveState?.final) {
        return 'is-final';
      }
      if (liveState?.in_progress) {
        return 'is-live';
      }
      return 'is-soft';
    }
    if (warnings.length) {
      return 'is-warn';
    }
    return 'is-soft';
  }

  function statusText(game) {
    if (mode === 'live') {
      const liveLens = getLiveLens(game);
      const liveState = getLiveState(game);
      if (!hasStartedGame(liveState)) {
        return 'Scheduled';
      }
      return liveLens?.statusLabel || String(liveState?.status || 'Live');
    }
    return 'Scheduled';
  }

  function liveSignalChipClass(signal) {
    if (signal?.klass === 'BET') {
      return 'cards-chip cards-chip--accent';
    }
    if (signal?.klass === 'WATCH') {
      return 'cards-chip cards-chip--warm';
    }
    return 'cards-chip';
  }

  function renderLiveSignalChip(signal) {
    if (!signal) {
      return '';
    }
    return `<span class="${liveSignalChipClass(signal)}" title="${escapeHtml(signal.detail || signal.compactLabel || '')}">${escapeHtml(signal.compactLabel || signal.label || 'Signal')}</span>`;
  }

  function renderLiveSignalTile(signal) {
    if (!signal) {
      return `
        <div class="cards-live-lens-tile is-empty">
          <div class="cards-market-kicker">Signal</div>
          <div class="cards-market-main">No live edge</div>
          <div class="cards-mini-copy">Waiting for in-game data.</div>
        </div>
      `;
    }
    const edgeText = signal.key === 'ml'
      ? `${fmtSigned((Number(signal.edge) || 0) * 100, 1)}pp`
      : fmtSigned(signal.edge, 1);
    return `
      <div class="cards-live-lens-tile ${signal.klass === 'BET' ? 'is-bet' : (signal.klass === 'WATCH' ? 'is-watch' : '')}">
        <div class="cards-live-lens-tile__head">
          <div class="cards-market-kicker">${escapeHtml(signal.label)}</div>
          <span class="${liveSignalChipClass(signal)}">${escapeHtml(signal.klass)}</span>
        </div>
        <div class="cards-market-main">${escapeHtml(signal.side || 'No edge')}</div>
        <div class="cards-live-lens-tile__edge">${escapeHtml(edgeText)}</div>
        <div class="cards-mini-copy">${escapeHtml(signal.detail || 'No live edge detail')}</div>
      </div>
    `;
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
    if (sourceMeta) {
      sourceMeta.innerHTML = '<span>Loading slate...</span>';
    }
    scoreboardRoot.innerHTML = '<div class="cards-loading-strip">Loading scoreboard...</div>';
    gridRoot.innerHTML = '<div class="cards-loading-state">Loading cards...</div>';
    state.boardInitialized = false;
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
      const liveState = getLiveState(game);
      if (liveState?.final) {
        return 'final';
      }
      if (liveState?.in_progress) {
        return 'live';
      }
      return 'upcoming';
    }
    return 'scheduled';
  }

  function gameStatusSortValue(game) {
    const status = cardStatus(game);
    if (status === 'live') {
      return 0;
    }
    if (status === 'upcoming' || status === 'scheduled') {
      return 1;
    }
    if (status === 'final') {
      return 2;
    }
    return 3;
  }

  function gameCommenceSortValue(game) {
    const raw = String(game?.odds?.commence_time || '').trim();
    if (!raw) {
      return Number.POSITIVE_INFINITY;
    }
    const value = new Date(raw).getTime();
    return Number.isFinite(value) ? value : Number.POSITIVE_INFINITY;
  }

  function sortGamesForDisplay(games) {
    return [...safeArray(games)].sort((left, right) => {
      const statusDiff = gameStatusSortValue(left) - gameStatusSortValue(right);
      if (statusDiff !== 0) {
        return statusDiff;
      }

      const leftStatus = cardStatus(left);
      const rightStatus = cardStatus(right);
      if (leftStatus === 'live' && rightStatus === 'live') {
        const leftElapsed = Number(liveElapsedMinutes(getLiveState(left)) || 0);
        const rightElapsed = Number(liveElapsedMinutes(getLiveState(right)) || 0);
        if (leftElapsed !== rightElapsed) {
          return rightElapsed - leftElapsed;
        }
      }

      const timeDiff = gameCommenceSortValue(left) - gameCommenceSortValue(right);
      if (timeDiff !== 0) {
        return timeDiff;
      }

      return cardId(left).localeCompare(cardId(right));
    });
  }

  function hasStartedGame(liveState) {
    return Boolean(liveState?.in_progress || liveState?.final);
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

  function buildFilters(games) {
    const counts = slateCounts(games);
    return [
      { key: 'all', label: 'All', count: games.length },
      { key: 'official', label: 'Official', count: counts.officialCount },
      { key: 'props', label: 'Props', count: counts.propsCount },
      { key: 'live', label: 'Live', count: counts.liveCount },
      { key: 'final', label: 'Final', count: counts.finalCount },
    ];
  }

  function payloadLookaheadText() {
    if (state.payload?.lookahead_applied && state.payload?.requested_date && state.payload?.date && state.payload.date !== state.payload.requested_date) {
      return `No slate for ${state.payload.requested_date}; showing next available board from ${state.payload.date}.`;
    }
    return '';
  }

  function renderHeaderMeta() {
    const games = safeArray(state.payload?.games);
    const counts = slateCounts(games);
    const boardDate = currentBoardDate();
    if (headerMeta) {
      const headline = `${games.length} games · ${counts.upcomingCount} upcoming · ${counts.liveCount} live · ${counts.officialCount} official`;
      const lookaheadText = payloadLookaheadText();
      headerMeta.textContent = lookaheadText
        ? `${headline} · ${lookaheadText}`
        : headline;
    }
  }

  function sourceMetaPill(label, variant) {
    return `<span class="cards-source-meta-pill${variant ? ` is-${escapeHtml(variant)}` : ''}">${escapeHtml(label)}</span>`;
  }

  function renderSourceMeta() {
    if (!sourceMeta) {
      return;
    }
    const games = safeArray(state.payload?.games);
    const counts = slateCounts(games);
    const pills = [
      sourceMetaPill(currentBoardDate()),
      sourceMetaPill(mode === 'live' ? 'Live board' : 'Pregame board', mode === 'live' ? 'live' : 'soft'),
      sourceMetaPill(`${counts.propsCount} props-ready games`, counts.propsCount ? 'accent' : 'soft'),
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
          ${escapeHtml(`${filter.label} ${filter.count}`)}
        </button>
      `)
      .join('');
  }

  function applySlateFilter(filterKey) {
    state.filter = String(filterKey || 'all');
    renderFilters();
    renderBoard();
  }

  function setPropsStripLoading() {
    if (!propsStripEl) {
      return;
    }
    propsStripEl.classList.remove('hidden');
    propsStripEl.innerHTML = '<div class="cards-empty">Loading player prop strip...</div>';
  }

  function clearPropsStrip() {
    if (!propsStripEl) {
      return;
    }
    propsStripEl.innerHTML = '';
    propsStripEl.classList.add('hidden');
  }

  function stripStatusText(item) {
    if (mode === 'live') {
      return String(item?.status_label || item?.klass || 'Live').trim() || 'Live';
    }
    if (Number.isFinite(Number(item?.line_move))) {
      return `Line move ${fmtSigned(item.line_move, 1)}`;
    }
    if (item?.book) {
      return String(item.book).toUpperCase();
    }
    return 'Pregame';
  }

  function stripSecondaryText(item) {
    if (mode === 'live') {
      const lineSource = String(item?.line_source || '').trim();
      const simValue = Number(item?.sim_mu);
      const simAdjusted = Number(item?.sim_mu_adjusted);
      const liveLine = Number(item?.line);
      const priorMult = Number(item?.pregame_stat_multiplier);
      const pieces = [];
      if (lineSource) {
        pieces.push(lineSource === 'oddsapi' ? 'Live OddsAPI' : titleCase(lineSource));
      }
      if (Number.isFinite(simAdjusted) && Number.isFinite(liveLine)) {
        pieces.push(`Adj sim ${fmtNumber(simAdjusted, 1)} vs ${fmtNumber(liveLine, 1)}`);
      } else if (Number.isFinite(simValue) && Number.isFinite(liveLine)) {
        pieces.push(`Sim ${fmtNumber(simValue, 1)} vs ${fmtNumber(liveLine, 1)}`);
      }
      if (Number.isFinite(priorMult) && Math.abs(priorMult - 1) >= 0.01) {
        pieces.push(`Prior x${fmtNumber(priorMult, 2)}`);
      }
      return pieces.join(' · ') || 'Live player prop lens';
    }
    const pieces = [];
    if (item?.book) {
      pieces.push(String(item.book).toUpperCase());
    }
    if (Number.isFinite(Number(item?.open_line))) {
      pieces.push(`Open ${fmtNumber(item.open_line, 1)}`);
    }
    return pieces.join(' · ') || 'Daily recommendations export';
  }

  function liveProjectionSummary(item) {
    if (mode !== 'live') {
      return '';
    }
    const actual = Number(item?.actual);
    const paceProj = Number(item?.pace_proj);
    const simValue = Number(item?.sim_mu);
    const simAdjusted = Number(item?.sim_mu_adjusted);
    const line = Number(item?.line);
    const teamRatio = Number(item?.pregame_team_total_ratio);
    const pieces = [];
    if (Number.isFinite(actual)) {
      pieces.push(`Actual ${fmtNumber(actual, 1)}`);
    }
    if (Number.isFinite(paceProj)) {
      pieces.push(`Proj ${fmtNumber(paceProj, 1)}`);
    }
    if (Number.isFinite(simAdjusted)) {
      pieces.push(`Adj ${fmtNumber(simAdjusted, 1)}`);
    }
    if (Number.isFinite(simValue)) {
      pieces.push(`Sim ${fmtNumber(simValue, 1)}`);
    }
    if (Number.isFinite(line) && Number.isFinite(paceProj)) {
      pieces.push(`Edge ${fmtSigned(paceProj - line, 1)}`);
    }
    if (Number.isFinite(teamRatio) && Math.abs(teamRatio - 1) >= 0.02) {
      pieces.push(`Team ${fmtSigned((teamRatio - 1) * 100, 0)}%`);
    }
    return pieces.join(' · ');
  }

  function normalizePlayerKey(value) {
    return String(value || '')
      .toUpperCase()
      .replace(/[^A-Z0-9\s]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function simStatMean(row, market) {
    const key = String(market || '').trim().toLowerCase();
    if (!row) {
      return null;
    }
    return {
      pts: Number(row?.pts_mean),
      reb: Number(row?.reb_mean),
      ast: Number(row?.ast_mean),
      threes: Number(row?.threes_mean),
      stl: Number(row?.stl_mean),
      blk: Number(row?.blk_mean),
      tov: Number(row?.tov_mean),
      pra: Number(row?.pra_mean),
    }[key] ?? null;
  }

  function actualStatValue(row, market) {
    const key = String(market || '').trim().toLowerCase();
    if (!row) {
      return null;
    }
    if (key === 'pra') {
      const pts = Number(row?.pts);
      const reb = Number(row?.reb);
      const ast = Number(row?.ast);
      return Number.isFinite(pts) && Number.isFinite(reb) && Number.isFinite(ast) ? pts + reb + ast : null;
    }
    return {
      pts: Number(row?.pts),
      reb: Number(row?.reb),
      ast: Number(row?.ast),
      threes: Number(row?.threes_made),
      stl: Number(row?.stl),
      blk: Number(row?.blk),
      tov: Number(row?.tov),
    }[key] ?? null;
  }

  function livePropThresholds(market) {
    const key = String(market || '').trim().toLowerCase();
    if (key === 'pts' || key === 'pra') {
      return { watch: 2.0, bet: 4.0 };
    }
    if (key === 'reb' || key === 'ast') {
      return { watch: 1.5, bet: 3.0 };
    }
    return { watch: 0.5, bet: 1.0 };
  }

  function classifyLivePropFallback(edge, market) {
    const thresholds = livePropThresholds(market);
    const absEdge = Math.abs(Number(edge) || 0);
    if (absEdge >= thresholds.bet) {
      return 'BET';
    }
    if (absEdge >= thresholds.watch) {
      return 'WATCH';
    }
    return 'NONE';
  }

  function propPhotoUrl(item) {
    const direct = String(item?.player_photo || item?.photo || '').trim();
    if (direct) {
      return direct;
    }
    const playerId = String(item?.player_id || '').trim();
    return playerId ? `https://cdn.nba.com/headshots/nba/latest/1040x760/${encodeURIComponent(playerId)}.png` : '';
  }

  function estimatedLiveProjection(actual, minutesPlayed, simMinutes, simValue) {
    const actualValue = Number(actual);
    const played = Number(minutesPlayed);
    const simMin = Number(simMinutes);
    const simMean = Number(simValue);
    if (!Number.isFinite(actualValue)) {
      return Number.isFinite(simMean) ? simMean : null;
    }
    if (!Number.isFinite(played) || played <= 0) {
      return Number.isFinite(simMean) ? simMean : actualValue;
    }
    const targetMinutes = Number.isFinite(simMin) && simMin > 0 ? Math.max(played, Math.min(48, simMin)) : 48;
    const rawProjection = (actualValue / played) * targetMinutes;
    if (!Number.isFinite(simMean)) {
      return rawProjection;
    }
    const blendWeight = clampNumber(played / Math.max(targetMinutes, 1), 0.25, 0.85);
    return ((1 - blendWeight) * simMean) + (blendWeight * rawProjection);
  }

  function buildLiveBoxscoreSimFallback(boxscorePayload, cardsGames, liveStatePayload, dateValue) {
    const liveGamesByEvent = new Map();
    safeArray(liveStatePayload?.games).forEach((game) => {
      const eventId = String(game?.event_id || '').trim();
      if (eventId) {
        liveGamesByEvent.set(eventId, game);
      }
    });

    const cardsByMatchup = new Map();
    safeArray(cardsGames).forEach((game) => {
      const key = matchupKey(game?.away_tri, game?.home_tri);
      if (key) {
        cardsByMatchup.set(key, game);
      }
    });

    const items = [];
    const seen = new Set();

    safeArray(boxscorePayload?.games).forEach((entry) => {
      const eventId = String(entry?.event_id || '').trim();
      const liveGame = liveGamesByEvent.get(eventId);
      const awayTri = String(liveGame?.away || '').trim().toUpperCase();
      const homeTri = String(liveGame?.home || '').trim().toUpperCase();
      const matchup = matchupKey(awayTri, homeTri);
      const cardGame = cardsByMatchup.get(matchup);
      if (!matchup || !cardGame) {
        return;
      }

      const simLookup = new Map();
      ['away', 'home'].forEach((side) => {
        const simRows = safeArray(cardGame?.sim?.players?.[side]);
        simRows.forEach((row) => {
          const teamTri = side === 'home' ? homeTri : awayTri;
          const playerKey = normalizePlayerKey(row?.player_name);
          if (teamTri && playerKey) {
            simLookup.set(`${teamTri}::${playerKey}`, row);
          }
        });
      });

      safeArray(entry?.players).forEach((actualRow) => {
        const teamTri = String(actualRow?.team_tri || '').trim().toUpperCase();
        const playerKey = normalizePlayerKey(actualRow?.player);
        const simRow = simLookup.get(`${teamTri}::${playerKey}`);
        if (!simRow) {
          return;
        }

        ['pts', 'reb', 'ast', 'threes', 'stl', 'blk', 'tov', 'pra'].forEach((market) => {
          const actual = actualStatValue(actualRow, market);
          const simMu = simStatMean(simRow, market);
          const line = Number(simRow?.prop_lines?.[market]);
          if (!Number.isFinite(actual) || !Number.isFinite(simMu) || !Number.isFinite(line)) {
            return;
          }
          const paceProj = estimatedLiveProjection(actual, Number(actualRow?.mp), Number(simRow?.min_mean), simMu);
          if (!Number.isFinite(paceProj)) {
            return;
          }
          const paceVsLine = paceProj - line;
          const simVsLine = simMu - line;
          const side = paceVsLine >= 0 ? 'OVER' : 'UNDER';
          const dedupeKey = `${matchup}::${teamTri}::${playerKey}::${market}`;
          if (seen.has(dedupeKey)) {
            return;
          }
          seen.add(dedupeKey);
          items.push({
            away_tri: awayTri,
            home_tri: homeTri,
            team_tri: teamTri,
            opponent_tri: teamTri === homeTri ? awayTri : homeTri,
            player: actualRow?.player,
            player_id: simRow?.player_id,
            player_photo: propPhotoUrl(simRow),
            market,
            side,
            lean: side,
            line,
            line_source: 'boxscore_sim',
            status_label: 'Live',
            actual,
            pace_proj: paceProj,
            pace_vs_line: paceVsLine,
            sim_mu: simMu,
            sim_vs_line: simVsLine,
            strength: Math.abs(paceVsLine),
            score_adj: Math.abs(paceVsLine),
            bettable_score: Math.abs(paceVsLine),
            klass: classifyLivePropFallback(paceVsLine, market),
          });
        });
      });
    });

    const sortedItems = sortedPropsStripItems(items);
    return {
      mode: 'live',
      date: dateValue,
      title: 'Live player props',
      subtitle: 'Live boxscore vs sim fallback when the live lens feed is thin.',
      rows: sortedItems.length,
      items: sortedItems,
      source: 'live_player_boxscore_fallback',
    };
  }

  function propGameLabel(item) {
    const away = String(item?.away_tri || '').trim().toUpperCase();
    const home = String(item?.home_tri || '').trim().toUpperCase();
    return away && home ? `${away} @ ${home}` : 'Unknown game';
  }

  function livePropPrimarySide(item) {
    const paceEdge = Number(item?.pace_vs_line);
    if (Number.isFinite(paceEdge) && Math.abs(paceEdge) > 0.01) {
      return paceEdge >= 0 ? 'OVER' : 'UNDER';
    }
    const simEdge = finiteFirst(item?.sim_vs_line_adjusted, item?.sim_vs_line);
    if (Number.isFinite(simEdge) && Math.abs(simEdge) > 0.01) {
      return simEdge >= 0 ? 'OVER' : 'UNDER';
    }
    const evSide = String(item?.ev_side || item?.side || item?.lean || '').trim().toUpperCase();
    return evSide === 'OVER' || evSide === 'UNDER' ? evSide : '';
  }

  function livePropPrimaryEdge(item) {
    const paceEdge = Number(item?.pace_vs_line);
    if (Number.isFinite(paceEdge)) {
      return Math.abs(paceEdge);
    }
    const simEdge = finiteFirst(item?.sim_vs_line_adjusted, item?.sim_vs_line);
    if (Number.isFinite(simEdge)) {
      return Math.abs(simEdge);
    }
    const evPct = Number(item?.ev_pct);
    return Number.isFinite(evPct) ? Math.abs(evPct) / 100 : 0;
  }

  function livePropSortScore(item) {
    const paceProj = Number(item?.pace_proj);
    const simValue = finiteFirst(item?.sim_mu_adjusted, item?.sim_mu);
    const line = Number(item?.line);
    const paceEdge = Number(item?.pace_vs_line);
    const simEdge = finiteFirst(item?.sim_vs_line_adjusted, item?.sim_vs_line);
    const strength = Number(item?.strength);
    const bettableScore = Number(item?.score_adj ?? item?.bettable_score);
    const evPct = Number(item?.ev_pct);
    const winProb = Number(item?.probability ?? item?.prob_calib ?? item?.win_prob);

    const hasPace = Number.isFinite(paceProj) && Number.isFinite(line);
    const hasSim = Number.isFinite(simValue) && Number.isFinite(line);
    const paceRank = Number.isFinite(paceEdge) ? Math.abs(paceEdge) : -1;
    const simRank = Number.isFinite(simEdge) ? Math.abs(simEdge) : -1;
    const strengthRank = Number.isFinite(strength) ? Math.abs(strength) : -1;
    const bettableRank = Number.isFinite(bettableScore) ? bettableScore : -1;
    const evRank = Number.isFinite(evPct) ? evPct : -999;
    const probRank = Number.isFinite(winProb) ? winProb : -1;

    return {
      hasPace: hasPace ? 1 : 0,
      paceRank,
      hasSim: hasSim ? 1 : 0,
      simRank,
      strengthRank,
      bettableRank,
      evRank,
      probRank,
    };
  }

  function propsStripSortOptions() {
    if (mode !== 'live') {
      return [];
    }
    return [
      { key: 'best', label: 'Best' },
      { key: 'proj', label: 'Projection' },
      { key: 'win', label: 'Win %' },
      { key: 'live', label: 'Live edge' },
    ];
  }

  function sortedPropsStripItems(items) {
    const next = [...safeArray(items)];
    if (mode !== 'live') {
      return next;
    }
    const sortKey = String(state.propsStripSort || 'best');
    next.sort((left, right) => {
      const leftRank = livePropSortScore(left);
      const rightRank = livePropSortScore(right);
      if (sortKey === 'proj') {
        const projLeft = Number(left?.pace_proj);
        const projRight = Number(right?.pace_proj);
        const leftEdge = finiteFirst(left?.pace_vs_line, left?.sim_vs_line_adjusted, left?.sim_vs_line);
        const rightEdge = finiteFirst(right?.pace_vs_line, right?.sim_vs_line_adjusted, right?.sim_vs_line);
        return (Math.abs(rightEdge) - Math.abs(leftEdge)) || (projRight - projLeft);
      }
      if (sortKey === 'win') {
        const winLeft = Number(left?.probability);
        const winRight = Number(right?.probability);
        return winRight - winLeft;
      }
      if (sortKey === 'live') {
        const edgeLeft = finiteFirst(left?.pace_vs_line, left?.sim_vs_line_adjusted, left?.sim_vs_line, ((Number(left?.pace_proj) - Number(left?.line)) || 0));
        const edgeRight = finiteFirst(right?.pace_vs_line, right?.sim_vs_line_adjusted, right?.sim_vs_line, ((Number(right?.pace_proj) - Number(right?.line)) || 0));
        return Math.abs(edgeRight) - Math.abs(edgeLeft);
      }
      return (rightRank.hasPace - leftRank.hasPace)
        || (rightRank.paceRank - leftRank.paceRank)
        || (rightRank.hasSim - leftRank.hasSim)
        || (rightRank.simRank - leftRank.simRank)
        || (rightRank.strengthRank - leftRank.strengthRank)
        || (rightRank.bettableRank - leftRank.bettableRank)
        || (rightRank.evRank - leftRank.evRank)
        || (rightRank.probRank - leftRank.probRank);
    });
    return next;
  }

  function propsStripFilterOptions(items) {
    const rows = safeArray(items);
    const markets = Array.from(new Set(rows.map((item) => String(item?.market || '').trim().toLowerCase()).filter(Boolean)))
      .sort((left, right) => marketLabel(left).localeCompare(marketLabel(right)));
    const games = Array.from(new Set(rows.map((item) => stripCardTarget(item)).filter(Boolean)))
      .sort((left, right) => left.localeCompare(right));
    return {
      markets,
      games,
      sides: ['OVER', 'UNDER'],
    };
  }

  function sanitizePropsStripFilters(filterOptions) {
    const next = state.propsStripFilters || {};
    if (next.market !== 'all' && !safeArray(filterOptions?.markets).includes(next.market)) {
      next.market = 'all';
    }
    if (next.side !== 'all' && !safeArray(filterOptions?.sides).includes(next.side)) {
      next.side = 'all';
    }
    if (next.game !== 'all' && !safeArray(filterOptions?.games).includes(next.game)) {
      next.game = 'all';
    }
    state.propsStripFilters = next;
  }

  function filteredPropsStripItems(items) {
    const rows = safeArray(items);
    const filters = state.propsStripFilters || {};
    return rows.filter((item) => {
      const gameKey = stripCardTarget(item);
      const marketKey = String(item?.market || '').trim().toLowerCase();
      const sideKey = livePropPrimarySide(item);
      if (filters.game && filters.game !== 'all' && gameKey !== filters.game) {
        return false;
      }
      if (filters.market && filters.market !== 'all' && marketKey !== filters.market) {
        return false;
      }
      if (filters.side && filters.side !== 'all' && sideKey !== filters.side) {
        return false;
      }
      return true;
    });
  }

  function visiblePropsStripItems(items) {
    const filtered = filteredPropsStripItems(items);
    if (mode !== 'live') {
      return filtered;
    }
    const visibleCount = Math.max(1, Number(state.propsStripVisibleCount) || Number(state.propsStripDefaultCount) || 18);
    return filtered.slice(0, visibleCount);
  }

  function renderPropsStripFilterGroup(title, options, activeValue, dataAttr, labelBuilder) {
    if (!safeArray(options).length) {
      return '';
    }
    return `
      <div class="cards-props-strip-filter-group">
        <div class="cards-props-strip-filter-title">${escapeHtml(title)}</div>
        <div class="cards-props-strip-filter-pills">
          ${options.map((option) => {
            const key = String(option?.key ?? option);
            const label = labelBuilder ? labelBuilder(option) : String(option?.label ?? option);
            return `<button type="button" class="cards-filter-pill ${String(activeValue) === key ? 'is-active' : ''}" ${dataAttr}="${escapeHtml(key)}">${escapeHtml(label)}</button>`;
          }).join('')}
        </div>
      </div>
    `;
  }

  function stripCardTarget(item) {
    const away = String(item?.away_tri || '').trim().toUpperCase();
    const home = String(item?.home_tri || '').trim().toUpperCase();
    return away && home ? `${away}@${home}` : '';
  }

  function sanitizePropsStripItem(rawItem) {
    if (!rawItem || typeof rawItem !== 'object') {
      return null;
    }
    const item = { ...rawItem };
    item.away_tri = String(item.away_tri || '').trim().toUpperCase();
    item.home_tri = String(item.home_tri || '').trim().toUpperCase();
    item.team_tri = String(item.team_tri || '').trim().toUpperCase();
    item.opponent_tri = String(item.opponent_tri || '').trim().toUpperCase();
    item.player = String(item.player || '').trim();
    item.market = String(item.market || '').trim().toLowerCase();
    item.side = String(item.side || item.ev_side || item.lean || '').trim().toUpperCase();
    item.ev_side = String(item.ev_side || '').trim().toUpperCase();
    item.lean = String(item.lean || '').trim().toUpperCase();
    item.klass = String(item.klass || '').trim().toUpperCase();
    item.line_source = String(item.line_source || '').trim().toLowerCase();
    item.status_label = String(item.status_label || '').trim();
    item.photo = String(item.photo || '').trim();
    item.player_photo = String(item.player_photo || '').trim();
    item.book = String(item.book || '').trim();
    return item;
  }

  function safePropsStripItems(items) {
    return safeArray(items)
      .map(sanitizePropsStripItem)
      .filter(Boolean);
  }

  function reportPropsStripError(stage, error, detail) {
    try {
      const suffix = detail ? ` ${detail}` : '';
      console.warn(`cards props strip ${stage} failed.${suffix}`, error);
    } catch (_error) {
      // Ignore console/reporting failures.
    }
  }

  function renderPropsStripItem(item) {
    try {
      const safeItem = sanitizePropsStripItem(item);
      if (!safeItem) {
        return '';
      }
      const photo = String(safeItem.photo || safeItem.player_photo || '').trim();
      const logo = logoForTri(safeItem.team_tri);
      const opponentTri = String(safeItem.opponent_tri || '').trim().toUpperCase();
      const market = marketLabel(safeItem.market);
      const side = livePropPrimarySide(safeItem) || String(safeItem.side || '').trim().toUpperCase();
      const line = Number(safeItem.line);
      const price = Number(safeItem.price);
      const evPct = Number(safeItem.ev_pct);
      const winProb = Number(safeItem.probability ?? safeItem.prob_calib);
      const cardTarget = resolveStripCardTarget(safeItem);
      const actionLabel = mode === 'live'
        ? String(safeItem.klass || '').trim().toUpperCase()
        : String(safeItem.tier || '').trim().toUpperCase();
      const actionClass = actionLabel === 'BET'
        ? 'cards-chip--accent'
        : (actionLabel === 'WATCH' || actionLabel === 'MEDIUM' ? 'cards-chip--warm' : '');
      const liveProjection = liveProjectionSummary(safeItem);
      return `
        <article class="cards-props-strip-card">
          <div class="cards-props-strip-card__top">
            <div class="cards-props-strip-card__context">${escapeHtml(String(safeItem.away_tri || '--'))} @ ${escapeHtml(String(safeItem.home_tri || '--'))}</div>
            <div class="cards-props-strip-card__status">${escapeHtml(stripStatusText(safeItem))}</div>
          </div>
          <div class="cards-props-strip-card__body">
            <div class="cards-props-strip-card__identity">
              <div class="cards-props-strip-card__media">
                ${photo ? `<img class="cards-props-strip-card__photo" src="${escapeHtml(photo)}" alt="${escapeHtml(String(safeItem.player || 'Player'))}" />` : `<div class="cards-props-strip-card__photo is-fallback">${escapeHtml(String(safeItem.team_tri || '?'))}</div>`}
                ${logo ? `<img class="cards-props-strip-card__logo" src="${escapeHtml(logo)}" alt="${escapeHtml(String(safeItem.team_tri || ''))} logo" />` : ''}
              </div>
              <div class="cards-props-strip-card__copy">
                <div class="cards-props-strip-card__name">${escapeHtml(String(safeItem.player || 'Unknown player'))}</div>
                <div class="cards-props-strip-card__matchup">${escapeHtml(String(safeItem.team_tri || '--'))}${opponentTri ? ` vs ${escapeHtml(opponentTri)}` : ''}</div>
              </div>
            </div>
            <div class="cards-props-strip-card__play">${escapeHtml(market)} ${escapeHtml(side)} ${Number.isFinite(line) ? fmtNumber(line, 1) : '--'}</div>
            ${liveProjection ? `<div class="cards-props-strip-card__projection">${escapeHtml(liveProjection)}</div>` : ''}
            <div class="cards-props-strip-card__sub">${escapeHtml(stripSecondaryText(safeItem))}</div>
            <div class="cards-strip-pills">
              ${actionLabel ? `<span class="cards-chip ${actionClass}">${escapeHtml(actionLabel)}</span>` : ''}
              ${Number.isFinite(price) ? `<span class="cards-chip">${escapeHtml(fmtAmerican(price))}</span>` : ''}
              ${Number.isFinite(evPct) ? `<span class="cards-chip cards-chip--accent">EV ${escapeHtml(fmtPercentValue(evPct))}</span>` : ''}
              ${Number.isFinite(winProb) ? `<span class="cards-chip">${escapeHtml(mode === 'live' ? fmtPercent(winProb, 0) : fmtPercentValue(winProb))}</span>` : ''}
            </div>
          </div>
          ${cardTarget ? `<button class="cards-props-strip-card__jump" type="button" data-jump-card="${escapeHtml(cardTarget)}">Jump to game</button>` : ''}
        </article>
      `;
    } catch (error) {
      reportPropsStripError('render-item', error, String(item?.player || item?.market || 'unknown-item'));
      return '';
    }
  }

  function renderPropsStrip() {
    if (!propsStripEl) {
      return;
    }
    try {
      const payload = state.propsStripPayload;
      const sortedItems = sortedPropsStripItems(safePropsStripItems(payload?.items));
      const filteredItems = filteredPropsStripItems(sortedItems);
      const visibleItems = visiblePropsStripItems(sortedItems);
      if (!sortedItems.length) {
        clearPropsStrip();
        return;
      }
      const sortOptions = propsStripSortOptions();
      const filterOptions = propsStripFilterOptions(sortedItems);
      sanitizePropsStripFilters(filterOptions);
      const activeFilters = state.propsStripFilters || {};
      const canShowMore = mode === 'live' && visibleItems.length < filteredItems.length;
      const renderedItems = visibleItems.map(renderPropsStripItem).filter(Boolean);
      const controlsMarkup = [
        sortOptions.length
          ? renderPropsStripFilterGroup('Sort', sortOptions, state.propsStripSort, 'data-strip-sort', (option) => option.label)
          : '',
        mode === 'live'
          ? renderPropsStripFilterGroup('Prop', [{ key: 'all', label: 'All props' }, ...filterOptions.markets.map((market) => ({ key: market, label: marketLabel(market) }))], activeFilters.market || 'all', 'data-strip-market')
          : '',
        mode === 'live'
          ? renderPropsStripFilterGroup('Side', [{ key: 'all', label: 'All sides' }, { key: 'OVER', label: 'Over' }, { key: 'UNDER', label: 'Under' }], activeFilters.side || 'all', 'data-strip-side')
          : '',
        mode === 'live'
          ? renderPropsStripFilterGroup('Game', [{ key: 'all', label: 'All games' }, ...filterOptions.games.map((gameKey) => ({ key: gameKey, label: gameKey.replace('@', ' @ ') }))], activeFilters.game || 'all', 'data-strip-game')
          : '',
      ].filter(Boolean).join('');
      propsStripEl.classList.remove('hidden');
      propsStripEl.innerHTML = `
        <div class="cards-props-strip-headline">
          <div>
            <h2>${escapeHtml(String(payload?.title || (mode === 'live' ? 'Live player props' : 'Pregame prop movement')))}</h2>
            <p>${escapeHtml(String(payload?.subtitle || ''))}</p>
          </div>
          <div class="cards-strip-pills">
            <span class="cards-source-meta-pill ${mode === 'live' ? 'is-live' : 'is-soft'}">${escapeHtml(String(renderedItems.length))} shown</span>
            ${mode === 'live' ? `<span class="cards-source-meta-pill is-soft">${escapeHtml(String(filteredItems.length))} match</span>` : ''}
            ${mode === 'live' ? `<span class="cards-source-meta-pill is-soft">${escapeHtml(String(sortedItems.length))} in pool</span>` : ''}
            <span class="cards-source-meta-pill">${escapeHtml(String(payload?.date || state.date || ''))}</span>
          </div>
        </div>
        ${controlsMarkup ? `<div class="cards-props-strip-controls">${controlsMarkup}</div>` : ''}
        <div class="cards-props-strip-grid">${renderedItems.length ? renderedItems.join('') : `<div class="cards-props-strip-empty">No player props are available right now.</div>`}</div>
        ${canShowMore ? `<div class="cards-props-strip-actions"><button type="button" class="cards-filter-pill" data-strip-show-more="1">Show more</button></div>` : ''}
      `;
    } catch (error) {
      reportPropsStripError('render', error);
      state.propsStripPayload = null;
      clearPropsStrip();
    }
  }

  function transformLiveStripPayload(payload, dateValue) {
    const items = [];
    safeArray(payload?.games).forEach((game) => {
      const status = game?.status || {};
      const gameItems = safeArray(game?.rows)
        .filter((row) => row && row.player && row.team_tri)
        .filter((row) => row.line_source && row.line_source !== 'model')
        .filter((row) => row.pace_proj != null || row.sim_mu_adjusted != null || row.sim_mu != null || row.ev_side || row.lean)
        .map((row) => {
          try {
            return {
              away_tri: game?.away,
              home_tri: game?.home,
              team_tri: row?.team_tri,
              opponent_tri: row?.team_tri === game?.home ? game?.away : game?.home,
              player: row?.player,
              player_photo: row?.player_photo,
              market: row?.stat,
              side: row?.lean || row?.ev_side,
              ev_side: row?.ev_side,
              line: row?.line_live ?? row?.line,
              price: String(row?.ev_side || row?.lean).toUpperCase() === 'UNDER' ? row?.price_under : row?.price_over,
              ev_pct: Number.isFinite(Number(row?.ev)) ? Number(row.ev) * 100 : null,
              probability: row?.win_prob,
              klass: row?.klass,
              line_source: row?.line_source,
              status_label: status?.final ? 'Final' : (status?.in_progress ? `Q${status?.period || '-'} ${status?.clock || ''}`.trim() : 'Live'),
              actual: row?.actual,
              pace_proj: row?.pace_proj,
              pace_vs_line: row?.pace_vs_line,
              strength: row?.strength,
              score_adj: row?.bettable_score ?? row?.strength ?? row?.ev,
              sim_mu: row?.sim_mu,
              sim_mu_adjusted: row?.sim_mu_adjusted,
              sim_vs_line: row?.sim_vs_line,
              sim_vs_line_adjusted: row?.sim_vs_line_adjusted,
              lean: row?.lean,
              bettable_score: row?.bettable_score,
              line_live: row?.line_live,
              line_pregame: row?.line_pregame,
              pregame_team_total_ratio: row?.pregame_team_total_ratio,
              pregame_game_total_ratio: row?.pregame_game_total_ratio,
              pregame_stat_multiplier: row?.pregame_stat_multiplier,
              pregame_margin_blended: row?.pregame_margin_blended,
            };
          } catch (error) {
            reportPropsStripError('transform-row', error, String(row?.player || row?.stat || 'unknown-row'));
            return null;
          }
        })
        .filter(Boolean);
      items.push(...gameItems);
    });
    const sortedItems = sortedPropsStripItems(items);
    return {
      mode: 'live',
      date: dateValue,
      title: 'Live player props',
      subtitle: 'Ranked by live projection first, then sim support, then betting edge.',
      rows: sortedItems.length,
      items: sortedItems,
    };
  }

  async function loadPropsStrip(dateValue, options = {}) {
    const silent = Boolean(options?.silent);
    const games = safeArray(options?.games || state.payload?.games);
    const previousPropsStripPayload = state.propsStripPayload;
    if (!propsStripEl) {
      return;
    }
    if (!silent || !state.propsStripPayload) {
      setPropsStripLoading();
    }
    try {
      if (mode === 'live') {
        const liveStatePayload = await fetchApiJson(
          `/api/live_state?date=${encodeURIComponent(dateValue)}`,
          'Failed to load live state for player props.',
          { retries: silent ? 2 : 1 }
        );
        const liveGames = safeArray(liveStatePayload?.games)
          .filter((game) => Boolean(game?.in_progress) && !Boolean(game?.final));
        const eventIds = liveGames
          .filter((game) => Boolean(game?.event_id))
          .map((game) => String(game?.event_id || '').trim())
          .filter(Boolean);

        if (!eventIds.length) {
          state.propsStripPayload = null;
          clearPropsStrip();
          return;
        }

        const lensQuery = eventIds.length
          ? `/api/live_player_lens?date=${encodeURIComponent(dateValue)}&event_ids=${encodeURIComponent(eventIds.join(','))}`
          : `/api/live_player_lens?date=${encodeURIComponent(dateValue)}`;
        const payload = await fetchApiJson(lensQuery, 'Failed to load live player props.', { retries: silent ? 2 : 1 });
        let transformed = transformLiveStripPayload(payload, dateValue);

        if ((!safeArray(transformed?.items).length) && eventIds.length && games.length) {
          const boxscorePayload = await fetchApiJson(
            `/api/live_player_boxscore?event_ids=${encodeURIComponent(eventIds.join(','))}`,
            'Failed to load live player boxscore.',
            { retries: silent ? 2 : 1 }
          );
          transformed = buildLiveBoxscoreSimFallback(boxscorePayload, games, liveStatePayload, dateValue);
        }

        state.propsStripPayload = transformed;
        state.propsStripVisibleCount = Number(state.propsStripDefaultCount) || 18;
      } else {
        const payload = await fetchApiJson(
          `/api/cards/props-strip?date=${encodeURIComponent(dateValue)}`,
          'Failed to load prop strip.',
          { retries: silent ? 2 : 1 }
        );
        state.propsStripPayload = payload;
      }
      renderPropsStrip();
      if (mode === 'live' && state.payload && (state.payload.date || state.date) === dateValue) {
        renderBoard();
      }
    } catch (error) {
      reportPropsStripError('load', error, dateValue);
      if (silent && previousPropsStripPayload) {
        state.propsStripPayload = previousPropsStripPayload;
        renderPropsStrip();
        showNote(error?.message || 'Failed to refresh player props.', 'warning');
        return;
      }
      state.propsStripPayload = null;
      clearPropsStrip();
    }
  }

  function livePropTeaserItems(game, limit = 2) {
    const matchup = gameMatchupKey(game);
    if (!matchup) {
      return [];
    }
    const items = sortedPropsStripItems(safePropsStripItems(state.propsStripPayload?.items));
    const matched = items.filter((item) => stripCardTarget(item) === matchup);
    if (!Number.isFinite(Number(limit)) || Number(limit) <= 0) {
      return matched;
    }
    return matched.slice(0, Number(limit));
  }

  function livePropTeaserText(item) {
    const market = marketLabel(item?.market);
    const side = livePropPrimarySide(item) || String(item?.side || '').trim().toUpperCase() || '--';
    const line = Number(item?.line);
    const projection = finiteFirst(item?.pace_proj, item?.sim_mu_adjusted, item?.sim_mu);
    const edge = finiteFirst(item?.pace_vs_line, item?.sim_vs_line_adjusted, item?.sim_vs_line);
    const parts = [
      `${String(item?.player || 'Player').trim()} ${market} ${side} ${Number.isFinite(line) ? fmtNumber(line, 1) : '--'}`,
    ];
    if (Number.isFinite(projection)) {
      parts.push(`Proj ${fmtNumber(projection, 1)}`);
    }
    if (Number.isFinite(edge)) {
      parts.push(`Edge ${fmtSigned(edge, 1)}`);
    }
    return parts.join(' · ');
  }

  function renderLivePropTeaser(game) {
    if (mode !== 'live') {
      return '';
    }
    const liveState = getLiveState(game);
    if (!hasStartedGame(liveState)) {
      return '';
    }
    const items = livePropTeaserItems(game, 2);
    if (!items.length) {
      return '';
    }
    return `
      <div class="cards-card-context">
        <div class="cards-box-head">
          <div class="cards-table-title"><strong>Live prop pulse</strong></div>
          <button type="button" class="cards-filter-pill" data-open-card-tab="props" data-card-target="${escapeHtml(cardId(game))}">Open props</button>
        </div>
        <div class="cards-callout-copy">Top live prop edges for this matchup, using the same feed as the global strip.</div>
        <div class="cards-source-meta">
          ${items.map((item) => `<span class="cards-source-meta-pill is-soft">${escapeHtml(livePropTeaserText(item))}</span>`).join('')}
        </div>
      </div>
    `;
  }

  function ensureBoardShell() {
    state.boardInitialized = true;
    return { scoreboardEl: scoreboardRoot, gridEl: gridRoot };
  }

  function updateDateControls() {
    const basePath = document.body?.dataset?.cardsBasePath || '/';
    const previousDate = shiftISODate(state.date, -1);
    const nextDate = shiftISODate(state.date, 1);
    const seasonYear = Number(String(state.date || getLocalDateISO()).slice(0, 4)) || Number(new Date().getFullYear());
    if (prevDateLink) {
      prevDateLink.href = `${basePath}?date=${encodeURIComponent(previousDate)}`;
    }
    if (nextDateLink) {
      nextDateLink.href = `${basePath}?date=${encodeURIComponent(nextDate)}`;
    }
    if (seasonBettingCardLink) {
      seasonBettingCardLink.href = `/season/${encodeURIComponent(seasonYear)}/betting-card?date=${encodeURIComponent(state.date || getLocalDateISO())}&profile=retuned`;
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

  function recommendationSortScore(row) {
    if (!row) {
      return Number.NEGATIVE_INFINITY;
    }
    const priority = Number(row.recommendationPriorityScore ?? row.recommendation_priority_score);
    if (Number.isFinite(priority)) {
      return priority;
    }
    const score = Number(row.score);
    if (Number.isFinite(score)) {
      return score;
    }
    const evPct = Number(row.evPct ?? row.ev_pct);
    return Number.isFinite(evPct) ? evPct : Number.NEGATIVE_INFINITY;
  }

  function officialCardRows(game) {
    const id = cardId(game);
    const marketRows = playableMarketRows(game, 'official').map((row) => `
      <li>
        <button class="cards-callout-item cards-callout-button" type="button" data-market-tab-target="game" data-card-target="${escapeHtml(id)}">
          <div>
            <div class="cards-callout-label">${escapeHtml(row.label)}</div>
            <div class="cards-callout-main">${escapeHtml(row.main)}</div>
            <div class="cards-callout-copy">${escapeHtml(row.sub)}</div>
          </div>
          <div class="cards-callout-meta">
            <span class="cards-chip cards-chip--accent">EV ${fmtPercentValue(row.ev)}</span>
            <span class="cards-chip">${fmtPercent(row.probability, 0)}</span>
          </div>
        </button>
      </li>
    `);
    const propRows = officialPropRows(game).map((row) => `
      <li>
        <button class="cards-callout-item cards-callout-button" type="button" data-prop-select="${escapeHtml(row.key)}" data-card-target="${escapeHtml(id)}">
          <div>
            <div class="cards-callout-label">${escapeHtml(row.teamTri)} prop</div>
            <div class="cards-callout-main">${escapeHtml(`${row.player} ${row.marketLabel} ${row.side} ${fmtNumber(row.line, 1)}`)}</div>
            <div class="cards-callout-copy">${escapeHtml(row.summary || `${row.teamTri} · ${fmtAmerican(row.price)} ${row.book || ''}`.trim())}</div>
          </div>
          <div class="cards-callout-meta">
            <span class="cards-chip cards-chip--accent">EV ${fmtPercentValue(row.evPct)}</span>
            <span class="cards-chip">${fmtPercent(row.pWin, 0)}</span>
          </div>
        </button>
      </li>
    `);
    const items = marketRows.concat(propRows);
    if (!items.length) {
      return `
        <li class="cards-callout-item">
          <div>
            <div class="cards-callout-label">Market board</div>
            <div class="cards-callout-main">No official betting card</div>
            <div class="cards-callout-copy">No saved game or player market snapshot was available for this matchup.</div>
          </div>
        </li>
      `;
    }
    return items.join('');
  }

  function playableMarketRows(game, bucket = 'all') {
    const sourceRows = safeArray(game?.game_market_recommendations);
    if (sourceRows.length) {
      return sourceRows
        .filter((row) => bucket === 'all' || String(row?.card_bucket || '').toLowerCase() === bucket)
        .sort((left, right) => recommendationSortScore(right) - recommendationSortScore(left))
        .map((row) => ({
          label: row.market_label || 'Market',
          main: row.display_pick || row.selection || 'No play',
          probability: row.p_win,
          ev: row.ev_pct,
          sub: row.basketball_summary || row.why_explain || `Score ${fmtNumber(row.recommendation_priority_score ?? row.score, 1)}`,
          bucket: row.card_bucket || 'playable',
        }));
    }

    const betting = game?.betting || {};
    const score = game?.sim?.score || {};
    const market = game?.sim?.market || {};
    const rows = [
      {
        label: 'Moneyline',
        pick: bestMarketPick(game, 'moneyline'),
        sub: `${game.home_tri} ${fmtAmerican(betting.home_ml)} / ${game.away_tri} ${fmtAmerican(betting.away_ml)}`,
        bucket: 'official',
      },
      {
        label: 'Spread',
        pick: bestMarketPick(game, 'spread'),
        sub: `Model margin ${fmtSigned(score.margin_mean, 1)} · Market ${fmtSigned(-Number(market.market_home_spread), 1)}`,
        bucket: 'official',
      },
      {
        label: 'Total',
        pick: bestMarketPick(game, 'total'),
        sub: `Model total ${fmtNumber(score.total_mean, 1)} · Market ${fmtNumber(betting.total, 1)}`,
        bucket: 'official',
      },
    ];
    return rows
      .filter((row) => bucket === 'all' || row.bucket === bucket)
      .filter((row) => row.pick)
      .map((row) => ({
        label: row.label,
        main: row.pick.detail,
        probability: row.pick.probability,
        ev: Number(row.pick.ev) * 100,
        sub: row.sub,
        bucket: row.bucket,
      }));
  }

  function officialPropRows(game) {
    const rows = allPropRows(game);
    const official = rows.filter((row) => row.bucket === 'official');
    if (official.length) {
      return official
        .sort((left, right) => recommendationSortScore(right) - recommendationSortScore(left))
        .slice(0, 3);
    }
    const primaryRows = rows.filter((row) => row.primary);
    const source = (primaryRows.length ? primaryRows : rows).slice();
    return source
      .sort((left, right) => recommendationSortScore(right) - recommendationSortScore(left))
      .slice(0, 3);
  }

  function liveOpportunityPropRows(game) {
    if (mode !== 'live') {
      return [];
    }
    const liveState = getLiveState(game);
    if (!hasStartedGame(liveState)) {
      return [];
    }
    return livePropTeaserItems(game, 0)
      .map((item, index) => {
        const teamTri = String(item?.team_tri || '').trim().toUpperCase();
        const awayTri = String(game?.away_tri || '').trim().toUpperCase();
        const homeTri = String(game?.home_tri || '').trim().toUpperCase();
        const sideKey = teamTri === awayTri ? 'away' : (teamTri === homeTri ? 'home' : '');
        const market = String(item?.market || '').trim().toLowerCase();
        const side = livePropPrimarySide(item) || String(item?.side || '').trim().toUpperCase();
        const klass = String(item?.klass || '').trim().toUpperCase();
        const projection = finiteFirst(item?.pace_proj, item?.sim_mu_adjusted, item?.sim_mu);
        const liveEdge = finiteFirst(item?.pace_vs_line, item?.sim_vs_line_adjusted, item?.sim_vs_line);
        if (klass !== 'BET' && klass !== 'WATCH') {
          return null;
        }
        const statusLabel = stripStatusText(item);
        const sourceSummary = stripSecondaryText(item);
        return {
          key: ['live', awayTri, homeTri, teamTri, String(item?.player || '').trim(), market, side, Number(item?.line), index].join('|'),
          cardId: cardId(game),
          teamTri,
          sideKey,
          player: String(item?.player || '').trim(),
          playerPhoto: item?.player_photo || item?.photo,
          market,
          marketLabel: marketLabel(market),
          side,
          line: Number(item?.line),
          price: Number(item?.price),
          book: item?.book,
          evPct: Number(item?.ev_pct),
          pWin: Number(item?.probability ?? item?.prob_calib),
          simMu: finiteFirst(item?.sim_mu_adjusted, item?.sim_mu),
          summary: [liveProjectionSummary(item), sourceSummary].filter(Boolean).join(' · '),
          reasons: [statusLabel, sourceSummary].filter(Boolean),
          matchup: `${awayTri} @ ${homeTri}`,
          rank: index + 1,
          primary: index === 0,
          bucket: 'live',
          actionLabel: klass || 'LIVE',
          statusLabel,
          lineSource: item?.line_source,
          actual: Number(item?.actual),
          liveProjection: Number(projection),
          liveEdge: Number(liveEdge),
        };
      })
      .filter(Boolean);
  }

  function propBucketSummary(game) {
    const rows = allPropRows(game);
    const official = rows.filter((row) => row.bucket === 'official').length;
    const playable = rows.filter((row) => row.bucket !== 'official').length;
    const live = liveOpportunityPropRows(game).length;
    if (!official && !playable && !live) {
      return 'No props';
    }
    const parts = [];
    if (live) {
      parts.push(`${live} live`);
    }
    if (official) {
      parts.push(`${official} official`);
    }
    if (playable) {
      parts.push(`${playable} playable`);
    }
    return parts.join(' · ');
  }

  function playableBoardMarkup(game) {
    const id = cardId(game);
    const gameRows = playableMarketRows(game);
    const propRows = allPropRows(game).sort((left, right) => recommendationSortScore(right) - recommendationSortScore(left));
    const sections = [];

    if (gameRows.length) {
      sections.push(`
        <div class="cards-playable-section">
          <div class="cards-table-title"><strong>Game markets</strong></div>
          <div class="cards-playable-list">
            ${gameRows.map((row) => `
              <button class="cards-playable-item" type="button" data-market-tab-target="game" data-card-target="${escapeHtml(id)}">
                <div class="cards-playable-head">
                  <span class="cards-callout-label">${escapeHtml(row.label)}</span>
                  <span class="cards-chip cards-chip--accent">EV ${fmtPercentValue(row.ev)}</span>
                </div>
                <div class="cards-playable-main">${escapeHtml(row.main)}</div>
                <div class="cards-callout-copy">${escapeHtml(`${row.sub} · Win ${fmtPercent(row.probability, 0)}`)}</div>
              </button>
            `).join('')}
          </div>
        </div>
      `);
    }

    if (propRows.length) {
      sections.push(`
        <div class="cards-playable-section">
          <div class="cards-table-title"><strong>Player props</strong></div>
          <div class="cards-playable-list">
            ${propRows.map((row) => `
              <button class="cards-playable-item" type="button" data-prop-select="${escapeHtml(row.key)}" data-card-target="${escapeHtml(id)}">
                <div class="cards-playable-head">
                  <span class="cards-callout-label">${escapeHtml(row.teamTri)} · ${escapeHtml(row.marketLabel)}</span>
                  <span class="cards-chip ${row.bucket === 'official' ? 'cards-chip--accent' : ''}">${row.bucket === 'official' ? 'Official' : 'Playable'}</span>
                </div>
                <div class="cards-playable-main">${escapeHtml(`${row.player} ${row.side} ${fmtNumber(row.line, 1)}`)}</div>
                <div class="cards-callout-copy">${escapeHtml(`${fmtAmerican(row.price)} ${row.book || ''} · EV ${fmtPercentValue(row.evPct)} · Win ${fmtPercent(row.pWin, 0)}`.trim())}</div>
              </button>
            `).join('')}
          </div>
        </div>
      `);
    }

    if (!sections.length) {
      return '<div class="cards-empty-copy">No playable markets or props qualified for this matchup.</div>';
    }

    return sections.join('');
  }

  function sidebarPropBoardMarkup(game) {
    const id = cardId(game);
    const liveRows = liveOpportunityPropRows(game).slice(0, 3);
    const playableRows = allPropRows(game)
      .sort((left, right) => recommendationSortScore(right) - recommendationSortScore(left))
      .slice(0, 3);
    const rows = mode === 'live' && liveRows.length ? liveRows : playableRows;
    if (!rows.length) {
      return '<div class="cards-empty-copy">No promoted prop lanes available for this matchup.</div>';
    }
    return `
      <div class="cards-prop-overview-grid">
        ${rows.map((row) => {
          const isLiveRow = row.bucket === 'live';
          const badgeClass = isLiveRow ? 'is-live' : '';
          const badgeLabel = isLiveRow ? (row.actionLabel || 'Live') : (row.bucket === 'official' ? 'Official' : 'Playable');
          const metrics = isLiveRow
            ? [
              { label: 'Current', value: Number.isFinite(Number(row.actual)) ? fmtNumber(row.actual, 1) : '-' },
              { label: 'Live proj', value: Number.isFinite(Number(row.liveProjection)) ? fmtNumber(row.liveProjection, 1) : '-' },
              { label: 'Line', value: `${row.side} ${fmtNumber(row.line, 1)}` },
              { label: 'Live edge', value: Number.isFinite(Number(row.liveEdge)) ? fmtSigned(row.liveEdge, 1) : '-' , tone: Number(row.liveEdge) >= 0 ? 'is-positive' : 'is-negative' },
            ]
            : [
              { label: 'Line', value: `${row.side} ${fmtNumber(row.line, 1)}` },
              { label: 'Odds', value: `${fmtAmerican(row.price)} ${row.book || ''}`.trim() },
              { label: 'Win', value: fmtPercent(row.pWin, 0) },
              { label: 'EV', value: fmtPercentValue(row.evPct), tone: Number(row.evPct) >= 0 ? 'is-positive' : 'is-negative' },
            ];
          return `
            <button class="cards-prop-overview-card" type="button" data-prop-select="${escapeHtml(row.key)}" data-card-target="${escapeHtml(id)}">
              <div class="cards-lens-head">
                <div>
                  <div class="cards-lens-label">${escapeHtml(isLiveRow ? `${row.teamTri} live lens` : `${row.teamTri} prop`)}</div>
                  <div class="cards-lens-main">${escapeHtml(row.player)}</div>
                  <div class="cards-subcopy">${escapeHtml(`${row.marketLabel} ${row.side} ${fmtNumber(row.line, 1)}`)}</div>
                </div>
                <span class="cards-lens-badge ${badgeClass}">${escapeHtml(badgeLabel)}</span>
              </div>
              <div class="cards-prop-overview-metrics">
                ${metrics.map((metric) => `
                  <div class="cards-data-pair ${metric.tone || ''}">
                    <span>${escapeHtml(metric.label)}</span>
                    <strong>${escapeHtml(metric.value)}</strong>
                  </div>
                `).join('')}
              </div>
              <div class="cards-prop-overview-foot">
                <span>${escapeHtml(row.summary || (isLiveRow ? `${row.statusLabel || 'Live'} · ${row.matchup || ''}` : `${row.teamTri} · ${fmtAmerican(row.price)} ${row.book || ''}`.trim()))}</span>
                <span>${escapeHtml(`${row.teamTri} | ${row.marketLabel}`)}</span>
              </div>
            </button>
          `;
        }).join('')}
      </div>
    `;
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

  function normalizeTwoWay(first, second) {
    const left = Number(first);
    const right = Number(second);
    if (Number.isFinite(left) && Number.isFinite(right) && (left + right) > 0) {
      return { first: left / (left + right), second: right / (left + right) };
    }
    if (Number.isFinite(left)) {
      const clamped = clampNumber(left, 0, 1);
      return { first: clamped, second: 1 - clamped };
    }
    if (Number.isFinite(right)) {
      const clamped = clampNumber(right, 0, 1);
      return { first: 1 - clamped, second: clamped };
    }
    return { first: 0.5, second: 0.5 };
  }

  function marginWinProb(margin, scale = 6) {
    const value = Number(margin);
    if (!Number.isFinite(value)) {
      return 0.5;
    }
    const divisor = Number(scale) > 0 ? Number(scale) : 6;
    return 1 / (1 + Math.exp(-(value / divisor)));
  }

  function segmentProjection(total, margin) {
    const totalValue = Number(total);
    const marginValue = Number(margin);
    if (!Number.isFinite(totalValue) || !Number.isFinite(marginValue)) {
      return null;
    }
    return {
      away: (totalValue - marginValue) / 2,
      home: (totalValue + marginValue) / 2,
      total: totalValue,
      homeMargin: marginValue,
    };
  }

  function baselineHomeWinProb(game, scopeKey) {
    const betting = game?.betting || {};
    if (scopeKey === 'full') {
      const normalized = normalizeTwoWay(
        impliedProbFromAmerican(betting.home_ml),
        impliedProbFromAmerican(betting.away_ml)
      );
      return normalized.first;
    }
    return 0.5;
  }

  function buildGameLensRows(game) {
    const betting = game?.betting || {};
    const score = game?.sim?.score || {};
    const periods = game?.sim?.periods || {};
    const rows = [
      {
        key: 'q1',
        label: 'Q1',
        lineLabel: 'Quarter 1',
        projection: segmentProjection(periods?.q1?.total_mean, periods?.q1?.margin_mean),
        modelHomeWinProb: Number.isFinite(Number(periods?.q1?.p_home_win)) ? Number(periods.q1.p_home_win) : marginWinProb(periods?.q1?.margin_mean, 3.4),
        baselineHomeWinProb: baselineHomeWinProb(game, 'q1'),
      },
      {
        key: 'q2',
        label: 'Q2',
        lineLabel: 'Quarter 2',
        projection: segmentProjection(periods?.q2?.total_mean, periods?.q2?.margin_mean),
        modelHomeWinProb: Number.isFinite(Number(periods?.q2?.p_home_win)) ? Number(periods.q2.p_home_win) : marginWinProb(periods?.q2?.margin_mean, 3.4),
        baselineHomeWinProb: baselineHomeWinProb(game, 'q2'),
      },
      {
        key: 'q3',
        label: 'Q3',
        lineLabel: 'Quarter 3',
        projection: segmentProjection(periods?.q3?.total_mean, periods?.q3?.margin_mean),
        modelHomeWinProb: Number.isFinite(Number(periods?.q3?.p_home_win)) ? Number(periods.q3.p_home_win) : marginWinProb(periods?.q3?.margin_mean, 3.4),
        baselineHomeWinProb: baselineHomeWinProb(game, 'q3'),
      },
      {
        key: 'q4',
        label: 'Q4',
        lineLabel: 'Quarter 4',
        projection: segmentProjection(periods?.q4?.total_mean, periods?.q4?.margin_mean),
        modelHomeWinProb: Number.isFinite(Number(periods?.q4?.p_home_win)) ? Number(periods.q4.p_home_win) : marginWinProb(periods?.q4?.margin_mean, 3.4),
        baselineHomeWinProb: baselineHomeWinProb(game, 'q4'),
      },
      {
        key: 'full',
        label: 'Game',
        lineLabel: 'Full game',
        projection: segmentProjection(score?.total_mean, score?.margin_mean),
        modelHomeWinProb: Number.isFinite(Number(betting.p_home_win)) ? Number(betting.p_home_win) : marginWinProb(score?.margin_mean, 6),
        baselineHomeWinProb: baselineHomeWinProb(game, 'full'),
        totalLine: Number(betting.total),
        spreadLine: Number(betting.home_spread),
      },
    ];

    return rows.filter((row) => row.projection);
  }

  function renderGameLens(game) {
    const rows = buildGameLensRows(game);
    if (!rows.length) {
      return '<div class="cards-empty-copy">No game lens projections available.</div>';
    }
    return rows.map((row) => {
      const projectionLine = `${game.away_tri} ${fmtNumber(row.projection.away, 1)} - ${game.home_tri} ${fmtNumber(row.projection.home, 1)} | Total ${fmtNumber(row.projection.total, 1)}`;
      const modelProb = Number.isFinite(Number(row.modelHomeWinProb)) ? Number(row.modelHomeWinProb) : 0.5;
      const baselineProb = Number.isFinite(Number(row.baselineHomeWinProb)) ? Number(row.baselineHomeWinProb) : 0.5;
      const homeDelta = (modelProb - baselineProb) * 100;
      const leanTeam = row.projection.homeMargin >= 0 ? game.home_tri : game.away_tri;
      const marketLine = [
        Number.isFinite(row.totalLine) ? `Total ${fmtNumber(row.totalLine, 1)}` : null,
        Number.isFinite(row.spreadLine) ? `Home ${fmtSigned(row.spreadLine, 1)}` : null,
      ].filter(Boolean).join(' | ');
      return `
        <div class="cards-live-lens-card">
          <div class="cards-live-lens-head">
            <div class="cards-live-lens-title-block">
              <strong>${escapeHtml(row.label)}</strong>
              <div class="cards-live-lens-subtitle">${escapeHtml(projectionLine)}</div>
            </div>
            <span class="cards-chip">Projection</span>
          </div>
          <div class="cards-live-lens-summary-row">
            <div class="cards-data-pair"><span>Home win</span><strong>${escapeHtml(fmtPercent(modelProb, 1))}</strong></div>
            <div class="cards-data-pair"><span>Baseline</span><strong>${escapeHtml(fmtPercent(baselineProb, 1))}</strong></div>
          </div>
          <div class="cards-live-lens-picks">
            <div class="cards-live-lens-pick">${escapeHtml(`Winner lean: ${leanTeam} ${fmtSigned(homeDelta, 1)} pp`)}</div>
            <div class="cards-live-lens-pick">${escapeHtml(`Margin: ${game.home_tri} ${fmtSigned(row.projection.homeMargin, 1)}`)}</div>
            <div class="cards-live-lens-pick">${escapeHtml(`Model total: ${fmtNumber(row.projection.total, 1)}`)}</div>
          </div>
          <div class="cards-live-lens-market">${escapeHtml(marketLine || `${row.lineLabel} projection`)}</div>
        </div>`;
    }).join('');
  }

  function segmentProbabilityRows(game) {
    return buildGameLensRows(game).map((row) => {
      const home = Number.isFinite(Number(row.modelHomeWinProb)) ? Number(row.modelHomeWinProb) : 0.5;
      const away = 1 - home;
      return `
        <div class="cards-prob-row">
          <div class="cards-prob-label">${escapeHtml(row.lineLabel)}</div>
          <div class="cards-prob-bar" style="--away-pct:${Math.max(10, away * 100).toFixed(1)}%; --home-pct:${Math.max(10, home * 100).toFixed(1)}%;">
            <div class="cards-prob-away"></div>
            <div class="cards-prob-home"></div>
          </div>
          <div class="cards-mini-copy">${escapeHtml(`${game.away_tri} ${fmtPercent(away, 1)} | ${game.home_tri} ${fmtPercent(home, 1)}`)}</div>
        </div>
      `;
    }).join('');
  }

  function miniMetrics(game) {
    const context = game?.sim?.context || {};
    const liveLens = getLiveLens(game);
    const counts = {
      home: safeArray(game?.prop_recommendations?.home).length,
      away: safeArray(game?.prop_recommendations?.away).length,
    };
    const awayPace = mode === 'live' && Number.isFinite(Number(liveLens?.awayPace))
      ? Number(liveLens.awayPace)
      : Number(context.away_pace);
    const homePace = mode === 'live' && Number.isFinite(Number(liveLens?.homePace))
      ? Number(liveLens.homePace)
      : Number(context.home_pace);
    const liveAwayPoss = Number(liveLens?.awayPossessions);
    const liveHomePoss = Number(liveLens?.homePossessions);

    function shootingBreakdown(bucket) {
      if (!bucket || typeof bucket !== 'object') {
        return '';
      }
      const ftAtt = Number(bucket.ft_att);
      const fg2Att = Number(bucket.fg2_att);
      const fg3Att = Number(bucket.fg3_att);
      const ftMade = Number(bucket.ft_made);
      const fg2Made = Number(bucket.fg2_made);
      const fg3Made = Number(bucket.fg3_made);
      const parts = [];
      if (Number.isFinite(ftAtt) && ftAtt > 0) {
        parts.push(`FT ${fmtInteger(ftMade)}/${fmtInteger(ftAtt)}`);
      }
      if (Number.isFinite(fg2Att) && fg2Att > 0) {
        parts.push(`2P ${fmtInteger(fg2Made)}/${fmtInteger(fg2Att)}`);
      }
      if (Number.isFinite(fg3Att) && fg3Att > 0) {
        parts.push(`3P ${fmtInteger(fg3Made)}/${fmtInteger(fg3Att)}`);
      }
      return parts.join(' · ');
    }

    function livePaceTile(teamTri, paceValue, possessions, attempts) {
      const hasLivePossessions = Number.isFinite(Number(possessions));
      const breakdown = shootingBreakdown(attempts);
      return {
        label: `${teamTri} pace`,
        value: fmtNumber(paceValue, 1),
        sub: hasLivePossessions ? `Poss est ${fmtNumber(possessions, 1)}` : 'live expected possessions',
        extra: breakdown ? [breakdown] : [],
      };
    }

    const entries = mode === 'live'
      ? [
        livePaceTile(game.away_tri, awayPace, liveAwayPoss, liveLens?.awayAttempts),
        livePaceTile(game.home_tri, homePace, liveHomePoss, liveLens?.homeAttempts),
        { label: 'Official props', value: String(counts.home + counts.away), sub: `${counts.away} away · ${counts.home} home` },
      ]
      : [
        { label: `${game.away_tri} pace`, value: fmtNumber(awayPace, 1), sub: 'expected possessions' },
        { label: `${game.home_tri} pace`, value: fmtNumber(homePace, 1), sub: 'expected possessions' },
        { label: 'Official props', value: String(counts.home + counts.away), sub: `${counts.away} away · ${counts.home} home` },
      ];

    return entries.map((entry) => `
      <div class="cards-mini-metric ${safeArray(entry.extra).length ? 'is-rich' : ''}">
        <span class="cards-section-label">${escapeHtml(entry.label)}</span>
        <strong>${escapeHtml(entry.value)}</strong>
        <div class="cards-mini-copy">${escapeHtml(entry.sub)}</div>
        ${safeArray(entry.extra).map((line) => `<div class="cards-mini-copy">${escapeHtml(line)}</div>`).join('')}
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

  function teamHeaderMarkup(teamTri, teamName) {
    const logo = logoForTri(teamTri);
    return `
      <div class="cards-team-line">
        ${logo
          ? `<img class="cards-logo" src="${escapeHtml(logo)}" alt="${escapeHtml(teamTri || teamName || 'Team')} logo" />`
          : `<span class="cards-logo-fallback">${escapeHtml(String(teamTri || teamName || 'TM').slice(0, 3).toUpperCase())}</span>`}
        <div class="cards-team-meta">
          <div class="cards-team-code">${escapeHtml(teamTri || 'TBD')}</div>
          <div class="cards-team-name">${escapeHtml(teamName || teamTri || 'Team')}</div>
        </div>
      </div>
    `;
  }

  function renderScoreboardItem(game) {
    const betting = game?.betting || {};
    const score = game?.sim?.score || {};
    const liveLens = getLiveLens(game);
    const liveState = getLiveState(game);
    const hasStarted = hasStartedGame(liveState);
    const id = cardId(game);
    const compactHeaderText = mode === 'live' && !hasStarted
      ? fmtTime(game?.odds?.commence_time)
      : statusText(game);
    const compactDetailText = mode === 'live'
      ? (hasStarted ? (liveState?.status || liveLens?.signals?.total?.detail || 'Monitoring live game lens') : marketCountSummary(game))
      : marketCountSummary(game);
    const awayScore = mode === 'live' && hasStarted && Number.isFinite(Number(liveState?.away_pts)) ? Number(liveState.away_pts) : score.away_mean;
    const homeScore = mode === 'live' && hasStarted && Number.isFinite(Number(liveState?.home_pts)) ? Number(liveState.home_pts) : score.home_mean;
    const stripMeta = mode === 'live'
      ? (hasStarted ? [
        liveLens?.compactSignals?.[0]?.detail,
        liveLens?.signals?.total?.detail,
        liveLens?.signals?.ats?.detail,
      ].filter(Boolean)[0] || 'Waiting for a live edge.' : 'Pregame betting board ready')
      : 'Pregame betting board ready';
    return `
      <a class="cards-strip-card ${liveLens?.overallClass === 'BET' ? 'cards-live-lens--bet' : (liveLens?.overallClass === 'WATCH' ? 'cards-live-lens--watch' : '')}" data-card-id="${escapeHtml(id)}" data-matchup-key="${escapeHtml(gameMatchupKey(game))}" href="#game-card-${encodeURIComponent(id)}">
        <div class="cards-strip-head">
          <span>${escapeHtml(compactHeaderText)}</span>
          <span>${escapeHtml(compactDetailText)}</span>
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
            <span class="cards-linescore-stat">${fmtInteger(awayScore)}</span>
            <span class="cards-linescore-stat">${fmtPercent(betting.p_away_win, 0)}</span>
            <span class="cards-linescore-stat">${fmtPercent(betting.p_away_cover, 0)}</span>
          </div>
          <div class="cards-linescore-row">
            <div class="cards-linescore-team">
              ${stripLogoMarkup(game.home_tri)}
              <strong>${escapeHtml(game.home_tri || 'HME')}</strong>
            </div>
            <span class="cards-linescore-stat">${fmtInteger(homeScore)}</span>
            <span class="cards-linescore-stat">${fmtPercent(betting.p_home_win, 0)}</span>
            <span class="cards-linescore-stat">${fmtPercent(betting.p_home_cover, 0)}</span>
          </div>
        </div>
        <div class="cards-strip-meta">${escapeHtml(stripMeta)}</div>
      </a>
    `;
  }

  function renderMarketTile(title, pick, auxLine, noteText, cardIdValue) {
    const hasMetrics = Number.isFinite(Number(pick?.probability)) || Number.isFinite(Number(pick?.ev));
    return `
      <button class="cards-market-tile" type="button" ${pick?.tabTarget ? `data-market-tab-target="${escapeHtml(pick.tabTarget)}" data-card-target="${escapeHtml(cardIdValue)}"` : ''}>
        <div class="cards-market-kicker">${escapeHtml(title)}</div>
        <div class="cards-market-main">${escapeHtml(pick?.detail || 'No playable edge')}</div>
        <div class="cards-mini-copy">${escapeHtml(pick ? (hasMetrics ? `Win ${fmtPercent(pick.probability, 0)} · EV ${fmtPercentValue(pick.ev)}` : (pick.meta || 'Off card')) : 'Off card')}</div>
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
    const liveLens = getLiveLens(game);
    const liveSignalTiles = safeArray(liveLens?.topSignals).slice(0, 6);
    return `
      <div class="cards-overview-grid">
        <div class="cards-panel-card">
          <div class="cards-box-head">
            <div class="cards-table-title"><strong>${escapeHtml(mode === 'live' ? 'Live game lens' : 'Game lens')}</strong></div>
            <span class="cards-overview-badge ${statusClass(game)}">${escapeHtml(mode === 'live' ? 'Live model' : 'Pregame model')}</span>
          </div>
          <div class="cards-live-lens-grid">${renderGameLens(game)}</div>
          <div class="cards-prob-grid">${segmentProbabilityRows(game)}</div>
          ${mode === 'live' && liveSignalTiles.length ? `
            <div class="cards-live-opportunity-stack">
              <div class="cards-section-label">BET / WATCH opportunities</div>
              <div class="cards-live-opportunity-list">${liveSignalTiles.map(renderLiveSignalTile).join('')}</div>
            </div>
          ` : ''}
        </div>
        <div class="cards-overview-side">
          <div class="cards-panel-card">
            <div class="cards-box-head">
              <div class="cards-table-title"><strong>Official card</strong></div>
              <span class="cards-chip">${escapeHtml(marketCountSummary(game) || 'Market board')}</span>
            </div>
            <ul class="cards-callout-list">${officialCardRows(game)}</ul>
            ${sidebarPropBoardMarkup(game)}
          </div>
        </div>
      </div>
    `;
  }

  function renderTabsRail(game, activeTab, id) {
    return `
      <div class="cards-tabs-rail">
        <div class="cards-tabs">
          <button class="cards-tab ${activeTab === 'game' ? 'is-active' : ''}" type="button" data-card-tab="game" data-card-target="${escapeHtml(id)}">Game</button>
          <button class="cards-tab ${activeTab === 'box' ? 'is-active' : ''}" type="button" data-card-tab="box" data-card-target="${escapeHtml(id)}">Box Score</button>
          <button class="cards-tab ${activeTab === 'props' ? 'is-active' : ''}" type="button" data-card-tab="props" data-card-target="${escapeHtml(id)}">Props</button>
        </div>
        <div class="cards-mini-metrics cards-mini-metrics--rail">${miniMetrics(game)}</div>
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

  function actualTeamTotals(players) {
    const rows = safeArray(players);
    return {
      pts: rows.reduce((sum, player) => sum + (Number(player?.pts) || 0), 0),
      reb: rows.reduce((sum, player) => sum + (Number(player?.reb) || 0), 0),
      ast: rows.reduce((sum, player) => sum + (Number(player?.ast) || 0), 0),
      pra: rows.reduce((sum, player) => sum + (Number(player?.pts) || 0) + (Number(player?.reb) || 0) + (Number(player?.ast) || 0), 0),
    };
  }

  function renderActualBoxTableRows(players) {
    return safeArray(players).map((player) => `
      <tr>
        <td>
          <div class="box-player-cell">
            <strong>${escapeHtml(player.player || 'Player')}</strong>
          </div>
        </td>
        <td>${escapeHtml(fmtMinutesPlayed(player.mp))}</td>
        <td>${fmtInteger(player.pts)}</td>
        <td>${fmtInteger(player.reb)}</td>
        <td>${fmtInteger(player.ast)}</td>
        <td>${fmtInteger(player.threes_made)}</td>
        <td>${fmtInteger((Number(player.pts) || 0) + (Number(player.reb) || 0) + (Number(player.ast) || 0))}</td>
      </tr>
    `).join('');
  }

  function renderActualBoxSection(teamTri, teamName, players, liveState) {
    const logo = logoForTri(teamTri);
    const totals = actualTeamTotals(players);
    const liveStatus = liveState?.final ? 'Final' : (liveState?.in_progress ? 'Live' : 'Awaiting tipoff');
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
            <tbody>${renderActualBoxTableRows(players)}</tbody>
          </table>
        </div>
      `
      : '<div class="box-empty">No live box score rows available yet.</div>';
    return `
      <div class="cards-panel-card cards-box-panel">
        <div class="cards-box-head">
          <div class="cards-table-title"><strong>${escapeHtml(teamTri)} live box</strong></div>
          <span class="cards-overview-badge ${liveState?.final ? 'is-final' : (liveState?.in_progress ? 'is-live' : '')}">${escapeHtml(liveStatus)}</span>
        </div>
        <div class="cards-box-team-head">
          ${logo ? `<img class="cards-box-team__logo" src="${escapeHtml(logo)}" alt="${escapeHtml(teamTri)} logo" />` : ''}
          <div>
            <div class="cards-box-team-title">${escapeHtml(teamName || teamTri)}</div>
            <div class="cards-mini-copy">${escapeHtml(teamTri)} live player box</div>
          </div>
        </div>
        <div class="cards-box-totals">${renderLinescoreSummary(teamTri, totals)}</div>
        ${tableMarkup}
      </div>
    `;
  }

  function renderBoxTableRows(players) {
    const sorted = [...safeArray(players)].sort((a, b) => Number(b.min_mean || 0) - Number(a.min_mean || 0));
    return sorted.map((player) => {
      return `
        <tr>
          <td>
            <div class="box-player-cell">
              <strong>${escapeHtml(player.player_name || 'Player')}</strong>
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

  function renderBoxScorePanel(game) {
    const detailLoaded = hasLoadedSimDetail(game);
    const detailLoading = state.simDetailLoading.has(cardId(game));
    if (!detailLoaded) {
      const counts = game?.sim?.players_summary || {};
      const totalRows = Number(counts.away || 0) + Number(counts.home || 0);
      return `
        <div class="cards-panel-card cards-box-panel">
          <div class="cards-box-head">
            <div class="cards-table-title"><strong>Sim box score</strong></div>
            <span class="cards-chip">${escapeHtml(totalRows ? `${totalRows} projected rows` : 'On demand')}</span>
          </div>
          <div class="cards-callout-copy">${escapeHtml(detailLoading ? 'Loading per-player sim rows for this matchup.' : 'Per-player SmartSim rows load only when you open a game detail tab, so the main betting card stays fast.')}</div>
        </div>
      `;
    }
    const liveState = getLiveState(game);
    const liveBoxscore = getLivePlayerBoxscore(game) || { away: [], home: [] };
    return `
      <div class="cards-box-grid">
        <div class="cards-box-column cards-box-column--actual">
          ${renderActualBoxSection(game.away_tri, game.away_name, liveBoxscore.away, liveState)}
          ${renderActualBoxSection(game.home_tri, game.home_name, liveBoxscore.home, liveState)}
        </div>
        <div class="cards-box-column cards-box-column--sim">
          ${renderBoxSection(game.away_tri, game.away_name, game?.sim?.players?.away || [], game?.sim?.injuries?.away || [], game?.sim?.missing_prop_players?.away || [])}
          ${renderBoxSection(game.home_tri, game.home_name, game?.sim?.players?.home || [], game?.sim?.injuries?.home || [], game?.sim?.missing_prop_players?.home || [])}
        </div>
      </div>
    `;
  }

  function ensurePropDetail(game) {
    const id = cardId(game);
    if (!state.propDetails.has(id)) {
      state.propDetails.set(id, {
        selectedKey: null,
        bucket: 'all',
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
          const bucket = index === 0 ? (row.card_bucket || 'playable') : 'playable';
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
            evPct: pick.ev_pct ?? row.ev_pct,
            pWin: pick.p_win ?? row.p_win,
            simMu: pick.sim_mu,
            simSd: pick.sim_sd,
            summary: row.basketball_summary || pick.basketball_summary || row.display_pick || '',
            reasons: safeArray(pick.reasons).length ? safeArray(pick.reasons) : safeArray(row.top_play_reasons),
            matchup: row.matchup,
            rank: index + 1,
            primary: index === 0,
            bucket,
            cardRank: row.card_rank,
            recommendationPriorityScore: row.recommendation_priority_score,
            score: row.score,
            tier: row.tier,
          });
        });
      });
    });
    rows.forEach((row) => {
      row.key = propKey(row);
    });
    return rows.sort((left, right) => {
      if (left.bucket !== right.bucket) {
        return left.bucket === 'official' ? -1 : 1;
      }
      const scoreDelta = recommendationSortScore(right) - recommendationSortScore(left);
      if (scoreDelta) {
        return scoreDelta;
      }
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
      if (detail.bucket !== 'all' && row.bucket !== detail.bucket) {
        return false;
      }
      return true;
    });
  }

  function filteredLivePropRows(game) {
    const detail = ensurePropDetail(game);
    const rows = liveOpportunityPropRows(game);
    return rows.filter((row) => {
      if (detail.side !== 'all' && row.sideKey !== detail.side) {
        return false;
      }
      if (detail.type !== 'all' && row.market !== detail.type) {
        return false;
      }
      if (detail.bucket !== 'all' && row.bucket !== detail.bucket) {
        return false;
      }
      return true;
    });
  }

  function splitFilteredPropRows(game) {
    const rows = filteredPropRows(game);
    return {
      live: filteredLivePropRows(game),
      official: rows.filter((row) => row.bucket === 'official'),
      playable: rows.filter((row) => row.bucket !== 'official'),
    };
  }

  function selectedPropRow(game) {
    const detail = ensurePropDetail(game);
    const groups = splitFilteredPropRows(game);
    const filtered = groups.live.concat(groups.official, groups.playable);
    let selected = filtered.find((row) => row.key === detail.selectedKey) || null;
    if (!selected) {
      selected = filtered[0] || null;
      detail.selectedKey = selected ? selected.key : null;
    }
    return selected;
  }

  function renderPropFilters(game) {
    const detail = ensurePropDetail(game);
    const rows = allPropRows(game);
    const liveRows = liveOpportunityPropRows(game);
    const rowsForCounts = rows.filter((row) => {
      if (detail.side !== 'all' && row.sideKey !== detail.side) {
        return false;
      }
      if (detail.type !== 'all' && row.market !== detail.type) {
        return false;
      }
      return true;
    });
    const liveRowsForCounts = liveRows.filter((row) => {
      if (detail.side !== 'all' && row.sideKey !== detail.side) {
        return false;
      }
      if (detail.type !== 'all' && row.market !== detail.type) {
        return false;
      }
      return true;
    });
    const liveCount = liveRowsForCounts.length;
    const officialCount = rowsForCounts.filter((row) => row.bucket === 'official').length;
    const playableCount = rowsForCounts.filter((row) => row.bucket !== 'official').length;
    const markets = Array.from(new Set(rows.concat(liveRows).map((row) => row.market))).sort();
    const sidePills = [
      { key: 'all', label: 'All sides' },
      { key: 'away', label: `${game.away_tri}` },
      { key: 'home', label: `${game.home_tri}` },
    ];
    const typePills = [{ key: 'all', label: 'All props' }].concat(markets.map((market) => ({ key: market, label: marketLabel(market) })));
    return `
      <div class="cards-filters cards-prop-filter-pills">
        ${liveCount || detail.bucket === 'live' ? `
        <button class="cards-filter-pill cards-prop-filter-pill ${detail.bucket === 'live' ? 'is-active' : ''}" type="button" data-prop-filter-bucket="live" data-card-target="${escapeHtml(cardId(game))}">
          <span>Live</span>
          <span class="cards-prop-filter-count">${escapeHtml(String(liveCount))}</span>
        </button>` : ''}
        <button class="cards-filter-pill cards-prop-filter-pill ${detail.bucket === 'official' ? 'is-active' : ''}" type="button" data-prop-filter-bucket="official" data-card-target="${escapeHtml(cardId(game))}">
          <span>Official</span>
          <span class="cards-prop-filter-count">${escapeHtml(String(officialCount))}</span>
        </button>
        <button class="cards-filter-pill cards-prop-filter-pill ${detail.bucket === 'playable' ? 'is-active' : ''}" type="button" data-prop-filter-bucket="playable" data-card-target="${escapeHtml(cardId(game))}">
          <span>Playable</span>
          <span class="cards-prop-filter-count">${escapeHtml(String(playableCount))}</span>
        </button>
      </div>
      <div class="cards-filters">
        ${sidePills.map((pill) => `<button class="cards-filter-pill ${detail.side === pill.key ? 'is-active' : ''}" type="button" data-prop-filter-side="${escapeHtml(pill.key)}" data-card-target="${escapeHtml(cardId(game))}">${escapeHtml(pill.label)}</button>`).join('')}
      </div>
      <div class="cards-filters">
        ${typePills.map((pill) => `<button class="cards-filter-pill ${detail.type === pill.key ? 'is-active' : ''}" type="button" data-prop-filter-type="${escapeHtml(pill.key)}" data-card-target="${escapeHtml(cardId(game))}">${escapeHtml(pill.label)}</button>`).join('')}
      </div>
    `;
  }

  function renderPropButtons(game, rows, selectedKey) {
    return `
      <div class="cards-prop-list">
        ${rows.map((row) => {
          const tierClass = row.bucket === 'official' ? 'is-official' : (row.bucket === 'live' ? 'is-live' : 'is-candidate');
          const tierLabel = row.bucket === 'official' ? 'Official' : (row.bucket === 'live' ? (row.actionLabel || 'Live') : 'Playable');
          const supportingCopy = row.bucket === 'live'
            ? `${row.teamTri} · ${row.statusLabel || 'Live'}${Number.isFinite(row.liveEdge) ? ` · Edge ${fmtSigned(row.liveEdge, 1)}` : ''}`
            : `${row.teamTri} · EV ${fmtPercentValue(row.evPct)} · ${fmtAmerican(row.price)} ${row.book || ''}`.trim();
          return `
            <button class="cards-prop-button ${tierClass} ${selectedKey === row.key ? 'is-active' : ''}" type="button" data-prop-select="${escapeHtml(row.key)}" data-card-target="${escapeHtml(cardId(game))}">
              <div class="cards-prop-button-head">
                <strong>${escapeHtml(row.player || 'Player')}</strong>
                <span class="cards-chip ${tierClass}">${escapeHtml(tierLabel)}</span>
              </div>
              <div class="cards-prop-button-main">${escapeHtml(row.marketLabel)} ${escapeHtml(row.side)} ${fmtNumber(row.line, 1)}</div>
              <small>${escapeHtml(supportingCopy)}</small>
            </button>
          `;
        }).join('')}
      </div>
    `;
  }

  function renderPropTeamStacks(game, rows, selectedKey) {
    const groups = [
      { key: 'away', label: `${game.away_tri} props` },
      { key: 'home', label: `${game.home_tri} props` },
    ];
    return groups
      .map((group) => {
        const teamRows = rows.filter((row) => row.sideKey === group.key);
        if (!teamRows.length) {
          return '';
        }
        return `
          <div class="cards-prop-stack">
            <div class="cards-section-label">${escapeHtml(group.label)}</div>
            ${renderPropButtons(game, teamRows, selectedKey)}
          </div>
        `;
      })
      .join('');
  }

  function renderPropGroups(game) {
    const groups = splitFilteredPropRows(game);
    const rows = groups.live.concat(groups.official, groups.playable);
    if (!rows.length) {
      return '<div class="cards-empty">No live, official, or playable props matched the current side and prop-type filters for this game.</div>';
    }

    const activeKey = selectedPropRow(game)?.key;

    const renderGroup = (title, chipText, groupRows, bucketKey, options = {}) => {
      if (!groupRows.length) {
        return '';
      }
      const chipClass = options.chipClass || '';
      const description = options.description || '';
      const body = options.teamStacks
        ? renderPropTeamStacks(game, groupRows, activeKey)
        : renderPropButtons(game, groupRows, activeKey);
      return `
        <div class="cards-prop-group ${options.secondary ? 'is-secondary' : ''}">
          <div class="cards-box-head cards-box-head--nested">
            <div class="cards-table-title"><strong>${escapeHtml(title)}</strong></div>
            <button class="cards-chip cards-chip-button ${chipClass}" type="button" data-prop-filter-bucket="${escapeHtml(bucketKey)}" data-card-target="${escapeHtml(cardId(game))}">${escapeHtml(chipText)}</button>
          </div>
          ${description ? `<div class="cards-callout-copy">${escapeHtml(description)}</div>` : ''}
          ${body}
        </div>
      `;
    };

    return `
      <div class="cards-prop-board">
        ${renderGroup('Live opportunities', `${groups.live.length} plays`, groups.live, 'live', {
          chipClass: 'is-live',
          description: 'Current market odds ranked by live projection first, then sim support and edge.',
        })}
        ${renderGroup('Official picks', `${groups.official.length} plays`, groups.official, 'official', {
          chipClass: 'cards-chip--accent',
          teamStacks: true,
        })}
        ${renderGroup('Other playable props', `${groups.playable.length} plays`, groups.playable, 'playable', {
          secondary: true,
          description: 'Qualified lanes that stayed off the official card after sleeve caps and one-prop-per-player selection.',
          teamStacks: true,
        })}
      </div>
    `;
  }

  function playerSimRow(game, selected) {
    if (!selected) {
      return null;
    }
    const players = selected.sideKey === 'away' || selected.sideKey === 'home'
      ? safeArray(game?.sim?.players?.[selected.sideKey]).concat(safeArray(game?.sim?.missing_prop_players?.[selected.sideKey]))
      : safeArray(game?.sim?.players?.away)
        .concat(safeArray(game?.sim?.players?.home))
        .concat(safeArray(game?.sim?.missing_prop_players?.away))
        .concat(safeArray(game?.sim?.missing_prop_players?.home));
    return players.find((player) => String(player.player_name || '').trim().toLowerCase() === String(selected.player || '').trim().toLowerCase()) || null;
  }

  function playerActualRow(game, selected) {
    if (!selected) {
      return null;
    }
    const liveBoxscore = getLivePlayerBoxscore(game) || { away: [], home: [] };
    const playerKey = normalizePlayerKey(selected.player);
    const preferredRows = selected.sideKey === 'away' || selected.sideKey === 'home'
      ? safeArray(liveBoxscore[selected.sideKey])
      : [];
    const fallbackRows = safeArray(liveBoxscore.away).concat(safeArray(liveBoxscore.home));
    return preferredRows.concat(fallbackRows).find((player) => normalizePlayerKey(player?.player) === playerKey) || null;
  }

  function propTierLabel(selected) {
    if (!selected) {
      return '-';
    }
    if (selected.bucket === 'live') {
      return selected.actionLabel || 'Live';
    }
    return selected.bucket === 'official' ? 'Official' : 'Playable';
  }

  function renderLensDetailGrid(pairs) {
    return `
      <div class="cards-detail-grid">
        ${pairs.map((pair) => `
          <div class="cards-data-pair">
            <span>${escapeHtml(pair.label)}</span>
            <strong>${escapeHtml(pair.value)}</strong>
          </div>
        `).join('')}
      </div>
    `;
  }

  function renderLensReasons(selected) {
    const reasons = safeArray(selected?.reasons).slice(0, 6);
    const pills = reasons.map((reason) => `<span class="cards-source-meta-pill">${escapeHtml(reason)}</span>`).join('');
    const summary = String(selected?.summary || '').trim();
    if (!summary && !pills) {
      return '';
    }
    return `
      <div class="cards-panel-card cards-prop-stack cards-lens-reasons">
        <div class="cards-table-head"><div class="cards-table-title">Why this lane</div></div>
        ${summary ? `<div class="cards-callout-copy">${escapeHtml(summary)}</div>` : ''}
        ${pills ? `<div class="cards-source-meta">${pills}</div>` : ''}
      </div>
    `;
  }

  function renderLensDetailPairs(selected, simRow) {
    const metricValue = simRow ? simRow[`${selected.market}_mean`] : null;
    const simValue = Number(metricValue ?? selected.simMu);
    return [
      { label: 'Tier', value: propTierLabel(selected) },
      { label: 'Line', value: `${escapeHtml(selected.side)} ${fmtNumber(selected.line, 1)}` },
      { label: 'Odds', value: `${fmtAmerican(selected.price)} ${selected.book || ''}`.trim() },
      { label: 'EV', value: fmtPercentValue(selected.evPct) },
      { label: 'Win prob', value: fmtPercent(selected.pWin, 0) },
      { label: 'Edge', value: Number.isFinite(Number(selected.edge)) ? fmtSigned(selected.edge, 1) : '-' },
      { label: 'Model mean', value: Number.isFinite(simValue) ? fmtNumber(simValue, 1) : '-' },
      { label: 'Sim row', value: Number.isFinite(simValue) ? fmtNumber(simValue, 1) : '-' },
    ];
  }

  function renderPlayerRowTable(simRow) {
    if (!simRow) {
      return '<div class="cards-empty">Load the game detail to view the full SmartSim player row for this pick.</div>';
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

  function renderActualPlayerRowTable(actualRow) {
    if (!actualRow) {
      return '<div class="cards-empty">No live or final player row matched this prop yet.</div>';
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
              <td>${escapeHtml(actualRow.player || 'Player')}</td>
              <td>${escapeHtml(fmtMinutesPlayed(actualRow.mp))}</td>
              <td>${fmtInteger(actualRow.pts)}</td>
              <td>${fmtInteger(actualRow.reb)}</td>
              <td>${fmtInteger(actualRow.ast)}</td>
              <td>${fmtInteger(actualRow.threes_made)}</td>
              <td>${fmtInteger((Number(actualRow.pts) || 0) + (Number(actualRow.reb) || 0) + (Number(actualRow.ast) || 0))}</td>
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
        <div class="cards-callout-copy">No live, official, or playable props matched the current side and prop-type filters for this game.</div>
      `;
    }
    const simRow = playerSimRow(game, selected);
    const actualRow = playerActualRow(game, selected);
    const actualValue = actualStatValue(actualRow, selected.market);
    const simValue = Number(simRow?.[`${selected.market}_mean`] ?? selected.simMu);
    const reasonsPanel = renderLensReasons(selected);
    const laddersHref = `/prop-ladders?date=${encodeURIComponent(state.date)}&team=${encodeURIComponent(selected.teamTri)}&player=${encodeURIComponent(selected.player)}&market=${encodeURIComponent(selected.market)}`;
    if (selected.bucket === 'live') {
      const liveActual = Number(selected.actual);
      const liveProjection = Number(selected.liveProjection);
      const liveEdge = Number(selected.liveEdge);
      const livePairs = [
        { label: 'Tier', value: propTierLabel(selected) },
        { label: 'Current', value: Number.isFinite(liveActual) ? fmtNumber(liveActual, 1) : '-' },
        { label: 'Live proj', value: Number.isFinite(liveProjection) ? fmtNumber(liveProjection, 1) : '-' },
        { label: 'Sim row', value: Number.isFinite(simValue) ? fmtNumber(simValue, 1) : '-' },
        { label: 'Model mean', value: Number.isFinite(Number(selected.simMu)) ? fmtNumber(selected.simMu, 1) : '-' },
        { label: 'Line', value: `${escapeHtml(selected.side)} ${fmtNumber(selected.line, 1)}` },
        { label: 'Odds', value: Number.isFinite(Number(selected.price)) ? `${fmtAmerican(selected.price)} ${selected.book || ''}`.trim() : (selected.book || '-') },
        { label: 'Live edge', value: Number.isFinite(liveEdge) ? fmtSigned(liveEdge, 1) : '-' },
        { label: 'Actual row', value: Number.isFinite(actualValue) ? fmtNumber(actualValue, 1) : '-' },
      ];
      return `
        <div class="cards-lens-head">
          <div>
            <div class="cards-lens-label">Prop lens</div>
            <div class="cards-lens-main">${escapeHtml(selected.player)} · ${escapeHtml(selected.marketLabel)} ${escapeHtml(selected.side)} ${fmtNumber(selected.line, 1)}</div>
            <div class="cards-subcopy">${escapeHtml(selected.teamTri)} · ${escapeHtml(selected.matchup || `${game.away_tri} at ${game.home_tri}`)}</div>
          </div>
          <span class="cards-lens-badge is-live">${escapeHtml(selected.actionLabel || 'Live')}</span>
        </div>
        ${renderLensDetailGrid(livePairs)}
        ${reasonsPanel}
        <div class="cards-box-grid">
          <div class="cards-panel-card cards-prop-stack">
            <div class="cards-table-head"><div class="cards-table-title">Live / final player row</div></div>
            ${renderActualPlayerRowTable(actualRow)}
          </div>
          <div class="cards-panel-card cards-prop-stack">
            <div class="cards-table-head"><div class="cards-table-title">Sim player row</div></div>
            ${renderPlayerRowTable(simRow)}
          </div>
        </div>
        <a class="cards-game-link" href="${escapeHtml(laddersHref)}">Open full ${escapeHtml(selected.marketLabel)} ladders board</a>
      `;
    }
    const defaultPairs = renderLensDetailPairs(selected, simRow).concat([
      { label: 'Actual row', value: Number.isFinite(actualValue) ? fmtNumber(actualValue, 1) : '-' },
      { label: 'Team', value: selected.teamTri || '-' },
    ]);
    return `
      <div class="cards-lens-head">
        <div>
          <div class="cards-lens-label">Prop lens</div>
          <div class="cards-lens-main">${escapeHtml(selected.player)} · ${escapeHtml(selected.marketLabel)} ${escapeHtml(selected.side)} ${fmtNumber(selected.line, 1)}</div>
          <div class="cards-subcopy">${escapeHtml(selected.teamTri)} · ${escapeHtml(selected.matchup || `${game.away_tri} at ${game.home_tri}`)}</div>
        </div>
        <span class="cards-lens-badge ${selected.bucket === 'official' ? '' : 'is-live'}">${selected.bucket === 'official' ? 'Official' : 'Playable'}</span>
      </div>
      ${renderLensDetailGrid(defaultPairs)}
      ${reasonsPanel}
      <div class="cards-box-grid">
        <div class="cards-panel-card cards-prop-stack">
          <div class="cards-table-head"><div class="cards-table-title">Live / final player row</div></div>
          ${renderActualPlayerRowTable(actualRow)}
        </div>
        <div class="cards-panel-card cards-prop-stack">
          <div class="cards-table-head"><div class="cards-table-title">Sim player row</div></div>
          ${renderPlayerRowTable(simRow)}
        </div>
      </div>
      <a class="cards-game-link" href="${escapeHtml(laddersHref)}">Open full ${escapeHtml(selected.marketLabel)} ladders board</a>
    `;
  }

  function renderPropsPanel(game) {
    return `
      <div class="cards-props-grid">
        <div class="cards-panel-card">
          <div class="cards-box-head">
            <div class="cards-table-title"><strong>Props board</strong></div>
            <span class="cards-chip">${escapeHtml(propBucketSummary(game))}</span>
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
    const matchup = gameMatchupKey(game);
    const activeTab = state.activeTabs.get(id) || 'game';
    const betting = game?.betting || {};
    const score = game?.sim?.score || {};
    const market = game?.sim?.market || {};
    const liveState = getLiveState(game);
    const liveLens = getLiveLens(game);
    const hasStarted = hasStartedGame(liveState);
    const awayScore = mode === 'live' && hasStarted && Number.isFinite(Number(liveState?.away_pts)) ? Number(liveState.away_pts) : score.away_mean;
    const homeScore = mode === 'live' && hasStarted && Number.isFinite(Number(liveState?.home_pts)) ? Number(liveState.home_pts) : score.home_mean;
    const simScoreLabel = `Sim score ${game.away_tri} ${fmtNumber(score.away_mean, 1)} - ${fmtNumber(score.home_mean, 1)} ${game.home_tri}`;
    const liveScoreLabel = mode === 'live' && hasStarted
      ? `Live score ${game.away_tri} ${fmtInteger(awayScore)} - ${fmtInteger(homeScore)} ${game.home_tri}`
      : simScoreLabel;
    const liveDataPending = mode === 'live' && state.liveDataLoading && !liveState;
    const liveLensPending = mode === 'live' && state.liveDataLoading && !liveLens;
    const scoreMetaPrimary = liveDataPending ? 'Loading live box...' : liveScoreLabel;
    const scoreMetaSecondary = liveLensPending
      ? 'Loading live lens...'
      : `${simScoreLabel} · Total ${fmtNumber(score.total_mean, 1)} · Margin ${fmtSigned(score.margin_mean, 1)}`;
    const propsSummary = propBucketSummary(game);
    const livePropCount = liveOpportunityPropRows(game).length;
    return `
      <article class="cards-game-card ${liveLens?.overallClass === 'BET' ? 'cards-live-lens--bet' : (liveLens?.overallClass === 'WATCH' ? 'cards-live-lens--watch' : '')}" data-card-id="${escapeHtml(id)}" data-matchup-key="${escapeHtml(matchup)}" id="game-card-${escapeHtml(id)}">
        <div class="cards-strip-head">
          <div class="cards-head-left cards-head-matchup">
            <div class="cards-head-team">
              ${teamHeaderMarkup(game.away_tri, game.away_name)}
              <div class="cards-head-team-score">${fmtNumber(awayScore, mode === 'live' ? 0 : 1)}</div>
            </div>
            <span class="cards-score-divider">@</span>
            <div class="cards-head-team">
              ${teamHeaderMarkup(game.home_tri, game.home_name)}
              <div class="cards-head-team-score">${fmtNumber(homeScore, mode === 'live' ? 0 : 1)}</div>
            </div>
          </div>
          <div class="cards-status-cluster">
            <div class="cards-game-time-row">
              <span class="cards-game-time-label">Tipoff</span>
              <span class="cards-game-time-value">${escapeHtml(fmtTime(game?.odds?.commence_time))}</span>
            </div>
            <span class="cards-status-badge ${statusClass(game)}">${escapeHtml(statusText(game))}</span>
            <div class="cards-start-time">${escapeHtml(mode === 'live' ? (hasStarted ? (liveState?.status || 'Live game lens active') : 'Pregame betting board ready') : 'Model slate snapshot')}</div>
            <a class="cards-game-link" href="#game-card-${encodeURIComponent(id)}">Open game view</a>
          </div>
        </div>

        <div class="cards-score-ribbon">
          <div class="cards-score-meta">
            <div class="cards-live-line">${escapeHtml(scoreMetaPrimary)}</div>
            <div class="cards-sim-line">${escapeHtml(scoreMetaSecondary)}</div>
            <div class="cards-mini-copy">${escapeHtml(mode === 'live' && hasStarted ? (liveState?.status || 'Live game lens active') : 'Pregame betting board ready')}</div>
          </div>
        </div>

        <div class="cards-market-row">
          ${renderMarketTile('Moneyline', bestMarketPick(game, 'moneyline'), `${game.home_tri} ${fmtAmerican(betting.home_ml)} / ${game.away_tri} ${fmtAmerican(betting.away_ml)}`, `Home win ${fmtPercent(betting.p_home_win, 0)} · Away win ${fmtPercent(betting.p_away_win, 0)}`, id)}
          ${renderMarketTile('Spread', bestMarketPick(game, 'spread'), `${game.home_tri} ${fmtSigned(betting.home_spread)} · ${game.away_tri} ${fmtSigned(-Number(betting.home_spread))}`, `Model margin ${fmtSigned(score.margin_mean, 1)} · Market ${fmtSigned(-Number(market.market_home_spread), 1)}`, id)}
          ${renderMarketTile('Total', bestMarketPick(game, 'total'), `Total ${fmtNumber(betting.total, 1)}`, `Model total ${fmtNumber(score.total_mean, 1)} · Over ${fmtPercent(betting.p_total_over, 0)}`, id)}
          ${renderMarketTile('Props', { detail: propsSummary, probability: null, ev: null, meta: livePropCount ? `${livePropCount} live opportunities` : 'Open prop board', tabTarget: 'props' }, livePropCount ? `${livePropCount} live opps · Open props board` : 'Open props board', livePropCount ? 'Live opportunities sit above official picks and extra playable props.' : 'Official picks and playable props are grouped by team in the Props tab.', id)}
        </div>

        ${renderTabsRail(game, activeTab, id)}

        <section class="cards-panel ${activeTab === 'game' ? 'is-active' : ''}" data-panel-id="game">${renderGamePanel(game)}</section>
        <section class="cards-panel ${activeTab === 'box' ? 'is-active' : ''}" data-panel-id="box">
          ${renderBoxScorePanel(game)}
        </section>
        <section class="cards-panel ${activeTab === 'props' ? 'is-active' : ''}" data-panel-id="props">${renderPropsPanel(game)}</section>
      </article>
    `;
  }

  function createElementFromMarkup(markup) {
    const template = document.createElement('template');
    template.innerHTML = String(markup || '').trim();
    return template.content.firstElementChild;
  }

  function syncMarkupCollection(container, items, renderItem, options = {}) {
    if (!container) {
      return;
    }
    const keyFromItem = options.keyFromItem || ((item) => cardId(item));
    const keyFromElement = options.keyFromElement || ((element) => String(element?.dataset?.cardId || '').trim());
    const existingByKey = new Map(
      Array.from(container.children)
        .map((element) => [keyFromElement(element), element])
        .filter(([key]) => key)
    );
    const fragment = document.createDocumentFragment();
    items.forEach((item) => {
      const nextElement = createElementFromMarkup(renderItem(item));
      if (!nextElement) {
        return;
      }
      const key = String(keyFromItem(item) || '').trim();
      const currentElement = key ? existingByKey.get(key) : null;
      if (key) {
        existingByKey.delete(key);
      }
      if (currentElement && currentElement.isEqualNode(nextElement)) {
        fragment.appendChild(currentElement);
        return;
      }
      fragment.appendChild(nextElement);
    });
    container.replaceChildren(fragment);
  }

  function syncRenderedBoard() {
    const games = sortGamesForDisplay(state.payload?.games);
    const filteredGames = sortGamesForDisplay(games.filter((game) => matchesFilter(game, state.filter)));
    const { scoreboardEl, gridEl } = ensureBoardShell();
    if (!games.length || !filteredGames.length || !state.boardInitialized) {
      renderBoard();
      return;
    }
    syncMarkupCollection(scoreboardEl, filteredGames, renderScoreboardItem, {
      keyFromItem: (game) => cardId(game),
      keyFromElement: (element) => String(element?.dataset?.cardId || '').trim(),
    });
    syncMarkupCollection(gridEl, filteredGames, renderGameCard, {
      keyFromItem: (game) => cardId(game),
      keyFromElement: (element) => String(element?.dataset?.cardId || '').trim(),
    });
  }

  function renderGameCardByTarget(cardTarget) {
    const game = findGameByCardId(cardTarget);
    const cardElement = findGameCardElement(cardTarget);
    if (!game || !cardElement) {
      return;
    }
    const nextElement = createElementFromMarkup(renderGameCard(game));
    if (!nextElement) {
      return;
    }
    if (!cardElement.isEqualNode(nextElement)) {
      cardElement.replaceWith(nextElement);
    }
    const scoreboardElement = findScoreboardItemElement(cardTarget);
    if (scoreboardElement) {
      const nextScoreboardElement = createElementFromMarkup(renderScoreboardItem(game));
      if (nextScoreboardElement && !scoreboardElement.isEqualNode(nextScoreboardElement)) {
        scoreboardElement.replaceWith(nextScoreboardElement);
      }
    }
  }

  function renderBoard() {
    const games = sortGamesForDisplay(state.payload?.games);
    const filteredGames = sortGamesForDisplay(games.filter((game) => matchesFilter(game, state.filter)));
    const { scoreboardEl, gridEl } = ensureBoardShell();
    if (!games.length) {
      scoreboardEl.innerHTML = '<div class="cards-loading-strip">No games on this slate.</div>';
      gridEl.innerHTML = '<div class="cards-empty-state">No game cards available for this date.</div>';
      state.boardInitialized = false;
      return;
    }
    if (!filteredGames.length) {
      scoreboardEl.innerHTML = games.map(renderScoreboardItem).join('');
      gridEl.innerHTML = '<div class="cards-empty-state">No games matched the selected slate filter.</div>';
      state.boardInitialized = false;
      return;
    }
    scoreboardEl.innerHTML = filteredGames.map(renderScoreboardItem).join('');
    gridEl.innerHTML = filteredGames.map(renderGameCard).join('');
  }

  async function loadBoard(options = {}) {
    const silent = Boolean(options?.silent);
    if (!silent && !state.boardInitialized) {
      setLoading();
    }
    try {
      const payload = await fetchApiJson(
        `/api/cards?date=${encodeURIComponent(state.date)}`,
        'Failed to load game cards.',
        { retries: silent ? 2 : 1 }
      );
      const previousDate = String(state.payload?.date || state.date || '');
      const previousGames = safeArray(state.payload?.games);
      const nextDate = String(payload?.date || state.date || '');
      const nextGames = safeArray(payload?.games);
      const slateUnchanged = previousDate === nextDate && sameSlate(nextGames, previousGames);
      state.payload = {
        ...payload,
        games: nextGames,
      };
      if (slateUnchanged) {
        reapplyCachedSimDetails();
      } else {
        state.simDetailCache.clear();
        state.simDetailLoading.clear();
      }
      const resolvedDate = payload.date || state.date;
      state.liveDataLoading = mode === 'live';
      updateDateControls();
      renderHeaderMeta();
      renderSourceMeta();
      renderFilters();
      if (payload.lookahead_applied && payload.date && payload.requested_date && payload.date !== payload.requested_date) {
        showNote(`No slate for ${payload.requested_date}. Showing next available cards from ${payload.date}.`, 'warning');
      } else {
        showNote('', 'info');
      }
      if (!silent || !slateUnchanged || !state.boardInitialized) {
        renderBoard();
      }

      void loadPropsStrip(resolvedDate, { silent, games: payload.games || [] });
      if (mode === 'live') {
        void loadLiveGameLens(resolvedDate, payload.games || [], { silent }).then(() => {
          if ((state.payload?.date || state.date) === resolvedDate) {
            renderSourceMeta();
            if (silent && slateUnchanged && state.boardInitialized) {
              syncRenderedBoard();
            } else {
              renderBoard();
            }
          }
        });
      } else {
        state.liveDataLoading = false;
      }
    } catch (error) {
      if (silent && state.payload && state.boardInitialized) {
        state.liveDataLoading = false;
        showNote(error?.message || 'Failed to refresh slate.', 'warning');
        return;
      }
      state.payload = null;
      state.propsStripPayload = null;
      clearPropsStrip();
      state.boardInitialized = false;
      state.liveDataLoading = false;
      if (headerMeta) {
        headerMeta.textContent = 'Failed to load slate.';
      }
      if (sourceMeta) {
        sourceMeta.innerHTML = `<span>${escapeHtml(error?.message || 'Failed to load slate metadata.')}</span>`;
      }
      scoreboardRoot.innerHTML = '<div class="cards-loading-strip">Failed to load scoreboard.</div>';
      gridRoot.innerHTML = `<div class="cards-empty-state">${escapeHtml(error?.message || 'Failed to load slate.')}</div>`;
      showNote(error?.message || 'Failed to load slate.', 'warning');
    }
  }

  function syncFromControls() {
    state.date = datePicker?.value || getLocalDateISO();
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
      loadBoard({ silent: true });
    }, pollIntervalMs);
  }

  boardShell.addEventListener('click', (event) => {
    const openCardTab = event.target.closest('[data-open-card-tab]');
    if (openCardTab) {
      const cardTarget = openCardTab.getAttribute('data-card-target') || '';
      const tabKey = openCardTab.getAttribute('data-open-card-tab') || 'game';
      state.activeTabs.set(cardTarget, tabKey);
      renderBoard();
      if (tabKey === 'box' || tabKey === 'props') {
        void ensureSimDetail(cardTarget);
      }
      const card = findGameCardElement(cardTarget);
      if (card) {
        card.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
      return;
    }

    const tabButton = event.target.closest('[data-card-tab]');
    if (tabButton) {
      const cardTarget = tabButton.getAttribute('data-card-target') || '';
      const tabKey = tabButton.getAttribute('data-card-tab') || 'game';
      state.activeTabs.set(cardTarget, tabKey);
      renderBoard();
      if (tabKey === 'box' || tabKey === 'props') {
        void ensureSimDetail(cardTarget);
      }
      return;
    }

    const jumpButton = event.target.closest('[data-jump-card]');
    if (jumpButton) {
      const cardTarget = jumpButton.getAttribute('data-jump-card') || '';
      const card = findGameCardElement(cardTarget);
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
      if (tabKey === 'box' || tabKey === 'props') {
        void ensureSimDetail(cardTarget);
      }
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

    const propBucket = event.target.closest('[data-prop-filter-bucket]');
    if (propBucket) {
      const cardTarget = propBucket.getAttribute('data-card-target') || '';
      const detail = state.propDetails.get(cardTarget);
      if (detail) {
        const nextBucket = propBucket.getAttribute('data-prop-filter-bucket') || 'all';
        detail.bucket = detail.bucket === nextBucket ? 'all' : nextBucket;
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
        state.activeTabs.set(cardTarget, 'props');
        renderBoard();
        void ensureSimDetail(cardTarget);
      }
    }
  });

  propsStripEl?.addEventListener('click', (event) => {
    const sortButton = event.target.closest('[data-strip-sort]');
    if (sortButton) {
      state.propsStripSort = sortButton.getAttribute('data-strip-sort') || 'best';
      state.propsStripVisibleCount = Number(state.propsStripDefaultCount) || 18;
      renderPropsStrip();
      return;
    }

    const marketButton = event.target.closest('[data-strip-market]');
    if (marketButton) {
      state.propsStripFilters.market = marketButton.getAttribute('data-strip-market') || 'all';
      state.propsStripVisibleCount = Number(state.propsStripDefaultCount) || 18;
      renderPropsStrip();
      return;
    }

    const sideButton = event.target.closest('[data-strip-side]');
    if (sideButton) {
      state.propsStripFilters.side = sideButton.getAttribute('data-strip-side') || 'all';
      state.propsStripVisibleCount = Number(state.propsStripDefaultCount) || 18;
      renderPropsStrip();
      return;
    }

    const gameButton = event.target.closest('[data-strip-game]');
    if (gameButton) {
      state.propsStripFilters.game = gameButton.getAttribute('data-strip-game') || 'all';
      state.propsStripVisibleCount = Number(state.propsStripDefaultCount) || 18;
      renderPropsStrip();
      return;
    }

    const showMoreButton = event.target.closest('[data-strip-show-more]');
    if (showMoreButton) {
      const step = Number(state.propsStripDefaultCount) || 18;
      state.propsStripVisibleCount = Math.max(step, Number(state.propsStripVisibleCount) || step) + step;
      renderPropsStrip();
      return;
    }

    const jumpButton = event.target.closest('[data-jump-card]');
    if (jumpButton) {
      const cardTarget = jumpButton.getAttribute('data-jump-card') || '';
      const card = findGameCardElement(cardTarget);
      if (card) {
        card.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    }
  });

  filtersEl?.addEventListener('click', (event) => {
    const button = event.target.closest('[data-filter-key]');
    if (!button) {
      return;
    }
    applySlateFilter(button.getAttribute('data-filter-key') || 'all');
  });

  applyBtn?.addEventListener('click', applyAndLoad);

  const initialDate = new URLSearchParams(window.location.search).get('date') || getLocalDateISO();
  state.date = initialDate;
  if (datePicker) {
    datePicker.value = initialDate;
  }
  syncFromControls();

  setupPolling();
  loadBoard({ silent: false });
})();