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
  const propsStripEl = document.getElementById('cardsPropsStrip');
  const note = document.getElementById('note');
  const pollIntervalMs = 30000;

  const state = {
    activeTabs: new Map(),
    boardInitialized: false,
    date: '',
    filter: 'all',
    liveGameLens: new Map(),
    liveStates: new Map(),
    payload: null,
    pollHandle: null,
    propDetails: new Map(),
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

  function matchupKey(awayTri, homeTri) {
    const away = String(awayTri || '').trim().toUpperCase();
    const home = String(homeTri || '').trim().toUpperCase();
    return away && home ? `${away}@${home}` : '';
  }

  function gameMatchupKey(game) {
    return matchupKey(game?.away_tri, game?.home_tri);
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

  function getLiveState(game) {
    return state.liveStates.get(gameMatchupKey(game)) || null;
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
    const pregameTotal = Number(score.total_mean);
    const pregameMargin = Number(score.margin_mean);
    const pregameHomeWin = Number(betting.p_home_win);
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
      totalSignal = buildSignal('total', 'G', klass, side, edge, lineTotal, projection, `Total ${fmtInteger(currentTotal)}`);
      totalSignal.score = signalScore(Math.abs(edge), thresholds.total.bet);
    }

    let halfSignal = null;
    if (liveState.in_progress && Number.isFinite(currentPeriod) && currentPeriod <= 2) {
      const halfLine = Number(periodTotals?.h1);
      const halfActual = halfTotalSoFar(liveState, pbpStats);
      const halfSim = simPeriodMean(game, 'h1');
      const halfMinutesElapsed = Number.isFinite(elapsedMinutes) ? elapsedMinutes : null;
      const halfMinutesRemaining = Number.isFinite(halfMinutesElapsed) ? Math.max(0, 24 - Math.min(24, halfMinutesElapsed)) : null;
      if (Number.isFinite(halfLine) && Number.isFinite(halfActual) && Number.isFinite(halfMinutesElapsed)) {
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
      if (Number.isFinite(quarterLine) && Number.isFinite(quarterActual) && Number.isFinite(quarterMinutesElapsed)) {
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
      if (Number.isFinite(actualHalfMargin) && Number.isFinite(halfMinutesElapsed)) {
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
        const halfMinutesRemaining = Math.max(0, 24 - halfMinutesElapsed);
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
      if (Number.isFinite(actualQuarterMargin) && Number.isFinite(quarterMinutesElapsed)) {
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
        const quarterMinutesRemaining = Math.max(0, 12 - quarterMinutesElapsed);
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
      atsSignal = buildSignal('ats', 'ATS', klass, side, edge, line, projection, `Margin ${fmtSigned(projectedMargin, 1)}`);
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
        mlSignal = buildSignal('ml', 'ML', klass, side, edge, line, projection, `Model ${fmtPercent(projection, 0)}`);
        mlSignal.score = signalScore(Math.abs(edge), thresholds.ml.bet);
      }
    }

    const signals = [quarterSignal, halfSignal, totalSignal, quarterAtsSignal, halfAtsSignal, atsSignal, quarterMlSignal, halfMlSignal, mlSignal].filter(Boolean);
    const topSignals = signals
      .filter((signal) => signal.klass === 'BET' || signal.klass === 'WATCH')
      .sort((left, right) => (signalPriority(right) - signalPriority(left)) || ((Number(right.score) || 0) - (Number(left.score) || 0)) || (Math.abs(Number(right.edge) || 0) - Math.abs(Number(left.edge) || 0)));

    const overallClass = topSignals[0]?.klass || 'NONE';
    const scoreLabel = Number.isFinite(awayPts) && Number.isFinite(homePts)
      ? `${game?.away_tri || 'AWY'} ${fmtInteger(awayPts)} - ${fmtInteger(homePts)} ${game?.home_tri || 'HME'}`
      : `${game?.away_tri || 'AWY'} at ${game?.home_tri || 'HME'}`;
    const statusLabel = liveState.final
      ? 'Final'
      : (liveState.in_progress ? String(liveState.status || `Q${liveState.period || ''} ${liveState.clock || ''}`).trim() : String(liveState.status || 'Pregame'));

    return {
      statusLabel,
      scoreLabel,
      currentTotal,
      currentMargin,
      elapsedMinutes,
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
      topSignals,
      overallClass,
    };
  }

  async function loadLiveGameLens(dateValue, games) {
    state.liveGameLens = new Map();
    state.liveStates = new Map();
    if (mode !== 'live') {
      return;
    }
    try {
      const liveStateResponse = await fetch(`/api/live_state?date=${encodeURIComponent(dateValue)}`, { cache: 'no-store' });
      const liveStatePayload = await liveStateResponse.json();
      if (!liveStateResponse.ok) {
        throw new Error(liveStatePayload?.error || 'Failed to load live state.');
      }

      const payloadGames = safeArray(games);
      const liveStateMap = new Map();
      safeArray(liveStatePayload?.games).forEach((item) => {
        const key = matchupKey(item?.away, item?.home);
        if (key) {
          liveStateMap.set(key, item);
          state.liveStates.set(key, item);
        }
      });

      const matchedStates = payloadGames
        .map((game) => ({ game, liveState: liveStateMap.get(gameMatchupKey(game)) || null }))
        .filter((entry) => entry.liveState);
      const eventIds = matchedStates
        .map((entry) => String(entry.liveState?.event_id || '').trim())
        .filter(Boolean);

      let liveLinesMap = new Map();
      let pbpMap = new Map();
      let tuning = null;

      if (eventIds.length) {
        const [linesResponse, pbpResponse, tuningResponse] = await Promise.all([
          fetch(`/api/live_lines?date=${encodeURIComponent(dateValue)}&event_ids=${encodeURIComponent(eventIds.join(','))}&include_period_totals=1`, { cache: 'no-store' }),
          fetch(`/api/live_pbp_stats?date=${encodeURIComponent(dateValue)}&event_ids=${encodeURIComponent(eventIds.join(','))}`, { cache: 'no-store' }),
          fetch('/api/live_lens_tuning?ttl=300', { cache: 'no-store' }),
        ]);
        const linesPayload = await linesResponse.json();
        const pbpPayload = await pbpResponse.json();
        tuning = tuningResponse.ok ? await tuningResponse.json() : null;

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
      }

      matchedStates.forEach(({ game, liveState }) => {
        const eventId = String(liveState?.event_id || '').trim();
        const liveLines = eventId ? liveLinesMap.get(eventId) : null;
        const pbpStats = eventId ? pbpMap.get(eventId) : null;
        const lens = computeLiveGameLens(game, liveState, liveLines, pbpStats, tuning);
        const key = gameMatchupKey(game);
        if (key && lens) {
          state.liveGameLens.set(key, lens);
        }
      });
    } catch (_error) {
      state.liveGameLens = new Map();
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
    root.classList.add('parity-root');
    root.innerHTML = '<div class="cards-empty">Loading slate...</div>';
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
      const liveLine = Number(item?.line);
      const pieces = [];
      if (lineSource) {
        pieces.push(lineSource === 'oddsapi' ? 'Live OddsAPI' : titleCase(lineSource));
      }
      if (Number.isFinite(simValue) && Number.isFinite(liveLine)) {
        pieces.push(`Sim ${fmtNumber(simValue, 1)} vs ${fmtNumber(liveLine, 1)}`);
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
    const line = Number(item?.line);
    const pieces = [];
    if (Number.isFinite(actual)) {
      pieces.push(`Actual ${fmtNumber(actual, 1)}`);
    }
    if (Number.isFinite(paceProj)) {
      pieces.push(`Proj ${fmtNumber(paceProj, 1)}`);
    }
    if (Number.isFinite(simValue)) {
      pieces.push(`Sim ${fmtNumber(simValue, 1)}`);
    }
    if (Number.isFinite(line) && Number.isFinite(paceProj)) {
      pieces.push(`Edge ${fmtSigned(paceProj - line, 1)}`);
    }
    return pieces.join(' · ');
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
    const simEdge = Number(item?.sim_vs_line);
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
    const simEdge = Number(item?.sim_vs_line);
    if (Number.isFinite(simEdge)) {
      return Math.abs(simEdge);
    }
    const evPct = Number(item?.ev_pct);
    return Number.isFinite(evPct) ? Math.abs(evPct) / 100 : 0;
  }

  function livePropSortScore(item) {
    const paceProj = Number(item?.pace_proj);
    const simValue = Number(item?.sim_mu);
    const line = Number(item?.line);
    const paceEdge = Number(item?.pace_vs_line);
    const simEdge = Number(item?.sim_vs_line);
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
        const leftEdge = Number(left?.pace_vs_line);
        const rightEdge = Number(right?.pace_vs_line);
        return (Math.abs(rightEdge) - Math.abs(leftEdge)) || (projRight - projLeft);
      }
      if (sortKey === 'win') {
        const winLeft = Number(left?.probability);
        const winRight = Number(right?.probability);
        return winRight - winLeft;
      }
      if (sortKey === 'live') {
        const edgeLeft = Number(left?.pace_vs_line ?? ((Number(left?.pace_proj) - Number(left?.line)) || 0));
        const edgeRight = Number(right?.pace_vs_line ?? ((Number(right?.pace_proj) - Number(right?.line)) || 0));
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

  function renderPropsStripItem(item) {
    const photo = String(item?.photo || item?.player_photo || '').trim();
    const logo = logoForTri(item?.team_tri);
    const opponentTri = String(item?.opponent_tri || '').trim().toUpperCase();
    const market = marketLabel(item?.market);
    const side = livePropPrimarySide(item) || String(item?.side || '').trim().toUpperCase();
    const line = Number(item?.line);
    const price = Number(item?.price);
    const evPct = Number(item?.ev_pct);
    const winProb = Number(item?.probability ?? item?.prob_calib);
    const cardTarget = stripCardTarget(item);
    const actionLabel = mode === 'live'
      ? String(item?.klass || '').trim().toUpperCase()
      : String(item?.tier || '').trim().toUpperCase();
    const actionClass = actionLabel === 'BET'
      ? 'cards-chip--accent'
      : (actionLabel === 'WATCH' || actionLabel === 'MEDIUM' ? 'cards-chip--warm' : '');
    const liveProjection = liveProjectionSummary(item);
    return `
      <article class="cards-props-strip-card">
        <div class="cards-props-strip-card__top">
          <div class="cards-props-strip-card__context">${escapeHtml(String(item?.away_tri || '--'))} @ ${escapeHtml(String(item?.home_tri || '--'))}</div>
          <div class="cards-props-strip-card__status">${escapeHtml(stripStatusText(item))}</div>
        </div>
        <div class="cards-props-strip-card__body">
          <div class="cards-props-strip-card__identity">
            <div class="cards-props-strip-card__media">
              ${photo ? `<img class="cards-props-strip-card__photo" src="${escapeHtml(photo)}" alt="${escapeHtml(String(item?.player || 'Player'))}" />` : `<div class="cards-props-strip-card__photo is-fallback">${escapeHtml(String(item?.team_tri || '?'))}</div>`}
              ${logo ? `<img class="cards-props-strip-card__logo" src="${escapeHtml(logo)}" alt="${escapeHtml(String(item?.team_tri || ''))} logo" />` : ''}
            </div>
            <div class="cards-props-strip-card__copy">
              <div class="cards-props-strip-card__name">${escapeHtml(String(item?.player || 'Unknown player'))}</div>
              <div class="cards-props-strip-card__matchup">${escapeHtml(String(item?.team_tri || '--'))}${opponentTri ? ` vs ${escapeHtml(opponentTri)}` : ''}</div>
            </div>
          </div>
          <div class="cards-props-strip-card__play">${escapeHtml(market)} ${escapeHtml(side)} ${Number.isFinite(line) ? fmtNumber(line, 1) : '--'}</div>
          ${liveProjection ? `<div class="cards-props-strip-card__projection">${escapeHtml(liveProjection)}</div>` : ''}
          <div class="cards-props-strip-card__sub">${escapeHtml(stripSecondaryText(item))}</div>
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
  }

  function renderPropsStrip() {
    if (!propsStripEl) {
      return;
    }
    const payload = state.propsStripPayload;
    const sortedItems = sortedPropsStripItems(safeArray(payload?.items));
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
    propsStripEl.classList.remove('hidden');
    propsStripEl.innerHTML = `
      <div class="cards-props-strip-headline">
        <div>
          <h2>${escapeHtml(String(payload?.title || (mode === 'live' ? 'Live player props' : 'Pregame prop movement')))}</h2>
          <p>${escapeHtml(String(payload?.subtitle || ''))}</p>
          ${sortOptions.length ? `<div class="cards-props-strip-sort">${sortOptions.map((option) => `<button type="button" class="cards-filter-pill ${state.propsStripSort === option.key ? 'is-active' : ''}" data-strip-sort="${escapeHtml(option.key)}">${escapeHtml(option.label)}</button>`).join('')}</div>` : ''}
          ${mode === 'live' ? `
            <div class="cards-props-strip-filters">
              ${renderPropsStripFilterGroup('Prop', [{ key: 'all', label: 'All props' }, ...filterOptions.markets.map((market) => ({ key: market, label: marketLabel(market) }))], activeFilters.market || 'all', 'data-strip-market')}
              ${renderPropsStripFilterGroup('Side', [{ key: 'all', label: 'All sides' }, { key: 'OVER', label: 'Over' }, { key: 'UNDER', label: 'Under' }], activeFilters.side || 'all', 'data-strip-side')}
              ${renderPropsStripFilterGroup('Game', [{ key: 'all', label: 'All games' }, ...filterOptions.games.map((gameKey) => ({ key: gameKey, label: gameKey.replace('@', ' @ ') }))], activeFilters.game || 'all', 'data-strip-game')}
            </div>
          ` : ''}
        </div>
        <div class="cards-strip-pills">
          <span class="cards-source-meta-pill ${mode === 'live' ? 'is-live' : 'is-soft'}">${escapeHtml(String(visibleItems.length))} shown</span>
          ${mode === 'live' ? `<span class="cards-source-meta-pill is-soft">${escapeHtml(String(filteredItems.length))} match</span>` : ''}
          ${mode === 'live' ? `<span class="cards-source-meta-pill is-soft">${escapeHtml(String(sortedItems.length))} in pool</span>` : ''}
          <span class="cards-source-meta-pill">${escapeHtml(String(payload?.date || state.date || ''))}</span>
        </div>
      </div>
      <div class="cards-props-strip-grid">${visibleItems.length ? visibleItems.map(renderPropsStripItem).join('') : `<div class="cards-props-strip-empty">No live props matched the current filters.</div>`}</div>
      ${canShowMore ? `<div class="cards-props-strip-actions"><button type="button" class="cards-filter-pill" data-strip-show-more="1">Show more</button></div>` : ''}
    `;
  }

  function transformLiveStripPayload(payload, dateValue) {
    const items = [];
    safeArray(payload?.games).forEach((game) => {
      const status = game?.status || {};
      const gameItems = safeArray(game?.rows)
        .filter((row) => row && row.player && row.team_tri)
        .filter((row) => row.line_source && row.line_source !== 'model')
        .filter((row) => row.pace_proj != null || row.sim_mu != null || row.ev_side || row.lean)
        .map((row) => ({
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
          sim_vs_line: row?.sim_vs_line,
          lean: row?.lean,
          bettable_score: row?.bettable_score,
          line_live: row?.line_live,
          line_pregame: row?.line_pregame,
        }));
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
    if (!propsStripEl) {
      return;
    }
    if (!silent || !state.propsStripPayload) {
      setPropsStripLoading();
    }
    try {
      if (mode === 'live') {
        const response = await fetch(`/api/live_player_lens?date=${encodeURIComponent(dateValue)}`, { cache: 'no-store' });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload?.error || 'Failed to load live player props.');
        }
        state.propsStripPayload = transformLiveStripPayload(payload, dateValue);
        state.propsStripVisibleCount = Number(state.propsStripDefaultCount) || 18;
      } else {
        const response = await fetch(`/api/cards/props-strip?date=${encodeURIComponent(dateValue)}`, { cache: 'no-store' });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload?.error || 'Failed to load prop strip.');
        }
        state.propsStripPayload = payload;
      }
      renderPropsStrip();
    } catch (_error) {
      state.propsStripPayload = null;
      clearPropsStrip();
    }
  }

  function ensureBoardShell() {
    let scoreboardEl = root.querySelector('.cards-scoreboard');
    let gridEl = root.querySelector('.cards-grid');
    if (!scoreboardEl || !gridEl) {
      root.classList.add('parity-root');
      root.innerHTML = `
        <section class="cards-scoreboard"></section>
        <section class="cards-grid"></section>
      `;
      scoreboardEl = root.querySelector('.cards-scoreboard');
      gridEl = root.querySelector('.cards-grid');
    }
    state.boardInitialized = true;
    return { scoreboardEl, gridEl };
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
    const liveLens = getLiveLens(game);
    const liveState = getLiveState(game);
    const compactHeaderText = mode === 'live'
      ? statusText(game)
      : fmtTime(game?.odds?.commence_time);
    const awayScore = mode === 'live' && Number.isFinite(Number(liveState?.away_pts)) ? Number(liveState.away_pts) : score.away_mean;
    const homeScore = mode === 'live' && Number.isFinite(Number(liveState?.home_pts)) ? Number(liveState.home_pts) : score.home_mean;
    const stripSignals = liveLens?.topSignals?.slice(0, 2) || [];
    const stripMeta = mode === 'live'
      ? [
        liveLens?.signals?.total?.detail,
        liveLens?.signals?.ats?.detail,
      ].filter(Boolean)[0] || 'Waiting for a live edge.'
      : marketCountSummary(game);
    return `
      <button class="cards-strip-card ${liveLens?.overallClass === 'BET' ? 'cards-live-lens--bet' : (liveLens?.overallClass === 'WATCH' ? 'cards-live-lens--watch' : '')}" type="button" data-jump-card="${escapeHtml(id)}">
        <div class="cards-strip-head">
          <span>${escapeHtml(compactHeaderText)}</span>
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
        ${stripSignals.length ? `<div class="cards-strip-pills">${stripSignals.map(renderLiveSignalChip).join('')}</div>` : ''}
        <div class="cards-strip-meta">${escapeHtml(stripMeta)}</div>
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
    const liveLens = getLiveLens(game);
    const liveState = getLiveState(game);
    const liveOverview = mode === 'live'
      ? (() => {
        const liveTiles = [
          liveLens?.signals?.quarter_total,
          liveLens?.signals?.half_total,
          liveLens?.signals?.total,
          liveLens?.signals?.quarter_ats,
          liveLens?.signals?.half_ats,
          liveLens?.signals?.ats,
          liveLens?.signals?.quarter_ml,
          liveLens?.signals?.half_ml,
          liveLens?.signals?.ml,
        ].filter(Boolean);
        const liveChips = [
          liveLens?.signals?.quarter_total,
          liveLens?.signals?.half_total,
          liveLens?.signals?.total,
          liveLens?.signals?.quarter_ats,
          liveLens?.signals?.half_ats,
          liveLens?.signals?.ats,
          liveLens?.signals?.quarter_ml,
          liveLens?.signals?.half_ml,
          liveLens?.signals?.ml,
        ].filter(Boolean);
        return `
        <div class="cards-live-lens-grid">
          ${liveTiles.map(renderLiveSignalTile).join('')}
        </div>
        <div class="cards-source-meta">
          ${liveChips.map(renderLiveSignalChip).join('')}
          ${liveLens?.scoreLabel ? `<span class="cards-source-meta-pill is-soft">${escapeHtml(liveLens.scoreLabel)}</span>` : ''}
          ${liveState?.status ? `<span class="cards-source-meta-pill is-live">${escapeHtml(String(liveState.status))}</span>` : ''}
        </div>
      `;
      })()
      : `${probabilityRows(game)}<div class="cards-mini-metrics">${miniMetrics(game)}</div>`;
    return `
      <div class="cards-overview-grid">
        <div class="cards-panel-card">
          <div class="cards-box-head">
            <div class="cards-table-title"><strong>${escapeHtml(mode === 'live' ? 'Live game lens' : 'Game lens')}</strong></div>
            <span class="cards-overview-badge ${statusClass(game)}">${escapeHtml(mode === 'live' ? 'Live model' : 'Pregame model')}</span>
          </div>
          ${liveOverview}
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
    const liveState = getLiveState(game);
    const liveLens = getLiveLens(game);
    const awayScore = mode === 'live' && Number.isFinite(Number(liveState?.away_pts)) ? Number(liveState.away_pts) : score.away_mean;
    const homeScore = mode === 'live' && Number.isFinite(Number(liveState?.home_pts)) ? Number(liveState.home_pts) : score.home_mean;
    const scoreMetaPrimary = mode === 'live'
      ? (liveLens?.topSignals?.map((signal) => signal.compactLabel).join(' · ') || 'Monitoring live game lens for fresh edges.')
      : 'Pregame matchup card.';
    const scoreMetaSecondary = mode === 'live'
      ? (liveLens?.signals?.total?.detail || `Model total ${fmtNumber(score.total_mean, 1)} · margin ${fmtSigned(score.margin_mean, 1)}`)
      : `Model total ${fmtNumber(score.total_mean, 1)} · margin ${fmtSigned(score.margin_mean, 1)}`;
    const awayLogo = logoForTri(game.away_tri);
    const homeLogo = logoForTri(game.home_tri);
    return `
      <article class="cards-game-card ${liveLens?.overallClass === 'BET' ? 'cards-live-lens--bet' : (liveLens?.overallClass === 'WATCH' ? 'cards-live-lens--watch' : '')}" data-card-id="${escapeHtml(id)}" id="game-card-${escapeHtml(id)}">
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
            <div class="cards-score-number">${fmtNumber(awayScore, mode === 'live' ? 0 : 1)}</div>
            <strong>${escapeHtml(game.away_tri)}</strong>
          </div>
          <div class="cards-score-divider">at</div>
          <div class="cards-score-side">
            <div class="cards-score-label">Home</div>
            <div class="cards-score-number">${fmtNumber(homeScore, mode === 'live' ? 0 : 1)}</div>
            <strong>${escapeHtml(game.home_tri)}</strong>
          </div>
          <div class="cards-score-meta">
            <div class="cards-live-line">${escapeHtml(scoreMetaPrimary)}</div>
            <div class="cards-sim-line">${escapeHtml(scoreMetaSecondary)}</div>
            ${mode === 'live' && liveLens?.topSignals?.length ? `<div class="cards-strip-pills">${liveLens.topSignals.slice(0, 3).map(renderLiveSignalChip).join('')}</div>` : ''}
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
    if (!games.length) {
      root.innerHTML = '<div class="cards-empty">No game cards available for this date.</div>';
      state.boardInitialized = false;
      return;
    }
    if (!filteredGames.length) {
      root.innerHTML = '<div class="cards-empty">No games matched the selected slate filter.</div>';
      state.boardInitialized = false;
      return;
    }
    const { scoreboardEl, gridEl } = ensureBoardShell();
    if (scoreboardEl) {
      scoreboardEl.innerHTML = filteredGames.map(renderScoreboardItem).join('');
    }
    if (gridEl) {
      gridEl.innerHTML = filteredGames.map(renderGameCard).join('');
    }
  }

  async function loadBoard(options = {}) {
    const silent = Boolean(options?.silent);
    if (!silent && !state.boardInitialized) {
      setLoading();
    }
    try {
      const response = await fetch(`/api/cards?date=${encodeURIComponent(state.date)}`, { cache: 'no-store' });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload?.error || 'Failed to load game cards.');
      }
      state.payload = payload;
      const resolvedDate = payload.date || state.date;
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

      void loadPropsStrip(resolvedDate, { silent });
      if (mode === 'live') {
        void loadLiveGameLens(resolvedDate, payload.games || []).then(() => {
          if ((state.payload?.date || state.date) === resolvedDate) {
            renderSourceMeta();
            renderBoard();
          }
        });
      }
    } catch (error) {
      state.payload = null;
      state.propsStripPayload = null;
      clearPropsStrip();
      state.boardInitialized = false;
      if (headerMeta) {
        headerMeta.textContent = 'Failed to load slate.';
      }
      root.innerHTML = `<div class="cards-empty">${escapeHtml(error?.message || 'Failed to load slate.')}</div>`;
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
      const card = root.querySelector(`[data-card-id="${CSS.escape(cardTarget)}"]`);
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

  const initialDate = new URLSearchParams(window.location.search).get('date') || getLocalDateISO();
  state.date = initialDate;
  if (datePicker) {
    datePicker.value = initialDate;
  }
  syncFromControls();
  setupPolling();
  loadBoard({ silent: false });
})();