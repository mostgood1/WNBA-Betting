(function () {
  function parseSeasonFromPath() {
    const match = window.location.pathname.match(/\/season\/(\d+)\/betting-card\/?$/);
    return match ? Number(match[1]) : Number(new Date().getFullYear());
  }

  function getLocalDateISO() {
    const now = new Date();
    const offsetMs = now.getTimezoneOffset() * 60000;
    return new Date(now.getTime() - offsetMs).toISOString().slice(0, 10);
  }

  const url = new URL(window.location.href);
  const state = {
    season: parseSeasonFromPath(),
    selectedDate: String(url.searchParams.get('date') || getLocalDateISO()),
    profile: String(url.searchParams.get('profile') || 'retuned'),
    monthFilter: 'all',
    dayPicksMode: 'official',
    manifest: null,
    day: null,
  };

  const MARKET_LABELS = {
    totals: 'Totals',
    ml: 'Moneyline',
    spreads: 'Spread',
    player_props: 'Player props',
  };

  const PROP_LABELS = {
    pts: 'Points',
    reb: 'Rebounds',
    ast: 'Assists',
    threes: '3PM',
    pra: 'PRA',
    pr: 'PR',
    pa: 'PA',
    ra: 'RA',
    stl: 'Steals',
    blk: 'Blocks',
    tov: 'Turnovers',
  };

  const PROFILE_LABELS = {
    retuned: 'Retuned profile',
  };

  const root = {
    title: document.getElementById('bettingCardPageTitle'),
    seasonPill: document.getElementById('bettingCardSeasonPill'),
    headerMeta: document.getElementById('bettingCardHeaderMeta'),
    statusPill: document.getElementById('bettingCardStatusPill'),
    profiles: document.getElementById('bettingCardProfiles'),
    summary: document.getElementById('bettingCardSummary'),
    months: document.getElementById('bettingCardMonths'),
    days: document.getElementById('bettingCardDays'),
    dayTitle: document.getElementById('bettingCardDayTitle'),
    dayMeta: document.getElementById('bettingCardDayMeta'),
    dayActions: document.getElementById('bettingCardDayActions'),
    dayMetrics: document.getElementById('bettingCardDayMetrics'),
    dayPicks: document.getElementById('bettingCardDayPicks'),
    games: document.getElementById('bettingCardGames'),
    dailyLink: document.getElementById('bettingCardDailyLink'),
    liveAuditLink: document.getElementById('bettingCardLiveAuditLink'),
  };

  function escapeHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function toNumber(value) {
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }

  function formatNumber(value, digits) {
    const num = toNumber(value);
    if (num == null) return '-';
    if (Number.isInteger(num) && (!digits || digits <= 0)) return String(num);
    return num.toFixed(digits == null ? 2 : digits);
  }

  function formatPercent(value, digits) {
    const num = toNumber(value);
    if (num == null) return '-';
    return `${(num * 100).toFixed(digits == null ? 1 : digits)}%`;
  }

  function formatCurrency(value, digits) {
    const num = toNumber(value);
    if (num == null) return '-';
    return new Intl.NumberFormat(undefined, {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: digits == null ? 0 : digits,
      maximumFractionDigits: digits == null ? 0 : digits,
    }).format(num);
  }

  function formatUnits(value, digits) {
    const num = toNumber(value);
    if (num == null) return '-';
    const fixed = num.toFixed(digits == null ? 2 : digits);
    return num > 0 ? `+${fixed}u` : `${fixed}u`;
  }

  function formatSignedPoints(value, digits) {
    const num = toNumber(value);
    if (num == null) return '-';
    const fixed = num.toFixed(digits == null ? 1 : digits);
    return num > 0 ? `+${fixed} pts` : `${fixed} pts`;
  }

  function formatSignedPercentPoints(value, digits) {
    const num = toNumber(value);
    if (num == null) return '-';
    const scaled = num * 100;
    const fixed = scaled.toFixed(digits == null ? 1 : digits);
    return scaled > 0 ? `+${fixed}%` : `${fixed}%`;
  }

  function formatEdge(row, digits) {
    const bucket = String(row?.market || '').toLowerCase();
    const rawEdge = toNumber(row?.edge);
    if (bucket === 'spreads' || bucket === 'totals') {
      return formatSignedPoints(rawEdge, digits == null ? 1 : digits);
    }

    const probEdge = toNumber(insightValue(row, 'model', 'prob_edge'));
    if (probEdge != null) {
      return formatSignedPercentPoints(probEdge, digits == null ? 1 : digits);
    }

    if (rawEdge != null && Math.abs(rawEdge) <= 1) {
      return formatSignedPercentPoints(rawEdge, digits == null ? 1 : digits);
    }

    return formatSignedPoints(rawEdge, digits == null ? 1 : digits);
  }

  function formatLine(value) {
    const num = toNumber(value);
    if (num == null) return '-';
    return Number.isInteger(num) ? String(num) : num.toFixed(1);
  }

  function formatOdds(value) {
    const num = toNumber(value);
    if (num == null) return '-';
    return num > 0 ? `+${Math.round(num)}` : `${Math.round(num)}`;
  }

  function formatSignedNumber(value, digits) {
    const num = toNumber(value);
    if (num == null) return '-';
    const fixed = num.toFixed(digits == null ? 1 : digits);
    return num > 0 ? `+${fixed}` : fixed;
  }

  function toPercentNumber(value) {
    const num = toNumber(value);
    if (num == null) return null;
    return num * 100;
  }

  function formatDateLong(dateStr) {
    const dt = new Date(`${dateStr}T12:00:00`);
    if (Number.isNaN(dt.getTime())) return dateStr;
    return dt.toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' });
  }

  function formatDateRail(dateStr) {
    const dt = new Date(`${dateStr}T12:00:00`);
    if (Number.isNaN(dt.getTime())) {
      return { dow: 'Date', monthDay: dateStr, month: '' };
    }
    return {
      dow: dt.toLocaleDateString(undefined, { weekday: 'short' }),
      monthDay: dt.toLocaleDateString(undefined, { month: 'short', day: 'numeric' }),
      month: dt.toLocaleDateString(undefined, { month: 'short', year: 'numeric' }),
    };
  }

  function monthLabel(monthKey) {
    const dt = new Date(`${monthKey}-01T12:00:00`);
    if (Number.isNaN(dt.getTime())) return monthKey;
    return dt.toLocaleDateString(undefined, { month: 'short', year: 'numeric' });
  }

  function profileLabel(profileKey) {
    return PROFILE_LABELS[profileKey] || profileKey;
  }

  function marketLabel(row) {
    const bucket = String(row?.market || '').toLowerCase();
    if (bucket !== 'player_props') return MARKET_LABELS[bucket] || 'Pick';
    const marketKey = String(row?.market_key || '').toLowerCase();
    return PROP_LABELS[marketKey] || MARKET_LABELS.player_props;
  }

  function formatPlayableSleeve(value) {
    const raw = String(value || '').trim().toLowerCase();
    if (!raw) return '';
    const [marketKey, sideKey] = raw.split(':');
    const market = PROP_LABELS[String(marketKey || '').toLowerCase()] || String(marketKey || '').toUpperCase();
    const side = sideKey === 'over' ? 'Over' : sideKey === 'under' ? 'Under' : String(sideKey || '').toUpperCase();
    if (!market || !side) return raw;
    return `${market} ${side}`;
  }

  function playableSleeveCounts(rows) {
    const counts = new Map();
    (Array.isArray(rows) ? rows : []).forEach((row) => {
      const key = String(row?.playable_sleeve || '').trim().toLowerCase();
      if (!key) return;
      counts.set(key, Number(counts.get(key) || 0) + 1);
    });
    return Array.from(counts.entries())
      .map(([key, count]) => ({ key, label: formatPlayableSleeve(key), count }))
      .sort((left, right) => right.count - left.count || left.label.localeCompare(right.label));
  }

  function renderPlayableSleeveSummary(rows) {
    const sleeves = playableSleeveCounts(rows);
    if (!sleeves.length) return '';
    return `
      <div class="season-day-sleeve-strip">
        ${sleeves.map((item) => `<span class="season-sleeve-pill is-summary">${escapeHtml(`${item.label} ${formatNumber(item.count, 0)}`)}</span>`).join('')}
      </div>`;
  }

  function portfolioSummary(day) {
    const portfolio = day?.pregame_portfolio;
    if (!portfolio || !portfolio.enabled) return null;
    return portfolio;
  }

  function renderPortfolioSummaryPills(day) {
    const portfolio = portfolioSummary(day);
    if (!portfolio) return '';
    const pills = [];
    pills.push(`Portfolio ${formatNumber(portfolio.selected, 0)}/${formatNumber(portfolio.candidates, 0)} selected`);
    if (toNumber(portfolio.selected_stake_total) != null) pills.push(`${formatCurrency(portfolio.selected_stake_total)} staked`);
    if (toNumber(portfolio.bankroll) != null) pills.push(`${formatCurrency(portfolio.bankroll)} bankroll`);
    if (toNumber(portfolio.reserve_pct) != null) pills.push(`${formatPercent(portfolio.reserve_pct, 0)} reserve`);
    if (!pills.length) return '';
    return `
      <div class="season-day-sleeve-strip">
        ${pills.map((item) => `<span class="season-sleeve-pill is-summary">${escapeHtml(item)}</span>`).join('')}
      </div>`;
  }

  function portfolioRowMeta(row) {
    const bits = [];
    const rank = toNumber(row?.portfolio_rank);
    const stake = toNumber(row?.stake_amount);
    if (rank != null) bits.push(`Portfolio #${formatNumber(rank, 0)}`);
    if (stake != null) bits.push(`Stake ${formatCurrency(stake)}`);
    return bits;
  }

  function insightValue(row, group, key) {
    return row && row.insights && row.insights[group] ? row.insights[group][key] : null;
  }

  function firstSentence(values) {
    const items = Array.isArray(values) ? values : [];
    for (const value of items) {
      const text = String(value || '').trim();
      if (text) return text;
    }
    return '';
  }

  function hitRateValue(hitObj) {
    if (hitObj == null) return null;
    if (typeof hitObj === 'number') return hitObj;
    if (typeof hitObj === 'object' && hitObj.rate != null) return toNumber(hitObj.rate);
    return null;
  }

  function lineDirectionWord(row) {
    const sel = String(row?.selection || '').toLowerCase();
    if (sel === 'under') return 'below';
    if (sel === 'over') return 'above';
    return 'clear of';
  }

  function lineLeanWord(row) {
    const sel = String(row?.selection || '').toLowerCase();
    if (sel === 'under') return 'under';
    if (sel === 'over') return 'over';
    return 'side';
  }

  function playerNameOrPronoun(row) {
    return String(row?.player_name || '').trim() || 'He';
  }

  function playerLastName(row) {
    const full = String(row?.player_name || '').trim();
    if (!full) return 'He';
    const parts = full.split(/\s+/).filter(Boolean);
    return parts.length ? parts[parts.length - 1] : full;
  }

  function chipText(label, value) {
    return `${label} ${value}`;
  }

  function statChip(label, value, tone) {
    if (!label || value == null || value === '') return '';
    return `<span class="season-detail-chip ${tone ? `is-${escapeHtml(tone)}` : ''}"><span class="season-detail-chip-label">${escapeHtml(label)}</span><span class="season-detail-chip-value">${escapeHtml(String(value))}</span></span>`;
  }

  function buildPropInsightChips(row) {
    if (String(row?.market || '') !== 'player_props') return [];
    const chips = [];
    const line = toNumber(row?.market_line);
    const baseline = toNumber(insightValue(row, 'model', 'baseline'));
    const last5 = toNumber(insightValue(row, 'history', 'last5_avg'));
    const last10 = toNumber(insightValue(row, 'history', 'last10_avg'));
    const season = toNumber(insightValue(row, 'history', 'season_avg'));
    const oppAvg = toNumber(insightValue(row, 'history', 'opponent_avg'));
    const oppGames = Number(insightValue(row, 'history', 'opponent_games') || 0);
    const oppAllowed = toNumber(insightValue(row, 'matchup', 'opponent_allowed_avg'));
    const oppRank = toNumber(insightValue(row, 'matchup', 'opponent_allowed_rank'));
    const oppVsLine = toNumber(insightValue(row, 'matchup', 'opponent_vs_line'));
    const pos = String(insightValue(row, 'matchup', 'position') || '').toUpperCase();
    const posAllowed = toNumber(insightValue(row, 'matchup', 'position_allowed_avg'));
    const posRank = toNumber(insightValue(row, 'matchup', 'position_allowed_rank'));
    const seasonHit = toNumber(insightValue(row, 'history', 'season_hit'));
    const last10Hit = toNumber(insightValue(row, 'history', 'last10_hit'));
    const oppHit = toNumber(insightValue(row, 'history', 'opponent_hit'));
    const winProb = toNumber(insightValue(row, 'model', 'win_prob'));

    if (baseline != null && line != null) chips.push(statChip('Model', `${formatNumber(baseline, 1)} vs ${formatLine(line)}`, baseline >= line ? 'good' : 'neutral'));
    if (last5 != null) chips.push(statChip('L5', formatNumber(last5, 1), 'neutral'));
    if (last10 != null) chips.push(statChip('L10', formatNumber(last10, 1), 'neutral'));
    if (season != null) chips.push(statChip('Season', formatNumber(season, 1), 'neutral'));
    if (oppAvg != null && oppGames > 0) chips.push(statChip('Vs Opp', `${formatNumber(oppAvg, 1)} in ${oppGames}`, 'neutral'));
    if (oppAllowed != null) {
      const rankText = oppRank != null ? ` (${Math.round(oppRank)}/30)` : '';
      chips.push(statChip('Opp Allow', `${formatNumber(oppAllowed, 1)}${rankText}`, 'warn'));
    }
    if (pos && posAllowed != null) {
      const rankText = posRank != null ? ` (${Math.round(posRank)}/30)` : '';
      chips.push(statChip(`Vs ${pos}`, `${formatNumber(posAllowed, 1)}${rankText}`, 'warn'));
    }
    if (oppVsLine != null) chips.push(statChip('Opp vs Line', formatSignedNumber(oppVsLine, 1), oppVsLine >= 0 ? 'good' : 'bad'));
    if (seasonHit != null) chips.push(statChip('Season Hit', formatPercent(seasonHit, 0), seasonHit >= 0.55 ? 'good' : 'neutral'));
    if (last10Hit != null) chips.push(statChip('L10 Hit', formatPercent(last10Hit, 0), last10Hit >= 0.55 ? 'good' : 'neutral'));
    if (oppHit != null && oppGames > 0) chips.push(statChip('Vs Opp Hit', formatPercent(oppHit, 0), oppHit >= 0.55 ? 'good' : 'neutral'));
    if (winProb != null) chips.push(statChip('Model Win', formatPercent(winProb, 0), winProb >= 0.55 ? 'good' : 'neutral'));
    return chips.filter(Boolean).slice(0, 8);
  }

  function buildRecapParagraph(row) {
    const bucket = String(row?.market || '').toLowerCase();
    const sentences = [];

    if (bucket === 'player_props') {
      const name = playerLastName(row);
      const market = marketLabel(row).toLowerCase();
      const lean = lineLeanWord(row);
      const line = toNumber(row?.market_line);
      const last5 = toNumber(insightValue(row, 'history', 'last5_avg'));
      const last10 = toNumber(insightValue(row, 'history', 'last10_avg'));
      const season = toNumber(insightValue(row, 'history', 'season_avg'));
      const careerOpp = toNumber(insightValue(row, 'history', 'career_opponent_avg'));
      const careerOppGames = Number(insightValue(row, 'history', 'career_opponent_games') || 0);
      const opponent = String(row?.opponent_tri || '').trim().toUpperCase();
      const baseline = toNumber(insightValue(row, 'model', 'baseline'));
      const winProbPct = toPercentNumber(insightValue(row, 'model', 'win_prob'));
      const evPct = toNumber(row?.ev_pct) != null ? toNumber(row?.ev_pct) : toNumber(insightValue(row, 'model', 'ev_pct'));
      const oppAllowed = toNumber(insightValue(row, 'matchup', 'opponent_allowed_avg'));
      const oppRank = toNumber(insightValue(row, 'matchup', 'opponent_allowed_rank'));
      const pos = String(insightValue(row, 'matchup', 'position') || '').trim().toUpperCase();
      const posAllowed = toNumber(insightValue(row, 'matchup', 'position_allowed_avg'));
      const posRank = toNumber(insightValue(row, 'matchup', 'position_allowed_rank'));
      const basketballLead = firstSentence(row?.basketball_reasons);

      if (line != null && last5 != null && last10 != null) {
        sentences.push(`${name}'s ${lean} case starts with the recent form: he is at ${formatNumber(last5, 1)} over his last five and ${formatNumber(last10, 1)} over his last 10 against a ${formatLine(line)} ${market} line`);
      } else if (line != null && season != null) {
        sentences.push(`${name} has been living close to this number, carrying a ${formatNumber(season, 1)} season average into a ${formatLine(line)} ${market} line`);
      }
      if (baseline != null && line != null) {
        sentences.push(`The model keeps him on the ${lean} side as well, landing at ${formatNumber(baseline, 1)} and leaving ${formatNumber(Math.abs(line - baseline), 1)} ${market} of cushion`);
      }
      if (careerOpp != null && careerOppGames > 0 && opponent) {
        sentences.push(`There is also career support in the matchup, with ${name} averaging ${formatNumber(careerOpp, 1)} ${market} in ${careerOppGames} games against ${opponent}`);
      }
      if (oppAllowed != null && opponent) {
        const rankText = oppRank != null ? `, which sits ${Math.round(oppRank)} of 30 in this market` : '';
        sentences.push(`${opponent} have been allowing ${formatNumber(oppAllowed, 1)} ${market} over the recent window${rankText}`);
      }
      if (pos && posAllowed != null) {
        const tone = posRank != null && posRank <= 8 ? 'a tougher than average' : posRank != null && posRank >= 23 ? 'a softer than average' : 'a fairly neutral';
        sentences.push(`Against ${pos}s specifically, it profiles as ${tone} matchup with roughly ${formatNumber(posAllowed, 1)} ${market} allowed`);
      }
      if (evPct != null && winProbPct != null) {
        sentences.push(`That leaves this number playable at ${formatSignedNumber(evPct, 1)}% projected value with a modeled win rate of ${formatNumber(winProbPct, 1)}%`);
      }
      if (basketballLead) {
        sentences.splice(Math.min(1, sentences.length), 0, basketballLead);
      }
      return Array.from(new Set(sentences.map(ensureSentence))).join(' ');
    }

    if (bucket === 'spreads') {
      const displayPick = String(row?.display_pick || '').trim();
      const modelLead = firstSentence(row?.model_reasons);
      const formLead = firstSentence(row?.basketball_reasons);
      const evPct = toNumber(row?.ev_pct);
      const edgePts = toNumber(row?.edge);
      if (displayPick) {
        sentences.push(`${displayPick} gets the nod because the game script and projection both lean that way`);
      }
      if (formLead) sentences.push(formLead);
      if (modelLead) sentences.push(modelLead);
      if (edgePts != null) sentences.push(`The spread still shows ${formatSignedPoints(edgePts, 1)} of model edge at the current number`);
      if (evPct != null) sentences.push(`It remains playable with ${formatSignedNumber(evPct, 1)}% projected value`);
      return Array.from(new Set(sentences.map(ensureSentence))).join(' ');
    }

    if (bucket === 'totals') {
      const displayPick = String(row?.display_pick || '').trim();
      const formLead = firstSentence(row?.basketball_reasons);
      const modelLead = firstSentence(row?.model_reasons);
      const evPct = toNumber(row?.ev_pct);
      const edgePts = toNumber(row?.edge);
      if (displayPick) {
        sentences.push(`${displayPick} stands out because the projected game environment is landing away from the market total`);
      }
      if (formLead) sentences.push(formLead);
      if (modelLead) sentences.push(modelLead);
      if (edgePts != null) sentences.push(`The total is still sitting ${formatSignedPoints(edgePts, 1)} from the model view`);
      if (evPct != null) sentences.push(`At the current price, that translates to ${formatSignedNumber(evPct, 1)}% projected value`);
      return Array.from(new Set(sentences.map(ensureSentence))).join(' ');
    }

    const generic = [firstSentence(row?.basketball_reasons), firstSentence(row?.model_reasons), firstSentence(row?.market_reasons)]
      .filter(Boolean)
      .map(ensureSentence);
    return Array.from(new Set(generic)).join(' ');
  }

  function ensureSentence(text) {
    const value = String(text || '').trim();
    if (!value) return '';
    if (/[.!?]$/.test(value)) return value;
    return `${value}.`;
  }

  function renderReasonWriteup(group) {
    const sentences = (Array.isArray(group?.values) ? group.values : [])
      .map(ensureSentence)
      .filter(Boolean);
    if (!sentences.length) return '';
    return `
      <div class="season-detail-writeup">
        <div class="season-detail-group-label">${escapeHtml(String(group.label || 'Support'))}</div>
        <p class="season-detail-paragraph">${escapeHtml(sentences.join(' '))}</p>
      </div>`;
  }

  function shouldShowSummary(row, chips, recap) {
    if (recap) return false;
    if (String(row?.market || '') !== 'player_props') return true;
    return !chips.length && !recap;
  }

  function rowDetailHtml(row) {
    const summary = row?.reason_summary || rowDetail(row);
    const chips = buildPropInsightChips(row);
    const recap = buildRecapParagraph(row);
    const fallback = [];
    if (!recap) {
      const reasons = Array.isArray(row?.reasons) ? row.reasons.filter(Boolean).slice(0, 3) : [];
      if (reasons.length) {
        fallback.push(`<p class="season-detail-paragraph">${escapeHtml(reasons.map(ensureSentence).join(' '))}</p>`);
      }
    }
    return `
      <div class="season-pick-detail">
        ${shouldShowSummary(row, chips, recap) ? `<div class="season-pick-summary">${escapeHtml(summary || 'Official card recommendation')}</div>` : ''}
        ${chips.length ? `<div class="season-detail-chip-row">${chips.join('')}</div>` : ''}
        ${recap ? `<p class="season-detail-paragraph season-detail-recap">${escapeHtml(recap)}</p>` : ''}
        ${fallback.join('')}
      </div>`;
  }

  function fetchJson(path) {
    return fetch(path, { cache: 'no-store' }).then(async (response) => {
      const text = await response.text();
      let payload = null;
      try {
        payload = text ? JSON.parse(text) : null;
      } catch (_error) {
        payload = null;
      }
      if (!response.ok) {
        const errorText = payload && typeof payload === 'object'
          ? (payload.error || payload.message || response.statusText)
          : response.statusText;
        throw new Error(errorText || `HTTP ${response.status}`);
      }
      return payload;
    });
  }

  function updateUrl() {
    const nextUrl = new URL(window.location.href);
    nextUrl.searchParams.set('date', state.selectedDate);
    nextUrl.searchParams.set('profile', state.profile);
    window.history.replaceState({}, '', nextUrl.toString());
  }

  function updateNavLinks() {
    if (root.title) {
      root.title.textContent = `NBA ${state.season} Betting Card`;
    }
    if (root.seasonPill) {
      root.seasonPill.textContent = String(state.season);
    }
    if (root.dailyLink) {
      root.dailyLink.href = `/betting-card?date=${encodeURIComponent(state.selectedDate)}`;
    }
    if (root.liveAuditLink) {
      root.liveAuditLink.href = `/live-player-props-audit?date=${encodeURIComponent(state.selectedDate)}`;
    }
  }

  function metricCard(label, value, subcopy) {
    return `
      <article class="season-metric-card">
        <div class="season-metric-label">${escapeHtml(label)}</div>
        <div class="season-metric-value">${escapeHtml(value)}</div>
        <div class="season-metric-sub">${escapeHtml(subcopy || '')}</div>
      </article>`;
  }

  function availableProfiles() {
    const profiles = Array.isArray(state.manifest?.available_profiles) ? state.manifest.available_profiles : [];
    return profiles.filter(Boolean);
  }

  function filteredDays() {
    const allDays = Array.isArray(state.manifest?.days) ? state.manifest.days : [];
    if (state.monthFilter === 'all') return allDays;
    return allDays.filter((row) => String(row?.month || '') === state.monthFilter);
  }

  function officialRowsForGame(game) {
    const rows = Array.isArray(game?.betting?.officialRows) ? game.betting.officialRows : [];
    return rows;
  }

  function playableRowsForGame(game) {
    const rows = Array.isArray(game?.betting?.playableRows) ? game.betting.playableRows : [];
    return rows;
  }

  function allOfficialRows(day) {
    const games = Array.isArray(day?.games) ? day.games : [];
    const rows = [];
    games.forEach((game) => {
      officialRowsForGame(game).forEach((row) => rows.push({ game, row, bucket: 'official' }));
    });
    return rows;
  }

  function allPlayableRows(day) {
    const games = Array.isArray(day?.games) ? day.games : [];
    const rows = [];
    games.forEach((game) => {
      playableRowsForGame(game).forEach((row) => rows.push({ game, row, bucket: 'playable' }));
    });
    return rows;
  }

  function normalizedDayPicksMode(officialItems, playableItems) {
    if (state.dayPicksMode === 'playable' && playableItems.length) return 'playable';
    if (state.dayPicksMode === 'all' && (officialItems.length || playableItems.length)) return 'all';
    if (officialItems.length) return 'official';
    if (playableItems.length) return 'playable';
    return 'official';
  }

  function resultTone(row) {
    const result = String(row?.settlement?.result || '').toLowerCase();
    if (result === 'win') return 'is-win';
    if (result === 'loss') return 'is-loss';
    return 'is-pending';
  }

  function resultLabel(row) {
    const result = String(row?.settlement?.result || '').toLowerCase();
    if (result === 'win') return 'Win';
    if (result === 'loss') return 'Loss';
    if (result === 'push') return 'Push';
    return 'Pending';
  }

  function rowDetail(row) {
    const reasons = Array.isArray(row?.reasons) ? row.reasons.filter(Boolean) : [];
    if (reasons.length) return reasons[0];
    if (row?.reason_summary) return row.reason_summary;
    const bits = [];
    if (toNumber(row?.ev) != null) bits.push(`EV ${formatUnits(row.ev, 2)}`);
    if (toNumber(row?.edge) != null || toNumber(insightValue(row, 'model', 'prob_edge')) != null) bits.push(`Edge ${formatEdge(row, 1)}`);
    if (toNumber(row?.model_prob) != null) bits.push(`Model ${formatPercent(row.model_prob, 1)}`);
    return bits.join(' | ') || 'Betting card recommendation';
  }

  function renderHeader() {
    const meta = state.manifest?.meta || {};
    const summary = state.manifest?.summary || {};
    if (root.headerMeta) {
      const parts = [];
      parts.push(`${formatNumber(summary?.cards, 0)} betting-card days`);
      parts.push(`${formatNumber(summary?.settled_recommendations, 0)} settled betting-card plays`);
      if (meta.first_date && meta.last_date) parts.push(`${meta.first_date} to ${meta.last_date}`);
      parts.push(profileLabel(state.profile));
      root.headerMeta.textContent = parts.join(' | ');
    }
    if (root.statusPill) {
      root.statusPill.textContent = meta.partial ? 'Partial' : 'Complete';
      root.statusPill.classList.toggle('is-partial', Boolean(meta.partial));
    }
  }

  function renderProfiles() {
    if (!root.profiles) return;
    const profiles = availableProfiles();
    root.profiles.innerHTML = profiles.map((profile) => `
      <button
        type="button"
        class="cards-filter-pill ${profile === state.profile ? 'is-active' : ''}"
        data-betting-card-profile="${escapeHtml(profile)}"
      >
        ${escapeHtml(profileLabel(profile))}
      </button>`).join('');
  }

  function renderSummary() {
    if (!root.summary) return;
    const summary = state.manifest?.summary;
    if (!summary) {
      root.summary.innerHTML = '';
      return;
    }
    const combined = summary?.results?.combined || summary?.combined || {};
    const playableCombined = summary?.playable_results?.combined || {};
    const allCombined = summary?.all_results?.combined || {};
    const daily = summary?.daily || {};
    const counts = summary?.selected_counts || {};
    const playableCounts = summary?.playable_selected_counts || {};
    const bestDay = daily?.best_day || {};
    const worstDay = daily?.worst_day || {};
    root.summary.innerHTML = [
      metricCard('Card days', formatNumber(summary?.cards, 0), `${profileLabel(state.profile)} betting-card dates`),
      metricCard('Official ROI', formatPercent(combined?.roi, 1), `${formatUnits(combined?.profit_u, 2)} on ${formatNumber(combined?.stake_u, 2)}u`),
      metricCard('Playable ROI', formatPercent(playableCombined?.roi, 1), `${formatUnits(playableCombined?.profit_u, 2)} on ${formatNumber(playableCombined?.stake_u, 2)}u`),
      metricCard('All-card ROI', formatPercent(allCombined?.roi, 1), `${formatUnits(allCombined?.profit_u, 2)} on ${formatNumber(allCombined?.stake_u, 2)}u`),
      metricCard('Season profit', formatUnits(combined?.profit_u, 2), `${formatNumber(combined?.wins, 0)} wins | ${formatNumber(combined?.losses, 0)} losses`),
      metricCard('Settled bets', formatNumber(combined?.n, 0), `${formatNumber(summary?.unresolved_recommendations, 0)} locked unresolved`),
      metricCard('Playable settled', formatNumber(summary?.playable_settled_recommendations, 0), `${formatNumber(summary?.playable_unresolved_recommendations, 0)} unresolved`),
      metricCard('All settled', formatNumber(summary?.all_settled_recommendations, 0), `${formatNumber(summary?.all_unresolved_recommendations, 0)} unresolved`),
      metricCard('Daily mean', formatUnits(daily?.mean_u, 2), `Median ${formatUnits(daily?.median_u, 2)}`),
      metricCard('Best day', formatUnits(bestDay?.profit_u, 2), String(bestDay?.date || '-')),
      metricCard('Worst day', formatUnits(worstDay?.profit_u, 2), String(worstDay?.date || '-')),
      metricCard('Selection mix', formatNumber(counts?.combined, 0), `Tot ${counts?.totals ?? 0} | ML ${counts?.ml ?? 0} | Spr ${counts?.spreads ?? 0} | Props ${counts?.player_props ?? 0}`),
      metricCard('Playable adds', formatNumber(playableCounts?.combined, 0), `Props ${playableCounts?.player_props ?? 0} | ${formatNumber(summary?.playable_unresolved_recommendations, 0)} unresolved`),
    ].join('');
  }

  function renderMonths() {
    if (!root.months) return;
    const months = Array.isArray(state.manifest?.months) ? state.manifest.months : [];
    const allDays = Array.isArray(state.manifest?.days) ? state.manifest.days : [];
    const options = [{ key: 'all', label: `All ${allDays.length}` }].concat(
      months.map((row) => ({
        key: String(row?.month || ''),
        label: `${monthLabel(String(row?.month || ''))} ${row?.days ?? 0}`,
      }))
    );
    root.months.innerHTML = options.map((option) => `
      <button
        type="button"
        class="cards-filter-pill ${option.key === state.monthFilter ? 'is-active' : ''}"
        data-betting-card-month="${escapeHtml(option.key)}"
      >
        ${escapeHtml(option.label)}
      </button>`).join('');
  }

  function renderDays() {
    if (!root.days) return;
    const days = filteredDays();
    if (!days.length) {
      root.days.innerHTML = '<div class="season-empty-copy">No betting-card days match the current month filter.</div>';
      return;
    }
    root.days.innerHTML = days.map((day) => {
      const isActive = String(day?.date || '') === state.selectedDate;
      const counts = day?.selected_counts || {};
      const playableCounts = day?.playable_selected_counts || {};
      const combined = (day?.results || {}).combined || {};
      const unresolved = Number(day?.unresolved_n || 0);
      const labels = formatDateRail(String(day?.date || ''));
      const badges = [];
      if (unresolved > 0) {
        badges.push(`<span class="season-day-pill is-empty">${escapeHtml(formatNumber(unresolved, 0))} unresolved</span>`);
      } else {
        badges.push(`<span class="season-day-pill is-official">${escapeHtml(formatNumber(counts?.combined, 0))} locked</span>`);
      }
      if (Number(playableCounts?.combined || 0) > 0) {
        badges.push(`<span class="season-day-pill is-playable">${escapeHtml(`+${formatNumber(playableCounts?.combined, 0)} playable`)}</span>`);
      }
      return `
        <article class="season-day-entry">
          <button type="button" class="season-day-button ${isActive ? 'is-active' : ''}" data-betting-card-date="${escapeHtml(String(day?.date || ''))}">
            <div class="season-day-row">
              <div class="season-day-stamp">
                <div class="season-day-dow">${escapeHtml(labels.dow)}</div>
                <div class="season-day-primary">${escapeHtml(labels.monthDay)}</div>
              </div>
              <span class="cards-chip">${escapeHtml(formatUnits(combined?.profit_u, 2))}</span>
            </div>
            <div class="season-day-secondary">ROI ${escapeHtml(formatPercent(combined?.roi, 1))} | ${escapeHtml(formatNumber(combined?.n, 0))} settled | ${escapeHtml(labels.month)}</div>
          </button>
          <div class="season-day-badges">${badges.join('')}</div>
        </article>`;
    }).join('');
  }

  function renderDaySummary() {
    if (!root.dayTitle || !root.dayMeta || !root.dayActions || !root.dayMetrics) return;
    if (!state.day) {
      root.dayTitle.textContent = 'No day selected';
      root.dayMeta.textContent = 'Pick a betting-card date from the rail to load the day details.';
      root.dayActions.innerHTML = '';
      root.dayMetrics.innerHTML = '<div class="season-empty-copy">No betting-card metrics available.</div>';
      return;
    }
    const combined = (state.day?.results || {}).combined || {};
    const playableCombined = (state.day?.playable_results || {}).combined || {};
    const counts = state.day?.selected_counts || {};
    const playableCounts = state.day?.playable_selected_counts || {};
    const playableRows = allPlayableRows(state.day).map((item) => item.row || {});
    const games = Array.isArray(state.day?.games) ? state.day.games : [];
    const unresolved = Number(state.day?.summary?.unresolved_n || 0);
    const playableUnresolved = Number(state.day?.summary?.playable_unresolved_n || 0);
    const playableTotal = Number(playableCounts?.combined || 0);
    const portfolio = portfolioSummary(state.day);
    root.dayTitle.textContent = formatDateLong(state.day.date);
    root.dayMeta.textContent = [
      `${games.length} games`,
      `${formatNumber(counts?.combined, 0)} locked-card picks${playableTotal ? ` | +${formatNumber(playableTotal, 0)} playable` : ''}`,
      portfolio ? `${formatNumber(portfolio.selected, 0)} portfolio-selected` : '',
      profileLabel(state.day?.profile || state.profile),
      String(state.day?.source_kind || 'season_manifest'),
    ].filter(Boolean).join(' | ');
    root.dayActions.innerHTML = `
      <a class="cards-nav-pill" href="/betting-card?date=${encodeURIComponent(state.day.date)}">Open daily cards</a>
      ${renderPortfolioSummaryPills(state.day)}
      ${renderPlayableSleeveSummary(playableRows)}`;
    const metricCards = [
      metricCard('Games', formatNumber(games.length, 0), 'Matchups with betting-card action'),
      metricCard('Locked card', formatNumber(counts?.combined, 0), `Tot ${counts?.totals ?? 0} | ML ${counts?.ml ?? 0} | Spr ${counts?.spreads ?? 0} | Props ${counts?.player_props ?? 0}`),
      metricCard('Playable adds', formatNumber(playableCounts?.combined, 0), `${formatNumber(playableCombined?.n, 0)} settled | ${formatNumber(playableUnresolved, 0)} unresolved`),
      metricCard('Locked profit', formatUnits(combined?.profit_u, 2), `${formatNumber(combined?.wins, 0)} wins | ${formatNumber(combined?.losses, 0)} losses`),
      metricCard('Locked ROI', formatPercent(combined?.roi, 1), `${formatNumber(combined?.stake_u, 2)}u staked`),
      metricCard('Settled', formatNumber(combined?.n, 0), `${formatNumber(unresolved, 0)} locked unresolved`),
      metricCard('Cap profile', String(state.day?.cap_profile || '-'), 'Locked betting-card view'),
    ];
    if (portfolio) {
      metricCards.splice(2, 0, metricCard('Portfolio stake', formatCurrency(portfolio.selected_stake_total), `${formatNumber(portfolio.selected, 0)} picks | ${formatCurrency(portfolio.bankroll)} bankroll`));
    }
    root.dayMetrics.innerHTML = metricCards.join('');
  }

  function pickTableRows(items) {
    return items.map((item) => {
      const game = item.game || {};
      const row = item.row || {};
      const bucket = String(item.bucket || row?.card_bucket || 'official').toLowerCase();
      const sleeve = formatPlayableSleeve(row?.playable_sleeve);
      const portfolioMeta = portfolioRowMeta(row);
      const gameLabel = `${game?.away?.abbr || 'Away'} @ ${game?.home?.abbr || 'Home'}`;
      const gameMeta = [game?.start_time ? `Tip-off ${game.start_time}` : '', String(game?.status?.abstract || '').trim()].filter(Boolean).join(' | ');
      const actualText = row?.settlement?.actual != null ? `Actual ${formatLine(row.settlement.actual)}` : 'Settlement unavailable';
      return `
        <tr>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(gameLabel)}</div>
            <div class="season-betting-cell-sub">${escapeHtml(gameMeta || `Game ${String(game?.game_pk || '-')}`)}</div>
          </td>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(marketLabel(row))}</div>
            <div class="season-betting-cell-sub">${escapeHtml(bucket === 'playable' ? 'Playable addition' : String(row?.market_family_label || 'Betting card'))}</div>
            ${bucket !== 'playable' && portfolioMeta.length ? `<div class="season-betting-tag-row">${portfolioMeta.map((bit) => `<span class="season-sleeve-pill is-summary">${escapeHtml(bit)}</span>`).join('')}</div>` : ''}
            ${bucket === 'playable' && sleeve ? `<div class="season-betting-tag-row"><span class="season-sleeve-pill">${escapeHtml(sleeve)}</span></div>` : ''}
          </td>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(String(row?.display_pick || '-'))}</div>
            ${rowDetailHtml(row)}
          </td>
          <td>${escapeHtml(formatOdds(row?.odds))}</td>
          <td>${escapeHtml(formatEdge(row, 1))}</td>
          <td><span class="season-ticket-pill ${resultTone(row)}">${escapeHtml(resultLabel(row))}</span></td>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(formatUnits(row?.settlement?.profit_u, 2))}</div>
            <div class="season-betting-cell-sub">${escapeHtml(actualText)}</div>
          </td>
        </tr>`;
    }).join('');
  }

  function pickMobileCards(items) {
    return items.map((item) => {
      const row = item.row || {};
      const game = item.game || {};
      const bucket = String(item.bucket || row?.card_bucket || 'official').toLowerCase();
      const sleeve = formatPlayableSleeve(row?.playable_sleeve);
      const portfolioMeta = portfolioRowMeta(row);
      const gameLabel = `${game?.away?.abbr || 'Away'} @ ${game?.home?.abbr || 'Home'}`;
      const actualText = row?.settlement?.actual != null ? `Actual ${formatLine(row.settlement.actual)}` : 'Settlement unavailable';
      return `
        <article class="betting-card-mobile-entry">
          <div class="betting-card-mobile-head">
            <div>
              <div class="betting-card-mobile-value">${escapeHtml(gameLabel)}</div>
              <div class="season-inline-note">${escapeHtml(marketLabel(row))} | ${escapeHtml(bucket === 'playable' ? 'Playable addition' : String(row?.market_family_label || 'Betting card'))}</div>
              ${bucket !== 'playable' && portfolioMeta.length ? `<div class="season-betting-tag-row">${portfolioMeta.map((bit) => `<span class="season-sleeve-pill is-summary">${escapeHtml(bit)}</span>`).join('')}</div>` : ''}
              ${bucket === 'playable' && sleeve ? `<div class="season-betting-tag-row"><span class="season-sleeve-pill">${escapeHtml(sleeve)}</span></div>` : ''}
            </div>
            <span class="season-ticket-pill ${resultTone(row)}">${escapeHtml(resultLabel(row))}</span>
          </div>
          <div class="betting-card-mobile-grid">
            <div><div class="betting-card-mobile-label">Pick</div><div class="betting-card-mobile-value">${escapeHtml(String(row?.display_pick || '-'))}</div></div>
            <div><div class="betting-card-mobile-label">Odds</div><div class="betting-card-mobile-value">${escapeHtml(formatOdds(row?.odds))}</div></div>
            <div><div class="betting-card-mobile-label">Edge</div><div class="betting-card-mobile-value">${escapeHtml(formatEdge(row, 1))}</div></div>
            <div><div class="betting-card-mobile-label">Profit</div><div class="betting-card-mobile-value">${escapeHtml(formatUnits(row?.settlement?.profit_u, 2))}</div></div>
          </div>
          ${rowDetailHtml(row)}
          <div class="season-inline-note">${escapeHtml(actualText)}</div>
        </article>`;
    }).join('');
  }

  function renderDayPicks() {
    if (!root.dayPicks) return;
    if (!state.day) {
      root.dayPicks.innerHTML = '<div class="season-empty-copy">Pick a betting-card date to inspect the day-level board.</div>';
      return;
    }
    const officialItems = allOfficialRows(state.day);
    const playableItems = allPlayableRows(state.day);
    const mode = normalizedDayPicksMode(officialItems, playableItems);
    const items = mode === 'playable'
      ? playableItems
      : mode === 'all'
        ? officialItems.concat(playableItems)
        : officialItems;
    const modeTitle = mode === 'playable'
      ? 'Playable additions by date'
      : mode === 'all'
        ? 'Locked card + playable additions'
        : 'Locked card by date';
    const modeCopy = mode === 'playable'
      ? `${formatNumber(playableItems.length, 0)} playable additions across the selected date under ${profileLabel(state.day?.profile || state.profile)}.`
      : mode === 'all'
        ? `${formatNumber(items.length, 0)} total recommendations across the selected date, combining locked card picks and playable additions.`
        : `${formatNumber(officialItems.length, 0)} locked-card picks across the selected date under ${profileLabel(state.day?.profile || state.profile)}.`;
    const sleeveSummary = mode === 'playable' || mode === 'all'
      ? renderPlayableSleeveSummary(playableItems.map((item) => item.row || {}))
      : '';
    root.dayPicks.innerHTML = `
      <div class="season-panel-head">
        <div>
          <div class="season-kicker">Selected day board</div>
          <div class="season-panel-title">${escapeHtml(modeTitle)}</div>
        </div>
      </div>
      <div class="season-day-badges">
        <button type="button" class="cards-filter-pill ${mode === 'official' ? 'is-active' : ''}" data-betting-card-day-picks="official">${escapeHtml(`Locked ${formatNumber(officialItems.length, 0)}`)}</button>
        ${playableItems.length ? `<button type="button" class="cards-filter-pill ${mode === 'playable' ? 'is-active' : ''}" data-betting-card-day-picks="playable">${escapeHtml(`Playable ${formatNumber(playableItems.length, 0)}`)}</button>` : ''}
        ${(officialItems.length && playableItems.length) ? `<button type="button" class="cards-filter-pill ${mode === 'all' ? 'is-active' : ''}" data-betting-card-day-picks="all">${escapeHtml(`All ${formatNumber(officialItems.length + playableItems.length, 0)}`)}</button>` : ''}
      </div>
      <div class="season-inline-note">${escapeHtml(modeCopy)}</div>
      ${sleeveSummary}
      ${items.length ? `
        <div class="season-calibration-table-wrap">
          <table class="season-calibration-table season-day-picks-table">
            <thead>
              <tr>
                <th>Game</th>
                <th>Market</th>
                <th>Pick</th>
                <th>Odds</th>
                <th>Edge</th>
                <th>Status</th>
                <th>Profit</th>
              </tr>
            </thead>
            <tbody>${pickTableRows(items)}</tbody>
          </table>
        </div>
        <div class="betting-card-mobile-list">${pickMobileCards(items)}</div>` : '<div class="season-empty-copy">No betting-card plays were logged for this date.</div>'}
    `;
  }

  function renderGames() {
    if (!root.games) return;
    if (!state.day) {
      root.games.innerHTML = '<div class="season-empty-copy">No betting-card games loaded.</div>';
      return;
    }
    const games = Array.isArray(state.day?.games) ? state.day.games : [];
    if (!games.length) {
      root.games.innerHTML = '<div class="season-empty-copy">No betting-card games were found for this date.</div>';
      return;
    }
    root.games.innerHTML = games.map((game) => {
      const rows = officialRowsForGame(game);
      const playableRows = playableRowsForGame(game);
      const hasPortfolioRows = rows.some((row) => toNumber(row?.portfolio_rank) != null || toNumber(row?.stake_amount) != null);
      const combined = ((game?.betting || {}).results || {}).combined || {};
      const playableCombined = ((game?.betting || {}).playable_results || {}).combined || {};
      const score = game?.matchup?.score || {};
      const scoreText = score.away != null || score.home != null
        ? `${game?.away?.abbr || 'Away'} ${score.away ?? '-'} - ${game?.home?.abbr || 'Home'} ${score.home ?? '-'}`
        : '';
      return `
        <article class="season-game-card">
          <div class="season-game-head">
            <div class="season-game-matchup">
              <div class="season-team-line">
                <span class="season-team-code">${escapeHtml(game?.away?.abbr || 'Away')}</span>
                <span class="season-team-name">${escapeHtml(game?.away?.name || game?.away?.abbr || 'Away')}</span>
              </div>
              <div class="season-team-line">
                <span class="season-team-code">${escapeHtml(game?.home?.abbr || 'Home')}</span>
                <span class="season-team-name">${escapeHtml(game?.home?.name || game?.home?.abbr || 'Home')}</span>
              </div>
              <div class="season-game-subcopy">Betting card matchup</div>
              <div class="season-game-time">${escapeHtml([game?.start_time ? `Tip-off ${game.start_time}` : '', String(game?.status?.detailed || game?.status?.abstract || '').trim()].filter(Boolean).join(' | '))}</div>
              ${scoreText ? `<div class="season-game-subcopy">${escapeHtml(scoreText)}</div>` : ''}
            </div>
            <div class="season-scorebox">
              <div class="season-score-label">Betting card</div>
              <div class="season-score-main">${escapeHtml(formatUnits(combined?.profit_u, 2))}</div>
              <div class="season-game-subcopy">ROI ${escapeHtml(formatPercent(combined?.roi, 1))} | ${escapeHtml(formatNumber(rows.length, 0))} locked${playableRows.length ? ` | +${escapeHtml(formatNumber(playableRows.length, 0))} playable` : ''}</div>
            </div>
          </div>
          <section class="season-game-betting-shell">
            <div class="season-stat-grid season-game-betting-stats">
              ${metricCard('Locked picks', formatNumber(rows.length, 0), 'Official card only')}
              ${metricCard('Playable adds', formatNumber(playableRows.length, 0), `${formatNumber(playableCombined?.n, 0)} settled`) }
              ${metricCard('Locked profit', formatUnits(combined?.profit_u, 2), `${formatNumber(combined?.wins, 0)} wins | ${formatNumber(combined?.losses, 0)} losses`) }
              ${metricCard('Locked ROI', formatPercent(combined?.roi, 1), `${formatNumber(combined?.stake_u, 2)}u staked`) }
              ${metricCard('Settled', formatNumber(combined?.n, 0), `Game ${formatNumber(game?.game_pk, 0)}`) }
            </div>
            <section class="season-breakdown-card season-game-betting-card">
              <div class="season-breakdown-title">Locked card</div>
              ${hasPortfolioRows ? '<div class="season-inline-note">Locked picks on this card were selected by the pregame portfolio ranker, with bankroll-aware stake sizing shown on each row.</div>' : ''}
              ${rows.length ? `
                <div class="season-calibration-table-wrap">
                  <table class="season-calibration-table season-game-betting-table">
                    <thead>
                      <tr>
                        <th>Market</th>
                        <th>Pick</th>
                        <th>Odds</th>
                        <th>Edge</th>
                        <th>Status</th>
                        <th>Profit</th>
                      </tr>
                    </thead>
                    <tbody>${pickTableRows(rows.map((row) => ({ game, row, bucket: 'official' })))}</tbody>
                  </table>
                </div>
                <div class="betting-card-mobile-list">${pickMobileCards(rows.map((row) => ({ game, row, bucket: 'official' })))}</div>` : '<div class="season-empty-copy">No locked-card plays for this matchup.</div>'}
              ${playableRows.length ? `
                <div class="season-inline-note">Playable additions are qualified props that stayed off the locked card after ranking and slot limits.</div>
                <div class="season-breakdown-title">Playable additions</div>
                <div class="season-calibration-table-wrap">
                  <table class="season-calibration-table season-game-betting-table">
                    <thead>
                      <tr>
                        <th>Market</th>
                        <th>Pick</th>
                        <th>Odds</th>
                        <th>Edge</th>
                        <th>Status</th>
                        <th>Profit</th>
                      </tr>
                    </thead>
                    <tbody>${pickTableRows(playableRows.map((row) => ({ game, row, bucket: 'playable' })))}</tbody>
                  </table>
                </div>
                <div class="betting-card-mobile-list">${pickMobileCards(playableRows.map((row) => ({ game, row, bucket: 'playable' })))}</div>` : ''}
            </section>
          </section>
        </article>`;
    }).join('');
  }

  function renderDay() {
    renderDays();
    renderDaySummary();
    renderDayPicks();
    renderGames();
    updateNavLinks();
  }

  async function loadDay(dateStr) {
    if (!dateStr) return;
    state.selectedDate = String(dateStr);
    state.dayPicksMode = 'official';
    const requestedDate = state.selectedDate;
    updateUrl();
    updateNavLinks();
    if (root.dayMeta) root.dayMeta.textContent = 'Loading betting-card detail...';
    if (root.dayMetrics) root.dayMetrics.innerHTML = '<div class="cards-loading-state">Loading day metrics...</div>';
    if (root.dayPicks) root.dayPicks.innerHTML = '<div class="cards-loading-state">Loading betting-card picks...</div>';
    if (root.games) root.games.innerHTML = '<div class="cards-loading-state">Loading betting-card games...</div>';
    try {
      const basePath = `/api/season/${encodeURIComponent(state.season)}/betting-card/day/${encodeURIComponent(state.selectedDate)}?profile=${encodeURIComponent(state.profile)}`;
      state.day = await fetchJson(basePath);
      if (state.selectedDate !== requestedDate) {
        return;
      }
      renderDay();
      try {
        const detailedDay = await fetchJson(`${basePath}&include_prop_insights=1`);
        if (state.selectedDate !== requestedDate) {
          return;
        }
        state.day = detailedDay;
        renderDay();
      } catch (_detailedError) {
        // Keep the fast-loaded board visible if the heavier insights request is slow or unavailable.
      }
    } catch (error) {
      const message = error && error.message ? error.message : 'Unknown error';
      if (root.dayMeta) root.dayMeta.textContent = `Failed to load ${state.selectedDate}.`;
      if (root.dayMetrics) root.dayMetrics.innerHTML = `<div class="cards-empty-state season-error">Failed to load day metrics.<div class="season-inline-note">${escapeHtml(message)}</div></div>`;
      if (root.dayPicks) root.dayPicks.innerHTML = `<div class="cards-empty-state season-error">Failed to load betting-card picks.<div class="season-inline-note">${escapeHtml(message)}</div></div>`;
      if (root.games) root.games.innerHTML = `<div class="cards-empty-state season-error">Failed to load betting-card games.<div class="season-inline-note">${escapeHtml(message)}</div></div>`;
    }
  }

  async function loadManifest() {
    updateNavLinks();
    if (root.summary) root.summary.innerHTML = '<div class="cards-loading-state">Loading betting-card summary...</div>';
    try {
      state.manifest = await fetchJson(`/api/season/${encodeURIComponent(state.season)}/betting-card?profile=${encodeURIComponent(state.profile)}&date=${encodeURIComponent(state.selectedDate)}`);
      state.profile = String(state.manifest?.profile || state.profile || 'retuned');
      renderHeader();
      renderProfiles();
      renderSummary();
      renderMonths();
      const days = Array.isArray(state.manifest?.days) ? state.manifest.days : [];
      if (!days.length) {
        if (root.days) root.days.innerHTML = '<div class="season-empty-copy">No betting-card days are available for this profile.</div>';
        if (root.dayMetrics) root.dayMetrics.innerHTML = '<div class="season-empty-copy">No betting-card dates are available to inspect.</div>';
        if (root.dayPicks) root.dayPicks.innerHTML = '<div class="season-empty-copy">No betting-card picks are available.</div>';
        if (root.games) root.games.innerHTML = '<div class="season-empty-copy">No betting-card games available.</div>';
        return;
      }
      if (!state.selectedDate || !days.some((row) => String(row?.date || '') === state.selectedDate)) {
        state.selectedDate = String(days[days.length - 1]?.date || '');
      }
      renderDays();
      await loadDay(state.selectedDate);
    } catch (error) {
      const message = error && error.message ? error.message : 'Unknown error';
      if (root.headerMeta) root.headerMeta.textContent = `Failed to load season ${state.season} betting-card data.`;
      if (root.summary) root.summary.innerHTML = `<div class="cards-empty-state season-error">Failed to load betting-card data.<div class="season-inline-note">${escapeHtml(message)}</div></div>`;
      if (root.days) root.days.innerHTML = '<div class="season-empty-copy">No betting-card manifest found.</div>';
      if (root.dayPicks) root.dayPicks.innerHTML = '<div class="season-empty-copy">No betting-card picks available.</div>';
      if (root.games) root.games.innerHTML = '<div class="season-empty-copy">No betting-card games available.</div>';
    }
  }

  if (root.profiles) {
    root.profiles.addEventListener('click', async function (event) {
      const button = event.target.closest('[data-betting-card-profile]');
      if (!button || !root.profiles.contains(button)) return;
      event.preventDefault();
      state.profile = String(button.getAttribute('data-betting-card-profile') || state.profile);
      await loadManifest();
    });
  }

  if (root.months) {
    root.months.addEventListener('click', function (event) {
      const button = event.target.closest('[data-betting-card-month]');
      if (!button || !root.months.contains(button)) return;
      event.preventDefault();
      state.monthFilter = String(button.getAttribute('data-betting-card-month') || 'all');
      renderMonths();
      renderDays();
      const visible = filteredDays();
      if (visible.length && !visible.some((row) => String(row?.date || '') === state.selectedDate)) {
        loadDay(String(visible[visible.length - 1]?.date || ''));
      }
    });
  }

  if (root.days) {
    root.days.addEventListener('click', async function (event) {
      const button = event.target.closest('[data-betting-card-date]');
      if (!button || !root.days.contains(button)) return;
      event.preventDefault();
      await loadDay(String(button.getAttribute('data-betting-card-date') || ''));
    });
  }

  if (root.dayPicks) {
    root.dayPicks.addEventListener('click', function (event) {
      const button = event.target.closest('[data-betting-card-day-picks]');
      if (!button || !root.dayPicks.contains(button)) return;
      event.preventDefault();
      const mode = String(button.getAttribute('data-betting-card-day-picks') || 'official');
      state.dayPicksMode = mode === 'playable' ? 'playable' : mode === 'all' ? 'all' : 'official';
      renderDayPicks();
    });
  }

  updateNavLinks();
  loadManifest();
})();