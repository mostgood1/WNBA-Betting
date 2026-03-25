(function () {
  const formEl = document.getElementById('propLadderForm');
  const dateInputEl = document.getElementById('propLadderDateInput');
  const propInputEl = document.getElementById('propLadderPropInput');
  const teamInputEl = document.getElementById('propLadderTeamInput');
  const playerInputEl = document.getElementById('propLadderPlayerInput');
  const sortInputEl = document.getElementById('propLadderSortInput');
  const headerMetaEl = document.getElementById('propLadderHeaderMeta');
  const sourceMetaEl = document.getElementById('propLadderSourceMeta');
  const summaryEl = document.getElementById('propLadderSummary');
  const selectedPlayerEl = document.getElementById('propLadderSelectedPlayer');
  const gridEl = document.getElementById('propLadderGrid');
  const noteEl = document.getElementById('propLadderNote');
  const dateBadgeEl = document.getElementById('propLadderDateBadge');
  const propBadgeEl = document.getElementById('propLadderPropBadge');
  const prevDateLinkEl = document.getElementById('propLadderPrevDateLink');
  const nextDateLinkEl = document.getElementById('propLadderNextDateLink');

  if (!formEl || !dateInputEl || !propInputEl || !teamInputEl || !playerInputEl || !sortInputEl || !headerMetaEl || !sourceMetaEl || !summaryEl || !selectedPlayerEl || !gridEl || !noteEl || !dateBadgeEl || !propBadgeEl || !prevDateLinkEl || !nextDateLinkEl) {
    return;
  }

  const params = new URLSearchParams(window.location.search);
  const state = {
    date: String(params.get('date') || getLocalDateISO()),
    prop: String(params.get('market') || 'pts'),
    team: String(params.get('team') || ''),
    player: String(params.get('player') || ''),
    sort: String(params.get('sort') || 'team'),
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

  function formatNumber(value, digits = 2) {
    const number = Number(value);
    return Number.isFinite(number) ? number.toFixed(digits) : '--';
  }

  function formatCount(value) {
    const number = Number(value);
    return Number.isFinite(number) ? `${Math.round(number)}` : '--';
  }

  function formatPercent(value) {
    const number = Number(value);
    return Number.isFinite(number) ? `${(number * 100).toFixed(1)}%` : '--';
  }

  function formatOdds(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) {
      return '--';
    }
    return number > 0 ? `+${Math.round(number)}` : `${Math.round(number)}`;
  }

  function pageHref(dateValue, propValue, teamValue, playerValue, sortValue) {
    const pageParams = new URLSearchParams();
    if (dateValue) pageParams.set('date', dateValue);
    if (propValue) pageParams.set('market', propValue);
    if (teamValue) pageParams.set('team', teamValue);
    if (playerValue) pageParams.set('player', playerValue);
    if (sortValue) pageParams.set('sort', sortValue);
    return `/prop-ladders?${pageParams.toString()}`;
  }

  function apiHref(dateValue, propValue, teamValue, playerValue, sortValue) {
    const apiParams = new URLSearchParams();
    if (dateValue) apiParams.set('date', dateValue);
    if (propValue) apiParams.set('market', propValue);
    if (teamValue) apiParams.set('team', teamValue);
    if (playerValue) apiParams.set('player', playerValue);
    if (sortValue) apiParams.set('sort', sortValue);
    return `/api/prop-ladders?${apiParams.toString()}`;
  }

  function playerInitial(name) {
    const text = String(name || '').trim();
    return escapeHtml((text.slice(0, 1) || '?').toUpperCase());
  }

  function fallbackHeadshotUrl(playerId) {
    const numericId = Number(playerId);
    if (!Number.isFinite(numericId) || numericId <= 0) {
      return '';
    }
    return `https://a.espncdn.com/i/headshots/nba/players/full/${Math.round(numericId)}.png`;
  }

  function renderHeadshotMedia(imageUrl, playerName, playerId, imageClass, fallbackClass) {
    const resolvedImageUrl = String(imageUrl || '').trim();
    const fallbackUrl = fallbackHeadshotUrl(playerId);
    const finalUrl = resolvedImageUrl || fallbackUrl;
    if (!finalUrl) {
      return `<div class="${fallbackClass}">${playerInitial(playerName)}</div>`;
    }
    const fallbackAttr = fallbackUrl && fallbackUrl !== resolvedImageUrl
      ? ` data-fallback-src="${escapeHtml(fallbackUrl)}"`
      : '';
    return `
      <span class="ladder-headshot-frame">
        <img class="${imageClass}" src="${escapeHtml(finalUrl)}" alt="${escapeHtml(playerName || 'Player')} headshot" loading="lazy"${fallbackAttr} onerror="if(!this.dataset.fallbackApplied&&this.dataset.fallbackSrc){this.dataset.fallbackApplied='1';this.src=this.dataset.fallbackSrc;return;}this.style.display='none';var fb=this.nextElementSibling;if(fb){fb.style.display='grid';}" />
        <span class="${fallbackClass}" style="display:none">${playerInitial(playerName)}</span>
      </span>
    `;
  }

  function thresholdBookOdds(row, total) {
    const entries = Array.isArray(row.marketLinesByStat) ? row.marketLinesByStat : [];
    const targetTotal = Number(total);
    if (!Number.isFinite(targetTotal) || !entries.length) {
      return '--';
    }
    const targetLines = [targetTotal - 0.5, targetTotal, targetTotal + 0.5];
    const match = targetLines
      .map((targetLine) => entries.find((entry) => Math.abs(Number(entry.line) - targetLine) < 0.26))
      .find(Boolean);
    if (!match) {
      return '--';
    }
    if (match.overOdds != null) {
      return `O ${formatOdds(match.overOdds)}`;
    }
    if (match.underOdds != null) {
      return `U ${formatOdds(match.underOdds)}`;
    }
    return '--';
  }

  function renderMarketLineChips(row) {
    const entries = Array.isArray(row.marketLinesByStat) ? row.marketLinesByStat : [];
    if (!entries.length) return '';
    return `
      <div class="ladder-market-lines">
        ${entries.map((entry) => {
          const oddsBits = [];
          if (entry.overOdds != null) oddsBits.push(`O ${escapeHtml(formatOdds(entry.overOdds))}`);
          if (entry.underOdds != null) oddsBits.push(`U ${escapeHtml(formatOdds(entry.underOdds))}`);
          return `
            <span class="ladder-market-line${entry.isPrimary ? ' is-active' : ''}">
              <span class="ladder-market-line-label">${escapeHtml(entry.label || 'Prop')}</span>
              <strong>${escapeHtml(formatNumber(entry.line, 1))}</strong>
              ${oddsBits.length ? `<span class="ladder-market-line-odds">${oddsBits.join(' / ')}</span>` : ''}
            </span>
          `;
        }).join('')}
      </div>
    `;
  }

  function renderTeamSelector(payload) {
    const options = Array.isArray(payload.teamOptions) ? payload.teamOptions : [];
    teamInputEl.innerHTML = [
      '<option value="">All teams</option>',
      ...options.map((option) => {
        const value = String(option.value || '');
        const selected = value === String(payload.selectedTeam || state.team || '') ? ' selected' : '';
        return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(option.label || value)}</option>`;
      }),
    ].join('');
    teamInputEl.value = String(payload.selectedTeam || state.team || '');
  }

  function renderPlayerSelector(payload) {
    const options = Array.isArray(payload.playerOptions) ? payload.playerOptions : [];
    playerInputEl.innerHTML = [
      '<option value="">All players</option>',
      ...options.map((option) => {
        const value = String(option.value || '');
        const selected = value === String(payload.selectedPlayer || state.player || '') ? ' selected' : '';
        return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(option.label || value)}</option>`;
      }),
    ].join('');
    playerInputEl.value = String(payload.selectedPlayer || state.player || '');
  }

  function renderSelectedPlayer(payload) {
    const currentValue = String(payload.selectedPlayer || state.player || '');
    if (!currentValue) {
      selectedPlayerEl.innerHTML = '';
      selectedPlayerEl.style.display = 'none';
      return;
    }
    const selected = (Array.isArray(payload.playerOptions) ? payload.playerOptions : []).find((option) => String(option.value || '') === currentValue)
      || (Array.isArray(payload.rows) ? payload.rows.find((row) => String(row.playerName || '') === currentValue) : null);
    if (!selected) {
      selectedPlayerEl.innerHTML = '';
      selectedPlayerEl.style.display = 'none';
      return;
    }
    const clearHref = pageHref(state.date, state.prop, state.team, '', state.sort);
    selectedPlayerEl.style.display = 'block';
    selectedPlayerEl.innerHTML = `
      <div class="ladder-selected-card">
        <div class="ladder-selected-identity">
          ${selected.teamLogoUrl ? `<img class="ladder-selected-team-logo" src="${escapeHtml(selected.teamLogoUrl)}" alt="team logo" loading="lazy" />` : ''}
          ${renderHeadshotMedia(selected.headshotUrl, selected.playerName || selected.hitterName || currentValue, selected.playerId || selected.hitterId, 'ladder-selected-headshot', 'ladder-selected-headshot ladder-player-headshot-fallback')}
          <div>
            <div class="ladder-selected-kicker">Selected player</div>
            <div class="ladder-selected-name">${escapeHtml(selected.playerName || selected.hitterName || currentValue)}</div>
            <div class="ladder-selected-meta">${escapeHtml(selected.label || selected.team || '')}</div>
          </div>
        </div>
        <a class="ladder-selected-clear" href="${clearHref}">Show all</a>
      </div>
    `;
  }

  function renderSummary(payload) {
    const summary = payload.summary || {};
    const simCounts = Array.isArray(summary.simCounts) ? summary.simCounts : [];
    summaryEl.innerHTML = `
      <article class="ladder-stat">
        <div class="ladder-stat-label">Date</div>
        <div class="ladder-stat-value">${escapeHtml(payload.date || state.date || '-')}</div>
      </article>
      <article class="ladder-stat">
        <div class="ladder-stat-label">Prop</div>
        <div class="ladder-stat-value">${escapeHtml(payload.propLabel || state.prop || '-')}</div>
      </article>
      <article class="ladder-stat">
        <div class="ladder-stat-label">Games</div>
        <div class="ladder-stat-value">${escapeHtml(formatCount(summary.games))}</div>
      </article>
      <article class="ladder-stat">
        <div class="ladder-stat-label">Players</div>
        <div class="ladder-stat-value">${escapeHtml(formatCount(summary.players))}</div>
      </article>
      <article class="ladder-stat">
        <div class="ladder-stat-label">Available players</div>
        <div class="ladder-stat-value">${escapeHtml(formatCount(summary.availablePlayers))}</div>
      </article>
      <article class="ladder-stat">
        <div class="ladder-stat-label">Sim counts seen</div>
        <div class="ladder-stat-value">${escapeHtml(simCounts.length ? simCounts.join(', ') : '-')}</div>
      </article>
    `;
  }

  function renderEmpty(payload) {
    const detail = payload && payload.note ? ` (${escapeHtml(payload.note)})` : '';
    gridEl.innerHTML = `<div class="ladder-empty">No player prop ladders found for this date and prop${detail}.</div>`;
  }

  function renderCard(row, payload) {
    const ladderRows = Array.isArray(row.ladder) ? row.ladder : [];
    const rowSourceMode = String(row.sourceMode || payload.sourceMode || '').toLowerCase();
    const isMarketFallback = rowSourceMode === 'market';
    const isEstimated = rowSourceMode === 'estimated' || String(row.ladderShape || '').toLowerCase() === 'estimated';
    const overLineText = row.marketLine == null || row.overLineCount == null
      ? ''
      : `<span class="ladder-pill"><span>Over ${escapeHtml(formatNumber(row.marketLine, 1))}</span><strong>${escapeHtml(formatCount(row.overLineCount))}</strong><span>${escapeHtml(formatPercent(row.overLineProb))}</span></span>`;
    const gameHref = row.gameId != null && payload.date
      ? `/game/${encodeURIComponent(String(row.gameId))}?date=${encodeURIComponent(String(payload.date))}`
      : '';
    const teamLogo = row.teamLogoUrl
      ? `<img class="ladder-team-logo ladder-team-logo-primary" src="${escapeHtml(row.teamLogoUrl)}" alt="${escapeHtml(row.team || 'Team')} logo" loading="lazy" />`
      : `<div class="ladder-team-logo ladder-team-logo-primary ladder-team-logo-fallback">${escapeHtml(String((row.team || '?').slice(0, 1) || '?'))}</div>`;
    const headshot = renderHeadshotMedia(row.headshotUrl, row.playerName || 'Player', row.playerId || row.hitterId, 'ladder-player-headshot', 'ladder-player-headshot ladder-player-headshot-fallback');
    const ladderTableRows = ladderRows.map((ladderRow) => `
      <tr>
        <td>${escapeHtml(formatCount(ladderRow.total))}</td>
        <td>${escapeHtml(formatCount(ladderRow.hitCount))}</td>
        <td>${escapeHtml(formatPercent(ladderRow.hitProb))}</td>
        <td>${escapeHtml(thresholdBookOdds(row, ladderRow.total))}</td>
        <td>${escapeHtml(formatCount(ladderRow.exactCount))}</td>
        <td>${escapeHtml(formatPercent(ladderRow.exactProb))}</td>
      </tr>
    `).join('');

    return `
      <article class="ladder-card">
        <div class="ladder-card-head">
          <div class="ladder-card-identity">
            ${headshot}
            <div>
              <h2 class="ladder-card-title">${escapeHtml(row.playerName || 'Unknown player')}</h2>
              <div class="ladder-card-subtitle">${escapeHtml(row.matchup || `${row.team || ''} vs ${row.opponent || ''}`)}</div>
            </div>
          </div>
          <div class="ladder-card-actions">
            ${gameHref ? `<a class="ladder-card-link" href="${gameHref}">Game view</a>` : ''}
            ${teamLogo}
          </div>
        </div>
        <div class="ladder-pills">
          <span class="ladder-pill"><span>Mean</span><strong>${escapeHtml(formatNumber(row.mean, 2))}</strong></span>
          <span class="ladder-pill"><span>Mode</span><strong>${escapeHtml(row.mode == null ? '-' : formatCount(row.mode))}</strong><span>${escapeHtml(row.modeProb == null ? '-' : formatPercent(row.modeProb))}</span></span>
          <span class="ladder-pill"><span>Sim count</span><strong>${escapeHtml(formatCount(row.simCount))}</strong></span>
          ${isEstimated ? '<span class="ladder-pill"><span>Source</span><strong>Estimated SmartSim</strong></span>' : ''}
          ${row.side ? `<span class="ladder-pill"><span>Side</span><strong>${escapeHtml(row.side)}</strong></span>` : ''}
          ${row.marketLine == null ? '' : `<span class="ladder-pill"><span>Market line</span><strong>${escapeHtml(formatNumber(row.marketLine, 1))}</strong></span>`}
          ${overLineText}
        </div>
        ${renderMarketLineChips(row)}
        ${isEstimated ? '<div class="ladder-empty">Exact ladder counts were not embedded in the SmartSim artifact for this date. This ladder is reconstructed from the same-day SmartSim mean and variance.</div>' : ''}
        ${ladderRows.length ? `
          <div class="ladder-table-wrap">
            <table class="ladder-table">
              <thead>
                <tr>
                  <th>Total</th>
                  <th>&ge; Total</th>
                  <th>Hit %</th>
                  <th>Hit Odds</th>
                  <th>Exact</th>
                  <th>Exact %</th>
                </tr>
              </thead>
              <tbody>
                ${ladderTableRows}
              </tbody>
            </table>
          </div>
        ` : isMarketFallback ? `<div class="ladder-empty">Exact SmartSim hit probabilities are not stored for this date yet. Showing the available ladder market rungs instead.</div>` : ''}
      </article>
    `;
  }

  function renderPayload(payload) {
    const sourceMode = String(payload.sourceMode || 'exact').toLowerCase();
    const summary = payload.summary || {};
    dateBadgeEl.textContent = payload.date || state.date || '-';
    propBadgeEl.textContent = payload.propLabel || payload.prop || state.prop || '-';
    renderTeamSelector(payload);
    renderPlayerSelector(payload);
    renderSelectedPlayer(payload);
    sortInputEl.value = String(payload.selectedSort || state.sort || 'team');
    const sortLabel = String((Array.isArray(payload.sortOptions) ? payload.sortOptions : []).find((option) => String(option.value || '') === String(payload.selectedSort || state.sort || 'team'))?.label || (payload.selectedSort || state.sort || 'team'));
    headerMetaEl.textContent = payload.found
      ? sourceMode === 'estimated'
        ? `${summary.players || 0} players across ${summary.games || 0} games from reconstructed SmartSim ladders built from same-day player summary moments. Exact rung counts were not embedded for this date. Sorted by ${sortLabel}.${state.team ? ` Filtered to team ${state.team}.` : ''}${state.player ? ` Filtered to player ${state.player}.` : ''}`
        : sourceMode === 'market'
        ? `${summary.players || 0} players across ${summary.games || 0} games from available market ladder rungs. Exact SmartSim distributions are not stored for this date yet. Sorted by ${sortLabel}.${state.team ? ` Filtered to team ${state.team}.` : ''}${state.player ? ` Filtered to player ${state.player}.` : ''}`
        : sourceMode === 'mixed'
          ? `${summary.players || 0} players across ${summary.games || 0} games with a mix of exact SmartSim ladders, reconstructed SmartSim ladders, and market fallback rows where needed. Sorted by ${sortLabel}.${state.team ? ` Filtered to team ${state.team}.` : ''}${state.player ? ` Filtered to player ${state.player}.` : ''}`
          : `${summary.players || 0} players across ${summary.games || 0} games from stored exact SmartSim player distributions. Sorted by ${sortLabel}.${state.team ? ` Filtered to team ${state.team}.` : ''}${state.player ? ` Filtered to player ${state.player}.` : ''}`
      : 'No player prop ladder data found for this selection.';
    sourceMetaEl.textContent = `Sim dir: ${payload.sourceDir || '-'} | Market source: ${payload.marketSource || '-'} | Default daily sims: ${payload.defaultSims || '-'} | Mode: ${sourceMode} | Shape: ${payload.ladderShape || 'exact'} ladder`;
    prevDateLinkEl.href = pageHref((payload.nav || {}).prevDate || state.date, state.prop, state.team, state.player, state.sort);
    nextDateLinkEl.href = pageHref((payload.nav || {}).nextDate || state.date, state.prop, state.team, state.player, state.sort);
    prevDateLinkEl.style.visibility = (payload.nav || {}).prevDate ? 'visible' : 'hidden';
    nextDateLinkEl.style.visibility = (payload.nav || {}).nextDate ? 'visible' : 'hidden';
    renderSummary(payload);
    if (!payload.found || !Array.isArray(payload.rows) || !payload.rows.length) {
      renderEmpty(payload);
      return;
    }
    gridEl.innerHTML = payload.rows.map((row) => renderCard(row, payload)).join('');
  }

  function showNote(message) {
    const text = String(message || '').trim();
    noteEl.textContent = text;
    noteEl.classList.toggle('hidden', !text);
  }

  async function loadPayload() {
    dateInputEl.value = state.date;
    propInputEl.value = state.prop;
    teamInputEl.value = state.team;
    playerInputEl.value = state.player;
    sortInputEl.value = state.sort;
    gridEl.innerHTML = '<div class="cards-loading-state">Loading player prop ladders...</div>';
    summaryEl.innerHTML = '<div class="cards-loading-state">Loading ladder summary...</div>';

    const response = await fetch(apiHref(state.date, state.prop, state.team, state.player, state.sort), { cache: 'no-store' });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload && payload.error ? payload.error : 'Failed to load player prop ladders.');
    }
    renderPayload(payload);
    showNote(payload.note || '');
  }

  formEl.addEventListener('submit', async (event) => {
    event.preventDefault();
    state.date = String(dateInputEl.value || getLocalDateISO());
    state.prop = String(propInputEl.value || 'pts');
    state.team = String(teamInputEl.value || '');
    state.player = String(playerInputEl.value || '');
    state.sort = String(sortInputEl.value || 'team');
    window.history.replaceState({}, '', pageHref(state.date, state.prop, state.team, state.player, state.sort));
    try {
      await loadPayload();
    } catch (error) {
      headerMetaEl.textContent = 'Failed to load player prop ladders.';
      sourceMetaEl.textContent = String(error && error.message ? error.message : error || 'unknown error');
      summaryEl.innerHTML = '<div class="ladder-empty">Player prop ladder data could not be loaded.</div>';
      selectedPlayerEl.innerHTML = '';
      gridEl.innerHTML = `<div class="ladder-empty">${escapeHtml(error && error.message ? error.message : error || 'unknown error')}</div>`;
      showNote(error && error.message ? error.message : String(error || 'unknown error'));
    }
  });

  teamInputEl.addEventListener('change', () => {
    if (formEl.requestSubmit) {
      formEl.requestSubmit();
      return;
    }
    formEl.dispatchEvent(new Event('submit', { cancelable: true }));
  });

  playerInputEl.addEventListener('change', () => {
    if (formEl.requestSubmit) {
      formEl.requestSubmit();
      return;
    }
    formEl.dispatchEvent(new Event('submit', { cancelable: true }));
  });

  sortInputEl.addEventListener('change', () => {
    if (formEl.requestSubmit) {
      formEl.requestSubmit();
      return;
    }
    formEl.dispatchEvent(new Event('submit', { cancelable: true }));
  });

  loadPayload().catch((error) => {
    headerMetaEl.textContent = 'Failed to load player prop ladders.';
    sourceMetaEl.textContent = String(error && error.message ? error.message : error || 'unknown error');
    summaryEl.innerHTML = '<div class="ladder-empty">Player prop ladder data could not be loaded.</div>';
    selectedPlayerEl.innerHTML = '';
    gridEl.innerHTML = `<div class="ladder-empty">${escapeHtml(error && error.message ? error.message : error || 'unknown error')}</div>`;
    showNote(error && error.message ? error.message : String(error || 'unknown error'));
  });
})();