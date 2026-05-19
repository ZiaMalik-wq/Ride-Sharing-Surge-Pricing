const API_BASE = 'http://127.0.0.1:8000';
const LIVE_POLL_INTERVAL = 3000;
const HEALTH_POLL_INTERVAL = 10000;

let zoneNames = {};
let selectedZone = null;
let selectedDays = 1;
let zoneOrder = [];
let chartInstance = null;
let lastLivePayload = [];
let previousState = {};
let liveZonesLoaded = false;
let _lastTopOrder = '';

const elements = {
    grid: document.getElementById('zone-grid'),
    connectionDot: document.getElementById('connection-dot'),
    connectionText: document.getElementById('connection-text'),
    syncTime: document.getElementById('sync-time'),
    pipelineState: document.getElementById('pipeline-state'),
    activityFeed: document.getElementById('activity-feed'),
    zoneSelect: document.getElementById('zone-select'),
    topZonesList: document.getElementById('top-zones-list'),
    healthGrid: document.getElementById('health-grid'),
    metricAverageSurge: document.getElementById('metric-average-surge'),
    metricMaxSurge: document.getElementById('metric-max-surge'),
    metricHealth: document.getElementById('metric-health'),
    metricLastUpdate: document.getElementById('metric-last-update'),
    metricLag: document.getElementById('metric-lag'),
    summaryActiveZones: document.getElementById('summary-active-zones'),
    summaryAverageSurge: document.getElementById('summary-average-surge'),
    summaryTopZoneName: document.getElementById('summary-top-zone-name'),
    summaryTopZoneVal: document.getElementById('summary-top-zone-val'),
    metricMaxZone: document.getElementById('metric-max-zone'),
    trendCanvas: document.getElementById('trend-chart'),
    zoneSemantics: document.getElementById('zone-semantics'),
    rangeFilter: document.getElementById('range-filter'),
    zoneSearch: document.getElementById('zone-search'),
};

function apiUrl(path) {
    return `${API_BASE}${path}`;
}

function getZoneName(id) {
    return zoneNames[String(id)] || `Zone ${id}`;
}

function formatMultiplier(val) {
    const num = Number(val || 0);
    return Number.isFinite(num) ? num.toFixed(1) : '1.0';
}

function determineState(multiplier) {
    if (multiplier >= 2.0) return { class: 'state-surge', text: 'High Surge' };
    if (multiplier >= 1.5) return { class: 'state-elevated', text: 'Elevated' };
    return { class: 'state-normal', text: 'Normal' };
}

function formatTime(value) {
    if (!value) return '--';
    try {
        // Ensure strings like "2026-05-08 15:13:16" are treated as UTC if they don't have a zone
        let dateStr = String(value);
        if (!dateStr.includes('Z') && !dateStr.includes('+')) {
            dateStr += 'Z'; 
        }
        return new Date(dateStr).toLocaleTimeString([], { 
            hour: '2-digit', 
            minute: '2-digit', 
            second: '2-digit',
            hour12: false 
        });
    } catch {
        return value;
    }
}

function logActivity(zone, oldVal, newVal) {
    const entry = document.createElement('div');
    const isIncrease = Number(newVal) > Number(oldVal);
    entry.className = `log-entry ${isIncrease ? 'increase' : 'decrease'}`;

    const timeStr = new Date().toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
    entry.innerHTML = `
        <span class="log-time">${timeStr}</span>
        <strong>${getZoneName(zone)}</strong> ${isIncrease ? 'rose' : 'dropped'} to ${formatMultiplier(newVal)}x
    `;

    elements.activityFeed.prepend(entry);
    while (elements.activityFeed.children.length > 18) {
        elements.activityFeed.removeChild(elements.activityFeed.lastChild);
    }
}

function ensureZoneOption(zoneId) {
    const exists = Array.from(elements.zoneSelect.options).some(option => option.value === String(zoneId));
    if (!exists) {
        const option = document.createElement('option');
        option.value = String(zoneId);
        option.textContent = getZoneName(zoneId);
        elements.zoneSelect.appendChild(option);
    }
}

function createZoneCard(zoneId) {
    const card = document.createElement('article');
    card.id = `card-${zoneId}`;
    card.className = 'zone-card state-normal';
    card.setAttribute('data-surge', '1.0');
    card.innerHTML = `
        <div class="zone-header">
            <span class="zone-title">${getZoneName(zoneId)}</span>
            <span class="status-badge" id="badge-${zoneId}">Normal</span>
        </div>
        <div class="surge-value">
            <div id="val-${zoneId}">1.0</div><span>x</span>
        </div>
        <div class="zone-details">
            <div>Demand<strong id="demand-${zoneId}">--</strong></div>
            <div>Supply<strong id="supply-${zoneId}">--</strong></div>
            <div title="Time in the 2023 dataset">Data Time<strong id="window-${zoneId}">--</strong></div>
            <div title="Actual time received">Last Sync<strong id="updated-${zoneId}">--</strong></div>
        </div>
    `;
    elements.grid.appendChild(card);
    return card;
}

function sortGrid() {
    const cards = Array.from(elements.grid.children);
    cards.sort((a, b) => Number.parseFloat(b.getAttribute('data-surge')) - Number.parseFloat(a.getAttribute('data-surge')));
    cards.forEach(card => elements.grid.appendChild(card));
    zoneOrder = cards.map(card => card.id.replace('card-', ''));
}

function updateConnectionStatus(isConnected) {
    elements.connectionDot.className = `status-dot ${isConnected ? 'connected' : 'disconnected'}`;
    elements.connectionText.textContent = isConnected ? 'Live' : 'Offline';
    if (isConnected) {
        elements.syncTime.textContent = new Date().toLocaleTimeString([], { hour12: false });
        elements.pipelineState.textContent = 'Streaming';
    } else {
        elements.pipelineState.textContent = 'Disconnected';
    }
}

function setChartData(points) {
    const labels = points.map(point => new Date(point.window_end || point.updated_at || point.window_start).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }));
    const values = points.map(point => Number(point.surge_multiplier || 0));

    if (!chartInstance) {
        const ctx = elements.trendCanvas.getContext('2d');
        chartInstance = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [{
                    label: 'Surge multiplier',
                    data: values,
                    borderColor: '#6ea8fe',
                    backgroundColor: 'rgba(110, 168, 254, 0.18)',
                    tension: 0.28,
                    fill: true,
                    pointRadius: 2,
                    pointHoverRadius: 5,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        backgroundColor: '#07111f',
                        borderColor: 'rgba(148, 163, 184, 0.18)',
                        borderWidth: 1,
                    },
                },
                scales: {
                    x: {
                        grid: { color: 'rgba(148, 163, 184, 0.08)' },
                        ticks: { color: '#90a3c4' },
                    },
                    y: {
                        grid: { color: 'rgba(148, 163, 184, 0.08)' },
                        ticks: { color: '#90a3c4' },
                    },
                },
            },
        });
    } else {
        chartInstance.data.labels = labels;
        chartInstance.data.datasets[0].data = values;
        chartInstance.update();
    }
}

function renderTopZones(zones) {
    elements.topZonesList.innerHTML = '';

    zones.slice(0, 10).forEach((zone, index) => {
        const row = document.createElement('div');
        row.className = 'rank-item';
        row.innerHTML = `
            <div>
                <span class="metric-label">#${index + 1}</span>
                <strong>${getZoneName(zone.zone_id)}</strong>
                <small>${zone.demand ?? 0} demand / ${zone.supply ?? 0} supply</small>
            </div>
            <strong>${formatMultiplier(zone.surge_multiplier)}x</strong>
        `;
        elements.topZonesList.appendChild(row);
    });
}

function renderHealth(health) {
    elements.healthGrid.innerHTML = '';

    const items = [
        { label: 'Redis', value: health.redis, detail: 'live state' },
        { label: 'Cassandra', value: health.cassandra, detail: 'historical archive' },
    ];

    items.forEach(item => {
        const row = document.createElement('div');
        row.className = 'health-item';
        row.innerHTML = `
            <div>
                <span class="metric-label">${item.label}</span>
                <strong>${item.detail}</strong>
            </div>
            <span class="health-pill ${item.value === 'up' ? 'pill-up' : 'pill-down'}">${item.value}</span>
        `;
        elements.healthGrid.appendChild(row);
    });

    elements.metricHealth.textContent = health.redis === 'up' && health.cassandra === 'up' ? 'Healthy' : 'Degraded';
}

function formatMaybeNumber(value, suffix = '') {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
        return '--';
    }
    return `${Number(value).toFixed(1)}${suffix}`;
}

function renderZoneSemantics(trend) {
    const items = [
        { label: 'Total demand', value: trend.total_demand ?? 0 },
        { label: 'Total supply', value: trend.total_supply ?? 0 },
        { label: 'Volatility', value: trend.surge_volatility ?? 0, suffix: 'x' },
        { label: 'Change rate', value: trend.surge_change_rate ?? 0, suffix: 'x' },
        { label: 'Freshness', value: trend.freshness_seconds, suffix: 's' },
        { label: 'Latest update', value: trend.latest_update ? formatTime(trend.latest_update) : '--' },
    ];

    elements.zoneSemantics.innerHTML = '';
    items.forEach(item => {
        const card = document.createElement('div');
        card.className = 'semantic-item';
        const displayValue = item.label === 'Latest update'
            ? item.value
            : item.label === 'Freshness'
                ? (item.value === null || item.value === undefined ? '--' : `${Number(item.value).toFixed(1)}${item.suffix || ''}`)
                : formatMaybeNumber(item.value, item.suffix || '');

        card.innerHTML = `
            <span>${item.label}</span>
            <strong>${displayValue}</strong>
        `;
        elements.zoneSemantics.appendChild(card);
    });
}

function setSummary(summary) {
    elements.summaryActiveZones.textContent = summary.active_zones ?? '--';
    elements.summaryAverageSurge.textContent = `${formatMultiplier(summary.average_surge)}x`;

    if (summary.top_zone) {
        elements.summaryTopZoneName.textContent = getZoneName(summary.top_zone.zone_id);
        elements.summaryTopZoneVal.textContent = `${formatMultiplier(summary.top_zone.surge_multiplier)}x`;
    } else {
        elements.summaryTopZoneName.textContent = '--';
        elements.summaryTopZoneVal.textContent = '--';
    }
    elements.metricAverageSurge.textContent = `${formatMultiplier(summary.average_surge)}x`;
    elements.metricMaxSurge.textContent = `${formatMultiplier(summary.max_surge)}x`;
    elements.metricMaxZone.textContent = summary.top_zone ? getZoneName(summary.top_zone.zone_id) : 'No active zone';
    elements.metricLastUpdate.textContent = formatTime(summary.latest_update);
    elements.metricLag.textContent = summary.latest_update ? `Updated ${formatTime(summary.latest_update)}` : 'Waiting for first update';
}

function updateLiveGrid(records) {
    lastLivePayload = records;
    const seenZones = new Set();

    // Sort records by multiplier ASC so that when we prepend them to the log,
    // the Highest surge values end up at the very top of the feed.
    const sortedRecords = [...records].sort((a, b) => 
        Number(a.surge_multiplier || 1.0) - Number(b.surge_multiplier || 1.0)
    );

    sortedRecords.forEach(record => {
        const zoneId = String(record.zone_id);
        seenZones.add(zoneId);
        ensureZoneOption(zoneId);

        let card = document.getElementById(`card-${zoneId}`);
        if (!card) {
            card = createZoneCard(zoneId);
            previousState[zoneId] = 1.0;
        }

        const multiplier = Number(record.surge_multiplier || 1.0);
        const prev = Number(previousState[zoneId] ?? 1.0);
        const stateInfo = determineState(multiplier);

        // Only update DOM if values changed
        if (card.getAttribute('data-surge') !== String(multiplier)) {
            card.className = `zone-card ${stateInfo.class}`;
            card.setAttribute('data-surge', String(multiplier));
            document.getElementById(`val-${zoneId}`).textContent = formatMultiplier(multiplier);
            document.getElementById(`badge-${zoneId}`).textContent = stateInfo.text;
        }

        const demandEl = document.getElementById(`demand-${zoneId}`);
        const demandVal = String(record.demand ?? 0);
        if (demandEl.textContent !== demandVal) demandEl.textContent = demandVal;

        const supplyEl = document.getElementById(`supply-${zoneId}`);
        const supplyVal = String(record.supply ?? 0);
        if (supplyEl.textContent !== supplyVal) supplyEl.textContent = supplyVal;

        const windowVal = record.window_end ? formatTime(record.window_end) : '--';
        const windowEl = document.getElementById(`window-${zoneId}`);
        if (windowEl.textContent !== windowVal) windowEl.textContent = windowVal;

        const updatedVal = record.updated_at ? formatTime(record.updated_at) : '--';
        const updatedEl = document.getElementById(`updated-${zoneId}`);
        if (updatedEl.textContent !== updatedVal) updatedEl.textContent = updatedVal;

        // Apply search filter
        const query = (elements.zoneSearch.value || '').toLowerCase();
        const zoneName = getZoneName(zoneId).toLowerCase();
        if (query && !zoneName.includes(query)) {
            card.style.display = 'none';
        } else {
            card.style.display = '';
        }

        if (multiplier.toFixed(1) !== prev.toFixed(1)) {
            // Only log activity if the card is currently visible (passing search filter)
            if (card.style.display !== 'none') {
                logActivity(zoneId, prev, multiplier);
            }
            previousState[zoneId] = multiplier;
        }
    });

    // Smart Sorting: Only sort if the top 5 zones changed (to save CPU)
    const currentTop = records.slice(0, 5).map(r => r.zone_id).join(',');
    if (_lastTopOrder !== currentTop) {
        sortGrid();
        _lastTopOrder = currentTop;
    }

    zoneOrder.forEach(zoneId => {
        if (!seenZones.has(zoneId)) {
            const card = document.getElementById(`card-${zoneId}`);
            if (card) {
                card.classList.add('state-normal');
                card.setAttribute('data-surge', '1.0');
            }
        }
    });

    sortGrid();

    if (!selectedZone && zoneOrder.length) {
        selectedZone = zoneOrder[0];
        elements.zoneSelect.value = selectedZone;
    }
}

async function loadZoneNames() {
    try {
        const response = await fetch('zones.json');
        zoneNames = await response.json();
    } catch (error) {
        console.error('Error loading zone names:', error);
        zoneNames = {};
    }
}

async function fetchJson(path) {
    try {
        const response = await fetch(apiUrl(path));
        if (!response.ok) {
            throw new Error(`Request failed: ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        showError(`API Error: ${error.message}`);
        throw error;
    }
}

function showError(message) {
    const toast = document.getElementById('error-toast');
    if (!toast) return;
    
    toast.textContent = message;
    toast.classList.add('visible');
    
    clearTimeout(toast._timeout);
    toast._timeout = setTimeout(() => {
        toast.classList.remove('visible');
    }, 5000);
}

async function refreshLive() {
    try {
        const [current, summary, topZones] = await Promise.all([
            fetchJson('/surge/current'),
            fetchJson('/analytics/summary'),
            fetchJson('/analytics/top-zones?limit=10'),
        ]);

        const liveRecords = current.data || [];
        updateLiveGrid(liveRecords);
        setSummary(summary.data || {});
        renderTopZones(topZones.data || []);

        if (!liveZonesLoaded) {
            liveZonesLoaded = true;
            if (!selectedZone && liveRecords.length) {
                selectedZone = String(liveRecords[0].zone_id);
            }
            syncZoneSelect();
        }

        updateConnectionStatus(true);
    } catch (error) {
        console.error('Live refresh error:', error);
        updateConnectionStatus(false);
    }
}

async function refreshHistory() {
    if (!selectedZone) return;

    try {
        const [history, trend] = await Promise.all([
            fetchJson(`/analytics/zones/${encodeURIComponent(selectedZone)}/history?limit=120&days=${selectedDays}`),
            fetchJson(`/analytics/zones/${encodeURIComponent(selectedZone)}/trend?limit=120`),
        ]);

        setChartData((history.data || []).slice().reverse());
        renderZoneSemantics(trend.data || {});
    } catch (error) {
        console.error('History refresh error:', error);
    }
}

async function refreshHealth() {
    try {
        const health = await fetchJson('/analytics/system/health');
        renderHealth(health.data || {});
    } catch (error) {
        console.error('Health refresh error:', error);
    }
}

function syncZoneSelect() {
    elements.zoneSelect.innerHTML = '';

    const source = zoneOrder.length ? zoneOrder : lastLivePayload.map(item => String(item.zone_id));
    source.forEach(zoneId => {
        const option = document.createElement('option');
        option.value = String(zoneId);
        option.textContent = getZoneName(zoneId);
        elements.zoneSelect.appendChild(option);
    });

    if (selectedZone) {
        elements.zoneSelect.value = selectedZone;
    } else if (source.length) {
        selectedZone = String(source[0]);
        elements.zoneSelect.value = selectedZone;
    }
}

elements.zoneSelect.addEventListener('change', async event => {
    selectedZone = String(event.target.value);
    await refreshHistory();
});

elements.rangeFilter.addEventListener('click', async event => {
    const button = event.target.closest('.segment');
    if (!button) return;

    Array.from(elements.rangeFilter.querySelectorAll('.segment')).forEach(segment => segment.classList.remove('active'));
    button.classList.add('active');
    selectedDays = Number(button.dataset.days || 1);
    await refreshHistory();
});

elements.zoneSearch.addEventListener('input', () => {
    const query = elements.zoneSearch.value.toLowerCase();
    const cards = Array.from(elements.grid.children);
    
    cards.forEach(card => {
        const zoneId = card.id.replace('card-', '');
        const name = getZoneName(zoneId).toLowerCase();
        card.style.display = name.includes(query) ? '' : 'none';
    });
});

(async () => {
    await loadZoneNames();
    await refreshLive();
    await refreshHistory();
    await refreshHealth();

    setInterval(refreshLive, LIVE_POLL_INTERVAL);
    setInterval(refreshHealth, HEALTH_POLL_INTERVAL);
    setInterval(() => {
        if (selectedZone) {
            refreshHistory();
        }
    }, 15000);
})();
