const API_URL = 'http://127.0.0.1:8000/surge';
const POLLING_INTERVAL = 2000; // 2 seconds

// State to track previous values to detect changes
let previousState = {};

// DOM Elements
const gridContainer = document.getElementById('zone-grid');
const connectionDot = document.getElementById('connection-dot');
const connectionText = document.getElementById('connection-text');
const syncTime = document.getElementById('sync-time');
const activityFeed = document.getElementById('activity-feed');

// Initialize the 5 zones to ensure they always show up
const initialZones = ['zone_1', 'zone_2', 'zone_3', 'zone_4', 'zone_5'];

function initGrid() {
    gridContainer.innerHTML = '';
    initialZones.forEach(zone => {
        const card = document.createElement('div');
        card.id = `card-${zone}`;
        card.className = 'zone-card state-normal';
        
        card.innerHTML = `
            <div class="zone-header">
                <span class="zone-title">${zone.replace('_', ' ')}</span>
                <span class="status-badge" id="badge-${zone}">Normal</span>
            </div>
            <div class="surge-value">
                <div id="val-${zone}">1.0</div><span>x</span>
            </div>
        `;
        gridContainer.appendChild(card);
        previousState[zone] = 1.0;
    });
}

function updateConnectionStatus(isConnected) {
    if (isConnected) {
        connectionDot.className = 'dot connected';
        connectionText.textContent = 'Live';
        
        const now = new Date();
        syncTime.textContent = now.toLocaleTimeString([], { hour12: false });
    } else {
        connectionDot.className = 'dot disconnected';
        connectionText.textContent = 'Offline';
    }
}

function formatMultiplier(val) {
    return Number(val).toFixed(1);
}

function determineState(multiplier) {
    if (multiplier >= 2.0) return { class: 'state-surge', text: 'High Surge' };
    if (multiplier >= 1.5) return { class: 'state-elevated', text: 'Elevated' };
    return { class: 'state-normal', text: 'Normal' };
}

function logActivity(zone, oldVal, newVal) {
    const entry = document.createElement('div');
    const isIncrease = newVal > oldVal;
    
    entry.className = `log-entry ${isIncrease ? 'increase' : 'decrease'}`;
    
    const now = new Date();
    const timeStr = now.toLocaleTimeString([], { hour12: false, hour: '2-digit', minute:'2-digit', second:'2-digit' });
    
    entry.innerHTML = `
        <span class="log-time">${timeStr}</span>
        <strong>${zone.replace('_', ' ')}</strong> ${isIncrease ? 'rose' : 'dropped'} to ${formatMultiplier(newVal)}x
    `;
    
    activityFeed.prepend(entry);
    
    // Keep feed clean (max 50 entries)
    if (activityFeed.children.length > 50) {
        activityFeed.removeChild(activityFeed.lastChild);
    }
}

async function fetchSurgeData() {
    try {
        const response = await fetch(API_URL);
        if (!response.ok) throw new Error('Network response was not ok');
        
        const json = await response.json();
        const data = json.data;
        
        updateConnectionStatus(true);
        
        // Update DOM
        data.forEach(item => {
            const zone = item.zone_id;
            const multiplier = item.surge_multiplier;
            
            // If it's a new zone we didn't init, we'd need to create it, 
            // but for this project we assume zone_1 to zone_5
            const card = document.getElementById(`card-${zone}`);
            if (!card) return;
            
            const prev = previousState[zone] || 1.0;
            
            if (prev !== multiplier) {
                // Update UI
                document.getElementById(`val-${zone}`).textContent = formatMultiplier(multiplier);
                
                const stateInfo = determineState(multiplier);
                card.className = `zone-card ${stateInfo.class}`;
                document.getElementById(`badge-${zone}`).textContent = stateInfo.text;
                
                // Log it
                logActivity(zone, prev, multiplier);
                
                previousState[zone] = multiplier;
            }
        });
        
    } catch (error) {
        console.error('Fetch error:', error);
        updateConnectionStatus(false);
    }
}

// Start app
initGrid();
fetchSurgeData(); // Initial fetch
setInterval(fetchSurgeData, POLLING_INTERVAL);
