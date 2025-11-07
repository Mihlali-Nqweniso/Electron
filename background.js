// Listens for a message from the popup script.
async function getSettings() {
    // Robust guard in case storage is not yet initialized
    const maxTries = 5;
    let tries = 0;
    while (tries < maxTries) {
        try {
            if (chrome && chrome.storage && chrome.storage.local) {
                return await new Promise((resolve) => {
                    chrome.storage.local.get(['geminiApiKey', 'geminiModel', 'visionEnabled', 'rateRpm', 'maxConcurrent', 'fastMode'], (res) => {
                        resolve({
                            apiKey: res.geminiApiKey || '',
                            model: res.geminiModel || 'gemini-2.5-flash-preview-09-2025',
                            vision: typeof res.visionEnabled === 'boolean' ? res.visionEnabled : true,
                            rateRpm: typeof res.rateRpm === 'number' ? res.rateRpm : 12,
                            maxConcurrent: typeof res.maxConcurrent === 'number' ? res.maxConcurrent : 1,
                            fastMode: !!res.fastMode
                        });
                    });
                });
            }
        } catch (_) {}
        await new Promise(r => setTimeout(r, 100));
        tries++;
    }
    // Fallback defaults if storage unavailable
    return { apiKey: '', model: 'gemini-2.5-flash-preview-09-2025', vision: true, rateRpm: 12, maxConcurrent: 1, fastMode: false };
}
function geminiUrl(model, apiKey) {
    return `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
}
// --- Sequential execution queue and notification ---
const execQueue = [];
let execProcessing = false;

function notifyComplete(title, message) {
    try { chrome.notifications.create('', { type: 'basic', iconUrl: 'images/icon128.png', title, message }); } catch (_) {}
}

async function executeTaskInternal(request) {
    const command = request.command;
    const settings = await getSettings();
    updateSchedulerFromSettings(settings);
    const apiKey = settings.apiKey;
    const model = settings.model;
    if (!apiKey) {
        return { success: false, needClarification: true, error: 'No API key set. Open Options and save your key, then run again.' };
    }
    await validateApiKey(apiKey, model);
    try {
        const tabs = await new Promise((resolve, reject) => {
            chrome.tabs.query({ active: true, currentWindow: true }, (res) => {
                if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
                resolve(res);
            });
        });
        const tab = tabs && tabs[0];
        if (!tab || !tab.id) throw new Error('No active tab');
        if (isRestrictedUrl(tab.url)) {
            const quickContext = { title: 'Restricted page', url: tab.url || '', text: '' };
            const quickPlan = await planQuickAction({ goalCommand: command, context: quickContext }, apiKey, model);
            if (quickPlan && quickPlan.action === 'navigate' && quickPlan.url) {
                const newTab = await openTabToUrl(quickPlan.url);
                const ctx = await getPageContextSnapshot(newTab.id);
                const followPlan = await planQuickAction({ goalCommand: command, context: ctx }, apiKey, model);
                if (followPlan && followPlan.action && followPlan.action !== 'ask' && followPlan.action !== 'done') {
                    await executePlannedAction(newTab.id, followPlan);
                    const postCtx = await getPageContextSnapshot(newTab.id);
                    const ok = await verifyGoalQuick(command, postCtx, apiKey, model);
                    if (ok) { return { success: true }; }
                }
            } else {
                return { success: false, needClarification: true, error: 'This page is restricted (chrome:// or Web Store). Switch to a normal site or include a target URL in your command.' };
            }
        }
        const quickContext = await getPageContextSnapshot(tab.id);
        const quickPlan = await planQuickAction({ goalCommand: command, context: quickContext }, apiKey, model);
        if (quickPlan && typeof quickPlan.action === 'string' && quickPlan.action !== 'ask' && quickPlan.action !== 'done') {
            if (request.preview) {
                return { success: false, plan: quickPlan };
            }
            await executePlannedAction(tab.id, quickPlan);
            const postContext = await getPageContextSnapshot(tab.id);
            const verified = await verifyGoalQuick(command, postContext, apiKey, model);
            if (verified) {
                return { success: true };
            }
        }
    } catch (_) { /* ignore and fall back to vision */ }

    if (settings.vision === false || (typeof visionPausedUntil === 'number' && Date.now() < visionPausedUntil)) {
        const waitSec = (typeof visionPausedUntil === 'number' ? Math.max(0, Math.ceil((visionPausedUntil - Date.now()) / 1000)) : 0);
        const msg = waitSec > 0
            ? `Vision is paused for ~${waitSec}s due to rate limits. You can: 1) Wait, 2) Turn Vision Off in Options and re-run, or 3) Use Plan (preview) first.`
            : 'Vision is off. Turn Vision On in Options, or use Plan (preview) for text-only steps.';
        return { success: false, needClarification: true, error: msg };
    }
    const finalResult = await runVisionPlanActLoop(command, apiKey, model);
    if (finalResult && finalResult.success) {
        return { success: true };
    }
    if (finalResult && finalResult.needClarification) {
        return { success: false, needClarification: true, error: finalResult.question || 'Need clarification' };
    }
    return { success: false, needClarification: true, error: finalResult && finalResult.reason ? `${finalResult.reason}. You can try: Plan first, scroll the page, or refine the command.` : 'Could not complete the task. Try Plan first or refine your command.' };
}

async function processExecQueue() {
    if (execProcessing) return;
    const item = execQueue.shift();
    if (!item) return;
    execProcessing = true;
    const { request, sendResponse } = item;
    try {
        const res = await executeTaskInternal(request);
        if (res && res.success) notifyComplete('Electron', 'Task completed successfully');
        sendResponse(res);
    } catch (error) {
        const message = error && error.message ? error.message : String(error);
        if (/HTTP 429/.test(message)) {
            const waitLeft = typeof apiScheduler === 'object' && apiScheduler.pausedUntil ? Math.max(0, Math.ceil((apiScheduler.pausedUntil - Date.now()) / 1000)) : 20;
            sendResponse({ success: false, needClarification: true, error: `Rate limit reached. Wait ~${waitLeft}s, then try again. Tip: lower RPM in Options (e.g., 6–10) and keep Concurrency at 1.` });
        } else if (/No active tab/i.test(message)) {
            sendResponse({ success: false, needClarification: true, error: 'No active tab detected. Open a normal website tab and try again.' });
        } else {
            sendResponse({ success: false, needClarification: true, error: `${message}. Try Plan first or refine your command.` });
        }
    } finally {
        execProcessing = false;
        setTimeout(() => processExecQueue(), 0);
    }
}

chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === "executeTask") {
        execQueue.push({ request, sendResponse });
        processExecQueue();
        return true;
    }
    if (request.action === 'planTask') {
        (async () => {
            try {
                const { apiKey, model, rateRpm, maxConcurrent } = await getSettings();
                updateSchedulerFromSettings({ rateRpm, maxConcurrent });
                if (!apiKey) { sendResponse({ success: false, needClarification: true, error: 'No API key set. Open Options and save your key, then run again.' }); return; }
                const tabs = await new Promise((resolve, reject) => {
                    chrome.tabs.query({ active: true, currentWindow: true }, (res) => {
                        if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
                        resolve(res);
                    });
                });
                const tab = tabs && tabs[0];
                if (!tab || !tab.id) throw new Error('No active tab');
                const ctx = await getPageContextSnapshot(tab.id);
                const plan = await planQuickAction({ goalCommand: request.command, context: ctx }, apiKey, model);
                sendResponse({ success: !!plan, plan: plan || null });
            } catch (e) {
                const msg = e && e.message ? e.message : String(e);
                sendResponse({ success: false, error: msg });
            }
        })();
        return true;
    }
    if (request.action === 'startContinuousTask') {
        (async () => {
            try {
                const tasks = await new Promise((resolve) => chrome.storage.local.get(['continuousTasks'], (r) => resolve(r.continuousTasks || [])));
                const newTask = { id: Date.now().toString(36), command: request.command || '', type: request.type || 'generic', url: request.url || '', createdAt: Date.now(), status: 'active' };
                tasks.push(newTask);
                await new Promise((resolve) => chrome.storage.local.set({ continuousTasks: tasks }, () => resolve(true)));
                chrome.alarms.create('continuous-tick', { periodInMinutes: 0.333 });
                // Fast-path: if it's a spotify play request with query, try immediately
                if (newTask.type === 'spotify_play') {
                    const q = (request.query && String(request.query).trim()) || extractSpotifyQuery(newTask.command);
                    if (q) {
                        ensureSpotifySearchAndPlay(q).catch(() => {});
                    } else {
                        ensureSpotifyPlaying().catch(() => {});
                    }
                }
                sendResponse({ success: true, task: newTask });
            } catch (e) {
                const msg = e && e.message ? e.message : String(e);
                sendResponse({ success: false, needClarification: true, error: `Could not start task: ${msg}` });
            }
        })();
        return true;
    }
    if (request.action === 'stopContinuousTask') {
        (async () => {
            try {
                const tasks = await new Promise((resolve) => chrome.storage.local.get(['continuousTasks'], (r) => resolve(r.continuousTasks || [])));
                const filtered = tasks.filter(t => t.id !== request.id);
                await new Promise((resolve) => chrome.storage.local.set({ continuousTasks: filtered }, () => resolve(true)));
                sendResponse({ success: true });
            } catch (e) {
                const msg = e && e.message ? e.message : String(e);
                sendResponse({ success: false, needClarification: true, error: `Could not stop task: ${msg}` });
            }
        })();
        return true;
    }
    return true; // keep the message channel open for async sendResponse
});

async function callGemini(command, apiKey, model) {
    const apiUrl = geminiUrl(model, apiKey);

    // This prompt forbids non-standard selectors and frameworks (Playwright/Selenium)
    const systemPrompt = `
You are a browser automation assistant. Convert a user's natural language command into a single, executable JavaScript function body that runs directly in the page (no Playwright/Selenium).
Rules:
- ONLY use standard DOM APIs (querySelector, querySelectorAll, click, value, dispatchEvent, etc.).
- NEVER use non-standard selectors like :has-text, :text, text=, or xpath=.
- Prefer robust selectors (labels via htmlFor, name, role, data-*). If you need to find by visible text, iterate DOM nodes and compare innerText.
- Respond with ONLY a JSON object containing key "code" with the raw JS (no wrappers/markdown).

Example Command: "highlight all links"
Example Response:
{
  "code": "const links = document.querySelectorAll('a'); links.forEach(link => { link.style.backgroundColor = 'yellow'; });"
}

Example Command: "fill the login form with user test@example.com and password StrongPassword123"
Example Response:
{
  "code": "const email = document.querySelector('input[type=\\"email\\"]') || document.querySelector('input[name*=email i]'); if (email) { email.value = 'test@example.com'; email.dispatchEvent(new Event('input',{bubbles:true})); } const pwd = document.querySelector('input[type=\\"password\\"]') || document.querySelector('input[name*=password i]'); if (pwd) { pwd.value = 'StrongPassword123'; pwd.dispatchEvent(new Event('input',{bubbles:true})); }"
}
`;

    const payload = {
        contents: [{
            parts: [{ text: `Command: "${command}"` }]
        }],
        systemInstruction: {
            parts: [{ text: systemPrompt }]
        }
    };

    const result = await fetchJsonScheduled(apiUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
    }, { retries: 3, backoffMs: 600 });
    // Be tolerant to different response shapes
    let textResponse = '';
    try {
        const candidate = result && result.candidates && result.candidates[0];
        const parts = candidate && candidate.content && candidate.content.parts;
        if (Array.isArray(parts)) {
            const firstText = parts.find(p => typeof p.text === 'string');
            if (firstText) textResponse = firstText.text;
        }
    } catch (_) {}

    if (typeof textResponse !== 'string' || !textResponse.trim()) {
        throw new Error('Model returned an unexpected response format.');
    }

    // Extract JSON object from the text if wrapped or contains extra text
    let jsonText = textResponse.trim();
    const match = jsonText.match(/\{[\s\S]*\}/);
    if (match) {
        jsonText = match[0];
    }

    const parsed = JSON.parse(jsonText);
    return parsed;
}


async function executeCodeInTab(codeToExecute) {
    // Disabled due to site CSPs disallowing eval; rely on plan/act loop instead
    throw new Error('Direct code execution is disabled by CSP. Use planner actions instead.');
}

let lastValidationOkTs = 0;
async function validateApiKey(apiKey, model) {
    if (Date.now() - lastValidationOkTs < 10 * 60 * 1000) return true;
    const apiUrl = geminiUrl(model, apiKey);
    const payload = {
        contents: [{ parts: [{ text: "Return the word OK." }] }]
    };
    const json = await fetchJsonScheduled(apiUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    }, { retries: 2, backoffMs: 400 });
    const text = json && json.candidates && json.candidates[0] && json.candidates[0].content && json.candidates[0].content.parts && json.candidates[0].content.parts[0] && json.candidates[0].content.parts[0].text;
    lastValidationOkTs = Date.now();
    return true;
}

async function runVisionPlanActLoop(goalCommand, apiKey, model) {
    const maxSteps = 6; // faster
    const history = [];

    const tabs = await new Promise((resolve, reject) => {
        chrome.tabs.query({ active: true, currentWindow: true }, (res) => {
            if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
            resolve(res);
        });
    });
    const tab = tabs && tabs[0];
    if (!tab || !tab.id) throw new Error("No active tab found.");
    if (isRestrictedUrl(tab.url)) {
        return { success: false, needClarification: true, question: 'This page is restricted (chrome:// or Web Store). Please switch to a normal website or provide a URL to navigate to.' };
    }

    for (let step = 1; step <= maxSteps; step++) {
        const dataUrl = await captureVisibleTabDataUrl();
        const context = await getPageContextSnapshot(tab.id);
        const plan = await planNextActionWithVision({ dataUrl, goalCommand, history, context }, apiKey, model);

        if (!plan || typeof plan.action !== 'string') {
            throw new Error('Planner did not return a valid action.');
        }

        if (plan.action === 'done') {
            return { success: true };
        }
        if (plan.action === 'ask') {
            return { success: false, needClarification: true, question: plan.question || 'Need more information to proceed.' };
        }

        try {
            await executePlannedAction(tab.id, plan);
            history.push({ step, plan, result: 'ok' });
            // Throttle verification to reduce API calls
            const shouldVerify = plan.action === 'navigate' || step % 2 === 0;
            if (shouldVerify) {
                const contextAfter = await getPageContextSnapshot(tab.id);
                const goalOk = await verifyGoalQuick(goalCommand, contextAfter, apiKey, model);
                if (goalOk) return { success: true };
            }
        } catch (e) {
            const msg = e && e.message ? e.message : String(e);
            if (/Element not found/i.test(msg)) {
                // Auto-recovery: scroll and retry once
                await injectAutoScroll(tab.id);
                await sleep(400);
                try {
                    await executePlannedAction(tab.id, plan);
                    history.push({ step, plan, result: 'ok-after-scroll' });
                    const shouldVerify2 = plan.action === 'navigate' || step % 2 === 0;
                    if (shouldVerify2) {
                        const contextAfter2 = await getPageContextSnapshot(tab.id);
                        const goalOk2 = await verifyGoalQuick(goalCommand, contextAfter2, apiKey, model);
                        if (goalOk2) return { success: true };
                    }
                } catch (e2) {
                    history.push({ step, plan, result: 'fail', error: msg });
                }
            } else {
                history.push({ step, plan, result: 'fail', error: msg });
            }
        }

        // ultra-low delay between actions for speed; configurable by fastMode
        try {
            const s = await getSettings();
            await sleep(s.fastMode ? 10 : 200);
        } catch (_) { await sleep(200); }
    }

    return { success: false, reason: 'Max steps reached' };
}

function sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
}

function isRestrictedUrl(url) {
    const u = url || '';
    return u.startsWith('chrome://') || u.startsWith('edge://') || u.startsWith('about:') || /chrome\.google\.com\/webstore/.test(u);
}

async function openTabToUrl(url, { timeoutMs = 15000 } = {}) {
    const created = await new Promise((resolve, reject) => {
        chrome.tabs.create({ url, active: true }, (tab) => {
            if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
            resolve(tab);
        });
    });

    const tabId = created.id;
    if (!tabId) return created;

    // Wait for the tab to finish loading
    await new Promise((resolve, reject) => {
        const start = Date.now();
        function onUpdated(id, info, tab) {
            if (id === tabId && info.status === 'complete') {
                chrome.tabs.onUpdated.removeListener(onUpdated);
                resolve(true);
            } else if (Date.now() - start > timeoutMs) {
                chrome.tabs.onUpdated.removeListener(onUpdated);
                resolve(false);
            }
        }
        chrome.tabs.onUpdated.addListener(onUpdated);
    });

    return created;
}

// Fast text-only planner to reduce latency when screenshot isn't necessary
async function planQuickAction({ goalCommand, context }, apiKey, model) {
    const apiUrl = geminiUrl(model, apiKey);
    const systemPrompt = `You are a web automation planner. Given a goal and a lightweight page context (title, URL, visible text snippet), return ONLY a JSON object describing the next action to take. Use the same schema as the vision planner. Prefer text targeting (targetText + tag) or simple selectors.

Schema:
{
  "action": "click" | "type" | "scroll" | "wait" | "waitForSelector" | "waitForText" | "navigate" | "done" | "ask",
  "selector": "VALID CSS (no :has-text)",
  "text": "for type",
  "targetText": "visible text to target",
  "tag": "e.g., button, a, input",
  "url": "for navigate",
  "question": "for ask",
  "reason": "brief",
  "waitMs": number
}`;
    const payload = {
        systemInstruction: { parts: [{ text: systemPrompt }] },
        contents: [{ parts: [{ text: `Goal: ${goalCommand}\nContext: ${context.title} | ${context.url}\nVisible text (truncated):\n${context.text}` }] }]
    };
    const json = await fetchJsonScheduled(apiUrl, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
    }, { retries: 1, backoffMs: 300 });
    let text = '';
    try {
        const parts = json.candidates[0].content.parts;
        const firstText = parts.find(p => typeof p.text === 'string');
        if (firstText) text = firstText.text;
    } catch (_) {}
    if (!text) return null;
    const match = text.match(/\{[\s\S]*\}/);
    const jsonText = match ? match[0] : text;
    try {
        return normalizePlan(JSON.parse(jsonText));
    } catch (_) {
        return null;
    }
}

async function verifyGoalQuick(goalCommand, context, apiKey, model) {
    const apiUrl = geminiUrl(model, apiKey);
    const systemPrompt = `You are a verifier. Given a goal and lightweight page context (title, URL, visible text), respond ONLY with JSON: {"done": true|false, "reason": "brief"}.`;
    const payload = {
        systemInstruction: { parts: [{ text: systemPrompt }] },
        contents: [{ parts: [{ text: `Goal: ${goalCommand}\nContext: ${context.title} | ${context.url}\nVisible text (truncated):\n${context.text}` }] }]
    };
    const json = await fetchJsonScheduled(apiUrl, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
    }, { retries: 1, backoffMs: 300 });
    let text = '';
    try {
        const parts = json.candidates[0].content.parts;
        const firstText = parts.find(p => typeof p.text === 'string');
        if (firstText) text = firstText.text;
    } catch (_) {}
    if (!text) return false;
    const match = text.match(/\{[\s\S]*\}/);
    const jsonText = match ? match[0] : text;
    try {
        const parsed = JSON.parse(jsonText);
        return !!parsed.done;
    } catch (_) {
        return false;
    }
}

// Retry helper for transient HTTP errors (e.g., 429/503)
async function fetchJsonWithRetry(url, options, { retries = 4, backoffMs = 1200 } = {}) {
    let attempt = 0;
    let lastErr;
    while (attempt <= retries) {
        try {
            const res = await fetch(url, options);
            if (!res.ok) {
                // Retry on 429/5xx
                if (res.status === 429 || (res.status >= 500 && res.status <= 599)) {
                    const err = new Error(`HTTP ${res.status}`);
                    err.status = res.status;
                    const ra = res.headers && (res.headers.get('Retry-After') || res.headers.get('x-ratelimit-reset'));
                    if (ra) {
                        const raNum = Number(ra);
                        if (!isNaN(raNum)) {
                            err.retryAfterMs = raNum * 1000;
                        } else {
                            const dateMs = Date.parse(ra);
                            if (!isNaN(dateMs)) {
                                const delta = dateMs - Date.now();
                                if (delta > 0) err.retryAfterMs = delta;
                            }
                        }
                    }
                    throw err;
                }
                // Non-retriable
                const text = await res.text().catch(() => '');
                const err = new Error(`API call failed (${res.status}) ${text}`);
                err.status = res.status;
                throw err;
            }
            return await res.json();
        } catch (e) {
            lastErr = e;
            if (attempt === retries) break;
            const retryAfter = e && e.retryAfterMs ? e.retryAfterMs : 0;
            const jitter = Math.floor(Math.random() * 200);
            const base = backoffMs * Math.pow(2, attempt) + jitter;
            const wait = Math.max(base, retryAfter);
            await sleep(wait);
            attempt++;
        }
    }
    throw lastErr || new Error('Network error');
}

// Global API scheduler to reduce 429s and support concurrent tasks without stampeding
const apiScheduler = {
    queue: [],
    running: 0,
    maxConcurrent: 1,
    minIntervalMs: 1200,
    lastStartTs: 0,
    cooldownTimer: null,
    pausedUntil: 0,
    // Simple per-host token bucket
    buckets: {},
    consecutive429: 0
};

function updateSchedulerFromSettings(settings) {
    try {
        const rpm = Math.max(1, Number(settings.rateRpm || 12));
        const conc = Math.max(1, Math.min(4, Number(settings.maxConcurrent || 1)));
        if (settings.fastMode) {
            apiScheduler.maxConcurrent = Math.max(conc, 2);
            apiScheduler.minIntervalMs = Math.max(100, Math.floor(60000 / (rpm * 3)));
        } else {
            apiScheduler.maxConcurrent = conc;
            apiScheduler.minIntervalMs = Math.max(600, Math.floor(60000 / rpm));
        }
        // Recompute token bucket refill
        const refillPerSec = (settings.fastMode ? rpm * 2 : rpm) / 60;
        for (const host of Object.keys(apiScheduler.buckets)) {
            apiScheduler.buckets[host].refillPerSec = refillPerSec;
            apiScheduler.buckets[host].capacity = Math.max(1, Math.min(5, conc * 2));
        }
    } catch (_) {}
}

function scheduleApi(fn) {
    return new Promise((resolve, reject) => {
        apiScheduler.queue.push({ fn, resolve, reject });
        drainScheduler();
    });
}

function drainScheduler() {
    while (apiScheduler.running < apiScheduler.maxConcurrent && apiScheduler.queue.length > 0) {
        const { fn, resolve, reject } = apiScheduler.queue.shift();
        const now = Date.now();
        if (now < apiScheduler.pausedUntil) {
            // requeue and try later
            setTimeout(drainScheduler, apiScheduler.pausedUntil - now);
            apiScheduler.queue.unshift({ fn, resolve, reject });
            return;
        }
        const since = now - apiScheduler.lastStartTs;
        const delay = Math.max(0, apiScheduler.minIntervalMs - since);
        apiScheduler.running++;
        setTimeout(async () => {
            apiScheduler.lastStartTs = Date.now();
            try {
                const result = await fn();
                resolve(result);
            } catch (e) {
                reject(e);
            } finally {
                apiScheduler.running--;
                drainScheduler();
            }
        }, delay);
    }
}

async function fetchJsonScheduled(url, options, retryOpts) {
    const host = (() => { try { return new URL(url).host; } catch (_) { return 'default'; } })();
    if (!apiScheduler.buckets[host]) apiScheduler.buckets[host] = { tokens: 2, capacity: 2, refillPerSec: 0.25, lastRefillTs: Date.now() };
    function takeToken() {
        const b = apiScheduler.buckets[host];
        const now = Date.now();
        const elapsed = (now - b.lastRefillTs) / 1000;
        const refill = elapsed * b.refillPerSec;
        if (refill > 0) {
            b.tokens = Math.min(b.capacity, b.tokens + refill);
            b.lastRefillTs = now;
        }
        if (b.tokens >= 1) { b.tokens -= 1; return 0; }
        const needed = 1 - b.tokens;
        const waitSec = needed / b.refillPerSec;
        return Math.ceil(waitSec * 1000);
    }
    return scheduleApi(async () => {
        const delayMs = takeToken();
        if (delayMs > 0) await sleep(delayMs);
        try {
            return await fetchJsonWithRetry(url, options, retryOpts);
        } catch (e) {
            if (e && e.status === 429) {
                // Enter cooldown: reduce concurrency and increase pacing briefly
                apiScheduler.maxConcurrent = 1;
                apiScheduler.minIntervalMs = Math.min(apiScheduler.minIntervalMs * 2, 8000);
                const pauseMs = (e.retryAfterMs && e.retryAfterMs > 0) ? e.retryAfterMs : 20000;
                apiScheduler.pausedUntil = Date.now() + pauseMs;
                apiScheduler.consecutive429++;
                // Pause vision planning temporarily to reduce heavy calls
                visionPausedUntil = Date.now() + Math.max(pauseMs, 15000);
                if (apiScheduler.consecutive429 >= 3) {
                    // Exponential circuit breaker up to 5 minutes
                    const extra = Math.min(Math.pow(2, apiScheduler.consecutive429 - 3) * 30000, 5 * 60 * 1000);
                    apiScheduler.pausedUntil = Date.now() + extra;
                    apiScheduler.minIntervalMs = Math.max(apiScheduler.minIntervalMs, 2000);
                    visionPausedUntil = Math.max(visionPausedUntil, Date.now() + extra);
                }
                if (apiScheduler.cooldownTimer) clearTimeout(apiScheduler.cooldownTimer);
                apiScheduler.cooldownTimer = setTimeout(() => {
                    apiScheduler.minIntervalMs = Math.max(800, Math.floor(apiScheduler.minIntervalMs / 2));
                    apiScheduler.consecutive429 = 0;
                    // Optionally raise concurrency after cooldown if stable
                }, 10000);
            }
            throw e;
        }
    });
}

// Collect lightweight page context to help planning
async function getPageContextSnapshot(tabId) {
    const results = await new Promise((resolve, reject) => {
        chrome.scripting.executeScript({
            target: { tabId },
            func: () => {
                const maxLen = 4000;
                const walker = document.createTreeWalker(document.body || document.documentElement, NodeFilter.SHOW_TEXT, {
                    acceptNode: (node) => {
                        const parent = node.parentElement;
                        if (!parent) return NodeFilter.FILTER_REJECT;
                        const style = window.getComputedStyle(parent);
                        const visible = style && style.visibility !== 'hidden' && style.display !== 'none' && (parent.offsetWidth || parent.offsetHeight || parent.getClientRects().length);
                        return visible ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT;
                    }
                });
                const chunks = [];
                while (walker.nextNode() && chunks.join(' ').length < maxLen) {
                    const t = (walker.currentNode.nodeValue || '').replace(/\s+/g, ' ').trim();
                    if (t) chunks.push(t);
                }
                const text = chunks.join(' ').slice(0, maxLen);
                return {
                    title: document.title || '',
                    url: location.href || '',
                    text
                };
            }
        }, (res) => {
            if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
            resolve(res);
        });
    });
    const first = Array.isArray(results) ? results[0] : undefined;
    return (first && first.result) ? first.result : { title: '', url: '', text: '' };
}

// Continuous tasks
chrome.runtime.onInstalled.addListener(() => {
    chrome.contextMenus.create({ id: 'electron-play-spotify', title: 'Electron: Play music on Spotify', contexts: ['action'] });
    chrome.alarms.create('continuous-tick', { periodInMinutes: 0.333 });
});

chrome.runtime.onStartup.addListener(() => {
    chrome.alarms.create('continuous-tick', { periodInMinutes: 0.333 });
});

chrome.contextMenus.onClicked.addListener((info) => {
    if (info && info.menuItemId === 'electron-play-spotify') {
        chrome.runtime.sendMessage({ action: 'startContinuousTask', type: 'spotify_play', command: 'play music on spotify', url: 'https://open.spotify.com/' });
    }
});

chrome.alarms.onAlarm.addListener((alarm) => {
    if (alarm && alarm.name === 'continuous-tick') {
        processContinuousTasks().catch(() => {});
        // Opportunistic fast follow-ups for responsiveness
        setTimeout(() => { processContinuousTasks().catch(() => {}); }, 50);
        setTimeout(() => { processContinuousTasks().catch(() => {}); }, 100);
    }
});

async function processContinuousTasks() {
    const tasks = await new Promise((resolve) => chrome.storage.local.get(['continuousTasks'], (r) => resolve(r.continuousTasks || [])));
    if (!Array.isArray(tasks) || tasks.length === 0) return;
    for (const task of tasks) {
        if (task.status !== 'active') continue;
        try {
            if (task.type === 'spotify_play') {
                const q = extractSpotifyQuery(task.command);
                if (q) {
                    await ensureSpotifySearchAndPlay(q);
                } else {
                    await ensureSpotifyPlaying();
                }
            } else {
                // Generic: attempt quick plan on current tab
                const tabs = await new Promise((resolve, reject) => {
                    chrome.tabs.query({ active: true, currentWindow: true }, (res) => {
                        if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
                        resolve(res);
                    });
                });
                const tab = tabs && tabs[0];
                if (!tab || !tab.id) continue;
                const ctx = await getPageContextSnapshot(tab.id);
                const settings = await getSettings();
                updateSchedulerFromSettings(settings);
                const apiKey = settings.apiKey;
                if (!apiKey) continue;
                const model = settings.model;
                const plan = await planQuickAction({ goalCommand: task.command, context: ctx }, apiKey, model);
                if (plan && plan.action && plan.action !== 'ask' && plan.action !== 'done') {
                    await executePlannedAction(tab.id, plan);
                }
            }
        } catch (_) { /* ignore single iteration errors */ }
    }
}

async function ensureSpotifyPlaying() {
    // Find or open Spotify Web
    let tab = null;
    const existing = await new Promise((resolve) => chrome.tabs.query({ url: 'https://open.spotify.com/*' }, (res) => resolve(res || [])));
    if (existing && existing[0]) {
        tab = existing[0];
        try { if (!tab.active) chrome.tabs.update(tab.id, { active: true }); } catch (_) {}
    } else {
        tab = await new Promise((resolve, reject) => {
            chrome.tabs.create({ url: 'https://open.spotify.com/', active: true }, (t) => {
                if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
                resolve(t);
            });
        });
        await waitForPageLoad(tab.id, 15000);
    }
    // Try to click Play
    try {
        await new Promise((resolve, reject) => {
            chrome.scripting.executeScript({
                target: { tabId: tab.id, allFrames: true },
                func: () => {
                    const norm = (s) => (s || '').replace(/\s+/g, ' ').trim().toLowerCase();
                    // Prefer explicit play button by aria-label
                    const candidates = Array.from(document.querySelectorAll('[aria-label], button'));
                    let play = candidates.find(el => /play/i.test(el.getAttribute && el.getAttribute('aria-label') || ''));
                    // If already playing, do nothing
                    const pause = candidates.find(el => /pause/i.test(el.getAttribute && el.getAttribute('aria-label') || ''));
                    if (!play && !pause) {
                        // Fallback: find button with innerText containing Play
                        play = candidates.find(el => /play/i.test(norm(el.innerText || el.textContent || '')));
                    }
                    if (pause) return { alreadyPlaying: true };
                    if (!play) return { ok: false, error: 'Play button not found' };
                    try { play.click(); return { ok: true }; } catch (e) { return { ok: false, error: 'Click failed' }; }
                }
            }, (res) => {
                if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
                resolve(res && res[0] && res[0].result ? res[0].result : { ok: false });
            });
        });
    } catch (_) {}
}

function extractSpotifyQuery(command) {
    try {
        const raw = String(command || '').toLowerCase();
        let q = raw;
        q = q.replace(/\b(play|resume|start)\b/g, '').replace(/\bon\s*spotify\b/g, '').trim();
        q = q.replace(/^music\s*/g, '').trim();
        return q.length >= 2 ? q : '';
    } catch (_) { return ''; }
}

async function ensureSpotifySearchAndPlay(query) {
    let tab = null;
    const existing = await new Promise((resolve) => chrome.tabs.query({ url: 'https://open.spotify.com/*' }, (res) => resolve(res || [])));
    if (existing && existing[0]) {
        tab = existing[0];
        try { chrome.tabs.update(tab.id, { active: true }); } catch (_) {}
    } else {
        tab = await new Promise((resolve, reject) => {
            chrome.tabs.create({ url: 'https://open.spotify.com/', active: true }, (t) => {
                if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
                resolve(t);
            });
        });
        await waitForPageLoad(tab.id, 12000);
    }

    // Navigate directly to search route for speed
    const searchUrl = 'https://open.spotify.com/search/' + encodeURIComponent(query);
    await new Promise((resolve, reject) => {
        chrome.scripting.executeScript({
            target: { tabId: tab.id },
            func: (url) => { try { window.location.assign(url); return true; } catch (e) { return false; } },
            args: [searchUrl]
        }, (res) => {
            if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
            resolve(res);
        });
    });
    await waitForPageLoad(tab.id, 12000);

    // Try to click the first relevant result and its play as fast as possible
    await new Promise((resolve) => setTimeout(resolve, 20));
    await new Promise((resolve, reject) => {
        chrome.scripting.executeScript({
            target: { tabId: tab.id, allFrames: true },
            func: () => {
                function first(arr) { return Array.isArray(arr) ? arr[0] : undefined; }
                const norm = (s) => (s || '').replace(/\s+/g, ' ').trim().toLowerCase();
                // 1) Prefer first search result card (album/artist/track) then click its play
                const section = document.querySelector('[data-testid="search-results"]') || document;
                const resultTile = section.querySelector('[data-testid*="entity"] a[href^="/track/"], a[href^="/track/"]')
                    || section.querySelector('a[href^="/album/"]')
                    || section.querySelector('a[href^="/artist/"]');
                if (resultTile) {
                    try { (resultTile).click(); } catch (_) {}
                }
                // 2) Try explicit play button in the view
                const byTestId = document.querySelector('[data-testid="play-button"]') || first(Array.from(document.querySelectorAll('[data-testid="play-button"]')));
                if (byTestId) { try { (byTestId).click(); return { ok: true, via: 'data-testid' }; } catch (_) {} }
                // 3) Try first row's play in tracklist
                const rowPlay = document.querySelector('[role="row"] [data-testid="play-button"], .main-trackList-trackListRow button[aria-label*="Play"]');
                if (rowPlay) { try { (rowPlay).click(); return { ok: true, via: 'row' }; } catch (_) {} }
                // 4) Generic aria-label Play
                const aria = Array.from(document.querySelectorAll('[aria-label]')).find(el => /play/i.test(el.getAttribute('aria-label') || ''));
                if (aria) { try { (aria).click(); return { ok: true, via: 'aria' }; } catch (_) {} }
                // 5) Fallback: any button with play text
                const buttons = Array.from(document.querySelectorAll('button'));
                const byText = buttons.find(b => /play/i.test(norm(b.innerText || b.textContent || '')));
                if (byText) { try { (byText).click(); return { ok: true, via: 'text' }; } catch (_) {} }
                return { ok: false };
            }
        }, (res) => {
            if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
            resolve(res);
        });
    });
}

// Scroll the page down to reveal lazy-loaded or offscreen elements
async function injectAutoScroll(tabId) {
    await new Promise((resolve, reject) => {
        chrome.scripting.executeScript({
            target: { tabId },
            func: () => {
                const amount = Math.max(400, Math.floor(window.innerHeight * 0.8));
                window.scrollBy({ top: amount, behavior: 'smooth' });
                return true;
            }
        }, (res) => {
            if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
            resolve(res);
        });
    });
}

// Normalize non-standard selectors (e.g., :has-text) into text-based targeting fields
function normalizePlan(plan) {
    if (!plan || typeof plan !== 'object') return plan;
    if (plan.selector && /:has-text\(/i.test(plan.selector)) {
        const m = plan.selector.match(/^\s*([a-zA-Z0-9_-]+)?\s*:has-text\(['"]([\s\S]*?)['"]\)\s*$/);
        if (m) {
            const tag = m[1] || undefined;
            const txt = m[2];
            if (!plan.tag && tag) plan.tag = tag;
            if (!plan.targetText && txt) plan.targetText = txt;
            delete plan.selector;
        } else {
            delete plan.selector; // drop invalid selector to avoid querySelector exceptions
        }
    }
    return plan;
}

async function captureVisibleTabDataUrl() {
    // Ensure we capture from the active window for the active tab
    const tabs = await new Promise((resolve, reject) => {
        chrome.tabs.query({ active: true, currentWindow: true }, (res) => {
            if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
            resolve(res);
        });
    });
    const tab = tabs && tabs[0];
    if (!tab) throw new Error('No active tab for screenshot.');

    const dataUrl = await new Promise((resolve, reject) => {
        try {
            chrome.tabs.captureVisibleTab(tab.windowId, { format: 'png' }, (url) => {
                if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
                resolve(url);
            });
        } catch (e) { reject(e); }
    });
    if (typeof dataUrl !== 'string' || !dataUrl.startsWith('data:image/png')) {
        throw new Error('Failed to capture screenshot.');
    }
    return dataUrl;
}

async function planNextActionWithVision({ dataUrl, goalCommand, history, context }, apiKey, model) {
    const apiUrl = geminiUrl(model, apiKey);
    const base64 = dataUrl.replace(/^data:image\/png;base64,/, '');

    const systemPrompt = `You are a web automation planner. Given a screenshot and a goal, respond with ONLY a JSON object describing the next action to take in the browser. Keep actions atomic and robust.

Schema (one of selector OR text targeting may be used for click/type):
{
  "action": "click" | "type" | "scroll" | "wait" | "waitForSelector" | "waitForText" | "navigate" | "done" | "ask",
  "selector": "VALID standard CSS selector (no Playwright/Selenium pseudos like :has-text)",
  "text": "text to type (only for action=type)",
  "targetText": "visible text to target when CSS is unreliable (e.g., New chat)",
  "tag": "optional HTML tag to narrow text targeting (e.g., button, a)",
  "url": "for action=navigate",
  "question": "natural-language clarification to ask the user (only when action=ask)",
  "reason": "brief rationale",
  "waitMs": number // optional suggested delay after the action
}

Rules:
- Prefer stable selectors (labels, data-* attributes, role, name). Use targetText only if necessary.
- NEVER use non-standard selectors like :has-text or text=.
- If the goal appears already complete, use action="done".
- If more information is required (e.g., which of several similar buttons?), return action="ask" with a clear question.
- Use wait action if the next step depends on network or UI loading.
- Do not include markdown or extra commentary. Only return a JSON object.`;

    const payload = {
        systemInstruction: { parts: [{ text: systemPrompt }] },
        contents: [{
            parts: [
                { text: `Goal: ${goalCommand}\nHistory: ${JSON.stringify(history).slice(0, 4000)}\nContext: ${context.title} | ${context.url}\nVisible text (truncated):\n${context.text}` },
                { inline_data: { mime_type: 'image/png', data: base64 } }
            ]
        }]
    };

    let json;
    try {
        json = await fetchJsonScheduled(apiUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        }, { retries: 2, backoffMs: 350 });
    } catch (e) {
        // surface the status succinctly to the caller
        throw new Error('Vision planning failed (transient error). Please retry.');
    }

    let text = '';
    try {
        const parts = json.candidates[0].content.parts;
        const firstText = parts.find(p => typeof p.text === 'string');
        if (firstText) text = firstText.text;
    } catch (_) {}
    if (!text) throw new Error('Planner returned empty response.');

    const match = text.match(/\{[\s\S]*\}/);
    const jsonText = match ? match[0] : text;
    const plan = JSON.parse(jsonText);
    return normalizePlan(plan);
}

async function executePlannedAction(tabId, plan) {
    const waitMs = typeof plan.waitMs === 'number' ? plan.waitMs : 0;

    if (plan.action === 'wait') {
        if (waitMs > 0) await sleep(waitMs);
        return;
    }
    if (plan.action === 'navigate') {
        const url = plan.url;
        if (!url || typeof url !== 'string') throw new Error('navigate requires url');
        await new Promise((resolve, reject) => {
            chrome.scripting.executeScript({
                target: { tabId },
                func: (u) => { window.location.assign(u); return true; },
                args: [url]
            }, (res) => {
                if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
                resolve(res);
            });
        });
        // Wait for the tab to finish loading before continuing planning
        await waitForPageLoad(tabId, Math.max(8000, waitMs || 0));
        return;
    }
    if (plan.action === 'waitForSelector') {
        const timeout = typeof plan.waitMs === 'number' ? plan.waitMs : 6000;
        if (!plan.selector) throw new Error('waitForSelector requires selector');
        const ok = await waitForSelectorPresence(tabId, plan.selector, timeout);
        if (!ok) throw new Error('waitForSelector timeout');
        return;
    }
    if (plan.action === 'waitForText') {
        const timeout = typeof plan.waitMs === 'number' ? plan.waitMs : 6000;
        if (!plan.targetText) throw new Error('waitForText requires targetText');
        const ok = await waitForTextPresence(tabId, plan.targetText, timeout, plan.tag);
        if (!ok) throw new Error('waitForText timeout');
        return;
    }

    if ((plan.action === 'click' || plan.action === 'type') && !plan.selector && !plan.targetText) {
        throw new Error('Planner missing selector or targetText for actionable step.');
    }

    const results = await new Promise((resolve, reject) => {
        chrome.scripting.executeScript({
            target: { tabId, allFrames: true },
            func: (actionPlan) => {
                function inViewport(el) {
                    const rect = el.getBoundingClientRect();
                    return rect.width > 0 && rect.height > 0 && rect.top < window.innerHeight && rect.bottom > 0;
                }
                function scrollIntoViewIfNeeded(el) {
                    try { el.scrollIntoView({ behavior: 'smooth', block: 'center', inline: 'center' }); } catch (_) {}
                }
                function visible(el) {
                    return !!(el && (el.offsetWidth || el.offsetHeight || el.getClientRects().length));
                }
                function queryShadowAll(root, selector) {
                    const results = [];
                    function walk(node) {
                        if (!node) return;
                        try {
                            const found = node.querySelectorAll(selector);
                            for (const f of found) results.push(f);
                        } catch (_) {}
                        const tree = node.querySelectorAll ? node.querySelectorAll('*') : [];
                        for (const el of tree) {
                            const sr = el.shadowRoot;
                            if (sr) walk(sr);
                        }
                    }
                    walk(root);
                    return results;
                }
                function safeQuerySelectorDeep(selector) {
                    try { const el = document.querySelector(selector); if (el) return el; } catch (_) {}
                    const results = queryShadowAll(document, selector);
                    return results[0] || null;
                }
                function findByTextDeep(tag, text) {
                    const norm = (s) => (s || '').replace(/\s+/g, ' ').trim().toLowerCase();
                    const target = norm(text);
                    const scopeTop = tag ? Array.from(document.getElementsByTagName(tag)) : Array.from(document.querySelectorAll('*'));
                    let candidates = scopeTop;
                    const shadowEls = queryShadowAll(document, tag || '*');
                    candidates = candidates.concat(shadowEls);
                    for (const el of candidates) {
                        if (!visible(el)) continue;
                        const txt = norm(el.innerText || el.textContent || '');
                        if (txt.includes(target)) return el;
                    }
                    return null;
                }
                function findInputByLabelTextDeep(text) {
                    const norm = (s) => (s || '').replace(/\s+/g, ' ').trim().toLowerCase();
                    const target = norm(text);
                    const labels = Array.from(document.querySelectorAll('label'));
                    for (const label of labels) {
                        const txt = norm(label.innerText || label.textContent || '');
                        if (txt.includes(target)) {
                            if (label.htmlFor) {
                                const byFor = document.getElementById(label.htmlFor);
                                if (byFor) return byFor;
                            }
                            const input = label.querySelector('input,textarea,select');
                            if (input) return input;
                        }
                    }
                    const shadowCandidates = queryShadowAll(document, 'label, input, textarea, select');
                    for (const el of shadowCandidates) {
                        if (el.tagName && el.tagName.toLowerCase() === 'label') {
                            const txt = norm(el.innerText || el.textContent || '');
                            if (txt.includes(target)) {
                                const input = el.querySelector('input,textarea,select');
                                if (input) return input;
                            }
                        }
                    }
                    return null;
                }
                function findByAria(text) {
                    const norm = (s) => (s || '').replace(/\s+/g, ' ').trim().toLowerCase();
                    const target = norm(text);
                    const candidates = Array.from(document.querySelectorAll('[aria-label], [title], [value]'));
                    for (const el of candidates) {
                        const lab = norm(el.getAttribute('aria-label') || el.getAttribute('title') || el.getAttribute('value') || '');
                        if (lab.includes(target)) return el;
                    }
                    return null;
                }
                function guessRole(el) {
                    const tag = (el.tagName || '').toLowerCase();
                    if (tag === 'button') return 'button';
                    if (tag === 'a') return 'link';
                    if (tag === 'input' || tag === 'textarea') return 'textbox';
                    return el.getAttribute && el.getAttribute('role') || '';
                }
                function findByRoleName(role, name) {
                    const norm = (s) => (s || '').replace(/\s+/g, ' ').trim().toLowerCase();
                    const target = norm(name || '');
                    const candidates = Array.from(document.querySelectorAll('[role],button,a,input,textarea,select'));
                    for (const el of candidates) {
                        if (!visible(el)) continue;
                        const r = (el.getAttribute('role') || guessRole(el) || '').toLowerCase();
                        if (r !== String(role || '').toLowerCase()) continue;
                        const label = norm(el.getAttribute('aria-label') || el.getAttribute('title') || el.value || el.innerText || '');
                        if (target && !label.includes(target)) continue;
                        return el;
                    }
                    return null;
                }
                function synthClick(el) {
                    scrollIntoViewIfNeeded(el);
                    if (!inViewport(el)) scrollIntoViewIfNeeded(el);
                    try {
                        el.focus && el.focus();
                        const rect = el.getBoundingClientRect();
                        const x = Math.max(0, rect.left + rect.width / 2);
                        const y = Math.max(0, rect.top + rect.height / 2);
                        const opts = { bubbles: true, cancelable: true, view: window, clientX: x, clientY: y };
                        el.dispatchEvent(new MouseEvent('pointerdown', opts));
                        el.dispatchEvent(new MouseEvent('mousedown', opts));
                        el.dispatchEvent(new MouseEvent('pointerup', opts));
                        el.dispatchEvent(new MouseEvent('mouseup', opts));
                        el.dispatchEvent(new MouseEvent('click', opts));
                        return true;
                    } catch (e) {
                        try { el.click(); return true; } catch (_) { return false; }
                    }
                }

                try {
                    if (actionPlan.action === 'scroll') {
                        window.scrollBy({ top: 600, behavior: 'smooth' });
                        return { ok: true };
                    }

                    if (actionPlan.action === 'click') {
                        let el = null;
                        if (actionPlan.selector) el = safeQuerySelectorDeep(actionPlan.selector);
                        if (!el && (actionPlan.role || actionPlan.name)) el = findByRoleName(actionPlan.role, actionPlan.name);
                        if (!el && actionPlan.targetText) el = findByTextDeep(actionPlan.tag || 'button', actionPlan.targetText) || findByAria(actionPlan.targetText) || findByTextDeep(null, actionPlan.targetText);
                        if (!el && actionPlan.selector && /:has-text\(/i.test(actionPlan.selector)) {
                            const m = actionPlan.selector.match(/^([a-zA-Z0-9_-]+):has-text\(['"]([\s\S]*?)['"]\)$/);
                            if (m) {
                                const tag = m[1];
                                const txt = m[2];
                                el = findByTextDeep(tag, txt);
                            }
                        }
                        if (!el) return { ok: false, error: 'Element not found' };
                        if (!synthClick(el)) return { ok: false, error: 'Click failed' };
                        return { ok: true };
                    }

                    if (actionPlan.action === 'type') {
                        let el = null;
                        if (actionPlan.selector) el = safeQuerySelectorDeep(actionPlan.selector);
                        if (!el && (actionPlan.role || actionPlan.name)) el = findByRoleName(actionPlan.role || 'textbox', actionPlan.name);
                        if (!el && actionPlan.targetText) el = findInputByLabelTextDeep(actionPlan.targetText) || findByAria(actionPlan.targetText) || findByTextDeep('input', actionPlan.targetText);
                        if (!el) return { ok: false, error: 'Element not found' };
                        scrollIntoViewIfNeeded(el);
                        if (el.isContentEditable) {
                            try {
                                el.focus();
                                el.innerHTML = (actionPlan.text || '').replace(/\n/g, '<br>');
                                el.dispatchEvent(new InputEvent('input', { bubbles: true }));
                                return { ok: true };
                            } catch (e) {
                                return { ok: false, error: 'ContentEditable type failed' };
                            }
                        }
                        el.focus && el.focus();
                        if ('value' in el) {
                            el.value = actionPlan.text || '';
                            el.dispatchEvent(new Event('input', { bubbles: true }));
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                        } else {
                            el.textContent = actionPlan.text || '';
                        }
                        return { ok: true };
                    }

                    return { ok: false, error: 'Unknown action' };
                } catch (e) {
                    return { ok: false, error: e && e.message ? e.message : String(e) };
                }
            },
            args: [plan]
        }, (injectionResults) => {
            if (chrome.runtime.lastError) return reject(new Error(chrome.runtime.lastError.message));
            resolve(injectionResults);
        });
    });

    const successful = Array.isArray(results) ? results.find(r => r && r.result && r.result.ok === true) : undefined;
    if (successful) {
        if (waitMs > 0) await sleep(waitMs);
        return;
    }
    const any = Array.isArray(results) ? results[0] : undefined;
    if (any && any.result && any.result.ok === false) {
        throw new Error(`Action failed: ${any.result.error}`);
    }

    if (waitMs > 0) await sleep(waitMs);
}

async function waitForSelectorPresence(tabId, selector, timeoutMs) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        const found = await new Promise((resolve) => {
            chrome.scripting.executeScript({
                target: { tabId, allFrames: true },
                func: (sel) => {
                    try { return !!document.querySelector(sel); } catch (_) { return false; }
                },
                args: [selector]
            }, (res) => {
                if (!Array.isArray(res)) return resolve(false);
                resolve(res.some(r => r && r.result === true));
            });
        });
        if (found) return true;
        await sleep(250);
    }
    return false;
}

async function waitForTextPresence(tabId, text, timeoutMs, tag) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        const found = await new Promise((resolve) => {
            chrome.scripting.executeScript({
                target: { tabId, allFrames: true },
                func: (t, tg) => {
                    const norm = (s) => (s || '').replace(/\s+/g, ' ').trim().toLowerCase();
                    const target = norm(t);
                    const scope = tg ? Array.from(document.getElementsByTagName(tg)) : Array.from(document.querySelectorAll('*'));
                    for (const el of scope) {
                        const visible = !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
                        if (!visible) continue;
                        const txt = norm(el.innerText || el.textContent || '');
                        if (txt.includes(target)) return true;
                    }
                    return false;
                },
                args: [text, tag || null]
            }, (res) => {
                if (!Array.isArray(res)) return resolve(false);
                resolve(res.some(r => r && r.result === true));
            });
        });
        if (found) return true;
        await sleep(250);
    }
    return false;
}

async function waitForPageLoad(tabId, timeoutMs = 10000) {
    const start = Date.now();
    // First attempt: use chrome.tabs.onUpdated
    const completed = await new Promise((resolve) => {
        let resolved = false;
        function onUpdated(id, info) {
            if (id === tabId && info.status === 'complete') {
                if (!resolved) { resolved = true; resolve(true); }
                chrome.tabs.onUpdated.removeListener(onUpdated);
            }
        }
        chrome.tabs.onUpdated.addListener(onUpdated);
        // Fallback timeout
        setTimeout(() => {
            if (!resolved) {
                resolved = true;
                try { chrome.tabs.onUpdated.removeListener(onUpdated); } catch (_) {}
                resolve(false);
            }
        }, timeoutMs);
    });
    if (completed) return true;
    // Second attempt: poll document.readyState in the page (covers SPA navigations)
    while (Date.now() - start < timeoutMs) {
        const ready = await new Promise((resolve) => {
            chrome.scripting.executeScript({
                target: { tabId },
                func: () => document.readyState === 'complete'
            }, (res) => {
                if (chrome.runtime.lastError) return resolve(false);
                if (!Array.isArray(res) || !res[0]) return resolve(false);
                resolve(!!res[0].result);
            });
        });
        if (ready) return true;
        await sleep(300);
    }
    return false;
}
