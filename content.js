// Lightweight helpers to read and write webpage content from the extension
// Exposes both message handlers and a global window.PCTaskAgent API for debugging

(function () {
  function normalizeWhitespace(text) {
    return (text || '').replace(/\s+/g, ' ').trim();
  }

  function visible(el) {
    try {
      return !!(el && (el.offsetWidth || el.offsetHeight || el.getClientRects().length));
    } catch (_) {
      return false;
    }
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
    try {
      const el = document.querySelector(selector);
      if (el) return el;
    } catch (_) {}
    const results = queryShadowAll(document, selector);
    return results[0] || null;
  }

  function findByTextDeep(tag, text) {
    const norm = (s) => normalizeWhitespace(String(s).toLowerCase());
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

  function setValue(el, value) {
    if (!el) return false;
    try {
      if (el.isContentEditable) {
        el.focus();
        el.innerHTML = String(value).replace(/\n/g, '<br>');
        el.dispatchEvent(new InputEvent('input', { bubbles: true }));
        return true;
      }
      if ('value' in el) {
        el.focus && el.focus();
        el.value = String(value);
        el.dispatchEvent(new Event('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
        return true;
      }
      el.textContent = String(value);
      return true;
    } catch (_) {
      return false;
    }
  }

  function clickEl(el) {
    if (!el) return false;
    try {
      el.scrollIntoView({ behavior: 'smooth', block: 'center', inline: 'center' });
      el.focus && el.focus();
      el.click();
      return true;
    } catch (_) {
      try { el.click(); return true; } catch (_) { return false; }
    }
  }

  async function getAllVisibleText(maxLen = 8000) {
    const walker = document.createTreeWalker(document.body || document.documentElement, NodeFilter.SHOW_TEXT, {
      acceptNode: (node) => {
        const parent = node.parentElement;
        if (!parent) return NodeFilter.FILTER_REJECT;
        const style = window.getComputedStyle(parent);
        const vis = style && style.visibility !== 'hidden' && style.display !== 'none' && (parent.offsetWidth || parent.offsetHeight || parent.getClientRects().length);
        return vis ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT;
      }
    });
    const chunks = [];
    while (walker.nextNode() && chunks.join(' ').length < maxLen) {
      const t = normalizeWhitespace(walker.currentNode.nodeValue || '');
      if (t) chunks.push(t);
    }
    return chunks.join(' ').slice(0, maxLen);
  }

  function handleMessage(request, sender, sendResponse) {
    if (!request || !request.type) return;
    try {
      switch (request.type) {
        case 'READ_PAGE_TEXT': {
          getAllVisibleText().then((text) => sendResponse({ ok: true, text })).catch((e) => sendResponse({ ok: false, error: String(e) }));
          return true; // async
        }
        case 'CLICK_SELECTOR': {
          const el = request.selector ? safeQuerySelectorDeep(request.selector) : (request.targetText ? findByTextDeep(request.tag || 'button', request.targetText) : null);
          const ok = clickEl(el);
          sendResponse({ ok, found: !!el });
          return;
        }
        case 'TYPE_SELECTOR': {
          const el = request.selector ? safeQuerySelectorDeep(request.selector) : (request.targetText ? findByTextDeep('input', request.targetText) : null);
          const ok = setValue(el, request.text || '');
          sendResponse({ ok, found: !!el });
          return;
        }
        case 'SET_VALUE': {
          const el = request.selector ? safeQuerySelectorDeep(request.selector) : null;
          const ok = setValue(el, request.value || '');
          sendResponse({ ok, found: !!el });
          return;
        }
        case 'GET_SELECTION': {
          const sel = window.getSelection();
          sendResponse({ ok: true, selection: sel ? String(sel) : '' });
          return;
        }
        case 'REPLACE_TEXT_BY_VISUAL': {
          const el = findByTextDeep(request.tag || null, request.targetText || '');
          if (!el) { sendResponse({ ok: false, error: 'Element not found' }); return; }
          const ok = setValue(el, request.newText || '');
          sendResponse({ ok, found: !!el });
          return;
        }
        default:
          sendResponse({ ok: false, error: 'Unknown request type' });
      }
    } catch (e) {
      sendResponse({ ok: false, error: e && e.message ? e.message : String(e) });
    }
  }

  // Register message listener
  try { chrome.runtime.onMessage.addListener(handleMessage); } catch (_) {}

  // Expose a minimal debugging API
  window.PCTaskAgent = Object.freeze({
    readText: getAllVisibleText,
    q: safeQuerySelectorDeep,
    click: (sel) => clickEl(safeQuerySelectorDeep(sel)),
    type: (sel, txt) => setValue(safeQuerySelectorDeep(sel), txt),
    findByText: findByTextDeep
  });
})();


