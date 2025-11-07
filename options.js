document.addEventListener('DOMContentLoaded', async () => {
  const apiKeyEl = document.getElementById('apiKey');
  const modelEl = document.getElementById('model');
  const visionEl = document.getElementById('vision');
  const statusEl = document.getElementById('status');

  chrome.storage.local.get(['geminiApiKey', 'geminiModel', 'visionEnabled'], (res) => {
    if (res.geminiApiKey) apiKeyEl.value = res.geminiApiKey;
    if (res.geminiModel) modelEl.value = res.geminiModel;
    if (typeof res.visionEnabled === 'boolean') visionEl.value = res.visionEnabled ? 'on' : 'off';
  });

  document.getElementById('save').addEventListener('click', () => {
    const key = apiKeyEl.value.trim();
    const model = modelEl.value;
    const vision = visionEl.value === 'on';
    chrome.storage.local.set({ geminiApiKey: key, geminiModel: model, visionEnabled: vision }, () => {
      statusEl.textContent = 'Saved';
      statusEl.className = 'success';
      setTimeout(() => { statusEl.textContent = ''; statusEl.className = 'note'; }, 1200);
    });
  });

  document.getElementById('clear').addEventListener('click', () => {
    chrome.storage.local.remove(['geminiApiKey', 'geminiModel', 'visionEnabled'], () => {
      apiKeyEl.value = '';
      statusEl.textContent = 'Cleared';
      statusEl.className = 'error';
      setTimeout(() => { statusEl.textContent = ''; statusEl.className = 'note'; }, 1200);
    });
  });
});


