document.addEventListener('DOMContentLoaded', () => {
    const executeBtn = document.getElementById('executeBtn');
    const planBtn = document.getElementById('planBtn');
    const taskInput = document.getElementById('taskInput');
    const statusMessage = document.getElementById('statusMessage');
    const previewToggle = document.getElementById('previewToggle');

    // API key is embedded; no need to load

    function setStatus(text, kind) {
        statusMessage.textContent = text || '';
        statusMessage.className = kind ? kind : '';
    }

    // --- Execute ---
    executeBtn.addEventListener('click', () => {
        const commandText = taskInput.value.trim();
        
        if (!commandText) {
            alert('Please enter a command.');
            return;
        }

        // --- Update UI to show processing state ---
        executeBtn.disabled = true;
        planBtn && (planBtn.disabled = true);
        setStatus('Agent is thinking...', '');

        // Send the command and API key to the background script
        chrome.runtime.sendMessage({
            action: "executeTask",
            command: commandText,
            preview: !!previewToggle?.checked
        }, (response) => {
            // --- Handle the response from the background script ---
            if (chrome.runtime.lastError) {
                // This catches errors if the background script couldn't be reached
                setStatus('Error: Could not connect to agent.', 'error');
                executeBtn.disabled = false;
                planBtn && (planBtn.disabled = false);
                return;
            }

            executeBtn.disabled = false;
            planBtn && (planBtn.disabled = false);
            if (response.success) {
                setStatus('Task executed successfully!', 'success');
                // Close the popup after a successful execution
                setTimeout(() => window.close(), 1500);
            } else if (response.plan) {
                setStatus(`Planned: ${response.plan.action || 'unknown'} ${response.plan.selector || response.plan.targetText || response.plan.name || ''}`, '');
            } else if (response.needClarification) {
                setStatus(`Need input: ${response.error}`, 'error');
                // keep popup open so user can refine the command
            } else {
                setStatus(`Error: ${response.error}`, 'error');
            }
        });
    });

    // --- Plan only (no execute) ---
    if (planBtn) {
        planBtn.addEventListener('click', () => {
            const commandText = taskInput.value.trim();
            if (!commandText) { alert('Please enter a command.'); return; }
            planBtn.disabled = true;
            executeBtn && (executeBtn.disabled = true);
            setStatus('Planning...', '');
            chrome.runtime.sendMessage({ action: 'planTask', command: commandText }, (response) => {
                planBtn.disabled = false;
                executeBtn && (executeBtn.disabled = false);
                if (chrome.runtime.lastError) { setStatus('Error: Could not connect to agent.', 'error'); return; }
                if (response && response.plan) {
                    setStatus(`Planned: ${response.plan.action || 'unknown'} ${response.plan.selector || response.plan.targetText || response.plan.name || ''}`, '');
                } else {
                    setStatus(response && response.error ? `Error: ${response.error}` : 'No plan generated.', 'error');
                }
            });
        });
    }
});

