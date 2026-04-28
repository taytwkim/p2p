const idlePollIntervalMs = 2000;
const activePollIntervalMs = 500;
const transferPollIntervalMs = 150;

const uiState = {
  dashboard: null,
  selectedTargets: {},
  isFetching: new Set(),
  backendReady: window.location.protocol !== "file:",
  suspendPolling: false,
  pollTimer: null,
  transferPollTimer: null,
  peerFilesModalPeerId: null,
};

async function apiFetch(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed: ${response.status}`);
  }

  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    return response.json();
  }
  return response.text();
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes < 0) {
    return "--";
  }
  const units = ["B", "KiB", "MiB", "GiB"];
  let value = bytes;
  let unit = units[0];
  for (const next of units.slice(1)) {
    if (value < 1024) {
      break;
    }
    value /= 1024;
    unit = next;
  }
  if (value >= 100 || unit === "B") {
    return `${Math.round(value)} ${unit}`;
  }
  return `${value.toFixed(1)} ${unit}`;
}

function providerColor(owner) {
  const palette = {
    "Peer A": "#39d98a",
    "Peer B": "#2aa8ff",
    "Peer C": "#c77dff",
    "Local cache": "#ffd166",
    Retrieved: "#4bd58f",
  };
  return palette[owner] || "#4bd58f";
}

function syncSelectedTargets() {
  const data = uiState.dashboard;
  if (!data) {
    return;
  }

  const validPeerIds = new Set(data.peers.filter((peer) => peer.online).map((peer) => peer.id));
  for (const file of data.files) {
    const current = uiState.selectedTargets[file.manifestCid];
    if (current && validPeerIds.has(current)) {
      continue;
    }
    uiState.selectedTargets[file.manifestCid] = file.defaultTargetPeerId || data.peers.find((peer) => peer.online)?.id || "";
  }
}

function renderStatusStrip() {
  const root = document.getElementById("status-strip");
  const summary = uiState.dashboard?.summary || { peersOnline: 0, seededFiles: 0 };
  const cards = [
    { value: summary.peersOnline, label: "Peers online" },
    { value: summary.seededFiles, label: "Seeded files" },
  ];

  root.innerHTML = cards
    .map(
      ({ value, label }) => `
        <div class="status-card">
          <strong>${value}</strong>
          <span>${label}</span>
        </div>
      `,
    )
    .join("");
}

function renderPeers() {
  const root = document.getElementById("peer-grid");
  const peers = uiState.dashboard?.peers || [];
  root.innerHTML = peers
    .map((peer, index) => {
      const accent = ["var(--peer-a)", "var(--peer-b)", "var(--peer-c)"][index % 3];
      return `
        <article class="peer-card" style="--peer-color: ${accent}">
          <div class="peer-card__top">
            <div>
              <h3>${peer.name}</h3>
            </div>
            <span class="chip ${peer.online ? "chip--online" : "chip--idle"}">${peer.status}</span>
          </div>

          <div class="peer-card__meta">
            <div class="meta-row">
              <span>RPC socket</span>
              <span>${peer.socket}</span>
            </div>
            <div class="meta-row">
              <span>Export</span>
              <span>${peer.exportDir}</span>
            </div>
          </div>

          <div class="peer-stats">
            <div class="stat">
              <strong>${peer.stats.files}</strong>
              <span>files</span>
            </div>
            <div class="stat">
              <strong>${peer.stats.pieces}</strong>
              <span>pieces</span>
            </div>
            <div class="stat">
              <strong>${peer.stats.activeTransfers}</strong>
              <span>active</span>
            </div>
          </div>

          <div class="peer-card__actions">
            <button class="button button--ghost button--small" data-action="view-files" data-peer-id="${peer.id}" type="button">View Files</button>
            <button class="button button--ghost button--small" data-action="tail-log" data-peer-id="${peer.id}" type="button">Tail Log</button>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderFiles() {
  const root = document.getElementById("file-list");
  const data = uiState.dashboard;
  if (!data || data.files.length === 0) {
    root.innerHTML = `
      <div class="transfer-empty">
        <strong>No files discovered yet</strong>
        <p>Start the local demo harness, then poll files once the peers finish bootstrapping.</p>
      </div>
    `;
    return;
  }

  root.innerHTML = data.files
    .map((file) => {
      const selectedTarget = uiState.selectedTargets[file.manifestCid] || "";
      const targetOptions = data.peers
        .filter((peer) => peer.online)
        .map(
          (peer) => `
            <option value="${peer.id}" ${peer.id === selectedTarget ? "selected" : ""}>${peer.name}</option>
          `,
        )
        .join("");
      const providers = file.providers.length > 0 ? file.providers.join(", ") : "None";
      const isSelected = data.activeTransfer?.manifestCid === file.manifestCid;
      const isFetching = uiState.isFetching.has(file.manifestCid);

      return `
        <article class="file-card ${isSelected ? "file-card--selected" : ""}">
          <div class="file-card__top">
            <div>
              <h4>${file.filename}</h4>
              <div class="role">${file.manifestCid}</div>
            </div>
            <div class="file-card__actions">
              <div class="field">
                <label>Download to</label>
                <select class="select" data-manifest-cid="${file.manifestCid}" aria-label="Download ${file.filename} to peer">
                  ${targetOptions}
                </select>
              </div>
              <button
                class="button button--primary button--small"
                data-action="fetch"
                data-manifest-cid="${file.manifestCid}"
                type="button"
                ${selectedTarget ? "" : "disabled"}
              >
                ${isFetching ? "Fetching..." : "Fetch"}
              </button>
            </div>
          </div>

          <div class="file-card__meta">
            <span>${formatBytes(file.size)}</span>
            <span>${file.pieceCount} pieces</span>
            <span>Providers: ${providers}</span>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderTransfer() {
  const chip = document.getElementById("transfer-chip");
  const title = document.getElementById("transfer-title");
  const root = document.getElementById("transfer-content");
  const transfer = uiState.dashboard?.activeTransfer;

  if (!transfer) {
    chip.className = "chip chip--idle";
    chip.textContent = "Idle";
    title.textContent = "Transfer details";
    root.innerHTML = `
      <div class="transfer-empty">
        <strong>No active transfer</strong>
        <p>Choose a destination peer and click Fetch to watch piece-by-piece progress here.</p>
      </div>
    `;
    return;
  }

  chip.className = "chip chip--live";
  chip.textContent = "Live";
  title.textContent = transfer.filename;

  const providers = Array.from(
    new Map(
      transfer.pieces
        .filter((piece) => piece.state === "done" && piece.owner && piece.owner !== "Pending")
        .map((piece) => [piece.owner, providerColor(piece.owner)]),
    ).entries(),
  );

  const summary = [
    { value: transfer.filename, label: "selected file" },
    { value: transfer.downloader, label: "downloader" },
    { value: `${transfer.completedPieces}/${transfer.totalPieces}`, label: "pieces retrieved" },
    { value: transfer.providersEngaged, label: "providers engaged" },
    { value: transfer.throughput, label: "current throughput" },
  ];

  root.innerHTML = `
    <div class="transfer-summary">
      ${summary
        .map(
          (item) => `
            <div class="summary-card">
              <strong>${item.value}</strong>
              <span>${item.label}</span>
            </div>
          `,
        )
        .join("")}
    </div>

    <div class="piece-map-caption">
      <span>${transfer.totalPieces} total pieces</span>
      <div class="provider-legend">
        ${providers
          .map(
            ([owner, color]) => `
              <span class="provider-chip">
                <i style="background:${color}"></i>${owner}
              </span>
            `,
          )
          .join("")}
      </div>
    </div>

    <div class="piece-grid">
      ${transfer.pieces
        .map(
          (piece) => `
            <div
              class="piece piece--${piece.state}"
              style="${piece.state === "done" ? `background:${providerColor(piece.owner)}; border-color:${providerColor(piece.owner)};` : ""}"
              title="Piece ${piece.index}: ${piece.owner}"
              aria-label="Piece ${piece.index}: ${piece.owner}"
            >
            </div>
          `,
        )
        .join("")}
    </div>
  `;
}

function renderAll() {
  renderStatusStrip();
  renderPeers();
  renderFiles();
  renderTransfer();
  renderPeerFilesModal();
}

function hasLiveTransfer() {
  return Boolean(uiState.dashboard?.activeTransfer) || uiState.isFetching.size > 0;
}

function currentPollInterval() {
  return hasLiveTransfer() ? activePollIntervalMs : idlePollIntervalMs;
}

function scheduleNextPoll(delay = currentPollInterval()) {
  if (!uiState.backendReady) {
    return;
  }
  if (uiState.pollTimer) {
    clearTimeout(uiState.pollTimer);
  }
  uiState.pollTimer = setTimeout(refreshState, delay);
}

function scheduleNextTransferPoll(delay = transferPollIntervalMs) {
  if (!uiState.backendReady) {
    return;
  }
  if (uiState.transferPollTimer) {
    clearTimeout(uiState.transferPollTimer);
  }
  if (!hasLiveTransfer()) {
    uiState.transferPollTimer = null;
    return;
  }
  uiState.transferPollTimer = setTimeout(refreshTransferState, delay);
}

async function refreshTransferState() {
  if (!uiState.backendReady) {
    return;
  }
  if (uiState.suspendPolling || !hasLiveTransfer()) {
    scheduleNextTransferPoll();
    return;
  }
  try {
    const data = await apiFetch("/api/transfer");
    if (uiState.dashboard) {
      uiState.dashboard.activeTransfer = data.activeTransfer || null;
      renderTransfer();
    }
  } catch (error) {
    console.error(error);
  } finally {
    scheduleNextTransferPoll();
  }
}

async function refreshState() {
  if (!uiState.backendReady) {
    renderAll();
    return;
  }
  if (uiState.suspendPolling) {
    scheduleNextPoll(idlePollIntervalMs);
    return;
  }
  try {
    const data = await apiFetch("/api/state");
    uiState.dashboard = data;
    syncSelectedTargets();
    renderAll();
  } catch (error) {
    console.error(error);
  } finally {
    scheduleNextPoll();
    scheduleNextTransferPoll();
  }
}

async function startFetch(manifestCid) {
  if (!uiState.backendReady) {
    showBackendNotice();
    return;
  }
  const targetPeerId = uiState.selectedTargets[manifestCid];
  if (!targetPeerId) {
    return;
  }

  uiState.isFetching.add(manifestCid);
  renderAll();
  scheduleNextPoll(activePollIntervalMs);
  scheduleNextTransferPoll(0);
  try {
    await apiFetch("/api/fetch", {
      method: "POST",
      body: JSON.stringify({ manifestCid, targetPeerId }),
    });
    setTimeout(refreshState, 150);
  } catch (error) {
    alert(error.message);
  } finally {
    uiState.isFetching.delete(manifestCid);
    renderAll();
    scheduleNextPoll();
    scheduleNextTransferPoll();
  }
}

function viewPeerFiles(peerId) {
  const peer = uiState.dashboard?.peers.find((entry) => entry.id === peerId);
  if (!peer) {
    return;
  }
  uiState.peerFilesModalPeerId = peerId;
  renderPeerFilesModal();
}

function closePeerFilesModal() {
  uiState.peerFilesModalPeerId = null;
  renderPeerFilesModal();
}

function renderPeerFilesModal() {
  const modal = document.getElementById("peer-files-modal");
  const title = document.getElementById("peer-files-title");
  const meta = document.getElementById("peer-files-meta");
  const body = document.getElementById("peer-files-body");

  const peer = uiState.dashboard?.peers.find((entry) => entry.id === uiState.peerFilesModalPeerId);
  if (!peer) {
    modal.hidden = true;
    body.innerHTML = "";
    return;
  }

  modal.hidden = false;
  title.textContent = peer.name;
  meta.textContent = `${peer.exportDir} · ${peer.files.length} complete file${peer.files.length === 1 ? "" : "s"}`;

  if (peer.files.length === 0) {
    body.innerHTML = `
      <div class="transfer-empty">
        <strong>No complete files yet</strong>
        <p>This peer has not fully reconstructed any files in its export directory.</p>
      </div>
    `;
    return;
  }

  body.innerHTML = peer.files
    .map(
      (file) => `
        <article class="modal-file">
          <div class="modal-file__top">
            <div>
              <h4>${file.filename}</h4>
              <div class="modal-file__cid">${file.manifestCid}</div>
            </div>
          </div>
          <div class="modal-file__meta">
            <span>${formatBytes(file.size)}</span>
            <span>${file.pieceCount} pieces</span>
          </div>
        </article>
      `,
    )
    .join("");
}

function bindEvents() {
  const pollButton = document.getElementById("poll-files");
  if (pollButton) {
    pollButton.addEventListener("click", refreshState);
  }

  document.addEventListener("change", (event) => {
    if (!event.target.matches(".select")) {
      return;
    }
    uiState.selectedTargets[event.target.dataset.manifestCid] = event.target.value;
  });

  document.addEventListener("focusin", (event) => {
    if (event.target.matches(".select")) {
      uiState.suspendPolling = true;
    }
  });

  document.addEventListener("focusout", (event) => {
    if (event.target.matches(".select")) {
      uiState.suspendPolling = false;
      setTimeout(refreshState, 0);
    }
  });

  document.addEventListener("click", (event) => {
    if (event.target.matches("[data-close-modal='true']")) {
      closePeerFilesModal();
      return;
    }

    const button = event.target.closest("button[data-action]");
    if (!button) {
      return;
    }

    const action = button.dataset.action;
    if (action === "fetch") {
      startFetch(button.dataset.manifestCid);
      return;
    }
    if (action === "tail-log") {
      window.open(`/api/peers/${button.dataset.peerId}/log`, "_blank", "noopener");
      return;
    }
    if (action === "view-files") {
      viewPeerFiles(button.dataset.peerId);
    }
  });

  const closePeerFilesButton = document.getElementById("close-peer-files");
  if (closePeerFilesButton) {
    closePeerFilesButton.addEventListener("click", closePeerFilesModal);
  }

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && uiState.peerFilesModalPeerId) {
      closePeerFilesModal();
    }
  });
}

function showBackendNotice() {
  const notice = document.getElementById("backend-notice");
  notice.hidden = false;
  notice.innerHTML = `
    <strong>Dashboard backend required</strong>
    <p>
      This page can render as a static file, but polling, fetches, and log views need the local
      dashboard server. Start the local demo harness first with <code>./demo/local/local_demo.sh start</code>,
      then run <code>./tinytorrent dashboard</code> from the repo root and open the served URL
      instead of opening <code>index.html</code> directly.
    </p>
  `;
}

function init() {
  bindEvents();
  if (!uiState.backendReady) {
    showBackendNotice();
  }
  refreshState();
}

init();
