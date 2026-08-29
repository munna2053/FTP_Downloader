const $ = (id) => document.getElementById(id);

const presets = {
  media: [".3g2", ".3gp", ".aac", ".aif", ".aiff", ".ape", ".arw", ".asf", ".avi", ".bmp", ".cr2", ".cr3", ".dng", ".flac", ".flv", ".gif", ".heic", ".heif", ".jpeg", ".jpg", ".m4a", ".m4v", ".mkv", ".mov", ".mp3", ".mp4", ".mpeg", ".mpg", ".nef", ".ogg", ".ogv", ".opus", ".orf", ".png", ".raf", ".raw", ".rw2", ".svg", ".tif", ".tiff", ".wav", ".webm", ".webp", ".wma", ".wmv"],
  image: [".arw", ".bmp", ".cr2", ".cr3", ".dng", ".gif", ".heic", ".heif", ".jpeg", ".jpg", ".nef", ".orf", ".png", ".raf", ".raw", ".rw2", ".svg", ".tif", ".tiff", ".webp"],
  video: [".3g2", ".3gp", ".asf", ".avi", ".flv", ".m4v", ".mkv", ".mov", ".mp4", ".mpeg", ".mpg", ".ogv", ".webm", ".wmv"],
  audio: [".aac", ".aif", ".aiff", ".ape", ".flac", ".m4a", ".mp3", ".ogg", ".opus", ".wav", ".wma"],
  document: [".csv", ".doc", ".docx", ".json", ".log", ".md", ".pdf", ".ppt", ".pptx", ".rtf", ".txt", ".xls", ".xlsx", ".xml"],
  archive: [".7z", ".bz2", ".gz", ".iso", ".rar", ".tar", ".tgz", ".zip"],
  all: [],
};

let currentJobId = null;
let pollTimer = null;
let currentBrowsePath = "/";
let historyPage = 1;
let historyPageSize = 25;
let historyTotal = 0;
let historyDebounce = null;
let lastHistoryRefresh = 0;
const storageKeys = {
  remotePath: "ftpHttpDownloader.remotePath",
  savePath: "ftpHttpDownloader.savePath",
  protocol: "ftpHttpDownloader.protocol",
  port: "ftpHttpDownloader.port",
  username: "ftpHttpDownloader.username",
  passive: "ftpHttpDownloader.passive",
  tls: "ftpHttpDownloader.tls",
  lastHost: "ftpHttpDownloader.lastHost",
};

// There's no separate Host field: the URL box carries the host. But the
// Source Browser replaces the URL box with a plain path (no scheme/host)
// the moment you open a folder, so the host has to be remembered somewhere
// else or every click after the first one loses it. This is that memory.
let capturedHost = localStorage.getItem(storageKeys.lastHost) || "";

function isFullUrl(value) {
  return /^(https?|ftps?):\/\//i.test(String(value || "").trim());
}

function parseConnectionUrl(value) {
  const text = String(value || "").trim();
  if (!isFullUrl(text)) return null;
  try {
    const parsed = new URL(text);
    const scheme = parsed.protocol.replace(":", "").toLowerCase();
    return {
      protocol: scheme === "ftps" ? "ftp" : scheme,
      host: parsed.hostname,
      port: parsed.port ? Number(parsed.port) : null,
      tls: scheme === "ftps",
    };
  } catch {
    return null;
  }
}

// Called whenever the URL box changes. If it holds a full URL, remember its
// host/protocol/port so later requests (after the box reverts to a plain
// path) still know where to connect.
function captureConnectionFromUrl(value) {
  const parsed = parseConnectionUrl(value);
  if (!parsed) return;
  capturedHost = parsed.host;
  saveFolderValue(storageKeys.lastHost, capturedHost);
  $("protocol").value = parsed.protocol;
  saveFolderValue(storageKeys.protocol, parsed.protocol);
  if (parsed.port) {
    $("port").value = parsed.port;
    saveFolderValue(storageKeys.port, String(parsed.port));
  }
  if (parsed.tls) $("tls").checked = true;
  updateProtocolControls();
}

function connectionPayload(timeout = 30) {
  const protocol = detectProtocol();
  const urlConn = parseConnectionUrl($("remotePath").value);
  const host = (urlConn && urlConn.host) || capturedHost;
  const port = (urlConn && urlConn.port) || Number($("port").value || defaultPort(protocol));
  return {
    protocol,
    host,
    port,
    username: $("username").value.trim() || "anonymous",
    password: $("password").value,
    passive: $("passive").checked,
    tls: $("tls").checked,
    timeout,
  };
}

function detectProtocol() {
  const urlConn = parseConnectionUrl($("remotePath").value);
  if (urlConn) return urlConn.protocol;
  return $("protocol").value;
}

function defaultPort(protocol) {
  if (protocol === "https") return 443;
  if (protocol === "http") return 80;
  return 21;
}

function updateProtocolControls() {
  const protocol = detectProtocol();
  const port = $("port");
  if (!port.value || ["21", "80", "443"].includes(port.value)) {
    port.value = defaultPort(protocol);
  }
  const isHttp = protocol === "http" || protocol === "https";
  $("username").disabled = isHttp;
  $("password").disabled = isHttp;
  $("passive").disabled = isHttp;
  $("tls").disabled = isHttp;
}

function extensionList() {
  const value = $("extensions").value.trim();
  if (!value) return [];
  return value.split(/[\s,;]+/).map((item) => item.trim()).filter(Boolean);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error || `Request failed: ${response.status}`);
  }
  return data;
}

function formatBytes(value) {
  if (!Number.isFinite(value) || value <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = value;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${size.toFixed(size >= 10 || index === 0 ? 0 : 1)} ${units[index]}`;
}

function formatTime(value) {
  if (!value) return "";
  return new Date(value * 1000).toLocaleString();
}

function percent(bytes, total) {
  if (!total) return 0;
  return Math.max(0, Math.min(100, (bytes / total) * 100));
}

function setStatus(text, mode = "") {
  const target = $("connectionStatus");
  target.textContent = text;
  target.className = `status-pill ${mode}`.trim();
}

function normalizeRemotePath(path) {
  const value = String(path || "/").replaceAll("\\", "/").trim();
  if (value.startsWith("http://") || value.startsWith("https://")) {
    try {
      return decodeURIComponent(new URL(value).pathname || "/");
    } catch {
      return "/";
    }
  }
  if (!value || value === ".") return "/";
  let decoded = value;
  try {
    decoded = decodeURIComponent(value);
  } catch {
    decoded = value;
  }
  const withRoot = decoded.startsWith("/") ? decoded : `/${decoded}`;
  return withRoot.replace(/\/+/g, "/");
}

function parentPath(path) {
  const normalized = normalizeRemotePath(path);
  if (normalized === "/") return "/";
  const parts = normalized.split("/").filter(Boolean);
  parts.pop();
  return parts.length ? `/${parts.join("/")}` : "/";
}

async function browse(path = $("remotePath").value) {
  const rawPath = String(path || "/").trim();
  const requestPath = isFullUrl(rawPath) ? rawPath : normalizeRemotePath(rawPath);
  const displayPath = isFullUrl(rawPath) ? normalizeRemotePath(rawPath) : requestPath;
  $("browserList").innerHTML = `<div class="empty">Loading ${displayPath}...</div>`;
  try {
    const data = await api("/api/browse", {
      method: "POST",
      body: JSON.stringify({ connection: connectionPayload(30), path: requestPath }),
    });
    currentBrowsePath = data.path;
    $("remotePath").value = data.path;
    $("browserPath").textContent = data.path;
    renderBrowser(data.entries);
    setStatus(`${(data.protocol || detectProtocol()).toUpperCase()} connected`, "ok");
  } catch (error) {
    $("browserList").innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`;
    setStatus("Browse failed", "bad");
  }
}

function renderBrowser(entries) {
  const list = $("browserList");
  if (!entries.length) {
    list.innerHTML = `<div class="empty">This folder is empty.</div>`;
    return;
  }
  list.innerHTML = "";
  for (const entry of entries) {
    const row = document.createElement("div");
    row.className = "file-row";
    const icon = entry.type === "directory" ? "F" : "-";
    const action = entry.type === "directory"
      ? `<button type="button" data-open="${escapeAttr(entry.path)}">Open</button>`
      : `<span class="file-sub">File</span>`;
    row.innerHTML = `
      <div class="file-icon">${icon}</div>
      <div>
        <div class="file-name" title="${escapeAttr(entry.name)}">${escapeHtml(entry.name)}</div>
        <div class="file-sub">${entry.type === "directory" ? "Folder" : formatBytes(entry.size || 0)}</div>
      </div>
      ${action}
    `;
    list.appendChild(row);
  }
}

function renderStats(job) {
  const totalFiles = job.files.length;
  const totals = job.totals;
  const chunks = [
    ["Done", totals.done],
    ["Active", totals.downloading],
    ["Queued", totals.queued],
    ["Skipped", totals.skipped],
    ["Failed", totals.error],
  ];
  $("jobStats").innerHTML = chunks.map(([label, value]) => `
    <div class="stat"><b>${value}</b><span>${label}</span></div>
  `).join("") + `<div class="stat"><b>${totalFiles}</b><span>Total</span></div>`;
}

function renderJob(job) {
  $("jobMessage").textContent = job.message || job.state;
  $("jobMessage").classList.toggle("error-text", job.state === "error");
  $("cancelButton").disabled = !["queued", "scanning", "downloading"].includes(job.state);
  renderStats(job);

  const totalBytes = job.totals.totalBytes;
  const doneBytes = job.files.reduce((sum, file) => {
    if (file.status === "skipped") return sum + (file.size || file.bytes || 0);
    return sum + (file.bytes || 0);
  }, 0);
  $("overallBar").style.width = `${percent(doneBytes, totalBytes)}%`;

  const downloads = $("downloads");
  if (!job.files.length) {
    downloads.innerHTML = `<div class="empty">${job.state === "scanning" ? "Scanning folders..." : "No matching files yet."}</div>`;
    return;
  }

  const activeFirst = [...job.files].sort((a, b) => {
    const order = { downloading: 0, error: 1, queued: 2, done: 3, skipped: 4 };
    return (order[a.status] ?? 9) - (order[b.status] ?? 9);
  });

  downloads.innerHTML = "";
  for (const file of activeFirst) {
    const fileTotal = file.size || 0;
    const width = file.status === "done" || file.status === "skipped" ? 100 : percent(file.bytes || 0, fileTotal);
    const row = document.createElement("div");
    row.className = "download-row";
    row.innerHTML = `
      <div class="download-top">
        <div class="download-name" title="${escapeAttr(file.relativePath)}">${escapeHtml(file.relativePath)}</div>
        <div class="download-status ${escapeAttr(file.status)}">${escapeHtml(file.status)}</div>
      </div>
      <div class="bar"><div style="width:${width}%"></div></div>
      <div class="download-meta">
        <span>${formatBytes(file.bytes || 0)}${fileTotal ? ` / ${formatBytes(fileTotal)}` : ""}</span>
        <span>${file.speed ? `${formatBytes(file.speed)}/s` : ""}</span>
      </div>
      ${file.error ? `<div class="download-error">${escapeHtml(file.error)}</div>` : ""}
    `;
    downloads.appendChild(row);
  }
}

async function loadDownloadHistory(showLoading = true) {
  if (showLoading) {
    $("historyBody").innerHTML = `<tr><td colspan="6">Loading...</td></tr>`;
  }
  const params = new URLSearchParams({
    page: String(historyPage),
    pageSize: String(historyPageSize),
  });
  const status = $("historyStatus").value;
  const query = $("historySearch").value.trim();
  if (status) params.set("status", status);
  if (query) params.set("q", query);
  try {
    const data = await api(`/api/downloads?${params.toString()}`);
    historyTotal = data.total;
    renderDownloadHistory(data);
    lastHistoryRefresh = Date.now();
  } catch (error) {
    $("historyBody").innerHTML = `<tr><td colspan="6">${escapeHtml(error.message)}</td></tr>`;
    $("historySummary").textContent = "Could not load saved downloads.";
  }
}

function renderDownloadHistory(data) {
  const body = $("historyBody");
  const start = data.total ? ((data.page - 1) * data.pageSize) + 1 : 0;
  const end = Math.min(data.total, data.page * data.pageSize);
  $("historySummary").textContent = data.total ? `Showing ${start}-${end} of ${data.total} saved downloads.` : "No saved downloads yet.";
  $("historyPage").textContent = `Page ${data.page}`;
  $("historyPrev").disabled = data.page <= 1;
  $("historyNext").disabled = end >= data.total;
  if (!data.items.length) {
    body.innerHTML = `<tr><td colspan="6">No downloads found.</td></tr>`;
    return;
  }
  body.innerHTML = "";
  for (const item of data.items) {
    const width = item.status === "done" || item.status === "skipped" ? 100 : percent(item.bytes || 0, item.size || 0);
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>
        <div class="history-file" title="${escapeAttr(item.relativePath || item.name)}">${escapeHtml(item.name)}</div>
        <div class="history-path" title="${escapeAttr(item.remotePath)}">${escapeHtml(item.remotePath)}</div>
      </td>
      <td><span class="download-status ${escapeAttr(item.status)}">${escapeHtml(item.status)}</span></td>
      <td>
        <div class="mini-progress">
          <div class="bar"><div style="width:${width}%"></div></div>
          <span class="file-sub">${formatBytes(item.bytes || 0)}${item.size ? ` / ${formatBytes(item.size)}` : ""}</span>
        </div>
      </td>
      <td>${item.size ? formatBytes(item.size) : ""}</td>
      <td>${escapeHtml(formatTime(item.updatedAt))}</td>
      <td><div class="history-path" title="${escapeAttr(item.localPath)}">${escapeHtml(item.localPath)}</div></td>
    `;
    body.appendChild(row);
  }
}

async function pollJob() {
  if (!currentJobId) return;
  try {
    const job = await api(`/api/jobs/${currentJobId}`);
    renderJob(job);
    if (Date.now() - lastHistoryRefresh > 5000) {
      loadDownloadHistory(false);
    }
    if (["completed", "cancelled", "error"].includes(job.state)) {
      clearInterval(pollTimer);
      pollTimer = null;
      $("startButton").disabled = false;
      loadDownloadHistory(false);
    }
  } catch (error) {
    $("jobMessage").textContent = error.message;
  }
}

async function startJob(event) {
  event.preventDefault();
  $("startButton").disabled = true;
  $("jobMessage").classList.remove("error-text");
  $("jobMessage").textContent = "Starting...";
  $("downloads").innerHTML = `<div class="empty">Preparing job...</div>`;
  try {
    const payload = {
      connection: connectionPayload(300),
      remotePath: isFullUrl($("remotePath").value) ? $("remotePath").value.trim() : normalizeRemotePath($("remotePath").value),
      savePath: $("savePath").value.trim(),
      fileLimit: Number($("fileLimit").value || 0),
      concurrency: Math.min(8, Math.max(1, Number($("concurrency").value || 4))),
      extensions: extensionList(),
      skipExisting: $("skipExisting").checked,
    };
    const job = await api("/api/jobs", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    currentJobId = job.id;
    renderJob(job);
    clearInterval(pollTimer);
    pollTimer = setInterval(pollJob, 800);
    pollJob();
  } catch (error) {
    $("jobMessage").textContent = error.message;
    $("jobMessage").classList.add("error-text");
    $("downloads").innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`;
    $("startButton").disabled = false;
  }
}

async function cancelJob() {
  if (!currentJobId) return;
  $("cancelButton").disabled = true;
  try {
    const job = await api(`/api/jobs/${currentJobId}/cancel`, { method: "POST", body: "{}" });
    renderJob(job);
  } catch (error) {
    $("jobMessage").textContent = error.message;
  }
}

function setPreset(name) {
  if (name === "custom") return;
  $("extensions").value = (presets[name] || []).join(", ");
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;",
  }[char]));
}

function escapeAttr(value) {
  return escapeHtml(value).replace(/`/g, "&#096;");
}

async function loadDefaults() {
  try {
    const defaults = await api("/api/defaults");
    $("savePath").value = localStorage.getItem(storageKeys.savePath) || defaults.savePath;
    if (defaults.mediaExtensions?.length) {
      presets.media = defaults.mediaExtensions;
      setPreset("media");
    }
  } catch {
    setPreset("media");
  }
}

function loadSavedFolders() {
  const remotePath = localStorage.getItem(storageKeys.remotePath);
  const savePath = localStorage.getItem(storageKeys.savePath);
  const protocol = localStorage.getItem(storageKeys.protocol);
  const port = localStorage.getItem(storageKeys.port);
  const username = localStorage.getItem(storageKeys.username);
  const passive = localStorage.getItem(storageKeys.passive);
  const tls = localStorage.getItem(storageKeys.tls);
  if (protocol) $("protocol").value = protocol;
  if (port) $("port").value = port;
  if (username) $("username").value = username;
  if (passive !== null) $("passive").checked = passive === "1";
  if (tls !== null) $("tls").checked = tls === "1";
  // Note: password is intentionally never persisted to localStorage.
  if (remotePath) {
    $("remotePath").value = remotePath;
    currentBrowsePath = normalizeRemotePath(remotePath);
    $("browserPath").textContent = currentBrowsePath;
    captureConnectionFromUrl(remotePath);
  }
  if (savePath) {
    $("savePath").value = savePath;
  }
}

function saveFolderValue(key, value) {
  localStorage.setItem(key, value);
}

$("downloadForm").addEventListener("submit", startJob);
$("cancelButton").addEventListener("click", cancelJob);
$("browseButton").addEventListener("click", () => browse($("remotePath").value));
$("browseRootButton").addEventListener("click", () => browse("/"));
$("upButton").addEventListener("click", () => browse(parentPath(currentBrowsePath)));
$("protocol").addEventListener("change", updateProtocolControls);
$("typePreset").addEventListener("change", (event) => setPreset(event.target.value));
$("extensions").addEventListener("input", () => {
  $("typePreset").value = "custom";
});
$("browserList").addEventListener("click", (event) => {
  const openPath = event.target?.dataset?.open;
  const selectPath = event.target?.dataset?.select;
  if (openPath) browse(openPath);
  if (selectPath) {
    $("remotePath").value = selectPath;
    saveFolderValue(storageKeys.remotePath, selectPath);
  }
});
$("remotePath").addEventListener("input", () => {
  saveFolderValue(storageKeys.remotePath, $("remotePath").value);
  captureConnectionFromUrl($("remotePath").value);
});
$("remotePath").addEventListener("change", () => saveFolderValue(storageKeys.remotePath, $("remotePath").value));
$("savePath").addEventListener("input", () => saveFolderValue(storageKeys.savePath, $("savePath").value));
$("savePath").addEventListener("change", () => saveFolderValue(storageKeys.savePath, $("savePath").value));
$("protocol").addEventListener("change", () => saveFolderValue(storageKeys.protocol, $("protocol").value));
$("port").addEventListener("change", () => saveFolderValue(storageKeys.port, $("port").value));
$("username").addEventListener("change", () => saveFolderValue(storageKeys.username, $("username").value));
$("passive").addEventListener("change", () => saveFolderValue(storageKeys.passive, $("passive").checked ? "1" : "0"));
$("tls").addEventListener("change", () => saveFolderValue(storageKeys.tls, $("tls").checked ? "1" : "0"));
$("historyRefresh").addEventListener("click", () => loadDownloadHistory());
$("historyPrev").addEventListener("click", () => {
  if (historyPage > 1) {
    historyPage -= 1;
    loadDownloadHistory();
  }
});
$("historyNext").addEventListener("click", () => {
  if (historyPage * historyPageSize < historyTotal) {
    historyPage += 1;
    loadDownloadHistory();
  }
});
$("historyStatus").addEventListener("change", () => {
  historyPage = 1;
  loadDownloadHistory();
});
$("historySearch").addEventListener("input", () => {
  clearTimeout(historyDebounce);
  historyDebounce = setTimeout(() => {
    historyPage = 1;
    loadDownloadHistory();
  }, 300);
});

loadSavedFolders();
loadDefaults();
updateProtocolControls();
loadDownloadHistory();
setInterval(() => loadDownloadHistory(false), 5000);
