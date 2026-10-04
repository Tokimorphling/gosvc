"use strict";

(() => {
  const byId = (id) => document.getElementById(id);
  const sessionKey = `gosvc-demo-key:${location.pathname}`;
  const ui = Object.fromEntries([
    "health-pill", "health-label", "overview-health", "health-detail", "uptime", "version", "storage",
    "task-form", "task-title", "add-task", "task-count", "task-list", "empty-state", "empty-title",
    "empty-copy", "count-all", "count-pending", "count-done", "list-caption", "sync-label", "refresh",
    "connect-form", "api-key", "connect-button", "connection-heading", "connection-copy",
    "connected-state", "disconnect", "notice",
  ].map((id) => [id, byId(id)]));
  let apiKey = "";
  let tasks = [];
  let filter = "all";
  let connected = false;
  let busy = false;
  let generation = 0;
  let noticeTimer;
  let startedAt = null;
  let healthBusy = false;

  function notify(message, error = false) {
    clearTimeout(noticeTimer);
    ui.notice.textContent = message;
    ui.notice.classList.toggle("error", error);
    ui.notice.hidden = false;
    noticeTimer = setTimeout(() => { ui.notice.hidden = true; }, 5000);
  }

  function saveKey(value) {
    try {
      if (value) sessionStorage.setItem(sessionKey, value);
      else sessionStorage.removeItem(sessionKey);
    } catch { /* A restricted browser can still use an in-memory connection. */ }
  }

  async function request(path, options = {}) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 10000);
    try {
      const headers = { "X-API-Key": apiKey };
      if (options.body !== undefined) headers["Content-Type"] = "application/json";
      const response = await fetch(path, { ...options, headers, signal: controller.signal, cache: "no-store" });
      if (!response.ok) {
        const error = new Error(response.status === 401 || response.status === 403
          ? "访问密钥无效或已失效，请重新输入。"
          : response.status === 404 ? "这条任务已不存在，请刷新列表。"
          : response.status === 409 ? "任务数量已达上限，请先删除一些任务。"
          : response.status === 429 ? "操作太频繁，请稍后再试。"
          : response.status === 400 ? "请求未通过校验，请检查输入内容。"
          : "服务暂时无法完成操作，请稍后重试。");
        error.status = response.status;
        throw error;
      }
      return response.status === 204 ? null : await response.json();
    } catch (error) {
      if (error.name === "AbortError") throw new Error("请求超时，请稍后重试。");
      if (error instanceof TypeError) throw new Error("暂时无法连接服务器，请检查网络后重试。");
      if (error instanceof SyntaxError) throw new Error("服务器返回了无法识别的数据，请稍后重试。");
      throw error;
    } finally { clearTimeout(timer); }
  }

  function updateControls() {
    ui["task-title"].disabled = !connected || busy;
    ui["add-task"].disabled = !connected || busy;
    ui.refresh.disabled = !connected || busy;
    ui["connect-button"].disabled = busy;
    ui["api-key"].disabled = busy;
    ui["connect-form"].hidden = connected;
    ui["connected-state"].hidden = !connected;
    ui["connection-heading"].textContent = connected ? "工作区已连接" : "连接你的工作区";
    ui["connection-copy"].textContent = connected
      ? "现在可以管理服务器上的任务。操作完成后，数据会自动保存。"
      : "首页公开可见。任务操作需要访问密钥，请向服务器管理员获取。";
    for (const button of ui["task-list"].querySelectorAll("button")) {
      button.disabled = busy || button.dataset.completed === "true";
    }
  }

  function disconnect() {
    generation++;
    apiKey = "";
    connected = false;
    tasks = [];
    startedAt = null;
    saveKey("");
    ui["api-key"].value = "";
    ui["task-title"].value = "";
    ui.uptime.textContent = "—";
    ui.version.textContent = "—";
    ui["sync-label"].textContent = "等待连接";
    updateControls();
    renderTasks();
  }

  function handleError(error) {
    if (error.status === 401 || error.status === 403) disconnect();
    notify(error.message || "操作失败，请稍后重试。", true);
  }

  function formatUptime(seconds) {
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor(seconds % 86400 / 3600);
    const minutes = Math.floor(seconds % 3600 / 60);
    if (days > 0) return `${days} 天 ${hours} 小时`;
    if (hours > 0) return `${hours} 小时 ${minutes} 分`;
    if (minutes > 0) return `${minutes} 分 ${Math.floor(seconds % 60)} 秒`;
    return `${Math.max(0, Math.floor(seconds))} 秒`;
  }

  function updateUptime() {
    if (startedAt !== null) ui.uptime.textContent = formatUptime((Date.now() - startedAt) / 1000);
  }

  function applyStatus(status) {
    startedAt = Date.now() - Math.max(0, Number(status.uptimeSeconds) || 0) * 1000;
    updateUptime();
    ui.version.textContent = status.version || "开发版本";
    ui.version.title = status.version || "开发版本";
    ui.storage.textContent = status.storage === "JSON file" ? "文件存储" : "持久化";
  }

  function formatDate(value) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return "";
    return new Intl.DateTimeFormat("zh-CN", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false }).format(date);
  }

  function renderTasks() {
    const pending = tasks.filter((task) => !task.done).length;
    ui["task-count"].textContent = connected ? String(tasks.length) : "—";
    ui["count-all"].textContent = String(tasks.length);
    ui["count-pending"].textContent = String(pending);
    ui["count-done"].textContent = String(tasks.length - pending);
    ui["list-caption"].textContent = connected ? `${pending} 件小事，等你完成` : "先连接，再开始";
    const visible = tasks.filter((task) => filter === "all" || (filter === "done" ? task.done : !task.done));
    const fragment = document.createDocumentFragment();
    for (const task of visible) {
      const item = document.createElement("li");
      item.className = `task-item${task.done ? " done" : ""}`;
      const complete = document.createElement("button");
      complete.type = "button";
      complete.className = "complete-button";
      complete.textContent = "✓";
      complete.title = task.done ? "已完成" : "标记为已完成";
      complete.setAttribute("aria-label", `${task.done ? "已完成" : "完成任务"}：${task.title}`);
      complete.dataset.completed = String(task.done);
      complete.disabled = busy || task.done;
      complete.addEventListener("click", () => mutateTask(task.id, "complete"));
      const content = document.createElement("div");
      content.className = "task-text";
      const title = document.createElement("div");
      title.className = "task-title";
      title.textContent = task.title;
      const date = document.createElement("div");
      date.className = "task-date";
      date.textContent = `创建于 ${formatDate(task.createdAt)}`;
      content.append(title, date);
      const remove = document.createElement("button");
      remove.type = "button";
      remove.className = "delete-button";
      remove.title = "删除任务";
      remove.setAttribute("aria-label", `删除任务：${task.title}`);
      remove.disabled = busy;
      const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
      svg.setAttribute("viewBox", "0 0 24 24");
      svg.setAttribute("fill", "none");
      svg.setAttribute("stroke", "currentColor");
      svg.setAttribute("stroke-width", "1.6");
      svg.setAttribute("aria-hidden", "true");
      const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
      path.setAttribute("d", "M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13M10 10v7M14 10v7");
      svg.append(path);
      remove.append(svg);
      remove.addEventListener("click", () => mutateTask(task.id, "delete"));
      item.append(complete, content, remove);
      fragment.append(item);
    }
    ui["task-list"].replaceChildren(fragment);
    ui["empty-state"].hidden = visible.length > 0;
    ui["empty-title"].textContent = !connected ? "你的工作区，准备好了"
      : filter === "done" ? "完成第一件事，就从现在开始"
      : filter === "pending" && tasks.length > 0 ? "全部完成，给自己一点掌声"
      : "从一件小事开始";
    ui["empty-copy"].textContent = !connected ? "输入访问密钥，查看和管理服务器上的任务。"
      : filter === "done" ? "完成的任务会出现在这里。"
      : filter === "pending" && tasks.length > 0 ? "暂时没有待办事项。休息一下，或写下新计划。"
      : "在上方添加任务，让今天的计划有个着落。";
  }

  function markSynced() {
    ui["sync-label"].textContent = `已同步 · ${new Intl.DateTimeFormat("zh-CN", { hour: "2-digit", minute: "2-digit", hour12: false }).format(new Date())}`;
  }

  async function loadWorkspace() {
    const current = generation;
    const [status, list] = await Promise.all([request("api/v1/status"), request("api/v1/tasks")]);
    if (current !== generation) return false;
    if (!Array.isArray(list)) throw new Error("服务器返回了无法识别的任务数据。");
    tasks = list.slice().sort((a, b) => b.id - a.id);
    applyStatus(status);
    connected = true;
    saveKey(apiKey);
    ui["api-key"].value = "";
    renderTasks();
    markSynced();
    return true;
  }

  async function connect(key) {
    if (busy) return;
    apiKey = key.trim();
    if (!apiKey) return;
    generation++;
    busy = true;
    ui["connect-button"].textContent = "正在连接…";
    updateControls();
    try {
      if (await loadWorkspace()) notify("工作区已连接，开始完成第一件事吧。");
    } catch (error) { handleError(error); }
    finally {
      busy = false;
      ui["connect-button"].textContent = "连接工作区 →";
      updateControls();
    }
  }

  async function refresh() {
    if (!connected || busy) return;
    busy = true;
    updateControls();
    try { await loadWorkspace(); }
    catch (error) { handleError(error); }
    finally { busy = false; updateControls(); }
  }

  async function mutateTask(id, action) {
    if (!connected || busy) return;
    const current = generation;
    busy = true;
    updateControls();
    try {
      const path = `api/v1/tasks/${encodeURIComponent(id)}`;
      const result = await request(action === "complete" ? `${path}/complete` : path, { method: action === "complete" ? "POST" : "DELETE" });
      if (current !== generation) return;
      tasks = action === "delete" ? tasks.filter((task) => task.id !== id) : tasks.map((task) => task.id === id ? result : task);
      renderTasks();
      markSynced();
      notify(action === "delete" ? "任务已删除。" : "又完成了一件事，做得不错。");
    } catch (error) { if (current === generation) handleError(error); }
    finally { busy = false; updateControls(); }
  }

  async function checkHealth() {
    if (healthBusy) return;
    healthBusy = true;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 8000);
    let healthy = false;
    try { healthy = (await fetch("healthz", { cache: "no-store", signal: controller.signal })).ok; }
    catch { /* The public page remains useful while the service is unavailable. */ }
    finally { clearTimeout(timer); healthBusy = false; }
    ui["health-pill"].classList.toggle("online", healthy);
    ui["health-pill"].classList.toggle("offline", !healthy);
    ui["health-label"].textContent = healthy ? "服务在线" : "暂时无法连接";
    ui["overview-health"].textContent = healthy ? "运行正常" : "连接异常";
    ui["health-detail"].textContent = healthy ? "健康检查已通过" : "请稍后重试";
  }

  ui["connect-form"].addEventListener("submit", (event) => { event.preventDefault(); connect(ui["api-key"].value); });
  ui.disconnect.addEventListener("click", () => { disconnect(); notify("已断开连接，本次会话中的密钥已清除。"); });
  ui.refresh.addEventListener("click", refresh);
  ui["task-form"].addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!connected || busy) return;
    const title = ui["task-title"].value.trim();
    if (!title) { ui["task-title"].focus(); return; }
    const current = generation;
    busy = true;
    updateControls();
    try {
      const task = await request("api/v1/tasks", { method: "POST", body: JSON.stringify({ title }) });
      if (current !== generation) return;
      tasks = [task, ...tasks];
      ui["task-title"].value = "";
      if (filter === "done") setFilter("all");
      else renderTasks();
      markSynced();
      notify("任务已添加，并保存在服务器上。");
    } catch (error) { if (current === generation) handleError(error); }
    finally { busy = false; updateControls(); if (connected) ui["task-title"].focus(); }
  });

  function setFilter(next) {
    filter = next;
    for (const button of document.querySelectorAll("[data-filter]")) {
      const active = button.dataset.filter === filter;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", String(active));
    }
    renderTasks();
  }
  for (const button of document.querySelectorAll("[data-filter]")) button.addEventListener("click", () => setFilter(button.dataset.filter));
  document.addEventListener("visibilitychange", () => { if (!document.hidden) checkHealth(); });
  setInterval(() => { if (!document.hidden) checkHealth(); }, 30000);
  setInterval(updateUptime, 1000);
  updateControls();
  renderTasks();
  checkHealth();
  try { const saved = sessionStorage.getItem(sessionKey); if (saved) connect(saved); } catch { /* Session storage is optional. */ }
})();
