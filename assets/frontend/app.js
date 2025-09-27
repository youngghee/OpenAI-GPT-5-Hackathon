const tabButtons = document.querySelectorAll(".tab-button");
const tabPanels = {
  dataset: document.getElementById("tab-dataset"),
  chat: document.getElementById("tab-chat"),
};

const datasetTableHead = document.getElementById("dataset-table-head");
const datasetTableBody = document.getElementById("dataset-table-body");
const datasetPrevBtn = document.getElementById("dataset-prev");
const datasetNextBtn = document.getElementById("dataset-next");
const datasetRange = document.getElementById("dataset-range");
const datasetCount = document.getElementById("dataset-count");
const datasetConfirmBtn = document.getElementById("dataset-confirm");
const datasetSelectionLabel = document.getElementById("dataset-selection-label");

const resumeButton = document.getElementById("resume-session");
const sessionDetails = document.getElementById("session-details");
const sessionIdElement = document.getElementById("session-id");
const sessionRecordElement = document.getElementById("session-record");
const candidateUrlsElement = document.getElementById("candidate-urls");
const recordContextElement = document.getElementById("record-context");
const questionForm = document.getElementById("question-form");
const questionInput = document.getElementById("question-input");
const conversationElement = document.getElementById("conversation");
const timelineTemplate = document.getElementById("timeline-item-template");

const state = {
  dataset: {
    columns: [],
    primaryKey: "BRIZO_ID",
    tableName: "dataset",
    rows: [],
    offset: 0,
    limit: 25,
    total: 0,
    hasMore: false,
  },
  selection: {
    recordId: null,
    row: null,
  },
  session: {
    id: null,
    recordId: null,
  },
  tickets: new Map(),
};

initialize();

function initialize() {
  tabButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const tab = button.dataset.tab;
      if (tab === "chat" && !state.session.id) {
        return;
      }
      switchTab(tab);
    });
  });

  datasetPrevBtn.addEventListener("click", () => {
    if (state.dataset.offset === 0) return;
    state.dataset.offset = Math.max(0, state.dataset.offset - state.dataset.limit);
    loadDatasetRows();
  });

  datasetNextBtn.addEventListener("click", () => {
    if (!state.dataset.hasMore) return;
    state.dataset.offset = state.dataset.offset + state.dataset.limit;
    loadDatasetRows();
  });

  datasetConfirmBtn.addEventListener("click", () => {
    if (!state.selection.recordId) return;
    startSession(state.selection.recordId);
  });

  datasetTableBody.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const rowElement = target.closest("tr[data-record-id]");
    if (!rowElement) return;
    const recordId = rowElement.dataset.recordId;
    if (!recordId) return;
    const rowIndex = Number(rowElement.dataset.index);
    const rowData = state.dataset.rows[rowIndex];
    selectRow(recordId, rowData, rowElement);
  });

  resumeButton.addEventListener("click", resumeSession);
  questionForm.addEventListener("submit", handleQuestionSubmit);

  loadDataset();
  switchTab("dataset");
}

async function loadDataset() {
  await loadDatasetMetadata();
  await loadDatasetRows();
}

async function loadDatasetMetadata() {
  if (state.dataset.columns.length) return;
  try {
    const response = await fetch("/api/dataset/columns");
    if (!response.ok) throw await response.json();
    const payload = await response.json();
    state.dataset.columns = payload.columns || [];
    state.dataset.primaryKey = payload.primary_key || state.dataset.primaryKey;
    state.dataset.tableName = payload.table_name || state.dataset.tableName;
    renderColumns();
  } catch (error) {
    console.error("Failed to load dataset metadata", error);
  }
}

async function loadDatasetRows() {
  try {
    datasetRange.textContent = "Loading rows...";
    const params = new URLSearchParams({
      offset: String(state.dataset.offset),
      limit: String(state.dataset.limit),
    });
    const response = await fetch(`/api/dataset/rows?${params.toString()}`);
    if (!response.ok) throw await response.json();
    const payload = await response.json();
    state.dataset.columns = payload.columns || state.dataset.columns;
    state.dataset.rows = payload.rows || [];
    state.dataset.total = payload.total || 0;
    state.dataset.limit = payload.limit || state.dataset.limit;
    state.dataset.hasMore = Boolean(payload.has_more);
    state.dataset.primaryKey = payload.primary_key || state.dataset.primaryKey;
    state.dataset.tableName = payload.table_name || state.dataset.tableName;
    renderColumns();
    renderRows();
    resetSelection();
  } catch (error) {
    console.error("Failed to load dataset rows", error);
    datasetRange.textContent = "Failed to load rows";
  }
}

function renderColumns() {
  // columns list removed from UI; reserve for future filters
}

function renderRows() {
  datasetTableHead.innerHTML = "";
  datasetTableBody.innerHTML = "";

  const headerCells = ["Select", ...state.dataset.columns];
  headerCells.forEach((column) => {
    const th = document.createElement("th");
    th.textContent = column;
    datasetTableHead.appendChild(th);
  });

  state.dataset.rows.forEach((row, index) => {
    const recordId = row[state.dataset.primaryKey];
    const tr = document.createElement("tr");
    tr.dataset.recordId = recordId || "";
    tr.dataset.index = String(index);

    const selectCell = document.createElement("td");
    const selectButton = document.createElement("button");
    selectButton.type = "button";
    selectButton.className = "secondary";
    selectButton.textContent = recordId ? `Select ${recordId}` : "Select";
    selectCell.appendChild(selectButton);
    tr.appendChild(selectCell);

    state.dataset.columns.forEach((column) => {
      const td = document.createElement("td");
      const value = row[column];
      const span = document.createElement("span");
      span.className = "cell-text";
      const displayValue = value === null || value === undefined ? "—" : String(value);
      span.textContent = displayValue;
      span.title = displayValue;
      td.appendChild(span);
      tr.appendChild(td);
    });

    datasetTableBody.appendChild(tr);
  });

  const start = state.dataset.total === 0 ? 0 : state.dataset.offset + 1;
  const end = state.dataset.offset + state.dataset.rows.length;
  datasetRange.textContent = `Rows ${start}-${end} of ${state.dataset.total}`;
  datasetCount.textContent = `${state.dataset.total.toLocaleString()} rows`;
  datasetPrevBtn.disabled = state.dataset.offset === 0;
  datasetNextBtn.disabled = !state.dataset.hasMore;
}

function resetSelection() {
  state.selection.recordId = null;
  state.selection.row = null;
  datasetSelectionLabel.textContent = "No row selected.";
  datasetConfirmBtn.disabled = true;
  datasetTableBody.querySelectorAll("tr").forEach((tr) => {
    tr.classList.remove("active-row");
  });
}

function selectRow(recordId, rowData, rowElement) {
  state.selection.recordId = recordId;
  state.selection.row = rowData;
  datasetTableBody.querySelectorAll("tr").forEach((tr) => tr.classList.remove("active-row"));
  if (rowElement) {
    rowElement.classList.add("active-row");
  }
  datasetSelectionLabel.textContent = recordId
    ? `Selected ${recordId}`
    : "Selected row";
  datasetConfirmBtn.disabled = !recordId;
}

async function startSession(recordId) {
  try {
    const response = await fetch("/api/session", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ record_id: recordId }),
    });
    if (!response.ok) {
      const detail = await response.json().catch(() => ({}));
      throw new Error(detail.detail || "Unable to start session");
    }
    const payload = await response.json();
    applySessionState(payload, { switchToChat: true, resetConversation: true });
    addSystemMessage("Session created. Ask your first question!");
    switchTab("chat");
  } catch (error) {
    console.error(error);
    alert(error.message || "Failed to open record");
  }
}

async function resumeSession() {
  const sessionId = window.prompt("Enter session id to resume");
  if (!sessionId) return;
  try {
    const response = await fetch(`/api/session/${encodeURIComponent(sessionId.trim())}`);
    if (!response.ok) {
      const detail = await response.json().catch(() => ({}));
      throw new Error(detail.detail || "Unable to resume session");
    }
    const data = await response.json();
    applySessionState(data, { switchToChat: true, resetConversation: true });
    addSystemMessage("Session resumed. Continue where you left off.");
    switchTab("chat");
  } catch (error) {
    console.error(error);
    alert(error.message || "Failed to resume session");
  }
}

function applySessionState(payload, options = {}) {
  state.session.id = payload.session_id;
  state.session.recordId = payload.record_id;

  sessionIdElement.textContent = payload.session_id;
  sessionRecordElement.textContent = payload.record_id;
  sessionDetails.hidden = false;
  questionForm.hidden = false;

  renderCandidateUrls(payload.candidate_urls || []);
  renderRecordContext(payload.record_context || {});

  enableChatTab();

  if (options.resetConversation) {
    resetConversation();
  }
}

function enableChatTab() {
  const chatButton = document.getElementById("tab-btn-chat");
  chatButton.disabled = false;
}

function switchTab(tab) {
  tabButtons.forEach((button) => {
    const isActive = button.dataset.tab === tab;
    button.classList.toggle("active", isActive);
    button.setAttribute("aria-selected", String(isActive));
  });
  Object.entries(tabPanels).forEach(([name, panel]) => {
    if (name === tab) {
      panel.hidden = false;
      panel.classList.add("active");
    } else {
      panel.hidden = true;
      panel.classList.remove("active");
    }
  });
}

function resetConversation() {
  conversationElement.innerHTML = "";
}

function handleQuestionSubmit(event) {
  event.preventDefault();
  if (!state.session.id) {
    alert("Open a record before asking questions.");
    return;
  }
  const question = questionInput.value.trim();
  if (!question) return;

  addUserMessage(question);
  questionInput.value = "";
  askQuestion(question);
}

async function askQuestion(question) {
  try {
    const response = await fetch(`/api/session/${encodeURIComponent(state.session.id)}/ask`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question }),
    });
    if (!response.ok) {
      const detail = await response.json().catch(() => ({}));
      throw new Error(detail.detail || "Agent workflow failed");
    }
    const data = await response.json();
    const refs = createAgentCard(data.ticket_id);
    state.tickets.set(data.ticket_id, refs);
    subscribeToTicket(data.ticket_id, refs);
  } catch (error) {
    console.error(error);
    addSystemMessage(error.message || "Failed to process question");
  }
}

function createAgentCard(ticketId) {
  const card = document.createElement("div");
  card.className = "message agent";

  const meta = document.createElement("div");
  meta.className = "meta";
  meta.textContent = `${ticketId} · status: processing`;

  const body = document.createElement("div");
  body.className = "body processing";
  const spinnerWrap = document.createElement("div");
  spinnerWrap.className = "spinner";
  body.appendChild(spinnerWrap);
  const placeholder = document.createElement("p");
  placeholder.className = "processing-text";
  placeholder.textContent = "Working on it...";
  body.appendChild(placeholder);

  const timeline = document.createElement("div");
  timeline.className = "timeline";
  timeline.hidden = true;

  card.appendChild(meta);
  card.appendChild(body);
  card.appendChild(timeline);

  conversationElement.appendChild(card);
  conversationElement.scrollTop = conversationElement.scrollHeight;

  return { card, meta, body, timeline };
}

function subscribeToTicket(ticketId, refs) {
  const source = new EventSource(`/api/tickets/${encodeURIComponent(ticketId)}/events`);

  source.addEventListener("timeline", (event) => {
    try {
      const payload = JSON.parse(event.data);
      appendTimelineEntry(refs, payload.event);
    } catch (error) {
      console.error("Failed to parse timeline event", error);
    }
  });

  source.addEventListener("result", (event) => {
    try {
      const payload = JSON.parse(event.data);
      applyResult(ticketId, refs, payload.result);
      if (Array.isArray(payload.timeline) && refs.timeline.childElementCount === 0) {
        payload.timeline.forEach((entry) => appendTimelineEntry(refs, entry));
      }
    } catch (error) {
      console.error("Failed to parse result event", error);
    } finally {
      source.close();
      state.tickets.delete(ticketId);
    }
  });

  source.onerror = (error) => {
    console.warn("EventSource error", error);
    source.close();
    state.tickets.delete(ticketId);
  };
}

function appendTimelineEntry(refs, entry) {
  if (!entry || !entry.message) return;
  if (refs.timeline.hidden) {
    refs.timeline.hidden = false;
  }
  const fragment = timelineTemplate.content.cloneNode(true);
  fragment.querySelector(".timeline-source").textContent = entry.source || "agent";
  fragment.querySelector(".timeline-message").textContent = entry.message;
  refs.timeline.appendChild(fragment);
  conversationElement.scrollTop = conversationElement.scrollHeight;
}

function applyResult(ticketId, refs, result) {
  if (result && result.status) {
    refs.meta.textContent = `${ticketId} · status: ${result.status}`;
  } else {
    refs.meta.textContent = `${ticketId} · status: complete`;
  }
  refs.body.classList.remove("processing");
  refs.body.innerHTML = "";
  refs.body.appendChild(renderResult(result || {}));
  conversationElement.scrollTop = conversationElement.scrollHeight;
}

function renderResult(result) {
  const container = document.createElement("div");

  if (result.answers) {
    const list = document.createElement("ul");
    list.className = "answers";
    Object.entries(result.answers).forEach(([key, value]) => {
      const item = document.createElement("li");
      item.textContent = `${key}: ${value}`;
      list.appendChild(item);
    });
    container.appendChild(withLabel("Answers", list));
  }

  if (Array.isArray(result.missing_columns) && result.missing_columns.length) {
    const list = document.createElement("ul");
    result.missing_columns.forEach((column) => {
      const item = document.createElement("li");
      item.textContent = column;
      list.appendChild(item);
    });
    container.appendChild(withLabel("Missing columns", list));
  }

  if (Array.isArray(result.scraper_tasks) && result.scraper_tasks.length) {
    const list = document.createElement("ul");
    result.scraper_tasks.forEach((task) => {
      const item = document.createElement("li");
      item.textContent = `${task.topic}: ${task.query}`;
      list.appendChild(item);
    });
    container.appendChild(withLabel("Scraper tasks", list));
  }

  if (result.scraper_findings) {
    const summary = document.createElement("p");
    summary.textContent = `${result.scraper_findings} finding(s) persisted.`;
    container.appendChild(withLabel("Scraper findings", summary));
  }

  if (result.update) {
    const entries = [];
    if (result.update.status) entries.push(`Status: ${result.update.status}`);
    if (Array.isArray(result.update.applied_fields) && result.update.applied_fields.length) {
      entries.push(`Applied fields: ${result.update.applied_fields.join(", ")}`);
    }
    if (result.update.escalated) entries.push("Escalated to schema agent");
    if (entries.length) {
      const list = document.createElement("ul");
      entries.forEach((line) => {
        const item = document.createElement("li");
        item.textContent = line;
        list.appendChild(item);
      });
      container.appendChild(withLabel("Update summary", list));
    }
  }

  if (result.schema_proposal && Array.isArray(result.schema_proposal.columns)) {
    const list = document.createElement("ul");
    result.schema_proposal.columns.forEach((column) => {
      const item = document.createElement("li");
      item.textContent = `${column.name} · ${column.data_type}`;
      list.appendChild(item);
    });
    container.appendChild(withLabel("Schema proposal", list));
  }

  return container;
}

function renderCandidateUrls(urls) {
  candidateUrlsElement.innerHTML = "";
  if (!urls.length) {
    const empty = document.createElement("li");
    empty.textContent = "None detected";
    candidateUrlsElement.appendChild(empty);
    return;
  }
  urls.forEach((url) => {
    const item = document.createElement("li");
    const link = document.createElement("a");
    link.href = url;
    link.textContent = url;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    item.appendChild(link);
    candidateUrlsElement.appendChild(item);
  });
}

function renderRecordContext(context) {
  recordContextElement.innerHTML = "";
  const entries = Object.entries(context);
  if (!entries.length) {
    const empty = document.createElement("p");
    empty.textContent = "No context available for this record.";
    recordContextElement.appendChild(empty);
    return;
  }
  entries.forEach(([key, value]) => {
    const row = document.createElement("div");
    row.className = "row";

    const label = document.createElement("span");
    label.className = "label";
    label.textContent = key;

    const text = document.createElement("span");
    text.textContent = value ?? "—";

    row.appendChild(label);
    row.appendChild(text);
    recordContextElement.appendChild(row);
  });
}

function addUserMessage(question) {
  const card = document.createElement("div");
  card.className = "message user";
  card.textContent = question;
  conversationElement.appendChild(card);
  conversationElement.scrollTop = conversationElement.scrollHeight;
}

function addSystemMessage(text) {
  const card = document.createElement("div");
  card.className = "message";
  card.textContent = text;
  conversationElement.appendChild(card);
  conversationElement.scrollTop = conversationElement.scrollHeight;
}

function withLabel(label, element) {
  const wrapper = document.createElement("div");
  const heading = document.createElement("h3");
  heading.textContent = label;
  heading.style.margin = "0 0 0.35rem";
  heading.style.fontSize = "1rem";
  wrapper.appendChild(heading);
  wrapper.appendChild(element);
  return wrapper;
}
