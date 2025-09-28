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
const schemaModal = createSchemaModal();

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

  const timeline = document.createElement("div");
  timeline.className = "timeline";
  timeline.hidden = true;

  const summary = document.createElement("div");
  summary.className = "result-summary processing";
  const spinnerWrap = document.createElement("div");
  spinnerWrap.className = "spinner";
  summary.appendChild(spinnerWrap);
  const placeholder = document.createElement("p");
  placeholder.className = "processing-text";
  placeholder.textContent = "Working on it...";
  summary.appendChild(placeholder);

  card.appendChild(meta);
  card.appendChild(timeline);
  card.appendChild(summary);

  conversationElement.appendChild(card);
  conversationElement.scrollTop = conversationElement.scrollHeight;

  return { card, meta, summary, timeline };
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
  refs.summary.classList.remove("processing");
  refs.summary.innerHTML = "";
  refs.summary.appendChild(renderResult(result || {}));
  conversationElement.scrollTop = conversationElement.scrollHeight;
}

function renderResult(result) {
  const container = document.createElement("div");

  const heading = document.createElement("h3");
  heading.textContent = "Agent Summary";
  heading.style.margin = "0";
  container.appendChild(heading);

  schemaModal.clear();

  if (Array.isArray(result.facts) && result.facts.length) {
    const list = document.createElement("ul");
    list.className = "facts";
    result.facts.forEach((fact) => {
      if (!fact || typeof fact !== "object") {
        return;
      }
      const concept = fact.concept || "(concept)";
      const value = fact.value;
      const item = document.createElement("li");
      item.textContent = `${concept}: ${value}`;
      list.appendChild(item);
    });
    container.appendChild(withLabel("Facts", list));
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
    if (Array.isArray(result.update.applied_columns) && result.update.applied_columns.length) {
      entries.push(`Applied columns: ${result.update.applied_columns.join(", ")}`);
    } else if (Array.isArray(result.update.applied_facts) && result.update.applied_facts.length) {
      result.update.applied_facts.forEach((fact) => {
        if (!fact) return;
        const concept = fact.concept || fact.column;
        const column = fact.column;
        entries.push(`Mapped fact '${concept}' to column '${column}'`);
      });
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

  if (
    result.schema_proposal &&
    Array.isArray(result.schema_proposal.columns) &&
    result.schema_proposal.columns.length
  ) {
    const unmatchedFacts =
      result.update &&
      result.update.escalated &&
      Array.isArray(result.update.escalated.unmatched_facts)
        ? result.update.escalated.unmatched_facts
        : [];
    schemaModal.setProposal(result.schema_proposal, {
      recordId: state.session.recordId,
      unmatchedFacts,
    });
    const wrapper = document.createElement("div");
    wrapper.className = "schema-proposal-summary";

    const summary = document.createElement("p");
    const columnCount = result.schema_proposal.columns.length;
    summary.textContent = `${columnCount} proposed column${columnCount === 1 ? "" : "s"} available.`;
    wrapper.appendChild(summary);

    if (result.schema_proposal.migration_path) {
      const path = document.createElement("p");
      path.textContent = `Migration path: ${result.schema_proposal.migration_path}`;
      wrapper.appendChild(path);
    }

    const button = document.createElement("button");
    button.type = "button";
    button.className = "secondary";
    button.textContent = "View schema proposal";
    button.addEventListener("click", () => schemaModal.open());
    wrapper.appendChild(button);

    container.appendChild(withLabel("Schema proposal", wrapper));
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

function createSchemaModal() {
  const root = document.getElementById("schema-modal");
  if (!root) {
    return {
      setProposal() {},
      open() {},
      clear() {},
    };
  }

  const dialog = root.querySelector(".modal-dialog");
  const body = root.querySelector("#schema-modal-body");
  const backdrop = root.querySelector(".modal-backdrop");
  const closeButtons = root.querySelectorAll('[data-action="close"]');
  const applyButton = root.querySelector('[data-action="apply-schema"]');
  let currentProposal = null;
  let currentContext = { recordId: null, assignments: [] };
  const defaultApplyLabel = applyButton ? applyButton.textContent : "Apply schema changes";

  const close = () => {
    root.classList.add("hidden");
    root.setAttribute("aria-hidden", "true");
    root.removeAttribute("data-open");
    document.body.style.removeProperty("overflow");
  };

  const focusDialog = () => {
    if (!dialog) return;
    const focusable = dialog.querySelector(
      'button:not([disabled]), [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
    );
    const target = focusable || dialog;
    if (typeof target.focus === "function") {
      target.focus({ preventScroll: true });
    }
  };

  const setApplyState = (loading) => {
    if (!applyButton) return;
    applyButton.disabled = loading;
    applyButton.textContent = loading ? "Applying…" : defaultApplyLabel;
  };

  const renderProposal = () => {
    if (!currentProposal) {
      body.innerHTML = "";
      return;
    }

    body.innerHTML = "";

    const meta = document.createElement("div");
    meta.className = "schema-meta";

    const appendMeta = (label, value) => {
      if (value === undefined || value === null || value === "") return;
      const paragraph = document.createElement("p");
      const strong = document.createElement("strong");
      strong.textContent = label;
      paragraph.appendChild(strong);
      paragraph.appendChild(document.createElement("br"));
      const span = document.createElement("span");
      span.textContent = String(value);
      paragraph.appendChild(span);
      meta.appendChild(paragraph);
    };

    appendMeta("Ticket", currentProposal.ticket_id);
    appendMeta("Migration Path", currentProposal.migration_path);
    if (typeof currentProposal.notes === "string" && currentProposal.notes.trim()) {
      appendMeta("Notes", currentProposal.notes.trim());
    }

    if (meta.childElementCount) {
      body.appendChild(meta);
    }

    const columns = Array.isArray(currentProposal.columns) ? currentProposal.columns : [];
    if (columns.length) {
      const table = document.createElement("table");
      table.className = "schema-table";

      const thead = document.createElement("thead");
      const headRow = document.createElement("tr");
      ["Column", "Type", "Nullable", "Description"].forEach((text) => {
        const th = document.createElement("th");
        th.textContent = text;
        headRow.appendChild(th);
      });
      thead.appendChild(headRow);
      table.appendChild(thead);

      const tbody = document.createElement("tbody");
      columns.forEach((column) => {
        const row = document.createElement("tr");
        const nameCell = document.createElement("td");
        nameCell.textContent = column.name || "—";
        row.appendChild(nameCell);

        const typeCell = document.createElement("td");
        typeCell.textContent = column.data_type || column.type || "—";
        row.appendChild(typeCell);

        const nullableCell = document.createElement("td");
        const nullable = typeof column.nullable === "boolean" ? column.nullable : true;
        nullableCell.textContent = nullable ? "Yes" : "No";
        row.appendChild(nullableCell);

        const descriptionCell = document.createElement("td");
        descriptionCell.textContent = column.description || "—";
        row.appendChild(descriptionCell);

        tbody.appendChild(row);
      });

      table.appendChild(tbody);
      body.appendChild(table);
    }

    if (Array.isArray(currentProposal.migration_statements) && currentProposal.migration_statements.length) {
      const pre = document.createElement("pre");
      pre.textContent = currentProposal.migration_statements.join("\n");
      body.appendChild(pre);
    }
  };

  const setProposal = (proposal, context = {}) => {
    if (
      !proposal ||
      !Array.isArray(proposal.columns) ||
      proposal.columns.length === 0
    ) {
      currentProposal = null;
      currentContext = { recordId: null, assignments: [] };
      if (!root.classList.contains("hidden")) {
        close();
      }
      body.innerHTML = "";
      setApplyState(false);
      return;
    }

    currentProposal = {
      ...proposal,
    };
    const unmatchedFacts = Array.isArray(context.unmatchedFacts)
      ? context.unmatchedFacts
      : [];
    currentContext = {
      recordId: context.recordId || null,
      assignments: buildAssignments(currentProposal.columns, unmatchedFacts),
    };
    setApplyState(false);
    renderProposal();
  };

  const open = () => {
    if (!currentProposal) return;
    renderProposal();
    root.classList.remove("hidden");
    root.setAttribute("aria-hidden", "false");
    root.setAttribute("data-open", "true");
    document.body.style.overflow = "hidden";
    focusDialog();
  };

  const handleKeydown = (event) => {
    if (event.key === "Escape" && !root.classList.contains("hidden")) {
      event.preventDefault();
      close();
    }
  };

  const applySchema = async () => {
    if (!currentProposal || !applyButton) return;
    const payload = {
      ticket_id: currentProposal.ticket_id || state.session.id || "manual",
      columns: currentProposal.columns || [],
      migration_statements: currentProposal.migration_statements || [],
      table_name: currentProposal.table_name || state.dataset.tableName,
      notes: currentProposal.notes || null,
      migration_name: currentProposal.migration_name || undefined,
      record_id: currentContext.recordId || state.session.recordId,
      primary_key: state.dataset.primaryKey,
      row_assignments: currentContext.assignments || [],
    };

    setApplyState(true);
    try {
      const response = await fetch("/api/schema/apply", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        const error = await response.json().catch(() => ({}));
        throw new Error(error.detail || "Failed to apply schema");
      }
      const result = await response.json();
      const path = result.migration_path;
      addSystemMessage(
        path
          ? `Schema migration written to ${path}.`
          : "Schema changes applied."
      );
      await Promise.all([
        loadDatasetMetadata(),
        loadDatasetRows(),
      ]);
      close();
    } catch (error) {
      console.error("Failed to apply schema proposal", error);
      const message = error instanceof Error ? error.message : "Unexpected error applying schema";
      addSystemMessage(`Schema application failed: ${message}`);
      alert(message);
    } finally {
      setApplyState(false);
    }
  };

  document.addEventListener("keydown", handleKeydown);
  if (backdrop) {
    backdrop.addEventListener("click", close);
  }
  closeButtons.forEach((button) => {
    button.addEventListener("click", close);
  });
  if (applyButton) {
    applyButton.addEventListener("click", () => {
      void applySchema();
    });
  }

  return {
    setProposal,
    open,
    clear() {
      setProposal(null);
    },
  };
}

function buildAssignments(columns, unmatchedFacts) {
  if (!Array.isArray(columns) || !Array.isArray(unmatchedFacts)) {
    return [];
  }

  const lookup = new Map();
  columns.forEach((column) => {
    if (!column || !column.name) return;
    lookup.set(normaliseConcept(column.name), column.name);
  });

  const assignments = [];
  unmatchedFacts.forEach((fact) => {
    if (!fact || typeof fact !== "object") return;
    const concept = typeof fact.concept === "string" && fact.concept
      ? fact.concept
      : typeof fact.column === "string"
        ? fact.column
        : null;
    if (!concept) return;
    const value = fact.value;
    const columnName = lookup.get(normaliseConcept(concept));
    if (!columnName) return;
    assignments.push({ column: columnName, value });
  });

  return assignments;
}

function normaliseConcept(raw) {
  return String(raw)
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "_");
}
