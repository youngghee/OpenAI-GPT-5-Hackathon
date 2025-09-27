const sessionForm = document.getElementById("session-form");
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

let activeSessionId = null;
let activeRecordId = null;
const activeTickets = new Map();

sessionForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const recordId = event.target.recordId.value.trim();
  if (!recordId) {
    return;
  }

  try {
    const payload = { record_id: recordId };
    const response = await fetch("/api/session", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const detail = await response.json().catch(() => ({}));
      throw new Error(detail.detail || "Unable to start session");
    }
    const data = await response.json();
    applySessionState(data);
    resetConversation();
    addSystemMessage("Session created. Ask your first question!");
  } catch (error) {
    console.error(error);
    alert(error.message || "Failed to create session");
  }
});

resumeButton.addEventListener("click", async () => {
  const sessionId = window.prompt("Enter session id to resume");
  if (!sessionId) {
    return;
  }
  try {
    const response = await fetch(`/api/session/${encodeURIComponent(sessionId.trim())}`);
    if (!response.ok) {
      const detail = await response.json().catch(() => ({}));
      throw new Error(detail.detail || "Unable to resume session");
    }
    const data = await response.json();
    applySessionState(data);
    resetConversation();
    addSystemMessage("Session resumed. Continue where you left off.");
  } catch (error) {
    console.error(error);
    alert(error.message || "Failed to resume session");
  }
});

questionForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!activeSessionId) {
    alert("Start a session before asking questions.");
    return;
  }
  const question = questionInput.value.trim();
  if (!question) {
    return;
  }

  addUserMessage(question);
  questionInput.value = "";

  try {
    const response = await fetch(`/api/session/${encodeURIComponent(activeSessionId)}/ask`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question }),
    });
    if (!response.ok) {
      const detail = await response.json().catch(() => ({}));
      throw new Error(detail.detail || "Agent workflow failed");
    }
    const data = await response.json();
    const ticketId = data.ticket_id;
    const refs = createAgentCard(ticketId);
    activeTickets.set(ticketId, refs);
    subscribeToTicket(ticketId, refs);
  } catch (error) {
    console.error(error);
    addSystemMessage(error.message || "Failed to process question");
  }
});

function applySessionState(payload) {
  activeSessionId = payload.session_id;
  activeRecordId = payload.record_id;

  sessionIdElement.textContent = payload.session_id;
  sessionRecordElement.textContent = payload.record_id;
  sessionDetails.hidden = false;
  questionForm.hidden = false;

  renderCandidateUrls(payload.candidate_urls || []);
  renderRecordContext(payload.record_context || {});
}

function resetConversation() {
  conversationElement.innerHTML = "";
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

  if (result.missing_columns) {
    const list = document.createElement("ul");
    result.missing_columns.forEach((column) => {
      const item = document.createElement("li");
      item.textContent = column;
      list.appendChild(item);
    });
    container.appendChild(withLabel("Missing columns", list));
  }

  if (result.scraper_tasks) {
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
    if (result.update.status) {
      entries.push(`Status: ${result.update.status}`);
    }
    if (Array.isArray(result.update.applied_fields) && result.update.applied_fields.length) {
      entries.push(`Applied fields: ${result.update.applied_fields.join(", ")}`);
    }
    if (result.update.escalated) {
      entries.push("Escalated to schema agent");
    }
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

  if (result.schema_proposal && result.schema_proposal.columns) {
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

function subscribeToTicket(ticketId, refs) {
  const source = new EventSource(
    `/api/tickets/${encodeURIComponent(ticketId)}/events`
  );

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
      activeTickets.delete(ticketId);
    }
  });

  source.onerror = (error) => {
    console.warn("EventSource error", error);
    source.close();
    activeTickets.delete(ticketId);
  };
}

function appendTimelineEntry(refs, entry) {
  if (!entry || !entry.message) {
    return;
  }
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
  refs.body.innerHTML = "";
  refs.body.appendChild(renderResult(result || {}));
  refs.body.classList.remove("processing");
  conversationElement.scrollTop = conversationElement.scrollHeight;
}
