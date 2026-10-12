(() => {
  let token = null;

  const elements = {
    loginForm: document.querySelector("#login-form"),
    logoutButton: document.querySelector("#logout-button"),
    sessionStatus: document.querySelector("#session-status"),
    message: document.querySelector("#message"),
    satelliteForm: document.querySelector("#satellite-form"),
    satelliteId: document.querySelector("#satellite-id"),
    telemetryForm: document.querySelector("#telemetry-form"),
    healthRows: document.querySelector("#health-rows"),
    sensorCount: document.querySelector("#sensor-count"),
    alertCount: document.querySelector("#alert-count"),
    alertList: document.querySelector("#alert-list"),
    dependencyList: document.querySelector("#dependency-list"),
    pipelineMode: document.querySelector("#pipeline-mode"),
  };

  function showMessage(message, isError = false) {
    elements.message.textContent = message;
    elements.message.classList.toggle("message-error", isError);
  }

  function setSession(accessToken) {
    token = accessToken;
    elements.sessionStatus.textContent = token ? "Authenticated" : "Signed out";
    elements.sessionStatus.classList.toggle("status-online", Boolean(token));
    elements.sessionStatus.classList.toggle("status-offline", !token);
    elements.logoutButton.hidden = !token;
  }

  async function request(path, options = {}) {
    if (!token) {
      throw new Error("Sign in before accessing mission data.");
    }
    const response = await fetch(path, {
      ...options,
      headers: {
        Authorization: `Bearer ${token}`,
        ...(options.body ? { "Content-Type": "application/json" } : {}),
        ...options.headers,
      },
    });
    if (!response.ok) {
      let detail = `Request failed (${response.status})`;
      try {
        const body = await response.json();
        detail = body.detail || detail;
      } catch {
        // Keep the HTTP status when the server response is not JSON.
      }
      const error = new Error(detail);
      error.status = response.status;
      throw error;
    }
    return response.status === 204 ? null : response.json();
  }

  async function loadPipelineMode() {
    const response = await fetch("/health");
    if (!response.ok) {
      throw new Error(`Could not check the service mode (${response.status}).`);
    }
    const health = await response.json();
    if (health.mode !== "demo" && health.mode !== "live") {
      throw new Error("The service returned an unknown pipeline mode.");
    }
    elements.pipelineMode.dataset.mode = health.mode;
    elements.pipelineMode.lastChild.textContent =
      health.mode === "demo" ? " DEMO · IN-MEMORY" : " LIVE · KAFKA";
  }

  function addCell(row, value) {
    const cell = document.createElement("td");
    cell.textContent = value == null ? "—" : String(value);
    row.append(cell);
  }

  function renderHealth(readings) {
    elements.sensorCount.textContent = String(readings.length);
    elements.healthRows.replaceChildren();
    if (readings.length === 0) {
      const row = document.createElement("tr");
      const cell = document.createElement("td");
      cell.colSpan = 4;
      cell.className = "empty-state";
      cell.textContent = "No sensor readings yet.";
      row.append(cell);
      elements.healthRows.append(row);
      return;
    }
    for (const reading of readings) {
      const row = document.createElement("tr");
      addCell(row, reading.sensor_id);
      addCell(row, reading.metric);
      addCell(row, `${reading.value} ${reading.unit}`);
      addCell(row, new Date(reading.observed_at).toLocaleString());
      elements.healthRows.append(row);
    }
  }

  function renderAlerts(alerts) {
    elements.alertCount.textContent = String(alerts.length);
    elements.alertList.replaceChildren();
    if (alerts.length === 0) {
      const item = document.createElement("li");
      item.className = "empty-state";
      item.textContent = "No recent alerts.";
      elements.alertList.append(item);
      return;
    }
    for (const alert of alerts) {
      const item = document.createElement("li");
      item.className = "alert-item";
      const heading = document.createElement("strong");
      heading.textContent = `${alert.severity.toUpperCase()} · ${alert.satellite_id}`;
      const detail = document.createElement("span");
      detail.textContent = alert.message;
      const time = document.createElement("time");
      time.dateTime = alert.occurred_at;
      time.textContent = new Date(alert.occurred_at).toLocaleString();
      item.append(heading, detail, time);
      elements.alertList.append(item);
    }
  }

  function renderDependencies(graph) {
    elements.dependencyList.replaceChildren();
    if (graph.nodes.length === 0) {
      const item = document.createElement("li");
      item.className = "empty-state";
      item.textContent = "No component relationships found.";
      elements.dependencyList.append(item);
      return;
    }
    for (const relationship of graph.relationships) {
      const item = document.createElement("li");
      const edge = document.createElement("span");
      edge.textContent = `${relationship.source} → ${relationship.target}`;
      const type = document.createElement("small");
      type.textContent = relationship.type.replaceAll("_", " ");
      item.append(edge, type);
      elements.dependencyList.append(item);
    }
  }

  async function refreshSatellite() {
    const satelliteId = elements.satelliteId.value.trim();
    if (!satelliteId) {
      throw new Error("Enter a satellite ID.");
    }
    const encodedId = encodeURIComponent(satelliteId);
    const [health, alerts, graph] = await Promise.all([
      request(`/v1/satellites/${encodedId}/health`),
      request(`/v1/alerts?limit=10&satellite_id=${encodedId}`),
      request(`/v1/satellites/${encodedId}/dependencies`),
    ]);
    renderHealth(health.readings);
    renderAlerts(alerts);
    renderDependencies(graph);
    return health.readings;
  }

  async function waitForTelemetry(packet, eventId) {
    const satelliteId = encodeURIComponent(packet.satellite_id);
    const sensorId = encodeURIComponent(packet.sensor_id);
    const path = `/v1/satellites/${satelliteId}/sensors/${sensorId}/latest`;
    for (let attempt = 0; attempt < 10; attempt += 1) {
      await new Promise((resolve) => window.setTimeout(resolve, 300));
      try {
        const latest = await request(path);
        if (latest.event_id === eventId) {
          return true;
        }
      } catch (error) {
        if (error.status !== 404) {
          throw error;
        }
      }
    }
    return false;
  }

  elements.loginForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(elements.loginForm);
    try {
      const response = await fetch("/v1/auth/token", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          username: form.get("username"),
          password: form.get("password"),
        }),
      });
      const body = await response.json();
      if (!response.ok) {
        throw new Error(body.detail || `Sign in failed (${response.status})`);
      }
      setSession(body.access_token);
      elements.loginForm.elements.password.value = "";
      showMessage("Session started. Loading the satellite overview…");
      await refreshSatellite();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  elements.logoutButton.addEventListener("click", () => {
    setSession(null);
    renderHealth([]);
    renderAlerts([]);
    renderDependencies({ nodes: [], relationships: [] });
    showMessage("Signed out.");
  });

  elements.satelliteForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      showMessage("Refreshing satellite data…");
      await refreshSatellite();
      showMessage("Satellite overview updated.");
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  elements.telemetryForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const fields = new FormData(elements.telemetryForm);
    const submitButton = elements.telemetryForm.querySelector('button[type="submit"]');
    const packet = {
      satellite_id: elements.satelliteId.value.trim(),
      module_id: fields.get("module_id"),
      sensor_id: fields.get("sensor_id"),
      metric: fields.get("metric"),
      value: Number(fields.get("value")),
      unit: fields.get("unit"),
      observed_at: new Date().toISOString(),
      sequence: Number(fields.get("sequence")),
    };
    submitButton.disabled = true;
    try {
      const accepted = await request("/v1/telemetry", {
        method: "POST",
        body: JSON.stringify(packet),
      });
      elements.telemetryForm.elements.sequence.value = String(packet.sequence + 1);
      const destination = elements.pipelineMode.dataset.mode === "demo"
        ? "the demo processor"
        : "Kafka";
      showMessage(`Packet ${accepted.event_id} accepted by ${destination}; waiting for processing…`);
      try {
        const processed = await waitForTelemetry(packet, accepted.event_id);
        if (processed) {
          await refreshSatellite();
        }
        showMessage(
          processed
            ? `Packet ${accepted.event_id} processed. Health and alerts are up to date.`
            : `Packet ${accepted.event_id} was accepted but is still processing. Refresh again shortly.`,
          !processed,
        );
      } catch (error) {
        showMessage(
          `Packet ${accepted.event_id} was accepted, but the dashboard could not refresh: ${error.message}`,
          true,
        );
      }
    } catch (error) {
      showMessage(error.message, true);
    } finally {
      submitButton.disabled = false;
    }
  });

  loadPipelineMode().catch((error) => {
    elements.pipelineMode.dataset.mode = "unavailable";
    elements.pipelineMode.lastChild.textContent = " UNAVAILABLE";
    showMessage(error.message, true);
  });
})();
