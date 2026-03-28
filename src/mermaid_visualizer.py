from __future__ import annotations

import json
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TypeAlias

from src.utils import console


DEFAULT_MERMAID_CODE = """graph LR
    subgraph chain["Multi hop chain and variable length path"]
        S["Seed address"]
        R1["Relay A"]
        R2["Relay B"]
        R3["Relay C"]
        H["Hub address"]
        D["Destination address"]

        S -->|TRANSFER| R1
        R1 -->|TRANSFER| R2
        R2 -->|TRANSFER| R3
        R3 -->|TRANSFER| H
        H -->|TRANSFER| D
    end

    subgraph shared["Shared counterparties"]
        X1["Trader X"]
        X2["Trader Y"]
        C1["Counterparty A"]
        C2["Counterparty B"]
        C3["Counterparty C"]

        X1 -->|TRANSFER| C1
        X1 -->|TRANSFER| C2
        X1 -->|TRANSFER| C3

        X2 -->|TRANSFER| C1
        X2 -->|TRANSFER| C2
        X2 -->|TRANSFER| C3
    end

    subgraph fanio["Fan in and fan out hub motif"]
        I1["Input wallet A"]
        I2["Input wallet B"]
        I3["Input wallet C"]
        COL["Collector hub"]
        O1["Output wallet A"]
        O2["Output wallet B"]
        O3["Output wallet C"]

        I1 -->|TRANSFER| COL
        I2 -->|TRANSFER| COL
        I3 -->|TRANSFER| COL

        COL -->|TRANSFER| O1
        COL -->|TRANSFER| O2
        COL -->|TRANSFER| O3
    end

    subgraph cycle["Cycle or ring pattern"]
        A["Ring node A"]
        B["Ring node B"]
        C["Ring node C"]
        D2["Ring node D"]

        A -->|TRANSFER| B
        B -->|TRANSFER| C
        C -->|TRANSFER| D2
        D2 -->|TRANSFER| A
    end

    H -.shared region.-> COL
    C3 -.possible overlap.-> COL
    O2 -.possible path continuation.-> A
"""


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Mermaid Graph Visualizer</title>
  <style>
    :root {
      --ink: #17324d;
      --muted: #5a738b;
      --accent: #ef7f45;
      --accent-2: #1f7a8c;
      --bg: #f8f3ea;
      --panel: rgba(255,255,255,.92);
      --line: rgba(23,50,77,.12);
      --shadow: 0 24px 70px rgba(23,50,77,.12);
    }
    * { box-sizing: border-box; }
    html, body { height: 100%; margin: 0; }
    body {
      font-family: Bahnschrift, "Trebuchet MS", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(239,127,69,.2), transparent 28%),
        radial-gradient(circle at top right, rgba(31,122,140,.16), transparent 24%),
        linear-gradient(135deg, #fbf2e6 0%, #f4f8fc 45%, #fffaf1 100%);
      overflow: hidden;
    }
    body::before {
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      background-image:
        linear-gradient(rgba(23,50,77,.04) 1px, transparent 1px),
        linear-gradient(90deg, rgba(23,50,77,.04) 1px, transparent 1px);
      background-size: 30px 30px;
    }
    .app {
      position: relative;
      z-index: 1;
      height: 100%;
      display: grid;
      grid-template-rows: auto 1fr;
      gap: 18px;
      padding: 22px;
    }
    .hero, .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }
    .hero {
      display: flex;
      justify-content: space-between;
      gap: 18px;
      align-items: end;
      padding: 22px 24px;
    }
    h1, h2 { margin: 0; }
    .hero p, .subtle { color: var(--muted); }
    .hero p { max-width: 800px; margin: 8px 0 0; line-height: 1.45; }
    .badges, .toolbar, .row { display: flex; gap: 10px; flex-wrap: wrap; }
    .badge {
      padding: 10px 12px;
      border-radius: 999px;
      background: rgba(255,255,255,.84);
      border: 1px solid rgba(23,50,77,.08);
      color: var(--muted);
      font-size: .92rem;
    }
    .workspace {
      min-height: 0;
      display: grid;
      grid-template-columns: minmax(340px, 430px) minmax(0, 1fr);
      gap: 18px;
    }
    .editor, .preview {
      min-height: 0;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }
    .header {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      padding: 16px 18px;
      border-bottom: 1px solid rgba(23,50,77,.08);
    }
    .header span { display: block; margin-top: 4px; color: var(--muted); font-size: .9rem; }
    button {
      appearance: none;
      border: 0;
      border-radius: 14px;
      padding: 10px 14px;
      font: inherit;
      cursor: pointer;
      color: var(--ink);
      background: rgba(255,255,255,.95);
      box-shadow: inset 0 0 0 1px rgba(23,50,77,.08);
      transition: transform .15s ease, box-shadow .15s ease;
    }
    button:hover {
      transform: translateY(-1px);
      box-shadow: inset 0 0 0 1px rgba(23,50,77,.12), 0 10px 20px rgba(23,50,77,.08);
    }
    .primary { color: #fff; background: linear-gradient(135deg, var(--accent), #cb5b22); box-shadow: none; }
    .cool { color: #fff; background: linear-gradient(135deg, var(--accent-2), #165c69); box-shadow: none; }
    .editor-body {
      min-height: 0;
      display: flex;
      flex-direction: column;
      gap: 12px;
      padding: 16px 18px 18px;
    }
    textarea {
      flex: 1;
      width: 100%;
      min-height: 0;
      resize: none;
      border: 1px solid rgba(23,50,77,.1);
      border-radius: 20px;
      padding: 18px;
      background: linear-gradient(180deg, rgba(255,255,255,.99), rgba(247,250,253,.98));
      color: #14314f;
      font: .95rem/1.55 "Cascadia Code", Consolas, monospace;
      outline: none;
    }
    textarea:focus { box-shadow: 0 0 0 4px rgba(31,122,140,.12); border-color: rgba(31,122,140,.32); }
    .footer-note { display: flex; justify-content: space-between; gap: 12px; color: var(--muted); font-size: .9rem; }
    .stats { display: flex; gap: 12px; flex-wrap: wrap; padding: 12px 16px 0; color: var(--muted); font-size: .9rem; }
    .stats strong { color: var(--ink); }
    .canvas-wrap { position: relative; flex: 1; min-height: 0; padding: 14px; }
    .stage {
      position: relative;
      height: 100%;
      min-height: 520px;
      overflow: hidden;
      border-radius: 22px;
      background:
        radial-gradient(circle at 14% 18%, rgba(239,127,69,.12), transparent 18%),
        radial-gradient(circle at 82% 22%, rgba(31,122,140,.12), transparent 20%),
        linear-gradient(180deg, rgba(255,255,255,.99), rgba(245,249,253,.98));
      box-shadow: inset 0 0 0 1px rgba(23,50,77,.08);
      cursor: grab;
    }
    .stage::before {
      content: "";
      position: absolute;
      inset: 0;
      pointer-events: none;
      background-image:
        linear-gradient(rgba(23,50,77,.05) 1px, transparent 1px),
        linear-gradient(90deg, rgba(23,50,77,.05) 1px, transparent 1px);
      background-size: 32px 32px;
    }
    .stage.dragging { cursor: grabbing; }
    #render-root { position: absolute; inset: 0; }
    #render-root svg {
      position: absolute;
      top: 0;
      left: 0;
      overflow: visible;
      max-width: none;
      filter: drop-shadow(0 16px 38px rgba(23,50,77,.1));
      transform-origin: 0 0;
    }
    .empty, .error {
      position: absolute;
      left: 50%;
      z-index: 2;
      width: min(90%, 700px);
      padding: 16px 18px;
      border-radius: 18px;
      transform: translateX(-50%);
    }
    .empty {
      top: 50%;
      transform: translate(-50%, -50%);
      text-align: center;
      color: var(--muted);
      background: rgba(255,255,255,.9);
      border: 1px solid rgba(23,50,77,.08);
    }
    .error {
      top: 18px;
      display: none;
      background: rgba(255,236,236,.97);
      border: 1px solid rgba(184,63,63,.24);
      color: #8f2a2a;
      font: .9rem/1.45 "Cascadia Code", Consolas, monospace;
      white-space: pre-wrap;
    }
    .zoom { margin-left: auto; color: var(--muted); font-size: .92rem; white-space: nowrap; }
    body.canvas-only .workspace { grid-template-columns: 1fr; }
    body.canvas-only .editor { display: none; }
    @media (max-width: 1180px) {
      body { overflow: auto; }
      .workspace { grid-template-columns: 1fr; }
      .editor { max-height: 46vh; }
      .stage { min-height: 62vh; }
    }
  </style>
</head>
<body>
  <div class="app">
    <section class="hero">
      <div>
        <h1>Mermaid Graph Visualizer</h1>
        <p>Paste Mermaid code, render it instantly, and inspect it on a large canvas with pan, zoom, fit, canvas-only mode, fullscreen preview, and SVG export.</p>
      </div>
      <div class="badges">
        <div class="badge">Ctrl+Enter render</div>
        <div class="badge">Wheel zoom</div>
        <div class="badge">Drag to pan</div>
      </div>
    </section>

    <main class="workspace">
      <section class="panel editor">
        <div class="header">
          <div>
            <h2>Mermaid Source</h2>
            <span>Fenced markdown is fine. The viewer strips it automatically.</span>
          </div>
          <div class="row">
            <button id="sample-button" type="button">Reload sample</button>
            <button id="copy-button" type="button">Copy code</button>
          </div>
        </div>
        <div class="editor-body">
          <textarea id="source" spellcheck="false" aria-label="Mermaid source code"></textarea>
          <div class="footer-note">
            <span>Edit on the left, then render again to refresh the canvas.</span>
            <span id="source-size">0 characters</span>
          </div>
        </div>
      </section>

      <section class="panel preview" id="preview-panel">
        <div class="header">
          <div>
            <h2>Full-Scale Canvas</h2>
            <span>The diagram opens fitted to the viewport so you can inspect the whole graph immediately.</span>
          </div>
          <div class="toolbar">
            <button class="primary" id="render-button" type="button">Render</button>
            <button id="fit-button" type="button">Fit</button>
            <button id="actual-button" type="button">100%</button>
            <button id="zoom-in-button" type="button">Zoom in</button>
            <button id="zoom-out-button" type="button">Zoom out</button>
            <button class="cool" id="focus-button" type="button">Canvas only</button>
            <button id="fullscreen-button" type="button">Fullscreen</button>
            <button id="download-button" type="button">Export SVG</button>
            <span class="zoom" id="zoom-label">Zoom 100%</span>
          </div>
        </div>

        <div class="stats">
          <span><strong>Nodes:</strong> <span id="node-count">0</span></span>
          <span><strong>Edges:</strong> <span id="edge-count">0</span></span>
          <span><strong>Groups:</strong> <span id="group-count">0</span></span>
        </div>

        <div class="canvas-wrap">
          <div class="error" id="error-box"></div>
          <div class="stage" id="canvas-stage">
            <div class="empty" id="empty-state">Render a Mermaid diagram to explore it here.</div>
            <div id="render-root"></div>
          </div>
        </div>
      </section>
    </main>
  </div>

  <script>window.__INITIAL_MERMAID__ = __INITIAL_CODE__;</script>
  <script type="module">
    import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";

    const STORAGE_KEY = "avalanche-mermaid-visualizer";
    const state = { scale: 1, x: 0, y: 0, width: 0, height: 0, dragging: false, grabX: 0, grabY: 0 };
    const source = document.getElementById("source");
    const sourceSize = document.getElementById("source-size");
    const renderRoot = document.getElementById("render-root");
    const stage = document.getElementById("canvas-stage");
    const emptyState = document.getElementById("empty-state");
    const errorBox = document.getElementById("error-box");
    const zoomLabel = document.getElementById("zoom-label");
    const previewPanel = document.getElementById("preview-panel");
    const focusButton = document.getElementById("focus-button");
    const nodeCount = document.getElementById("node-count");
    const edgeCount = document.getElementById("edge-count");
    const groupCount = document.getElementById("group-count");

    mermaid.initialize({
      startOnLoad: false,
      securityLevel: "loose",
      theme: "base",
      flowchart: { useMaxWidth: false, htmlLabels: true, curve: "basis" },
      themeVariables: {
        fontFamily: "Bahnschrift, Trebuchet MS, sans-serif",
        primaryColor: "#fff1df",
        primaryTextColor: "#17324d",
        primaryBorderColor: "#ef7f45",
        lineColor: "#1f6a95",
        secondaryColor: "#dff1f5",
        tertiaryColor: "#f5f1e8",
        clusterBkg: "#eef5fb",
        clusterBorder: "#2d6f97",
        textColor: "#17324d",
      },
    });

    function normalizeCode(raw) {
      const text = raw.trim();
      if (text.startsWith("```")) {
        return text.replace(/^```[a-zA-Z0-9_-]*\\s*/u, "").replace(/\\s*```$/u, "").trim();
      }
      return text;
    }

    function svgNode() {
      return renderRoot.querySelector("svg");
    }

    function updateSourceStats() {
      sourceSize.textContent = `${source.value.length} characters`;
      localStorage.setItem(STORAGE_KEY, source.value);
    }

    function updateZoomLabel() {
      zoomLabel.textContent = `Zoom ${Math.round(state.scale * 100)}%`;
    }

    function applyTransform() {
      const svg = svgNode();
      if (!svg) return;
      svg.style.transform = `translate(${state.x}px, ${state.y}px) scale(${state.scale})`;
      updateZoomLabel();
    }

    function centerAt(scale) {
      const bounds = stage.getBoundingClientRect();
      state.scale = scale;
      state.x = (bounds.width - state.width * scale) / 2;
      state.y = (bounds.height - state.height * scale) / 2;
      applyTransform();
    }

    function fitToCanvas() {
      if (!state.width || !state.height) return;
      const bounds = stage.getBoundingClientRect();
      const pad = 96;
      const scale = Math.min(Math.max((bounds.width - pad) / state.width, .05), Math.max((bounds.height - pad) / state.height, .05), 3);
      centerAt(scale);
    }

    function setActualSize() {
      if (!state.width || !state.height) return;
      centerAt(1);
    }

    function zoomAround(multiplier, cx, cy) {
      const svg = svgNode();
      if (!svg) return;
      const current = state.scale;
      const next = Math.min(Math.max(current * multiplier, .08), 6);
      if (next === current) return;
      state.x = cx - ((cx - state.x) * next) / current;
      state.y = cy - ((cy - state.y) * next) / current;
      state.scale = next;
      applyTransform();
    }

    async function renderDiagram() {
      const code = normalizeCode(source.value);
      renderRoot.innerHTML = "";
      errorBox.style.display = "none";
      nodeCount.textContent = "0";
      edgeCount.textContent = "0";
      groupCount.textContent = "0";
      emptyState.style.display = code ? "none" : "block";
      if (!code) return;

      try {
        const { svg, bindFunctions } = await mermaid.render(`graph-${Date.now().toString(36)}`, code);
        renderRoot.innerHTML = svg;
        const node = svgNode();
        if (!node) throw new Error("Mermaid did not return an SVG document.");
        node.removeAttribute("height");
        node.removeAttribute("width");
        node.style.width = "auto";
        node.style.height = "auto";
        node.setAttribute("preserveAspectRatio", "xMidYMid meet");
        const box = node.viewBox.baseVal;
        state.width = box && box.width ? box.width : node.getBBox().width;
        state.height = box && box.height ? box.height : node.getBBox().height;
        if (!state.width || !state.height) throw new Error("The rendered diagram has no visible size.");
        fitToCanvas();
        if (bindFunctions) bindFunctions(renderRoot);
        nodeCount.textContent = String(renderRoot.querySelectorAll(".node").length);
        edgeCount.textContent = String(renderRoot.querySelectorAll(".flowchart-link").length);
        groupCount.textContent = String(renderRoot.querySelectorAll(".cluster").length);
      } catch (error) {
        emptyState.style.display = "block";
        errorBox.style.display = "block";
        errorBox.textContent = error instanceof Error ? error.message : String(error);
      }
    }

    function toggleCanvasOnly() {
      const active = document.body.classList.toggle("canvas-only");
      focusButton.textContent = active ? "Split view" : "Canvas only";
      window.setTimeout(fitToCanvas, 120);
    }

    function copyCode() {
      navigator.clipboard.writeText(source.value).catch(() => {});
    }

    function exportSvg() {
      const svg = svgNode();
      if (!svg) return;
      const blob = new Blob([svg.outerHTML], { type: "image/svg+xml;charset=utf-8" });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = "mermaid-diagram.svg";
      link.click();
      URL.revokeObjectURL(url);
    }

    source.value = localStorage.getItem(STORAGE_KEY) || window.__INITIAL_MERMAID__;
    updateSourceStats();
    source.addEventListener("input", updateSourceStats);
    source.addEventListener("keydown", (event) => {
      if (event.ctrlKey && event.key === "Enter") {
        event.preventDefault();
        renderDiagram();
      }
    });
    document.getElementById("render-button").addEventListener("click", renderDiagram);
    document.getElementById("fit-button").addEventListener("click", fitToCanvas);
    document.getElementById("actual-button").addEventListener("click", setActualSize);
    document.getElementById("zoom-in-button").addEventListener("click", () => zoomAround(1.18, stage.clientWidth / 2, stage.clientHeight / 2));
    document.getElementById("zoom-out-button").addEventListener("click", () => zoomAround(.84, stage.clientWidth / 2, stage.clientHeight / 2));
    document.getElementById("focus-button").addEventListener("click", toggleCanvasOnly);
    document.getElementById("fullscreen-button").addEventListener("click", () => previewPanel.requestFullscreen?.().catch(() => {}));
    document.getElementById("sample-button").addEventListener("click", () => { source.value = window.__INITIAL_MERMAID__; updateSourceStats(); renderDiagram(); });
    document.getElementById("copy-button").addEventListener("click", copyCode);
    document.getElementById("download-button").addEventListener("click", exportSvg);
    stage.addEventListener("wheel", (event) => {
      event.preventDefault();
      const rect = stage.getBoundingClientRect();
      zoomAround(event.deltaY < 0 ? 1.12 : .88, event.clientX - rect.left, event.clientY - rect.top);
    }, { passive: false });
    stage.addEventListener("pointerdown", (event) => {
      if (!svgNode()) return;
      state.dragging = true;
      state.grabX = event.clientX - state.x;
      state.grabY = event.clientY - state.y;
      stage.classList.add("dragging");
      stage.setPointerCapture(event.pointerId);
    });
    stage.addEventListener("pointermove", (event) => {
      if (!state.dragging) return;
      state.x = event.clientX - state.grabX;
      state.y = event.clientY - state.grabY;
      applyTransform();
    });
    function stopDragging(event) {
      if (!state.dragging) return;
      state.dragging = false;
      stage.classList.remove("dragging");
      if (event.pointerId !== undefined) stage.releasePointerCapture(event.pointerId);
    }
    stage.addEventListener("pointerup", stopDragging);
    stage.addEventListener("pointercancel", stopDragging);
    window.addEventListener("resize", fitToCanvas);
    renderDiagram();
  </script>
</body>
</html>
"""


HandlerType: TypeAlias = type[BaseHTTPRequestHandler]


def _load_mermaid_code(mermaid_file: Path | None) -> str:
    if mermaid_file is None:
        return DEFAULT_MERMAID_CODE
    return mermaid_file.read_text(encoding="utf-8")


def _build_html(initial_code: str) -> bytes:
    return HTML_TEMPLATE.replace("__INITIAL_CODE__", json.dumps(initial_code)).encode("utf-8")


def _make_handler(html_bytes: bytes) -> HandlerType:
    class MermaidHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path in {"/", "/index.html"}:
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html_bytes)))
                self.end_headers()
                self.wfile.write(html_bytes)
                return

            if self.path == "/health":
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(b'{"status":"ok"}')
                return

            self.send_error(404, "Not found")

        def log_message(self, format: str, *args: object) -> None:
            return

    return MermaidHandler


def _start_server(host: str, port: int, handler_class: HandlerType) -> ThreadingHTTPServer:
    last_error: OSError | None = None
    for candidate in range(port, port + 20):
        try:
            server = ThreadingHTTPServer((host, candidate), handler_class)
            server.daemon_threads = True
            return server
        except OSError as exc:
            last_error = exc

    if last_error is None:
        raise OSError("Unable to start the Mermaid visualizer server.")
    raise last_error


def visualize_mermaid(
    *,
    mermaid_file: Path | None = None,
    host: str = "127.0.0.1",
    port: int = 8765,
    open_browser: bool = True,
) -> None:
    html_bytes = _build_html(_load_mermaid_code(mermaid_file))
    handler_class = _make_handler(html_bytes)
    server = _start_server(host, port, handler_class)

    try:
        server_port = server.server_address[1]
        url = f"http://{host}:{server_port}"
        console.print(f"Mermaid visualizer is running at {url}")
        console.print("Press Ctrl+C to stop the server.")
        if open_browser:
            webbrowser.open(url)
        server.serve_forever()
    except KeyboardInterrupt:
        console.print("Stopping Mermaid visualizer.")
    finally:
        server.server_close()
