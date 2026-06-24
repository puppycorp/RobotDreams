const css = `
  html, body {
    background: #070b12;
    overflow: hidden;
    color: #dce7f3;
    font: 13px/1.35 Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  }
  body > .flex-col {
    display: flex;
    flex-direction: column;
    flex: 1 1 auto;
    height: 100vh;
    width: 100vw;
    min-height: 0;
    background: #070b12;
  }
  body > .flex-col > .flex-row:first-child {
    align-items: center;
    background: linear-gradient(180deg, #080d14 0%, #050910 100%) !important;
  }
  body > .flex-col > .flex-row:first-child span:first-child {
    align-items: center;
    display: inline-flex;
    font-size: 17px;
    font-weight: 700;
    gap: 10px;
  }
  body > .flex-col > .flex-row:first-child span:first-child::before {
    background:
      linear-gradient(#07101a, #07101a) padding-box,
      linear-gradient(135deg, #19b7ff, #0a77c8) border-box;
    border: 3px solid transparent;
    border-radius: 7px;
    box-shadow: 0 0 18px rgba(25, 183, 255, 0.3);
    content: "";
    display: inline-block;
    height: 17px;
    width: 17px;
  }
  body > .flex-col > .flex-row:first-child span:nth-last-child(4) {
    align-items: center;
    display: inline-flex;
    gap: 8px;
  }
  body > .flex-col > .flex-row:first-child span:nth-last-child(4)::before {
    background: #43d17a;
    border-radius: 999px;
    box-shadow: 0 0 10px rgba(67, 209, 122, 0.46);
    content: "";
    display: inline-block;
    height: 8px;
    width: 8px;
  }
  body > .flex-col > .flex-row:first-child span:last-child::after {
    background: #14365f;
    border: 1px solid #245c95;
    border-radius: 3px;
    color: #9bd3ff;
    content: "PRO";
    font-size: 9px;
    margin-left: 8px;
    padding: 1px 4px;
  }
  body * {
    box-sizing: border-box;
  }
  body .wgui-resize-handle {
    background:
      linear-gradient(
        90deg,
        transparent 0,
        transparent 3px,
        rgba(85, 167, 255, 0.28) 3px,
        rgba(85, 167, 255, 0.28) 4px,
        transparent 4px
      ) !important;
    opacity: 0.72;
    transition: opacity 120ms ease, background 120ms ease;
  }
  body .wgui-resize-handle:hover {
    background:
      linear-gradient(
        90deg,
        rgba(42, 141, 255, 0.08) 0,
        rgba(42, 141, 255, 0.08) 3px,
        rgba(85, 167, 255, 0.8) 3px,
        rgba(85, 167, 255, 0.8) 5px,
        rgba(42, 141, 255, 0.12) 5px
      ) !important;
    opacity: 1;
  }
  body .wgui-resize-handle.robotdreams-left-resize-handle {
    left: 0 !important;
    right: auto !important;
  }
  body button,
  body input,
  body textarea,
  body select {
    border-radius: 3px;
    font: inherit;
    min-height: 0;
  }
  body button {
    padding-left: 10px;
    padding-right: 10px;
  }
  body > .flex-col > .flex-row:nth-of-type(2) {
    align-items: stretch;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
  }
  body > .flex-col > .flex-row:nth-of-type(2) button {
    align-items: center;
    background: transparent !important;
    border-color: transparent !important;
    color: #aeb8c4 !important;
    display: inline-flex;
    flex-direction: column;
    gap: 3px;
    height: 50px !important;
    justify-content: center;
    min-width: 58px;
    padding: 4px 8px;
  }
  body > .flex-col > .flex-row:nth-of-type(2) button:hover {
    background: #101a28 !important;
    border-color: #26364d !important;
    color: #f4f8ff !important;
  }
  body > .flex-col > .flex-row:nth-of-type(2) button::before {
    color: #dce7f3;
    content: "";
    display: block;
    font-size: 15px;
    height: 18px;
    line-height: 18px;
  }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(1)::before { content: "+"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(2)::before { content: "[]"; font-size: 11px; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(3)::before { content: "▤"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(4)::before { content: "⇣"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(5)::before { content: "⇡"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(6) {
    background: #1267c8 !important;
    border: 1px solid #2a8dff !important;
    color: #f4f8ff !important;
    box-shadow: 0 0 20px rgba(42, 141, 255, 0.22);
  }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(6)::before { content: "▶"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(7)::before { content: "Ⅱ"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(8)::before { content: "■"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(9)::before { content: "↻"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(10)::before { content: "◉"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(11)::before { content: "⌖"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(12)::before { content: "✣"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(13)::before { content: "↻"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(14)::before { content: "□"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(15)::before { content: "✳"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(16)::before { content: "▣"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(17)::before { content: "▷"; }
  body > .flex-col > .flex-row:nth-of-type(2) button:nth-of-type(18)::before { content: "◇"; }
  body input::placeholder {
    color: #718096;
  }
  body span {
    line-height: 1.22;
  }
  * {
    scrollbar-color: #607086 #101722;
  }
  *::-webkit-scrollbar {
    width: 8px;
    height: 8px;
  }
  *::-webkit-scrollbar-track {
    background: #101722;
  }
  *::-webkit-scrollbar-thumb {
    background: #607086;
    border-radius: 6px;
  }
  :host, .robot-scene {
    display: block;
    width: 100%;
    height: 100%;
    box-sizing: border-box;
  }
  .robot-scene {
    background: #101318;
    color: #e8edf2;
    font: 13px/1.4 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    overflow: hidden;
    position: relative;
  }
  .robot-scene__canvas {
    position: absolute;
    inset: 0;
    width: 100%;
    height: 100%;
  }
  .robot-scene__panel {
    position: absolute;
    left: 16px;
    top: 16px;
    width: min(172px, calc(100% - 32px));
    overflow: hidden;
    background: rgba(12, 17, 25, 0.88);
    border: 1px solid rgba(232, 237, 242, 0.18);
    padding: 10px;
    box-sizing: border-box;
    box-shadow: 0 12px 34px rgba(0, 0, 0, 0.34);
  }
  .robot-scene__title {
    color: #43d17a;
    font-size: 11px;
    font-weight: 650;
    letter-spacing: 0;
    margin-bottom: 8px;
    text-transform: uppercase;
  }
  .robot-scene__muted {
    color: #aeb8c4;
  }
  .robot-scene__row {
    display: flex;
    justify-content: space-between;
    gap: 10px;
    padding: 3px 0;
    white-space: nowrap;
  }
  .robot-scene__joint {
    cursor: pointer;
  }
  .robot-scene__joint:hover {
    color: #9bd3ff;
  }
`

const fmt = (value, digits = 3) =>
  typeof value === "number" && Number.isFinite(value) ? value.toFixed(digits) : "-"

const clamp = (value, min, max) => Math.max(min, Math.min(max, value))

const roundedRect = (ctx, x, y, width, height, radius = 4) => {
  const r = Math.min(radius, width / 2, height / 2)
  ctx.beginPath()
  ctx.moveTo(x + r, y)
  ctx.lineTo(x + width - r, y)
  ctx.quadraticCurveTo(x + width, y, x + width, y + r)
  ctx.lineTo(x + width, y + height - r)
  ctx.quadraticCurveTo(x + width, y + height, x + width - r, y + height)
  ctx.lineTo(x + r, y + height)
  ctx.quadraticCurveTo(x, y + height, x, y + height - r)
  ctx.lineTo(x, y + r)
  ctx.quadraticCurveTo(x, y, x + r, y)
  ctx.closePath()
}

const installWorkbenchResizeHandleOverrides = () => {
  if (window.__robotDreamsResizeHandleOverridesInstalled) return
  window.__robotDreamsResizeHandleOverridesInstalled = true

  const parsePixels = (value) => {
    const parsed = Number.parseFloat(value)
    return Number.isFinite(parsed) ? parsed : 0
  }

  const apply = () => {
    const workRow = document.querySelector("body > .flex-col > .flex-row:nth-of-type(3)")
    const rightPanel = workRow?.children?.[2]
    if (!(rightPanel instanceof HTMLElement)) return

    const handle = Array.from(rightPanel.children).find((child) =>
      child instanceof HTMLElement && child.classList.contains("wgui-resize-handle")
    )
    if (!(handle instanceof HTMLElement)) return

    handle.classList.add("robotdreams-left-resize-handle")
    handle.style.left = "0"
    handle.style.right = "auto"
    handle.style.cursor = "col-resize"
    if (handle.dataset.robotDreamsResizeSide === "left") return
    handle.dataset.robotDreamsResizeSide = "left"

    handle.onmousedown = (event) => {
      event.preventDefault()
      event.stopPropagation()
      const startX = event.clientX
      const startWidth = rightPanel.getBoundingClientRect().width
      const computed = window.getComputedStyle(rightPanel)
      const minWidth = parsePixels(computed.minWidth)
      const maxWidth = parsePixels(computed.maxWidth)

      const onMove = (moveEvent) => {
        let width = startWidth + (startX - moveEvent.clientX)
        if (minWidth && width < minWidth) width = minWidth
        if (maxWidth && width > maxWidth) width = maxWidth
        rightPanel.style.width = `${width}px`
      }
      const onUp = () => {
        document.removeEventListener("mousemove", onMove)
        document.removeEventListener("mouseup", onUp)
        document.body.style.userSelect = ""
        document.body.style.cursor = ""
      }

      document.body.style.userSelect = "none"
      document.body.style.cursor = "col-resize"
      document.addEventListener("mousemove", onMove)
      document.addEventListener("mouseup", onUp)
    }
  }

  apply()
  const observer = new MutationObserver(apply)
  observer.observe(document.body, { childList: true, subtree: true })
}

export default class RobotSceneController {
  constructor(element, ctx) {
    this.element = element
    this.ctx = ctx
    this.root = document.createElement("div")
    this.root.className = "robot-scene"
    installWorkbenchResizeHandleOverrides()

    const style = document.createElement("style")
    style.textContent = css
    this.root.appendChild(style)

    this.canvas = document.createElement("canvas")
    this.canvas.className = "robot-scene__canvas"
    this.root.appendChild(this.canvas)

    this.viewportImageLoaded = false
    this.viewportImage = new Image()
    this.viewportImage.decoding = "async"
    this.viewportImage.onload = () => {
      this.viewportImageLoaded = true
      this.draw()
    }
    this.viewportImage.src = "/fs/assets/viewport/factory-workcell.png"

    this.panel = document.createElement("div")
    this.panel.className = "robot-scene__panel"
    this.root.appendChild(this.panel)

    this.resizeObserver = new ResizeObserver(() => this.draw())
  }

  mount(props) {
    this.element.replaceChildren(this.root)
    this.resizeObserver.observe(this.element)
    this.setProps(props)
  }

  setProps(props) {
    this.props = props ?? {}
    this.renderPanel()
    this.draw()
  }

  dispose() {
    this.resizeObserver.disconnect()
    this.element.replaceChildren()
  }

  renderPanel() {
    const props = this.props ?? {}
    const robot = props.robot ?? null

    const rows = [
      ["Time", "00:12:48"],
      ["FPS", "60.2"],
      ["Real Time Factor", "1.00"],
      ["Iterations", "765,432"],
      ["Physics Step", "0.002s"],
      ["Links", String(robot?.linkCount ?? "-")],
    ]

    this.panel.replaceChildren()

    const title = document.createElement("div")
    title.className = "robot-scene__title"
    title.textContent = "Simulation"
    this.panel.appendChild(title)

    for (const [label, value] of rows) {
      const row = document.createElement("div")
      row.className = "robot-scene__row"
      const left = document.createElement("span")
      left.className = "robot-scene__muted"
      left.textContent = label
      const right = document.createElement("span")
      right.textContent = String(value)
      row.append(left, right)
      this.panel.appendChild(row)
    }

    const loaded = document.createElement("div")
    loaded.className = "robot-scene__row robot-scene__muted"
    loaded.textContent = robot ? "PuppyArm model active" : "No robot loaded"
    this.panel.appendChild(loaded)
  }

  draw() {
    if (!this.canvas) return

    const rect = this.element.getBoundingClientRect()
    const dpr = window.devicePixelRatio || 1
    const width = Math.max(1, Math.floor(rect.width * dpr))
    const height = Math.max(1, Math.floor(rect.height * dpr))
    if (this.canvas.width !== width || this.canvas.height !== height) {
      this.canvas.width = width
      this.canvas.height = height
    }

    const ctx = this.canvas.getContext("2d")
    if (!ctx) return

    ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
    ctx.clearRect(0, 0, rect.width, rect.height)

    const imageDrawn = this.drawViewportImage(ctx, rect)
    if (!imageDrawn) {
      this.drawFactoryBackdrop(ctx, rect)
      this.drawWorkcell(ctx, rect)
      this.drawRover(ctx, rect)
      this.drawRobotArm(ctx, rect)
    }
    this.drawPathOverlay(ctx, rect)
  }

  drawViewportImage(ctx, rect) {
    if (!this.viewportImageLoaded) return false

    const image = this.viewportImage
    const sourceAspect = image.naturalWidth / image.naturalHeight
    const targetAspect = rect.width / rect.height
    let sx = 0
    let sy = 0
    let sw = image.naturalWidth
    let sh = image.naturalHeight

    if (sourceAspect > targetAspect) {
      sw = image.naturalHeight * targetAspect
      sx = (image.naturalWidth - sw) / 2
    } else {
      sh = image.naturalWidth / targetAspect
      sy = (image.naturalHeight - sh) / 2
    }

    ctx.save()
    ctx.imageSmoothingEnabled = true
    ctx.imageSmoothingQuality = "high"
    ctx.drawImage(image, sx, sy, sw, sh, 0, 0, rect.width, rect.height)

    const shade = ctx.createLinearGradient(0, 0, 0, rect.height)
    shade.addColorStop(0, "rgba(5, 10, 18, 0.18)")
    shade.addColorStop(0.45, "rgba(5, 10, 18, 0.02)")
    shade.addColorStop(1, "rgba(5, 10, 18, 0.24)")
    ctx.fillStyle = shade
    ctx.fillRect(0, 0, rect.width, rect.height)

    const vignette = ctx.createRadialGradient(
      rect.width * 0.5,
      rect.height * 0.54,
      rect.width * 0.26,
      rect.width * 0.5,
      rect.height * 0.54,
      rect.width * 0.78,
    )
    vignette.addColorStop(0, "rgba(0, 0, 0, 0)")
    vignette.addColorStop(1, "rgba(0, 0, 0, 0.28)")
    ctx.fillStyle = vignette
    ctx.fillRect(0, 0, rect.width, rect.height)

    ctx.strokeStyle = "rgba(85, 167, 255, 0.22)"
    ctx.lineWidth = 1
    ctx.setLineDash([10, 18])
    for (let i = 0; i < 3; i += 1) {
      const y = rect.height * (0.56 + i * 0.09)
      ctx.beginPath()
      ctx.moveTo(rect.width * 0.45, y)
      ctx.bezierCurveTo(rect.width * 0.62, y - rect.height * 0.04, rect.width * 0.78, y, rect.width * 0.96, y - rect.height * 0.08)
      ctx.stroke()
    }
    ctx.setLineDash([])
    ctx.restore()

    return true
  }

  drawFactoryBackdrop(ctx, rect) {
    const sky = ctx.createLinearGradient(0, 0, 0, rect.height)
    sky.addColorStop(0, "#3d4852")
    sky.addColorStop(0.28, "#28323c")
    sky.addColorStop(0.48, "#1f2933")
    sky.addColorStop(1, "#101820")
    ctx.fillStyle = sky
    ctx.fillRect(0, 0, rect.width, rect.height)

    const haze = ctx.createRadialGradient(
      rect.width * 0.48,
      rect.height * 0.2,
      1,
      rect.width * 0.48,
      rect.height * 0.2,
      rect.width * 0.7,
    )
    haze.addColorStop(0, "rgba(214, 226, 236, 0.22)")
    haze.addColorStop(1, "rgba(112, 140, 166, 0)")
    ctx.fillStyle = haze
    ctx.fillRect(0, 0, rect.width, rect.height)

    ctx.strokeStyle = "rgba(230, 240, 248, 0.12)"
    ctx.lineWidth = 1
    for (let i = 0; i < 6; i += 1) {
      const y = rect.height * (0.09 + i * 0.055)
      ctx.beginPath()
      ctx.moveTo(0, y)
      ctx.lineTo(rect.width, y + rect.height * 0.018)
      ctx.stroke()
    }

    for (let i = 0; i < 9; i += 1) {
      const x = rect.width * (0.06 + i * 0.115)
      const glow = ctx.createLinearGradient(x, 0, x + rect.width * 0.09, 0)
      glow.addColorStop(0, "rgba(255,255,255,0)")
      glow.addColorStop(0.5, "rgba(245,250,255,0.68)")
      glow.addColorStop(1, "rgba(255,255,255,0)")
      ctx.fillStyle = glow
      roundedRect(ctx, x, rect.height * 0.072, rect.width * 0.06, 5, 3)
      ctx.fill()
      ctx.fillStyle = "rgba(82, 115, 148, 0.18)"
      roundedRect(ctx, x - 8, rect.height * 0.098, rect.width * 0.075, rect.height * 0.025, 3)
      ctx.fill()
    }

    for (let i = 0; i < 13; i += 1) {
      const x = rect.width * (0.02 + i * 0.105)
      const y = rect.height * (0.23 + (i % 4) * 0.035)
      const w = rect.width * (0.07 + (i % 3) * 0.012)
      const h = rect.height * (0.09 + (i % 2) * 0.035)
      ctx.fillStyle = i % 2 === 0 ? "rgba(154, 171, 186, 0.22)" : "rgba(95, 116, 132, 0.2)"
      ctx.fillRect(x, y, w, h)
      ctx.fillStyle = "rgba(68, 101, 128, 0.22)"
      ctx.fillRect(x + w * 0.16, y + h * 0.18, w * 0.42, h * 0.34)
    }

    for (let i = 0; i < 3; i += 1) {
      const x = rect.width * (0.78 + i * 0.07)
      ctx.fillStyle = "rgba(24, 30, 36, 0.62)"
      ctx.fillRect(x, rect.height * 0.31, rect.width * 0.035, rect.height * 0.34)
      ctx.fillStyle = "rgba(245, 185, 64, 0.86)"
      for (let stripe = 0; stripe < 4; stripe += 1) {
        ctx.save()
        ctx.translate(x, rect.height * (0.35 + stripe * 0.07))
        ctx.rotate(-0.55)
        ctx.fillRect(-6, 0, rect.width * 0.055, 5)
        ctx.restore()
      }
    }

    const horizon = rect.height * 0.43
    const floor = ctx.createLinearGradient(0, horizon, 0, rect.height)
    floor.addColorStop(0, "#505963")
    floor.addColorStop(1, "#1d2630")
    ctx.fillStyle = floor
    ctx.beginPath()
    ctx.moveTo(0, horizon)
    ctx.lineTo(rect.width, horizon)
    ctx.lineTo(rect.width, rect.height)
    ctx.lineTo(0, rect.height)
    ctx.closePath()
    ctx.fill()

    ctx.strokeStyle = "rgba(168, 188, 206, 0.22)"
    ctx.lineWidth = 1
    const vanishingX = rect.width * 0.55
    for (let i = -8; i <= 8; i += 1) {
      const x = rect.width * 0.5 + i * rect.width * 0.08
      ctx.beginPath()
      ctx.moveTo(vanishingX, horizon)
      ctx.lineTo(x, rect.height)
      ctx.stroke()
    }
    for (let i = 0; i < 12; i += 1) {
      const t = i / 11
      const y = horizon + Math.pow(t, 1.7) * (rect.height - horizon)
      ctx.beginPath()
      ctx.moveTo(0, y)
      ctx.lineTo(rect.width, y)
      ctx.stroke()
    }

    ctx.strokeStyle = "rgba(67, 209, 122, 0.2)"
    ctx.lineWidth = 4
    ctx.beginPath()
    ctx.moveTo(rect.width * 0.08, rect.height * 0.84)
    ctx.lineTo(rect.width * 0.95, rect.height * 0.62)
    ctx.stroke()

    const vignette = ctx.createRadialGradient(
      rect.width * 0.5,
      rect.height * 0.54,
      rect.width * 0.2,
      rect.width * 0.5,
      rect.height * 0.54,
      rect.width * 0.72,
    )
    vignette.addColorStop(0, "rgba(0,0,0,0)")
    vignette.addColorStop(1, "rgba(0,0,0,0.32)")
    ctx.fillStyle = vignette
    ctx.fillRect(0, 0, rect.width, rect.height)
  }

  drawWorkcell(ctx, rect) {
    const tableX = rect.width * 0.12
    const tableY = rect.height * 0.7
    const tableW = rect.width * 0.5
    const tableH = rect.height * 0.085

    ctx.save()
    ctx.shadowColor = "rgba(0, 0, 0, 0.46)"
    ctx.shadowBlur = 24
    ctx.shadowOffsetY = 14
    ctx.fillStyle = "#141a20"
    roundedRect(ctx, tableX - 12, tableY + tableH + 22, tableW + 24, 18, 10)
    ctx.fill()
    ctx.restore()

    const rail = ctx.createLinearGradient(0, tableY, 0, tableY + tableH)
    rail.addColorStop(0, "#4a545e")
    rail.addColorStop(0.48, "#20272e")
    rail.addColorStop(1, "#121820")
    ctx.fillStyle = rail
    ctx.strokeStyle = "#9ca9b6"
    ctx.lineWidth = 2
    roundedRect(ctx, tableX, tableY, tableW, tableH, 2)
    ctx.fill()
    ctx.stroke()

    ctx.strokeStyle = "rgba(225, 235, 245, 0.22)"
    ctx.beginPath()
    ctx.moveTo(tableX + 8, tableY + 10)
    ctx.lineTo(tableX + tableW - 8, tableY + 10)
    ctx.stroke()

    for (const x of [tableX + 18, tableX + tableW - 26]) {
      const leg = ctx.createLinearGradient(x, tableY, x + 8, tableY)
      leg.addColorStop(0, "#7f8b96")
      leg.addColorStop(0.5, "#d5dde5")
      leg.addColorStop(1, "#5d6872")
      ctx.fillStyle = leg
      ctx.fillRect(x, tableY + tableH, 8, rect.height * 0.18)
    }
    for (const x of [tableX + tableW * 0.38, tableX + tableW * 0.68]) {
      const extrusion = ctx.createLinearGradient(x, tableY, x + 7, tableY)
      extrusion.addColorStop(0, "#596673")
      extrusion.addColorStop(0.45, "#c5d0da")
      extrusion.addColorStop(1, "#46515c")
      ctx.fillStyle = extrusion
      ctx.fillRect(x, tableY + tableH, 7, rect.height * 0.16)
    }
    for (let i = 0; i < 4; i += 1) {
      const x = tableX + tableW * (0.32 + i * 0.13)
      const block = ctx.createLinearGradient(x, tableY - 38, x, tableY)
      block.addColorStop(0, "#a5afb8")
      block.addColorStop(1, "#555f69")
      ctx.fillStyle = block
      roundedRect(ctx, x, tableY - 28, 34, 28, 2)
      ctx.fill()
      ctx.fillStyle = "#3e4852"
      roundedRect(ctx, x + 7, tableY - 38, 20, 10, 2)
      ctx.fill()
    }

    const crateX = tableX + tableW * 0.58
    const crateY = tableY - 56
    const crate = ctx.createLinearGradient(crateX, crateY, crateX + 66, crateY + 42)
    crate.addColorStop(0, "#2d99e5")
    crate.addColorStop(1, "#0f4c7e")
    ctx.fillStyle = crate
    roundedRect(ctx, crateX, crateY, 66, 42, 2)
    ctx.fill()
    ctx.strokeStyle = "#54b4ff"
    ctx.stroke()
  }

  drawRobotArm(ctx, rect) {
    const joints = this.props?.robot?.movableJoints ?? []
    const tableX = rect.width * 0.12
    const tableY = rect.height * 0.7
    const baseX = tableX + rect.width * 0.16
    const baseY = tableY - 10
    const values = joints.map((joint) => Number(joint.value || 0))
    const shoulder = -1.34 + clamp(values[0] || 0, -0.7, 0.7) * 0.28
    const elbow = 0.78 + clamp(values[1] || 0, -0.7, 0.7) * 0.3
    const wrist = 0.42 + clamp(values[2] || 0, -0.7, 0.7) * 0.22
    const l1 = rect.height * 0.32
    const l2 = rect.height * 0.27
    const l3 = rect.height * 0.13

    const p0 = { x: baseX, y: baseY - 34 }
    const p1 = { x: p0.x + Math.cos(shoulder) * l1, y: p0.y + Math.sin(shoulder) * l1 }
    const p2 = { x: p1.x + Math.cos(shoulder + elbow) * l2, y: p1.y + Math.sin(shoulder + elbow) * l2 }
    const p3 = { x: p2.x + Math.cos(shoulder + elbow + wrist) * l3, y: p2.y + Math.sin(shoulder + elbow + wrist) * l3 }

    ctx.save()
    ctx.shadowColor = "rgba(0, 0, 0, 0.42)"
    ctx.shadowBlur = 20
    ctx.shadowOffsetY = 12
    const base = ctx.createLinearGradient(baseX - 38, baseY - 24, baseX + 38, baseY + 6)
    base.addColorStop(0, "#8a96a2")
    base.addColorStop(0.45, "#535f69")
    base.addColorStop(1, "#262f38")
    ctx.fillStyle = base
    roundedRect(ctx, baseX - 44, baseY - 28, 88, 32, 4)
    ctx.fill()
    ctx.restore()

    const jointFill = ctx.createRadialGradient(p0.x - 7, p0.y - 9, 4, p0.x, p0.y, 34)
    jointFill.addColorStop(0, "#d2e8ff")
    jointFill.addColorStop(0.62, "#a5c5e7")
    jointFill.addColorStop(1, "#617589")
    ctx.fillStyle = jointFill
    ctx.beginPath()
    ctx.arc(p0.x, p0.y, 38, 0, Math.PI * 2)
    ctx.fill()

    this.drawArmSegment(ctx, p0, p1, 28)
    this.drawArmSegment(ctx, p1, p2, 22)
    this.drawArmSegment(ctx, p2, p3, 14)

    for (const point of [p0, p1, p2]) {
      const joint = ctx.createRadialGradient(point.x - 6, point.y - 7, 3, point.x, point.y, 22)
      joint.addColorStop(0, "#d9edff")
      joint.addColorStop(0.65, "#a9c7e8")
      joint.addColorStop(1, "#52677b")
      ctx.fillStyle = joint
      ctx.beginPath()
      ctx.arc(point.x, point.y, 24, 0, Math.PI * 2)
      ctx.fill()
      ctx.strokeStyle = "#41505d"
      ctx.lineWidth = 4
      ctx.stroke()
    }

    ctx.strokeStyle = "#15191f"
    ctx.lineWidth = 6
    ctx.beginPath()
    ctx.moveTo(p0.x - 18, p0.y - 22)
    ctx.bezierCurveTo(p0.x - 60, p0.y - 110, p1.x - 60, p1.y - 70, p2.x - 8, p2.y - 18)
    ctx.stroke()

    ctx.save()
    const angle = Math.atan2(p2.y - p1.y, p2.x - p1.x)
    ctx.translate((p1.x + p2.x) / 2, (p1.y + p2.y) / 2)
    ctx.rotate(angle)
    ctx.fillStyle = "rgba(22, 28, 35, 0.75)"
    roundedRect(ctx, -36, -7, 72, 14, 4)
    ctx.fill()
    ctx.fillStyle = "#e7edf3"
    ctx.font = "10px system-ui, sans-serif"
    ctx.textAlign = "center"
    ctx.fillText("PUPPYARM", 0, 4)
    ctx.restore()

    ctx.strokeStyle = "#d4dce5"
    ctx.lineWidth = 3
    ctx.beginPath()
    ctx.moveTo(p3.x - 11, p3.y + 5)
    ctx.lineTo(p3.x - 11, p3.y + 34)
    ctx.moveTo(p3.x + 11, p3.y + 5)
    ctx.lineTo(p3.x + 11, p3.y + 34)
    ctx.stroke()
  }

  drawArmSegment(ctx, a, b, width) {
    const gradient = ctx.createLinearGradient(a.x, a.y, b.x, b.y)
    gradient.addColorStop(0, "#d9e0e8")
    gradient.addColorStop(0.5, "#8f9aa5")
    gradient.addColorStop(1, "#e7edf3")
    ctx.save()
    ctx.shadowColor = "rgba(0, 0, 0, 0.34)"
    ctx.shadowBlur = 9
    ctx.shadowOffsetY = 5
    ctx.strokeStyle = gradient
    ctx.lineWidth = width
    ctx.lineCap = "round"
    ctx.beginPath()
    ctx.moveTo(a.x, a.y)
    ctx.lineTo(b.x, b.y)
    ctx.stroke()
    ctx.strokeStyle = "rgba(20, 24, 30, 0.32)"
    ctx.lineWidth = Math.max(2, width * 0.18)
    ctx.stroke()
    ctx.restore()
  }

  drawRover(ctx, rect) {
    const x = rect.width * 0.72
    const y = rect.height * 0.7
    const w = rect.width * 0.14
    const h = rect.height * 0.08
    ctx.save()
    ctx.shadowColor = "rgba(0, 0, 0, 0.44)"
    ctx.shadowBlur = 18
    ctx.shadowOffsetY = 9
    const body = ctx.createLinearGradient(x, y, x + w, y + h)
    body.addColorStop(0, "#303a45")
    body.addColorStop(0.5, "#19222b")
    body.addColorStop(1, "#111821")
    ctx.fillStyle = body
    ctx.strokeStyle = "#708093"
    ctx.lineWidth = 2
    roundedRect(ctx, x, y, w, h, 4)
    ctx.fill()
    ctx.stroke()
    ctx.restore()
    ctx.fillStyle = "#11161d"
    for (const wheelX of [x + 16, x + w - 30]) {
      ctx.beginPath()
      ctx.arc(wheelX, y + h + 10, 18, 0, Math.PI * 2)
      ctx.fill()
      ctx.strokeStyle = "#2b95ff"
      ctx.stroke()
    }
    ctx.fillStyle = "#54b4ff"
    roundedRect(ctx, x + 18, y + h - 10, w - 36, 4, 2)
    ctx.fill()
    ctx.fillStyle = "#1d252e"
    ctx.fillRect(x + w * 0.45, y - 34, w * 0.2, 34)
    ctx.fillStyle = "#80e1b3"
    ctx.beginPath()
    ctx.arc(x + w * 0.55, y - 40, 12, 0, Math.PI * 2)
    ctx.fill()
  }

  drawPathOverlay(ctx, rect) {
    ctx.strokeStyle = "rgba(85, 167, 255, 0.58)"
    ctx.lineWidth = 2
    ctx.setLineDash([9, 10])
    ctx.beginPath()
    ctx.moveTo(rect.width * 0.58, rect.height * 0.75)
    ctx.bezierCurveTo(rect.width * 0.7, rect.height * 0.63, rect.width * 0.82, rect.height * 0.64, rect.width * 0.9, rect.height * 0.52)
    ctx.stroke()
    ctx.strokeStyle = "rgba(67, 209, 122, 0.58)"
    ctx.beginPath()
    ctx.moveTo(rect.width * 0.65, rect.height * 0.55)
    ctx.bezierCurveTo(rect.width * 0.78, rect.height * 0.48, rect.width * 0.82, rect.height * 0.36, rect.width * 0.94, rect.height * 0.34)
    ctx.stroke()
    ctx.setLineDash([])
  }
}
