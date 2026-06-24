const css = `
  :host, .dashboard-preview {
    display: block;
    width: 100%;
    height: 100%;
    min-height: 0;
    box-sizing: border-box;
  }
  .dashboard-preview {
    background: #08111d;
    overflow: hidden;
    position: relative;
  }
  .dashboard-preview__canvas {
    display: block;
    width: 100%;
    height: 100%;
  }
`

const palette = ["#2aa7ff", "#43d17a", "#f3b84f", "#ff4f70", "#d45cff"]

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

export default class DashboardPreviewController {
  constructor(element) {
    this.element = element
    this.root = document.createElement("div")
    this.root.className = "dashboard-preview"

    const style = document.createElement("style")
    style.textContent = css
    this.root.appendChild(style)

    this.canvas = document.createElement("canvas")
    this.canvas.className = "dashboard-preview__canvas"
    this.root.appendChild(this.canvas)

    this.resizeObserver = new ResizeObserver(() => this.draw())
  }

  mount(props) {
    this.element.replaceChildren(this.root)
    this.resizeObserver.observe(this.element)
    this.setProps(props)
  }

  setProps(props) {
    this.props = props ?? {}
    this.draw()
  }

  dispose() {
    this.resizeObserver.disconnect()
    this.element.replaceChildren()
  }

  draw() {
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

    const mode = this.props?.mode ?? "plot"
    if (mode === "asset") this.drawAsset(ctx, rect)
    else if (mode === "lidar") this.drawLidar(ctx, rect)
    else if (mode === "camera") this.drawCamera(ctx, rect)
    else this.drawPlot(ctx, rect)
  }

  drawPanelBackground(ctx, rect) {
    const gradient = ctx.createLinearGradient(0, 0, rect.width, rect.height)
    gradient.addColorStop(0, "#101a28")
    gradient.addColorStop(1, "#070d16")
    ctx.fillStyle = gradient
    ctx.fillRect(0, 0, rect.width, rect.height)
  }

  drawLidar(ctx, rect) {
    this.drawPanelBackground(ctx, rect)
    const cx = rect.width * 0.5
    const cy = rect.height * 0.52
    const radius = Math.min(rect.width, rect.height) * 0.38

    ctx.strokeStyle = "rgba(138, 162, 190, 0.18)"
    ctx.lineWidth = 1
    for (let r = radius * 0.25; r <= radius; r += radius * 0.25) {
      ctx.beginPath()
      ctx.arc(cx, cy, r, 0, Math.PI * 2)
      ctx.stroke()
    }
    for (let a = 0; a < Math.PI * 2; a += Math.PI / 6) {
      ctx.beginPath()
      ctx.moveTo(cx, cy)
      ctx.lineTo(cx + Math.cos(a) * radius, cy + Math.sin(a) * radius)
      ctx.stroke()
    }

    for (let i = 0; i < 460; i += 1) {
      const a = (i / 460) * Math.PI * 2
      const wave = Math.sin(a * 4.2) * 0.18 + Math.cos(a * 7.4) * 0.08
      const rr = radius * (0.35 + ((i * 37) % 100) / 154 + wave)
      const x = cx + Math.cos(a) * rr
      const y = cy + Math.sin(a) * rr
      const color = palette[i % palette.length]
      ctx.fillStyle = color
      ctx.globalAlpha = 0.82
      ctx.fillRect(x, y, 1.8, 1.8)
    }
    ctx.globalAlpha = 1
    ctx.fillStyle = "#0b1220"
    ctx.beginPath()
    ctx.arc(cx, cy, radius * 0.16, 0, Math.PI * 2)
    ctx.fill()
    ctx.strokeStyle = "#55a7ff"
    ctx.stroke()

    ctx.fillStyle = "#dce7f3"
    ctx.font = "11px system-ui, sans-serif"
    ctx.fillText("Points", 8, rect.height - 22)
    ctx.fillText(String(this.props?.points ?? "124,567"), 8, rect.height - 8)
    ctx.fillStyle = "#43d17a"
    ctx.fillText(this.props?.rate ?? "10 Hz", rect.width - 42, rect.height - 8)
  }

  drawAsset(ctx, rect) {
    const accent = this.props?.accent ?? "#245c95"
    const badge = this.props?.badge ?? "RD"
    const name = this.props?.name ?? ""
    const kind = this.props?.kind ?? ""
    const background = ctx.createLinearGradient(0, 0, rect.width, rect.height)
    background.addColorStop(0, "#172334")
    background.addColorStop(1, "#0b121d")
    ctx.fillStyle = background
    ctx.fillRect(0, 0, rect.width, rect.height)

    const glow = ctx.createRadialGradient(rect.width * 0.65, rect.height * 0.25, 1, rect.width * 0.65, rect.height * 0.25, rect.width * 0.52)
    glow.addColorStop(0, `${accent}aa`)
    glow.addColorStop(1, `${accent}00`)
    ctx.fillStyle = glow
    ctx.fillRect(0, 0, rect.width, rect.height)

    ctx.strokeStyle = "rgba(174, 196, 216, 0.18)"
    ctx.lineWidth = 1
    const horizon = rect.height * 0.62
    for (let i = -3; i <= 3; i += 1) {
      const x = rect.width * 0.5 + i * rect.width * 0.16
      ctx.beginPath()
      ctx.moveTo(rect.width * 0.52, horizon)
      ctx.lineTo(x, rect.height)
      ctx.stroke()
    }
    for (let i = 0; i < 4; i += 1) {
      const y = horizon + (i + 1) * rect.height * 0.08
      ctx.beginPath()
      ctx.moveTo(0, y)
      ctx.lineTo(rect.width, y)
      ctx.stroke()
    }

    if (badge === "ARM") this.drawAssetArm(ctx, rect, accent)
    else if (badge === "ST") this.drawAssetServo(ctx, rect, accent)
    else if (badge === "BUS" || badge === "FW") this.drawAssetBus(ctx, rect, accent)
    else if (badge === "ENV") this.drawAssetEnvironment(ctx, rect, accent)
    else if (badge === "PLOT") this.drawAssetPlot(ctx, rect)
    else if (badge === "X1") this.drawAssetRover(ctx, rect, accent)
    else this.drawAssetBlock(ctx, rect, accent, badge)

    ctx.fillStyle = "rgba(6, 12, 20, 0.64)"
    ctx.fillRect(0, rect.height - 14, rect.width, 14)
    ctx.fillStyle = "#f4f8ff"
    ctx.font = "10px system-ui, sans-serif"
    ctx.fillText(badge, 6, rect.height - 4)
    ctx.fillStyle = "#9fb0c5"
    const label = kind || name
    ctx.fillText(label.slice(0, 14), rect.width * 0.42, rect.height - 4)
  }

  drawAssetArm(ctx, rect, accent) {
    const baseX = rect.width * 0.28
    const baseY = rect.height * 0.75
    const p1 = { x: rect.width * 0.42, y: rect.height * 0.42 }
    const p2 = { x: rect.width * 0.68, y: rect.height * 0.28 }
    ctx.strokeStyle = "#d8e3ef"
    ctx.lineWidth = 7
    ctx.lineCap = "round"
    ctx.beginPath()
    ctx.moveTo(baseX, baseY)
    ctx.lineTo(p1.x, p1.y)
    ctx.lineTo(p2.x, p2.y)
    ctx.stroke()
    ctx.strokeStyle = accent
    ctx.lineWidth = 2
    ctx.stroke()
    for (const point of [{ x: baseX, y: baseY }, p1, p2]) {
      ctx.fillStyle = "#a9c7e8"
      ctx.beginPath()
      ctx.arc(point.x, point.y, 8, 0, Math.PI * 2)
      ctx.fill()
      ctx.strokeStyle = "#3a4a5b"
      ctx.stroke()
    }
    ctx.fillStyle = "#5c6874"
    roundedRect(ctx, baseX - 17, baseY + 7, 34, 8, 3)
    ctx.fill()
  }

  drawAssetBlock(ctx, rect, accent, badge) {
    ctx.fillStyle = accent
    roundedRect(ctx, rect.width * 0.25, rect.height * 0.24, rect.width * 0.5, rect.height * 0.32, 4)
    ctx.fill()
    ctx.fillStyle = "#f4f8ff"
    ctx.font = "12px system-ui, sans-serif"
    ctx.textAlign = "center"
    ctx.fillText(badge, rect.width * 0.5, rect.height * 0.45)
    ctx.textAlign = "start"
  }

  drawAssetBus(ctx, rect, accent) {
    ctx.strokeStyle = accent
    ctx.lineWidth = 2
    for (let i = 0; i < 3; i += 1) {
      const y = rect.height * (0.3 + i * 0.16)
      ctx.beginPath()
      ctx.moveTo(rect.width * 0.2, y)
      ctx.lineTo(rect.width * 0.8, y)
      ctx.stroke()
    }
    ctx.fillStyle = "#101a28"
    for (let i = 0; i < 4; i += 1) {
      roundedRect(ctx, rect.width * (0.2 + i * 0.16), rect.height * 0.24, 16, 26, 3)
      ctx.fill()
      ctx.strokeStyle = "#708093"
      ctx.stroke()
    }
  }

  drawAssetEnvironment(ctx, rect, accent) {
    ctx.fillStyle = "#2f3944"
    roundedRect(ctx, rect.width * 0.18, rect.height * 0.32, rect.width * 0.64, rect.height * 0.28, 2)
    ctx.fill()
    ctx.fillStyle = accent
    for (let i = 0; i < 4; i += 1) {
      ctx.fillRect(rect.width * (0.23 + i * 0.13), rect.height * 0.38, rect.width * 0.08, rect.height * 0.08)
    }
    ctx.strokeStyle = "#dce7f3"
    ctx.beginPath()
    ctx.moveTo(rect.width * 0.18, rect.height * 0.32)
    ctx.lineTo(rect.width * 0.5, rect.height * 0.16)
    ctx.lineTo(rect.width * 0.82, rect.height * 0.32)
    ctx.stroke()
  }

  drawAssetPlot(ctx, rect) {
    for (let s = 0; s < 3; s += 1) {
      ctx.strokeStyle = palette[s]
      ctx.lineWidth = 2
      ctx.beginPath()
      for (let i = 0; i <= 18; i += 1) {
        const x = rect.width * 0.15 + (i / 18) * rect.width * 0.68
        const y = rect.height * (0.42 + Math.sin(i * 0.6 + s) * 0.16)
        if (i === 0) ctx.moveTo(x, y)
        else ctx.lineTo(x, y)
      }
      ctx.stroke()
    }
  }

  drawAssetRover(ctx, rect, accent) {
    ctx.fillStyle = "#1a2430"
    roundedRect(ctx, rect.width * 0.2, rect.height * 0.44, rect.width * 0.56, rect.height * 0.22, 5)
    ctx.fill()
    ctx.strokeStyle = "#708093"
    ctx.stroke()
    ctx.fillStyle = "#10151c"
    for (const x of [rect.width * 0.28, rect.width * 0.65]) {
      ctx.beginPath()
      ctx.arc(x, rect.height * 0.7, 8, 0, Math.PI * 2)
      ctx.fill()
      ctx.strokeStyle = accent
      ctx.stroke()
    }
    ctx.fillStyle = accent
    ctx.fillRect(rect.width * 0.31, rect.height * 0.53, rect.width * 0.34, 3)
  }

  drawAssetServo(ctx, rect, accent) {
    ctx.fillStyle = "#6f7d8b"
    roundedRect(ctx, rect.width * 0.34, rect.height * 0.22, rect.width * 0.32, rect.height * 0.46, 4)
    ctx.fill()
    ctx.fillStyle = accent
    ctx.beginPath()
    ctx.arc(rect.width * 0.5, rect.height * 0.38, 9, 0, Math.PI * 2)
    ctx.fill()
    ctx.strokeStyle = "#dce7f3"
    ctx.stroke()
    ctx.fillStyle = "#101820"
    ctx.fillRect(rect.width * 0.41, rect.height * 0.61, rect.width * 0.18, 4)
  }

  drawCamera(ctx, rect) {
    const background = ctx.createLinearGradient(0, 0, 0, rect.height)
    background.addColorStop(0, "#273544")
    background.addColorStop(0.46, "#1d2833")
    background.addColorStop(1, "#39414a")
    ctx.fillStyle = background
    ctx.fillRect(0, 0, rect.width, rect.height)

    const horizon = rect.height * 0.48
    const vpX = rect.width * 0.55
    ctx.fillStyle = "rgba(11, 18, 28, 0.35)"
    ctx.fillRect(0, 0, rect.width, horizon)

    ctx.strokeStyle = "rgba(210, 228, 245, 0.2)"
    ctx.lineWidth = 1
    for (let i = 0; i < 5; i += 1) {
      const y = rect.height * (0.12 + i * 0.06)
      ctx.beginPath()
      ctx.moveTo(rect.width * 0.08, y)
      ctx.lineTo(rect.width * 0.92, y - rect.height * 0.05)
      ctx.stroke()
    }

    for (let i = 0; i < 4; i += 1) {
      const x = rect.width * (0.2 + i * 0.18)
      const light = ctx.createRadialGradient(x, rect.height * 0.16, 2, x, rect.height * 0.16, rect.width * 0.16)
      light.addColorStop(0, "rgba(255, 255, 238, 0.42)")
      light.addColorStop(1, "rgba(255, 255, 238, 0)")
      ctx.fillStyle = light
      ctx.fillRect(x - rect.width * 0.18, 0, rect.width * 0.36, rect.height * 0.36)
      ctx.fillStyle = "#f0f6ed"
      ctx.fillRect(x - 11, rect.height * 0.1, 22, 3)
    }

    const floorGradient = ctx.createLinearGradient(0, horizon, 0, rect.height)
    floorGradient.addColorStop(0, "#46505b")
    floorGradient.addColorStop(1, "#2f363f")
    ctx.fillStyle = floorGradient
    ctx.fillRect(0, horizon, rect.width, rect.height - horizon)

    ctx.strokeStyle = "rgba(184, 203, 220, 0.18)"
    for (let i = 1; i <= 8; i += 1) {
      const t = i / 8
      const y = horizon + (rect.height - horizon) * (t * t)
      ctx.beginPath()
      ctx.moveTo(0, y)
      ctx.lineTo(rect.width, y)
      ctx.stroke()
    }
    for (let i = -5; i <= 5; i += 1) {
      const x = vpX + i * rect.width * 0.12
      ctx.beginPath()
      ctx.moveTo(vpX, horizon)
      ctx.lineTo(x, rect.height)
      ctx.stroke()
    }

    ctx.fillStyle = "rgba(18, 29, 39, 0.8)"
    for (let i = 0; i < 3; i += 1) {
      const x = rect.width * (0.08 + i * 0.09)
      const top = rect.height * (0.25 + i * 0.025)
      ctx.fillRect(x, top, rect.width * 0.045, rect.height * 0.34)
      ctx.strokeStyle = "rgba(118, 149, 175, 0.45)"
      ctx.strokeRect(x, top, rect.width * 0.045, rect.height * 0.34)
    }

    ctx.fillStyle = "#7a838b"
    ctx.fillRect(rect.width * 0.4, rect.height * 0.48, rect.width * 0.34, rect.height * 0.16)
    ctx.fillStyle = "#5b6570"
    ctx.fillRect(rect.width * 0.36, rect.height * 0.62, rect.width * 0.42, rect.height * 0.08)
    ctx.strokeStyle = "rgba(222, 233, 244, 0.65)"
    ctx.strokeRect(rect.width * 0.4, rect.height * 0.48, rect.width * 0.34, rect.height * 0.16)

    ctx.fillStyle = "#1768aa"
    ctx.fillRect(rect.width * 0.47, rect.height * 0.43, rect.width * 0.18, rect.height * 0.11)
    ctx.fillStyle = "#1f8cd8"
    ctx.fillRect(rect.width * 0.485, rect.height * 0.455, rect.width * 0.15, rect.height * 0.035)
    ctx.strokeStyle = "rgba(99, 203, 255, 0.75)"
    ctx.strokeRect(rect.width * 0.47, rect.height * 0.43, rect.width * 0.18, rect.height * 0.11)

    ctx.fillStyle = "rgba(15, 22, 31, 0.78)"
    ctx.fillRect(rect.width * 0.74, rect.height * 0.24, rect.width * 0.16, rect.height * 0.38)
    ctx.fillStyle = "#65d6ff"
    ctx.fillRect(rect.width * 0.755, rect.height * 0.28, rect.width * 0.13, 2)
    ctx.fillStyle = "rgba(255, 208, 72, 0.85)"
    ctx.fillRect(rect.width * 0.755, rect.height * 0.34, rect.width * 0.13, 2)

    const vignette = ctx.createRadialGradient(
      rect.width * 0.5,
      rect.height * 0.48,
      rect.width * 0.18,
      rect.width * 0.5,
      rect.height * 0.48,
      rect.width * 0.72,
    )
    vignette.addColorStop(0, "rgba(0, 0, 0, 0)")
    vignette.addColorStop(1, "rgba(0, 0, 0, 0.38)")
    ctx.fillStyle = vignette
    ctx.fillRect(0, 0, rect.width, rect.height)

    ctx.strokeStyle = "rgba(84, 180, 255, 0.72)"
    ctx.lineWidth = 1
    const pad = 8
    const corner = 16
    const right = rect.width - pad
    const bottom = rect.height - pad
    ctx.beginPath()
    ctx.moveTo(pad, pad + corner)
    ctx.lineTo(pad, pad)
    ctx.lineTo(pad + corner, pad)
    ctx.moveTo(right - corner, pad)
    ctx.lineTo(right, pad)
    ctx.lineTo(right, pad + corner)
    ctx.moveTo(right, bottom - corner)
    ctx.lineTo(right, bottom)
    ctx.lineTo(right - corner, bottom)
    ctx.moveTo(pad + corner, bottom)
    ctx.lineTo(pad, bottom)
    ctx.lineTo(pad, bottom - corner)
    ctx.stroke()

    ctx.fillStyle = "rgba(8, 17, 29, 0.68)"
    ctx.fillRect(8, rect.height - 24, rect.width - 16, 16)
    ctx.fillStyle = "#dce7f3"
    ctx.font = "11px system-ui, sans-serif"
    ctx.fillText(this.props?.subtitle ?? "Wrist_Cam", 12, rect.height - 12)
    ctx.fillStyle = "#43d17a"
    ctx.fillText(this.props?.rate ?? "30 Hz", rect.width - 42, rect.height - 12)
  }

  drawPlot(ctx, rect) {
    this.drawPanelBackground(ctx, rect)
    const left = 42
    const top = 14
    const right = rect.width - 92
    const bottom = rect.height - 28
    const width = Math.max(20, right - left)
    const height = Math.max(20, bottom - top)

    ctx.strokeStyle = "rgba(138, 162, 190, 0.2)"
    ctx.lineWidth = 1
    for (let i = 0; i <= 4; i += 1) {
      const y = top + (height * i) / 4
      ctx.beginPath()
      ctx.moveTo(left, y)
      ctx.lineTo(right, y)
      ctx.stroke()
    }
    for (let i = 0; i <= 6; i += 1) {
      const x = left + (width * i) / 6
      ctx.beginPath()
      ctx.moveTo(x, top)
      ctx.lineTo(x, bottom)
      ctx.stroke()
    }

    ctx.fillStyle = "#aeb8c4"
    ctx.font = "10px system-ui, sans-serif"
    ctx.fillText("180", 10, top + 4)
    ctx.fillText("0", 24, top + height * 0.52)
    ctx.fillText("-180", 6, bottom)
    ctx.fillText("Time (s)", left + width * 0.42, rect.height - 8)

    for (let s = 0; s < 5; s += 1) {
      ctx.strokeStyle = palette[s]
      ctx.lineWidth = 2
      ctx.beginPath()
      for (let i = 0; i <= 72; i += 1) {
        const x = left + (i / 72) * width
        const value =
          Math.sin(i * 0.13 + s * 0.8) * (0.16 + s * 0.025) +
          Math.cos(i * 0.31 + s) * 0.05 +
          (s - 2) * 0.12
        const y = top + height * (0.5 - value)
        if (i === 0) ctx.moveTo(x, y)
        else ctx.lineTo(x, y)
      }
      ctx.stroke()
    }

    const series = this.props?.series ?? []
    for (let i = 0; i < series.length; i += 1) {
      const y = top + i * 18
      ctx.fillStyle = series[i].color ?? palette[i % palette.length]
      ctx.fillRect(rect.width - 78, y - 8, 12, 3)
      ctx.fillStyle = "#dce7f3"
      ctx.fillText(series[i].name ?? `Series ${i + 1}`, rect.width - 60, y - 4)
    }
  }
}
