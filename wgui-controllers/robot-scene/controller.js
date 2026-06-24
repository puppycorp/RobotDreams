const css = `
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
    left: 12px;
    top: 12px;
    width: min(360px, calc(100% - 24px));
    max-height: calc(100% - 24px);
    overflow: auto;
    background: rgba(16, 19, 24, 0.82);
    border: 1px solid rgba(232, 237, 242, 0.18);
    padding: 12px;
    box-sizing: border-box;
  }
  .robot-scene__title {
    font-size: 15px;
    font-weight: 650;
    margin-bottom: 6px;
  }
  .robot-scene__muted {
    color: #aeb8c4;
  }
  .robot-scene__row {
    display: flex;
    justify-content: space-between;
    gap: 14px;
    border-top: 1px solid rgba(232, 237, 242, 0.12);
    padding: 6px 0;
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

export default class RobotSceneController {
  constructor(element, ctx) {
    this.element = element
    this.ctx = ctx
    this.root = document.createElement("div")
    this.root.className = "robot-scene"

    const style = document.createElement("style")
    style.textContent = css
    this.root.appendChild(style)

    this.canvas = document.createElement("canvas")
    this.canvas.className = "robot-scene__canvas"
    this.root.appendChild(this.canvas)

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
    const model = props.model ?? {}
    const robot = props.robot ?? null
    const base = props.base ?? {}
    const movableJoints = robot?.movableJoints ?? []

    const rows = [
      ["Model", model.path ?? model.src ?? "none"],
      ["Status", props.status ?? ""],
      ["Links", robot?.linkCount ?? "-"],
      ["Joints", robot?.jointCount ?? "-"],
      ["Base xyz", (base.translation ?? []).map((v) => fmt(v)).join(", ")],
      ["Base rpy", (base.rotation ?? []).map((v) => fmt(v)).join(", ")],
    ]

    this.panel.replaceChildren()

    const title = document.createElement("div")
    title.className = "robot-scene__title"
    title.textContent = "Robot Scene"
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

    if (movableJoints.length === 0) {
      const empty = document.createElement("div")
      empty.className = "robot-scene__row robot-scene__muted"
      empty.textContent = "No movable joints reported"
      this.panel.appendChild(empty)
      return
    }

    for (const joint of movableJoints.slice(0, 12)) {
      const row = document.createElement("div")
      row.className = "robot-scene__row robot-scene__joint"
      row.onclick = () => this.ctx.emit("jointSelected", { joint: joint.name })
      const left = document.createElement("span")
      left.textContent = joint.name
      const right = document.createElement("span")
      right.textContent = `${fmt(joint.value)} rad`
      row.append(left, right)
      this.panel.appendChild(row)
    }
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

    const gradient = ctx.createLinearGradient(0, 0, rect.width, rect.height)
    gradient.addColorStop(0, "#151a22")
    gradient.addColorStop(1, "#0b0d12")
    ctx.fillStyle = gradient
    ctx.fillRect(0, 0, rect.width, rect.height)

    ctx.strokeStyle = "rgba(155, 211, 255, 0.16)"
    ctx.lineWidth = 1
    for (let x = 0; x < rect.width; x += 32) {
      ctx.beginPath()
      ctx.moveTo(x, 0)
      ctx.lineTo(x, rect.height)
      ctx.stroke()
    }
    for (let y = 0; y < rect.height; y += 32) {
      ctx.beginPath()
      ctx.moveTo(0, y)
      ctx.lineTo(rect.width, y)
      ctx.stroke()
    }

    const joints = this.props?.robot?.movableJoints ?? []
    const cx = rect.width * 0.58
    const cy = rect.height * 0.58
    let angle = -Math.PI / 2
    let x = cx
    let y = cy
    const segment = Math.max(28, Math.min(rect.width, rect.height) * 0.095)

    ctx.lineCap = "round"
    ctx.lineJoin = "round"
    ctx.strokeStyle = "#9bd3ff"
    ctx.lineWidth = 5
    ctx.beginPath()
    ctx.moveTo(x, y)
    for (const joint of joints.slice(0, 8)) {
      angle += Number(joint.value || 0)
      x += Math.cos(angle) * segment
      y += Math.sin(angle) * segment
      ctx.lineTo(x, y)
    }
    ctx.stroke()

    ctx.fillStyle = "#ffffff"
    ctx.beginPath()
    ctx.arc(cx, cy, 6, 0, Math.PI * 2)
    ctx.fill()
    ctx.fillStyle = "#80e1b3"
    ctx.beginPath()
    ctx.arc(x, y, 7, 0, Math.PI * 2)
    ctx.fill()
  }
}
