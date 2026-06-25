const css = `
  :host, .dashboard-preview {
    display: block;
    width: 100%;
    height: 100%;
    min-height: 0;
    box-sizing: border-box;
  }
  .dashboard-preview {
    width: 100%;
    height: 100%;
    position: relative;
    overflow: hidden;
    background: #08111d;
    color: #dce7f3;
    font: 11px/1.2 Inter, ui-sans-serif, system-ui, sans-serif;
  }
  .dashboard-preview__stage {
    position: absolute;
    inset: 0;
    background:
      linear-gradient(135deg, rgba(85, 167, 255, 0.12), transparent 42%),
      linear-gradient(180deg, #101a28, #070d16);
  }
  .dashboard-preview__grid {
    position: absolute;
    inset: 0;
    opacity: 0.35;
    background-image:
      linear-gradient(rgba(143, 189, 242, 0.16) 1px, transparent 1px),
      linear-gradient(90deg, rgba(143, 189, 242, 0.16) 1px, transparent 1px);
    background-size: 28px 28px;
  }
  .dashboard-preview__badge {
    position: absolute;
    left: 7px;
    bottom: 6px;
    color: #f4f8ff;
    font-weight: 650;
  }
  .dashboard-preview__label {
    position: absolute;
    right: 7px;
    bottom: 6px;
    max-width: 58%;
    overflow: hidden;
    color: #9fb0c5;
    text-align: right;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .dashboard-preview__arm,
  .dashboard-preview__block,
  .dashboard-preview__camera,
  .dashboard-preview__lidar,
  .dashboard-preview__plot {
    position: absolute;
    inset: 8px;
  }
  .dashboard-preview__arm::before {
    content: "";
    position: absolute;
    left: 18%;
    top: 62%;
    width: 54%;
    height: 7px;
    border-radius: 999px;
    background: #d8e3ef;
    box-shadow: 22px -22px 0 -1px #d8e3ef;
    transform: rotate(-32deg);
    transform-origin: left center;
  }
  .dashboard-preview__arm::after {
    content: "";
    position: absolute;
    left: 52%;
    top: 20%;
    width: 13px;
    height: 13px;
    border-radius: 50%;
    background: #a9c7e8;
    box-shadow: -28px 22px 0 #a9c7e8, -54px 48px 0 #a9c7e8;
  }
  .dashboard-preview__block {
    margin: auto;
    width: 52%;
    height: 34%;
    border: 1px solid rgba(244, 248, 255, 0.36);
    background: var(--accent, #245c95);
    transform: translateY(-4px) skewY(-5deg);
  }
  .dashboard-preview__lidar {
    border-radius: 50%;
    border: 1px solid rgba(85, 167, 255, 0.55);
    background:
      radial-gradient(circle at 50% 50%, #08111d 0 12%, transparent 13%),
      repeating-conic-gradient(from 20deg, #2aa7ff 0 4deg, #43d17a 4deg 7deg, #f3b84f 7deg 10deg, transparent 10deg 14deg);
    clip-path: circle(42% at 50% 50%);
  }
  .dashboard-preview__camera {
    background:
      linear-gradient(160deg, transparent 0 42%, rgba(244, 248, 255, 0.15) 43% 44%, transparent 45%),
      linear-gradient(20deg, transparent 0 55%, rgba(67, 209, 122, 0.24) 56% 58%, transparent 59%);
  }
  .dashboard-preview__camera::after {
    content: "";
    position: absolute;
    left: 35%;
    top: 45%;
    width: 34%;
    height: 22%;
    border: 1px solid rgba(244, 248, 255, 0.55);
    background: rgba(42, 167, 255, 0.45);
  }
  .dashboard-preview__plot {
    background:
      linear-gradient(90deg, transparent 0 16%, rgba(143, 189, 242, 0.18) 17% 18%, transparent 19%),
      linear-gradient(180deg, transparent 0 48%, rgba(143, 189, 242, 0.18) 49% 50%, transparent 51%);
  }
  .dashboard-preview__plot::after {
    content: "";
    position: absolute;
    left: 18%;
    right: 8%;
    top: 20%;
    height: 48%;
    border-bottom: 3px solid #2aa7ff;
    border-radius: 50%;
    box-shadow: 0 12px 0 -1px #43d17a, 0 24px 0 -1px #f3b84f;
    transform: skewX(-14deg);
  }
`

const makeElement = (tag, className, text = "") => {
  const node = document.createElement(tag)
  node.className = className
  node.textContent = text
  return node
}

const labelFor = (props) => props?.kind ?? props?.subtitle ?? props?.rate ?? ""

const badgeFor = (props) => props?.badge ?? props?.rate ?? ""

const visualClassFor = (props) => {
  const mode = props?.mode ?? "plot"
  const badge = props?.badge ?? ""
  if (mode === "lidar") return "dashboard-preview__lidar"
  if (mode === "camera") return "dashboard-preview__camera"
  if (mode === "plot") return "dashboard-preview__plot"
  if (badge === "ARM") return "dashboard-preview__arm"
  return "dashboard-preview__block"
}

export default class DashboardPreviewController {
  constructor(element) {
    this.element = element
    this.props = {}
    this.root = document.createElement("div")
    this.root.className = "dashboard-preview"

    const style = document.createElement("style")
    style.textContent = css
    this.root.appendChild(style)

    this.stage = makeElement("div", "dashboard-preview__stage")
    this.root.appendChild(this.stage)
  }

  mount(props) {
    this.element.replaceChildren(this.root)
    this.setProps(props)
  }

  setProps(props) {
    this.props = props ?? {}
    this.render()
  }

  dispose() {
    this.element.replaceChildren()
  }

  render() {
    const props = this.props
    this.stage.replaceChildren(makeElement("div", "dashboard-preview__grid"))
    this.stage.style.setProperty("--accent", props?.accent ?? "#245c95")
    this.stage.appendChild(makeElement("div", visualClassFor(props)))
    this.stage.appendChild(makeElement("div", "dashboard-preview__badge", badgeFor(props)))
    this.stage.appendChild(makeElement("div", "dashboard-preview__label", labelFor(props)))
  }
}
