export default class Controller {
  constructor(element, ctx) {
    this.element = element
    this.ctx = ctx
    this.timer = 0
    this.enabled = false
    this.intervalMs = 100
  }

  mount(props) {
    this.setProps(props)
  }

  setProps(props) {
    const next = props && typeof props === "object" ? props : {}
    const enabled = Boolean(next.enabled)
    const intervalMs = Number.isFinite(next.intervalMs)
      ? Math.max(50, Math.min(1000, next.intervalMs))
      : 100

    if (this.enabled === enabled && this.intervalMs === intervalMs) {
      return
    }

    this.enabled = enabled
    this.intervalMs = intervalMs
    this.restart()
  }

  restart() {
    this.stop()
    if (!this.enabled) {
      return
    }

    this.timer = window.setInterval(() => {
      this.refresh()
    }, this.intervalMs)
  }

  refresh() {
    this.ctx.emit("refresh")
  }

  stop() {
    if (this.timer) {
      window.clearInterval(this.timer)
      this.timer = 0
    }
  }

  dispose() {
    this.stop()
  }
}
