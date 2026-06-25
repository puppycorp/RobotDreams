import * as THREE from "/fs/third_party/three/three.module.js"

const css = `
  :host, .robot-scene {
    display: block;
    width: 100%;
    height: 100%;
    box-sizing: border-box;
  }
  .robot-scene {
    width: 100%;
    height: 100%;
    position: relative;
    overflow: hidden;
    background: #07101a;
    color: #dce7f3;
    font: 13px/1.35 Inter, ui-sans-serif, system-ui, sans-serif;
  }
  .robot-scene canvas {
    display: block;
    width: 100%;
    height: 100%;
  }
  .robot-scene__panel {
    position: absolute;
    left: 14px;
    top: 14px;
    width: min(210px, calc(100% - 28px));
    padding: 10px;
    background: rgba(8, 13, 20, 0.86);
    border: 1px solid rgba(85, 167, 255, 0.24);
    pointer-events: none;
  }
  .robot-scene__title {
    color: #43d17a;
    font-size: 11px;
    font-weight: 650;
    margin-bottom: 7px;
    text-transform: uppercase;
  }
  .robot-scene__row {
    display: flex;
    justify-content: space-between;
    gap: 8px;
    padding: 2px 0;
  }
  .robot-scene__row span:first-child {
    color: #aeb8c4;
  }
`

const clamp = (value, min, max) => Math.max(min, Math.min(max, value))
const finite = (value, fallback = 0) =>
  typeof value === "number" && Number.isFinite(value) ? value : fallback

const jointValues = (props) => {
  const joints = Array.isArray(props?.robot?.movableJoints) ? props.robot.movableJoints : []
  return joints.map((joint) => finite(joint.value, 0))
}

const material = (color, options = {}) =>
  new THREE.MeshStandardMaterial({
    color,
    metalness: options.metalness ?? 0.08,
    roughness: options.roughness ?? 0.72,
  })

const disposeObject = (object) => {
  object.traverse((child) => {
    child.geometry?.dispose?.()
    if (Array.isArray(child.material)) {
      child.material.forEach((item) => item.dispose?.())
    } else {
      child.material?.dispose?.()
    }
  })
}

const addBox = (group, center, size, mat) => {
  const mesh = new THREE.Mesh(new THREE.BoxGeometry(size.x, size.y, size.z), mat)
  mesh.position.copy(center)
  group.add(mesh)
  return mesh
}

const addSphere = (group, position, radius, mat) => {
  const mesh = new THREE.Mesh(new THREE.SphereGeometry(radius, 32, 16), mat)
  mesh.position.copy(position)
  group.add(mesh)
  return mesh
}

const addCylinderBetween = (group, start, end, radius, mat) => {
  const delta = new THREE.Vector3().subVectors(end, start)
  const length = delta.length()
  if (length < 0.0001) return null
  const mesh = new THREE.Mesh(new THREE.CylinderGeometry(radius, radius, length, 24), mat)
  mesh.position.copy(start).addScaledVector(delta, 0.5)
  mesh.quaternion.setFromUnitVectors(new THREE.Vector3(0, 1, 0), delta.normalize())
  group.add(mesh)
  return mesh
}

class CameraRig {
  constructor(element, camera, render) {
    this.element = element
    this.camera = camera
    this.render = render
    this.target = new THREE.Vector3(0, 0.65, 0)
    this.distance = 4.7
    this.theta = 0.72
    this.phi = 0.86
    this.drag = null

    this.pointerDown = this.pointerDown.bind(this)
    this.pointerMove = this.pointerMove.bind(this)
    this.pointerUp = this.pointerUp.bind(this)
    this.wheel = this.wheel.bind(this)

    element.addEventListener("pointerdown", this.pointerDown)
    element.addEventListener("wheel", this.wheel, { passive: false })
    element.addEventListener("contextmenu", (event) => event.preventDefault())
    this.updateCamera()
  }

  dispose() {
    this.element.removeEventListener("pointerdown", this.pointerDown)
    this.element.removeEventListener("wheel", this.wheel)
    window.removeEventListener("pointermove", this.pointerMove)
    window.removeEventListener("pointerup", this.pointerUp)
  }

  cameraBasis() {
    const forward = new THREE.Vector3()
    this.camera.getWorldDirection(forward)
    const right = new THREE.Vector3().crossVectors(forward, this.camera.up).normalize()
    const up = new THREE.Vector3().crossVectors(right, forward).normalize()
    return { right, up }
  }

  pointerDown(event) {
    this.drag = { x: event.clientX, y: event.clientY, button: event.button }
    this.element.setPointerCapture?.(event.pointerId)
    window.addEventListener("pointermove", this.pointerMove)
    window.addEventListener("pointerup", this.pointerUp)
  }

  pointerMove(event) {
    if (!this.drag) return
    const dx = event.clientX - this.drag.x
    const dy = event.clientY - this.drag.y
    this.drag.x = event.clientX
    this.drag.y = event.clientY
    if (this.drag.button === 2 || event.shiftKey) {
      const { right, up } = this.cameraBasis()
      this.target.addScaledVector(right, -dx * this.distance * 0.0015)
      this.target.addScaledVector(up, dy * this.distance * 0.0015)
    } else {
      this.theta -= dx * 0.006
      this.phi = clamp(this.phi - dy * 0.005, 0.18, Math.PI * 0.48)
    }
    this.updateCamera()
  }

  pointerUp(event) {
    if (this.element.hasPointerCapture?.(event.pointerId)) {
      this.element.releasePointerCapture(event.pointerId)
    }
    this.drag = null
    window.removeEventListener("pointermove", this.pointerMove)
    window.removeEventListener("pointerup", this.pointerUp)
  }

  wheel(event) {
    event.preventDefault()
    this.distance = clamp(this.distance * Math.exp(event.deltaY * 0.001), 1.2, 14)
    this.updateCamera()
  }

  updateCamera() {
    const sin = Math.sin(this.phi)
    this.camera.position.set(
      this.target.x + this.distance * sin * Math.sin(this.theta),
      this.target.y + this.distance * Math.cos(this.phi),
      this.target.z + this.distance * sin * Math.cos(this.theta),
    )
    this.camera.lookAt(this.target)
    this.render()
  }
}

export default class RobotSceneController {
  constructor(element) {
    this.element = element
    this.props = {}
    this.root = document.createElement("div")
    this.root.className = "robot-scene"

    const style = document.createElement("style")
    style.textContent = css
    this.root.appendChild(style)

    this.canvas = document.createElement("canvas")
    this.root.appendChild(this.canvas)

    this.panel = document.createElement("div")
    this.panel.className = "robot-scene__panel"
    this.root.appendChild(this.panel)

    this.scene = new THREE.Scene()
    this.scene.background = new THREE.Color("#07101a")
    this.camera = new THREE.PerspectiveCamera(52, 1, 0.05, 100)
    this.renderer = new THREE.WebGLRenderer({ canvas: this.canvas, antialias: true })
    this.renderer.setClearColor("#07101a", 1)
    this.renderer.outputColorSpace = THREE.SRGBColorSpace

    this.worldGroup = new THREE.Group()
    this.robotGroup = new THREE.Group()
    this.scene.add(this.worldGroup, this.robotGroup)

    this.resize = this.resize.bind(this)
    this.render = this.render.bind(this)
    this.cameraRig = new CameraRig(this.canvas, this.camera, this.render)
    this.buildWorld()
  }

  mount(props) {
    this.element.replaceChildren(this.root)
    window.addEventListener("resize", this.resize)
    this.setProps(props)
    requestAnimationFrame(this.resize)
  }

  setProps(props) {
    this.props = props ?? {}
    this.renderPanel()
    this.buildRobot()
    this.render()
  }

  dispose() {
    window.removeEventListener("resize", this.resize)
    this.cameraRig.dispose()
    disposeObject(this.scene)
    this.renderer.dispose()
    this.element.replaceChildren()
  }

  buildWorld() {
    const grid = new THREE.GridHelper(14, 14, 0x2a8dff, 0x243348)
    this.worldGroup.add(grid)
    this.worldGroup.add(new THREE.AmbientLight(0x9fb6d0, 0.75))

    const key = new THREE.DirectionalLight(0xffffff, 1.6)
    key.position.set(3.6, 5.2, 2.4)
    this.worldGroup.add(key)

    const fill = new THREE.DirectionalLight(0x55a7ff, 0.65)
    fill.position.set(-4.0, 2.5, -3.0)
    this.worldGroup.add(fill)

    const tableMat = material(0x9aa8b8, { metalness: 0.18, roughness: 0.58 })
    const darkMat = material(0x607086)
    const crateMat = material(0x2aa7ff)
    const markerMat = material(0x70e0b0)

    addBox(this.worldGroup, new THREE.Vector3(-0.25, 0.58, 0), new THREE.Vector3(2.4, 0.16, 0.7), tableMat)
    for (const x of [-1.25, 0.75]) {
      for (const z of [-0.26, 0.26]) {
        addBox(this.worldGroup, new THREE.Vector3(x, 0.28, z), new THREE.Vector3(0.07, 0.58, 0.07), tableMat)
      }
    }
    addBox(this.worldGroup, new THREE.Vector3(0.22, 0.83, 0), new THREE.Vector3(0.42, 0.34, 0.36), crateMat)
    addBox(this.worldGroup, new THREE.Vector3(2.1, 0.32, -0.7), new THREE.Vector3(0.82, 0.28, 0.42), darkMat)
    addSphere(this.worldGroup, new THREE.Vector3(2.1, 0.76, -0.7), 0.09, markerMat)
  }

  buildRobot() {
    disposeObject(this.robotGroup)
    this.robotGroup.clear()

    const base = this.props?.base ?? {}
    const translation = Array.isArray(base.translation) ? base.translation : [0, 0, 0]
    const values = jointValues(this.props)
    const root = new THREE.Vector3(
      -0.92 + finite(translation[0]),
      0.66 + finite(translation[2]),
      finite(translation[1]),
    )
    const yaw = clamp(values[0] ?? 0, -Math.PI, Math.PI)
    const a1 = -0.72 + clamp(values[1] ?? 0, -1.4, 1.4) * 0.35
    const a2 = 1.04 + clamp(values[2] ?? 0, -1.4, 1.4) * 0.35
    const a3 = -0.24 + clamp(values[3] ?? 0, -1.4, 1.4) * 0.25
    const dir = (angle, len) => new THREE.Vector3(
      Math.sin(yaw) * Math.cos(angle) * len,
      Math.sin(angle) * len,
      Math.cos(yaw) * Math.cos(angle) * len,
    )
    const p0 = root.clone().add(new THREE.Vector3(0, 0.34, 0))
    const p1 = p0.clone().add(dir(a1, 0.78))
    const p2 = p1.clone().add(dir(a1 + a2, 0.66))
    const p3 = p2.clone().add(dir(a1 + a2 + a3, 0.32))

    const jointMat = material(0x9fc5ff, { metalness: 0.22, roughness: 0.42 })
    const linkMat = material(0xdce7f3, { metalness: 0.16, roughness: 0.5 })
    const wristMat = material(0x9bd3ff)
    const toolMat = material(0x43d17a)
    const baseMat = material(0x1688d7)

    addSphere(this.robotGroup, root, 0.13, baseMat)
    addCylinderBetween(this.robotGroup, p0, p1, 0.045, linkMat)
    addCylinderBetween(this.robotGroup, p1, p2, 0.038, linkMat)
    addCylinderBetween(this.robotGroup, p2, p3, 0.026, wristMat)
    addSphere(this.robotGroup, p0, 0.13, jointMat)
    addSphere(this.robotGroup, p1, 0.105, jointMat)
    addSphere(this.robotGroup, p2, 0.085, jointMat)
    addSphere(this.robotGroup, p3, 0.055, toolMat)
  }

  renderPanel() {
    const robot = this.props?.robot
    const rows = [
      ["Camera", "Perspective"],
      ["Renderer", "Three.js"],
      ["Links", robot?.linkCount ?? "-"],
      ["Joints", robot?.jointCount ?? "-"],
      ["Movable", robot?.movableJoints?.length ?? "-"],
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
      left.textContent = label
      const right = document.createElement("span")
      right.textContent = String(value)
      row.append(left, right)
      this.panel.appendChild(row)
    }
  }

  resize() {
    const rect = this.element.getBoundingClientRect()
    if (rect.width < 1 || rect.height < 1) return
    const dpr = Math.min(window.devicePixelRatio || 1, 2)
    this.renderer.setPixelRatio(dpr)
    this.renderer.setSize(rect.width, rect.height, false)
    this.camera.aspect = rect.width / rect.height
    this.camera.updateProjectionMatrix()
    this.render()
  }

  render() {
    if (!this.renderer || !this.scene || !this.camera) return
    this.renderer.render(this.scene, this.camera)
  }
}
