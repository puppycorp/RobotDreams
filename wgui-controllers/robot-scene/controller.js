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
    width: min(230px, calc(100% - 28px));
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

const gltfDataCache = new Map()

const clamp = (value, min, max) => Math.max(min, Math.min(max, value))

const finite = (value, fallback = 0) =>
  typeof value === "number" && Number.isFinite(value) ? value : fallback

const isObject = (value) => value !== null && typeof value === "object" && !Array.isArray(value)

const propValue = (prop) => {
  if (!isObject(prop)) return undefined
  switch (prop.type) {
    case "number":
      return finite(prop.value)
    case "bool":
      return Boolean(prop.value)
    case "string":
      return String(prop.value ?? "")
    case "vec3":
      return new THREE.Vector3(finite(prop.x), finite(prop.y), finite(prop.z))
    case "color":
      return new THREE.Color(
        clamp(finite(prop.r) / 255, 0, 1),
        clamp(finite(prop.g) / 255, 0, 1),
        clamp(finite(prop.b) / 255, 0, 1),
      )
    default:
      return undefined
  }
}

const nodeProps = (node) => {
  const props = new Map()
  for (const prop of Array.isArray(node?.props) ? node.props : []) {
    props.set(prop.key, propValue(prop.value))
  }
  return props
}

const propByKey = (node, key) => nodeProps(node).get(key)

const material = (color, options = {}) =>
  new THREE.MeshStandardMaterial({
    color,
    metalness: options.metalness ?? 0.08,
    roughness: options.roughness ?? 0.72,
    side: THREE.DoubleSide,
    vertexColors: options.vertexColors ?? false,
  })

const disposeObject = (object) => {
  object.traverse((child) => {
    if (child.geometry && !child.geometry.userData?.shared) {
      child.geometry.dispose?.()
    }
    if (Array.isArray(child.material)) {
      child.material.forEach((item) => item.dispose?.())
    } else {
      child.material?.dispose?.()
    }
  })
}

const dataUriBuffer = (uri) => {
  const comma = uri.indexOf(",")
  if (comma < 0) throw new Error("invalid data URI")
  const meta = uri.slice(5, comma)
  const payload = uri.slice(comma + 1)
  if (meta.includes(";base64")) {
    const binary = atob(payload)
    const bytes = new Uint8Array(binary.length)
    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index)
    }
    return bytes.buffer
  }
  return new TextEncoder().encode(decodeURIComponent(payload)).buffer
}

const fetchArrayBuffer = async (src) => {
  if (src.startsWith("data:")) return dataUriBuffer(src)
  const response = await fetch(src)
  if (!response.ok) throw new Error(`HTTP ${response.status}`)
  return response.arrayBuffer()
}

const textFromBuffer = (buffer, byteOffset = 0, byteLength = buffer.byteLength - byteOffset) =>
  new TextDecoder().decode(new Uint8Array(buffer, byteOffset, byteLength))

const isGlbBuffer = (buffer) => {
  if (buffer.byteLength < 12) return false
  return new DataView(buffer).getUint32(0, true) === 0x46546c67
}

const parseGlb = (buffer) => {
  const view = new DataView(buffer)
  const version = view.getUint32(4, true)
  if (version !== 2) throw new Error(`unsupported GLB version ${version}`)

  let json = null
  const buffers = []
  let offset = 12
  while (offset + 8 <= buffer.byteLength) {
    const chunkLength = view.getUint32(offset, true)
    const chunkType = view.getUint32(offset + 4, true)
    offset += 8
    if (chunkType === 0x4e4f534a) {
      json = JSON.parse(textFromBuffer(buffer, offset, chunkLength))
    } else if (chunkType === 0x004e4942) {
      buffers.push(buffer.slice(offset, offset + chunkLength))
    }
    offset += chunkLength
  }

  if (!json) throw new Error("GLB missing JSON chunk")
  return { json, buffers }
}

const resolveBufferUri = (uri, src) => new URL(uri, new URL(src, window.location.href)).href

const loadGltfData = async (src) => {
  const sourceBuffer = await fetchArrayBuffer(src)
  if (isGlbBuffer(sourceBuffer)) {
    const data = parseGlb(sourceBuffer)
    data.geometryCache = new Map()
    return data
  }

  const json = JSON.parse(textFromBuffer(sourceBuffer))
  const buffers = []
  for (const bufferDef of json.buffers ?? []) {
    if (!bufferDef.uri) {
      buffers.push(sourceBuffer)
      continue
    }
    const bufferSrc = bufferDef.uri.startsWith("data:")
      ? bufferDef.uri
      : resolveBufferUri(bufferDef.uri, src)
    buffers.push(await fetchArrayBuffer(bufferSrc))
  }
  return { json, buffers, geometryCache: new Map() }
}

const gltfData = (src) => {
  if (!gltfDataCache.has(src)) {
    gltfDataCache.set(src, loadGltfData(src))
  }
  return gltfDataCache.get(src)
}

const componentArray = (componentType) => {
  switch (componentType) {
    case 5120:
      return Int8Array
    case 5121:
      return Uint8Array
    case 5122:
      return Int16Array
    case 5123:
      return Uint16Array
    case 5124:
      return Int32Array
    case 5125:
      return Uint32Array
    case 5126:
      return Float32Array
    default:
      throw new Error(`unsupported GLTF component type ${componentType}`)
  }
}

const accessorItemSize = (type) => {
  switch (type) {
    case "SCALAR":
      return 1
    case "VEC2":
      return 2
    case "VEC3":
      return 3
    case "VEC4":
      return 4
    case "MAT4":
      return 16
    default:
      throw new Error(`unsupported GLTF accessor type ${type}`)
  }
}

const readAccessor = (data, accessorIndex) => {
  const accessor = data.json.accessors?.[accessorIndex]
  if (!accessor) throw new Error(`missing GLTF accessor ${accessorIndex}`)
  const bufferView = data.json.bufferViews?.[accessor.bufferView]
  if (!bufferView) throw new Error(`missing GLTF bufferView ${accessor.bufferView}`)
  const buffer = data.buffers[bufferView.buffer]
  if (!buffer) throw new Error(`missing GLTF buffer ${bufferView.buffer}`)

  const ArrayType = componentArray(accessor.componentType)
  const itemSize = accessorItemSize(accessor.type)
  const byteOffset = (bufferView.byteOffset ?? 0) + (accessor.byteOffset ?? 0)
  const byteStride = bufferView.byteStride ?? ArrayType.BYTES_PER_ELEMENT * itemSize
  const length = accessor.count * itemSize

  if (byteStride === ArrayType.BYTES_PER_ELEMENT * itemSize) {
    return new THREE.BufferAttribute(
      new ArrayType(buffer, byteOffset, length),
      itemSize,
      accessor.normalized ?? false,
    )
  }

  const source = new DataView(buffer)
  const out = new ArrayType(length)
  const reader = `get${ArrayType.name.replace("Array", "")}`
  const littleEndian = ArrayType.BYTES_PER_ELEMENT > 1
  for (let row = 0; row < accessor.count; row += 1) {
    for (let col = 0; col < itemSize; col += 1) {
      out[row * itemSize + col] = source[reader](
        byteOffset + row * byteStride + col * ArrayType.BYTES_PER_ELEMENT,
        littleEndian,
      )
    }
  }
  return new THREE.BufferAttribute(out, itemSize, accessor.normalized ?? false)
}

const primitiveGeometry = (data, primitive, cacheKey) => {
  if (cacheKey && data.geometryCache?.has(cacheKey)) {
    return data.geometryCache.get(cacheKey)
  }

  const geometry = new THREE.BufferGeometry()
  const attributes = primitive.attributes ?? {}
  const names = [
    ["POSITION", "position"],
    ["NORMAL", "normal"],
    ["TEXCOORD_0", "uv"],
    ["COLOR_0", "color"],
  ]
  for (const [gltfName, threeName] of names) {
    if (attributes[gltfName] !== undefined) {
      geometry.setAttribute(threeName, readAccessor(data, attributes[gltfName]))
    }
  }
  if (primitive.indices !== undefined) {
    geometry.setIndex(readAccessor(data, primitive.indices))
  }
  if (!geometry.getAttribute("normal")) {
    geometry.computeVertexNormals()
  }
  geometry.computeBoundingSphere()
  geometry.userData.shared = true
  if (cacheKey) data.geometryCache?.set(cacheKey, geometry)
  return geometry
}

const createGltfMaterial = (data, materialIndex, vertexColors) => {
  const materialDef = data.json.materials?.[materialIndex]
  const pbr = materialDef?.pbrMetallicRoughness ?? {}
  const base = pbr.baseColorFactor ?? [0.75, 0.78, 0.82, 1]
  return material(new THREE.Color(base[0], base[1], base[2]), {
    metalness: finite(pbr.metallicFactor, 0.08),
    roughness: finite(pbr.roughnessFactor, 0.72),
    vertexColors,
  })
}

const applyGltfNodeTransform = (object, nodeDef) => {
  if (Array.isArray(nodeDef.matrix) && nodeDef.matrix.length === 16) {
    const matrix = new THREE.Matrix4().fromArray(nodeDef.matrix)
    matrix.decompose(object.position, object.quaternion, object.scale)
  }
  if (Array.isArray(nodeDef.translation)) {
    object.position.fromArray(nodeDef.translation)
  }
  if (Array.isArray(nodeDef.rotation)) {
    object.quaternion.fromArray(nodeDef.rotation)
  }
  if (Array.isArray(nodeDef.scale)) {
    object.scale.fromArray(nodeDef.scale)
  }
  if (typeof nodeDef.name === "string") {
    object.name = nodeDef.name
  }
}

const buildGltfNode = (data, nodeIndex, overrideMaterial) => {
  const nodeDef = data.json.nodes?.[nodeIndex]
  const group = new THREE.Group()
  if (!nodeDef) return group

  applyGltfNodeTransform(group, nodeDef)
  const meshDef = data.json.meshes?.[nodeDef.mesh]
  for (const [primitiveIndex, primitive] of (meshDef?.primitives ?? []).entries()) {
    if (primitive.mode !== undefined && primitive.mode !== 4) continue
    const geometry = primitiveGeometry(data, primitive, `${nodeDef.mesh}:${primitiveIndex}`)
    const hasVertexColors = Boolean(geometry.getAttribute("color"))
    const meshMaterial = overrideMaterial
      ? overrideMaterial.clone()
      : createGltfMaterial(data, primitive.material, hasVertexColors)
    meshMaterial.side = THREE.DoubleSide
    const mesh = new THREE.Mesh(geometry, meshMaterial)
    group.add(mesh)
  }

  for (const childIndex of nodeDef.children ?? []) {
    group.add(buildGltfNode(data, childIndex, overrideMaterial))
  }
  return group
}

const buildGltfScene = (data, overrideMaterial) => {
  const group = new THREE.Group()
  const sceneDef = data.json.scenes?.[data.json.scene ?? 0]
  for (const nodeIndex of sceneDef?.nodes ?? []) {
    group.add(buildGltfNode(data, nodeIndex, overrideMaterial))
  }
  return group
}

const loadGltfScene = async (src, overrideMaterial) => buildGltfScene(await gltfData(src), overrideMaterial)

const createMaterialFromNode = (node) => {
  const props = nodeProps(node)
  return material(props.get("color") ?? new THREE.Color(0xdce7f3), {
    metalness: props.get("metalness") ?? 0.12,
    roughness: props.get("roughness") ?? 0.68,
  })
}

const createGeometryFromNode = (node) => {
  const props = nodeProps(node)
  switch (node?.kind) {
    case "boxGeometry":
      return new THREE.BoxGeometry(
        props.get("width") ?? 1,
        props.get("height") ?? 1,
        props.get("depth") ?? 1,
      )
    case "sphereGeometry":
      return new THREE.SphereGeometry(
        props.get("radius") ?? 1,
        props.get("widthSegments") ?? 32,
        props.get("heightSegments") ?? 16,
      )
    case "cylinderGeometry":
      return new THREE.CylinderGeometry(
        props.get("radiusTop") ?? 1,
        props.get("radiusBottom") ?? 1,
        props.get("height") ?? 1,
        props.get("radialSegments") ?? 24,
      )
    default:
      return new THREE.BoxGeometry(0.02, 0.02, 0.02)
  }
}

const applyObjectProps = (object, node) => {
  const props = nodeProps(node)
  if (props.get("position")?.isVector3) object.position.copy(props.get("position"))
  if (props.get("rotation")?.isVector3) {
    const rotation = props.get("rotation")
    object.rotation.set(rotation.x, rotation.y, rotation.z)
  }
  if (typeof props.get("rotationOrder") === "string" && object.rotation) {
    object.rotation.order = props.get("rotationOrder")
  }
  if (props.get("scale")?.isVector3) object.scale.copy(props.get("scale"))
  if (typeof props.get("name") === "string") object.name = props.get("name")
  if (typeof props.get("includeInFit") === "boolean") {
    object.userData.excludeFromFit = !props.get("includeInFit")
  } else {
    delete object.userData.excludeFromFit
  }
}

const updateObjectProps = (object, node) => {
  if (!object) return
  applyObjectProps(object, node)
}

const sceneStructureSignature = (node) => {
  if (!isObject(node)) return "null"
  const src = node.kind === "stlGeometry" ? propByKey(node, "src") ?? "" : ""
  const children = (node.children ?? []).map(sceneStructureSignature).join(",")
  return `${node.id}:${node.kind}:${src}[${children}]`
}

const vectorFromArray = (value) => {
  if (!Array.isArray(value) || value.length < 3) return null
  return new THREE.Vector3(finite(value[0]), finite(value[1]), finite(value[2]))
}

const axisAngleFromArray = (value) => {
  if (!Array.isArray(value) || value.length < 4) return null
  const axis = new THREE.Vector3(finite(value[0]), finite(value[1]), finite(value[2]))
  if (axis.lengthSq() < 0.000001) axis.set(0, 0, 1)
  axis.normalize()
  return { axis, angle: finite(value[3]) }
}

const expandFitBox = (object, box) => {
  if (!object || object.userData?.excludeFromFit) return false
  let included = false
  if (object.isMesh || object.isLine || object.isPoints || object.isSprite) {
    box.expandByObject(object)
    included = true
  }
  for (const child of object.children ?? []) {
    included = expandFitBox(child, box) || included
  }
  return included
}

const applyTransformValue = (object, transform) => {
  if (!object || !isObject(transform)) return
  const position = vectorFromArray(transform.position)
  const rotation = vectorFromArray(transform.rotation)
  const scale = vectorFromArray(transform.scale)
  const axisAngle = axisAngleFromArray(transform.axisAngle)

  if (position) object.position.copy(position)
  if (axisAngle) {
    object.quaternion.setFromAxisAngle(axisAngle.axis, axisAngle.angle)
  } else if (rotation) {
    object.rotation.set(rotation.x, rotation.y, rotation.z)
  }
  if (typeof transform.rotationOrder === "string" && object.rotation) {
    object.rotation.order = transform.rotationOrder
  }
  if (scale) object.scale.copy(scale)
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

const jointValues = (props) => {
  const joints = Array.isArray(props?.robot?.movableJoints) ? props.robot.movableJoints : []
  return joints.map((joint) => finite(joint.value, 0))
}

const servoTicksToJointValue = (ticks, joint) => {
  const lower = finite(joint?.lower, -Math.PI)
  const upper = finite(joint?.upper, Math.PI)
  const t = clamp(finite(ticks, 0), 0, 4095) / 4095
  return lower + (upper - lower) * t
}

class CameraRig {
  constructor(element, camera, render) {
    this.element = element
    this.camera = camera
    this.render = render
    this.target = new THREE.Vector3(0, 0.18, 0)
    this.distance = 1.1
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
    this.distance = clamp(this.distance * Math.exp(event.deltaY * 0.001), 0.08, 14)
    this.updateCamera()
  }

  frameBox(box) {
    if (box.isEmpty()) return
    const center = box.getCenter(new THREE.Vector3())
    const size = box.getSize(new THREE.Vector3()).length()
    this.target.copy(center)
    this.distance = clamp(size * 1.8, 0.18, 8)
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
    this.camera = new THREE.PerspectiveCamera(52, 1, 0.005, 100)
    this.renderer = new THREE.WebGLRenderer({ canvas: this.canvas, antialias: true })
    this.renderer.setClearColor("#07101a", 1)
    this.renderer.outputColorSpace = THREE.SRGBColorSpace

    this.worldGroup = new THREE.Group()
    this.robotGroup = new THREE.Group()
    this.scene.add(this.worldGroup, this.robotGroup)

    this.fitAfterBuild = true
    this.modelKey = null
    this.nodeObjects = new Map()
    this.pendingMeshLoads = 0
    this.robotGeneration = 0
    this.sceneStructureKey = null
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
    const nextProps = props ?? {}
    const nextModelKey = nextProps?.model?.src ?? nextProps?.model?.path ?? ""
    const nextStaticScene = nextProps?.staticScene ?? nextProps?.scene
    const nextSceneStructureKey = typeof nextProps?.staticSceneKey === "string"
      ? nextProps.staticSceneKey
      : isObject(nextStaticScene)
      ? sceneStructureSignature(nextStaticScene)
      : null
    const canUpdateTransforms =
      nextModelKey === this.modelKey &&
      nextSceneStructureKey !== null &&
      nextSceneStructureKey === this.sceneStructureKey &&
      this.nodeObjects.size > 0

    this.props = nextProps
    this.fitAfterBuild = nextModelKey !== this.modelKey
    this.modelKey = nextModelKey
    this.sceneStructureKey = nextSceneStructureKey
    this.renderPanel()

    if (canUpdateTransforms) {
      if (!this.applyDynamicState(this.props.dynamicState)) {
        this.updateThreeNode(nextStaticScene)
      }
      this.render()
      return
    }

    this.buildRobot()
    this.render()
  }

  onData(name, payload) {
    if (name !== "servoSnapshots") {
      return
    }

    const liveProps = isObject(payload?.robotSceneProps) ? payload.robotSceneProps : null
    if (liveProps) {
      this.setProps({
        ...this.props,
        ...liveProps,
      })
      return
    }

    const snapshots = Array.isArray(payload?.snapshots) ? payload.snapshots : []
    const robot = this.props?.robot
    const joints = Array.isArray(robot?.movableJoints) ? robot.movableJoints : []
    if (!snapshots.length || !joints.length) {
      return
    }

    const movableJoints = joints.map((joint, index) => {
      const snapshot = snapshots[index]
      if (!snapshot) {
        return joint
      }
      return {
        ...joint,
        value: servoTicksToJointValue(snapshot.presentPosition, joint),
      }
    })

    this.setProps({
      ...this.props,
      robot: {
        ...robot,
        movableJoints,
      },
    })
  }

  dispose() {
    window.removeEventListener("resize", this.resize)
    this.cameraRig.dispose()
    disposeObject(this.scene)
    this.renderer.dispose()
    this.element.replaceChildren()
  }

  buildWorld() {
    const grid = new THREE.GridHelper(2.4, 12, 0x2a8dff, 0x243348)
    this.worldGroup.add(grid)
    this.worldGroup.add(new THREE.AmbientLight(0x9fb6d0, 0.75))

    const key = new THREE.DirectionalLight(0xffffff, 1.6)
    key.position.set(0.8, 1.2, 0.7)
    this.worldGroup.add(key)

    const fill = new THREE.DirectionalLight(0x55a7ff, 0.65)
    fill.position.set(-1.0, 0.8, -0.8)
    this.worldGroup.add(fill)
  }

  buildRobot() {
    this.robotGeneration += 1
    const generation = this.robotGeneration
    this.nodeObjects.clear()
    this.pendingMeshLoads = 0
    disposeObject(this.robotGroup)
    this.robotGroup.clear()

    const staticScene = this.props?.staticScene ?? this.props?.scene
    if (isObject(staticScene)) {
      const object = this.buildThreeNode(staticScene, generation)
      if (object) {
        this.robotGroup.add(object)
        this.applyDynamicState(this.props?.dynamicState)
        if (this.pendingMeshLoads === 0) this.fitRobotIfNeeded()
        this.render()
      }
      return
    }

    this.buildFallbackRobot()
  }

  buildThreeNode(node, generation) {
    switch (node?.kind) {
      case "group": {
        const group = new THREE.Group()
        applyObjectProps(group, node)
        this.nodeObjects.set(node.id, group)
        for (const child of node.children ?? []) {
          const childObject = this.buildThreeNode(child, generation)
          if (childObject) group.add(childObject)
        }
        return group
      }
      case "mesh":
        return this.buildMeshNode(node, generation)
      default:
        return null
    }
  }

  updateThreeNode(node) {
    if (!isObject(node)) return
    updateObjectProps(this.nodeObjects.get(node.id), node)
    for (const child of node.children ?? []) {
      this.updateThreeNode(child)
    }
  }

  applyDynamicState(dynamicState) {
    const transforms = dynamicState?.transforms
    if (!isObject(transforms)) return false
    for (const [id, transform] of Object.entries(transforms)) {
      applyTransformValue(this.nodeObjects.get(Number(id)), transform)
    }
    return true
  }

  buildMeshNode(node, generation) {
    const children = Array.isArray(node.children) ? node.children : []
    const geometryNode = children.find((child) => child?.kind?.endsWith("Geometry"))
    const materialNode = children.find((child) => child?.kind?.endsWith("Material"))
    const meshMaterial = materialNode ? createMaterialFromNode(materialNode) : material(0xdce7f3)
    const src = geometryNode?.kind === "stlGeometry" ? propByKey(geometryNode, "src") : null

    if (typeof src === "string" && /\.(glb|gltf)(\?|#|$)/i.test(src)) {
      const group = new THREE.Group()
      applyObjectProps(group, node)
      this.nodeObjects.set(node.id, group)
      this.pendingMeshLoads += 1
      loadGltfScene(src, meshMaterial)
        .then((object) => {
          if (this.robotGeneration !== generation) {
            disposeObject(object)
            return
          }
          group.add(object)
          this.render()
        })
        .catch((err) => console.warn(`Failed to load GLTF mesh from "${src}"`, err))
        .finally(() => {
          meshMaterial.dispose()
          if (this.robotGeneration !== generation) return
          this.pendingMeshLoads = Math.max(0, this.pendingMeshLoads - 1)
          if (this.pendingMeshLoads === 0) this.fitRobotIfNeeded()
          this.render()
        })
      return group
    }

    const mesh = new THREE.Mesh(createGeometryFromNode(geometryNode), meshMaterial)
    applyObjectProps(mesh, node)
    this.nodeObjects.set(node.id, mesh)
    return mesh
  }

  buildFallbackRobot() {
    const base = this.props?.base ?? {}
    const translation = Array.isArray(base.translation) ? base.translation : [0, 0, 0]
    const values = jointValues(this.props)
    const root = new THREE.Vector3(finite(translation[0]), 0.05 + finite(translation[2]), finite(translation[1]))
    const yaw = clamp(values[0] ?? 0, -Math.PI, Math.PI)
    const a1 = -0.72 + clamp(values[1] ?? 0, -1.4, 1.4) * 0.35
    const a2 = 1.04 + clamp(values[2] ?? 0, -1.4, 1.4) * 0.35
    const a3 = -0.24 + clamp(values[3] ?? 0, -1.4, 1.4) * 0.25
    const dir = (angle, len) => new THREE.Vector3(
      Math.sin(yaw) * Math.cos(angle) * len * 0.22,
      Math.sin(angle) * len * 0.22,
      Math.cos(yaw) * Math.cos(angle) * len * 0.22,
    )
    const p0 = root.clone().add(new THREE.Vector3(0, 0.08, 0))
    const p1 = p0.clone().add(dir(a1, 0.78))
    const p2 = p1.clone().add(dir(a1 + a2, 0.66))
    const p3 = p2.clone().add(dir(a1 + a2 + a3, 0.32))

    const jointMat = material(0x9fc5ff, { metalness: 0.22, roughness: 0.42 })
    const linkMat = material(0xdce7f3, { metalness: 0.16, roughness: 0.5 })
    const wristMat = material(0x9bd3ff)
    const toolMat = material(0x43d17a)
    const baseMat = material(0x1688d7)

    addSphere(this.robotGroup, root, 0.035, baseMat)
    addCylinderBetween(this.robotGroup, p0, p1, 0.012, linkMat)
    addCylinderBetween(this.robotGroup, p1, p2, 0.01, linkMat)
    addCylinderBetween(this.robotGroup, p2, p3, 0.008, wristMat)
    addSphere(this.robotGroup, p0, 0.025, jointMat)
    addSphere(this.robotGroup, p1, 0.022, jointMat)
    addSphere(this.robotGroup, p2, 0.018, jointMat)
    addSphere(this.robotGroup, p3, 0.014, toolMat)
  }

  fitRobotIfNeeded() {
    if (!this.fitAfterBuild) return
    const box = new THREE.Box3()
    if (!expandFitBox(this.robotGroup, box) || box.isEmpty()) return
    this.fitAfterBuild = false
    this.cameraRig.frameBox(box)
  }

  renderPanel() {
    const robot = this.props?.robot
    const hasMeshes = isObject(this.props?.staticScene) || isObject(this.props?.scene)
    const rows = [
      ["Camera", "Perspective"],
      ["Renderer", "Three.js"],
      ["Meshes", hasMeshes ? "GLTF" : "Fallback"],
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
