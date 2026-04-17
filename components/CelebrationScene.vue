<template>
  <div ref="canvasContainer" class="hero-canvas-container">
    <div class="hero-gradient-overlay"></div>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, watch } from 'vue'
import * as THREE from 'three'
import { usePremiumAvatar } from '~/composables/usePremiumAvatar'

const props = defineProps({ faces: { type: Array, default: () => [] } })
const canvasContainer = ref(null)

let scene, camera, renderer, animationId
const characters = []
let confettiMesh = null
const confettiCount = 350
const dummy = new THREE.Object3D()

const { processAvatar } = usePremiumAvatar()

const initScene = () => {
  if (!canvasContainer.value) return

  canvasContainer.value.innerHTML = '<div class="hero-gradient-overlay"></div>'

  scene = new THREE.Scene()
  scene.background = new THREE.Color('#FAF9F6')
  scene.fog = new THREE.FogExp2('#FAF9F6', 0.05)

  camera = new THREE.PerspectiveCamera(35, window.innerWidth / 400, 0.1, 100)
  camera.position.set(0, 4, 16)
  camera.lookAt(0, 1.5, 0)

  renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true })
  renderer.setSize(window.innerWidth, 400)
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2))
  renderer.shadowMap.enabled = true
  renderer.shadowMap.type = THREE.PCFSoftShadowMap
  canvasContainer.value.appendChild(renderer.domElement)

  // Stage Floor
  const floorGeo = new THREE.PlaneGeometry(60, 60)
  const floorMat = new THREE.MeshStandardMaterial({ 
    color: '#FFFFFF', roughness: 0.8, metalness: 0.1 
  })
  const floor = new THREE.Mesh(floorGeo, floorMat)
  floor.rotation.x = -Math.PI / 2
  floor.receiveShadow = true
  scene.add(floor)

  // Lighting
  const ambientLight = new THREE.AmbientLight('#ffffff', 1.4)
  scene.add(ambientLight)

  const spotLight = new THREE.SpotLight('#FFF8E7', 150)
  spotLight.position.set(2, 12, 6)
  spotLight.angle = Math.PI / 4
  spotLight.penumbra = 0.8
  spotLight.castShadow = true
  spotLight.shadow.bias = -0.0005
  scene.add(spotLight)

  const fillLight = new THREE.PointLight('#E2E8F0', 80, 20)
  fillLight.position.set(-6, 5, -2)
  scene.add(fillLight)

  // High-fidelity Confetti (Instanced Mesh for tumbling paper effect)
  const confettiGeo = new THREE.PlaneGeometry(0.12, 0.12)
  const confettiMat = new THREE.MeshBasicMaterial({ color: 0xffffff, side: THREE.DoubleSide })
  confettiMesh = new THREE.InstancedMesh(confettiGeo, confettiMat, confettiCount)
  
  const colors = [new THREE.Color('#D4AF37'), new THREE.Color('#ED8936'), new THREE.Color('#48BB78'), new THREE.Color('#4299E1'), new THREE.Color('#F56565')]
  const confettiData = []
  
  for(let i=0; i<confettiCount; i++) {
    const x = (Math.random() - 0.5) * 20
    const y = Math.random() * 12 + 2
    const z = (Math.random() - 0.5) * 10 - 2
    
    dummy.position.set(x, y, z)
    dummy.rotation.set(Math.random()*Math.PI, Math.random()*Math.PI, Math.random()*Math.PI)
    dummy.updateMatrix()
    confettiMesh.setMatrixAt(i, dummy.matrix)
    confettiMesh.setColorAt(i, colors[Math.floor(Math.random() * colors.length)])
    
    confettiData.push({
      x, y, z,
      rx: Math.random() * 0.1,
      ry: Math.random() * 0.1,
      speed: Math.random() * 0.05 + 0.02
    })
  }
  scene.add(confettiMesh)

  const textureLoader = new THREE.TextureLoader()

  const createCharacter = (faceUrl, index) => {
    const group = new THREE.Group()

    // Soft 3D bodies
    const bodyGeo = new THREE.CapsuleGeometry(0.35, 0.5, 4, 16)
    const colorOpts = ['#F6E05E', '#9AE6B4', '#FBD38D', '#E2E8F0', '#BEE3F8']
    const bodyMat = new THREE.MeshStandardMaterial({ color: colorOpts[index % colorOpts.length], roughness: 0.8 })
    const body = new THREE.Mesh(bodyGeo, bodyMat)
    body.position.y = 0.85
    body.castShadow = true
    group.add(body)

    // Billboard Sprite-like Face
    // AlphaTest is implemented to automatically discard bounding box pixels, removing clipping bugs.
    // DepthWrite is safely restored to true so depth sorting works properly with the Confetti.
    const headGeo = new THREE.PlaneGeometry(1.2, 1.2)
    const headMat = new THREE.MeshStandardMaterial({
      color: 0xffffff,
      side: THREE.DoubleSide,
      transparent: true,
      alphaTest: 0.1,
      depthWrite: true,
      roughness: 0.6
    })
    
    const head = new THREE.Mesh(headGeo, headMat)
    head.position.y = 1.7
    head.castShadow = true
    group.add(head)

    // Fetch and apply the PremiumAvatar cropped/masked base64 texture
    if (faceUrl) {
      processAvatar(faceUrl).then(base64 => {
        textureLoader.load(base64, (tex) => {
          tex.colorSpace = THREE.SRGBColorSpace
          headMat.map = tex
          headMat.needsUpdate = true
        })
      })
    }

    if (index === 1) {
      const giftGroup = new THREE.Group()
      
      const boxMat = new THREE.MeshStandardMaterial({ color: '#FC8181', roughness: 0.4 })
      const box = new THREE.Mesh(new THREE.BoxGeometry(0.5, 0.5, 0.5), boxMat)
      box.castShadow = true
      giftGroup.add(box)
      
      const ribbonMat = new THREE.MeshStandardMaterial({ color: '#FAF089', metalness: 0.6, roughness: 0.2 })
      const r1 = new THREE.Mesh(new THREE.BoxGeometry(0.52, 0.05, 0.52), ribbonMat)
      const r2 = new THREE.Mesh(new THREE.BoxGeometry(0.52, 0.52, 0.05), ribbonMat)
      const r3 = new THREE.Mesh(new THREE.BoxGeometry(0.05, 0.52, 0.52), ribbonMat)
      giftGroup.add(r1, r2, r3)
      
      const bow = new THREE.Mesh(new THREE.TorusKnotGeometry(0.08, 0.02, 64, 8), ribbonMat)
      bow.position.y = 0.28
      giftGroup.add(bow)
      
      giftGroup.position.set(0, 0.8, 0.45)
      group.add(giftGroup)
    }

    const targetPos = new THREE.Vector3()
    if (index === 0) {
      targetPos.set(0, 0, 2.5)
    } else {
      const angle = ((index - 1) / 3) * Math.PI + Math.PI/4
      targetPos.set(Math.cos(angle) * 3, 0, Math.sin(angle) * 1.5 + 2.5)
    }
    
    group.position.set(targetPos.x + (Math.random()-0.5)*10, 0, targetPos.z - 6)

    scene.add(group)
    return { 
      mesh: group, 
      headMesh: head,
      targetPos,
      isBirthdayPerson: index === 0,
      timeOffset: Math.random() * 100 
    }
  }

  const facesToUse = props.faces.length > 0 ? props.faces : Array(5).fill(null)
  facesToUse.slice(0, 5).forEach((face, i) => {
    characters.push(createCharacter(face, i))
  })

  const onWindowResize = () => {
    if (!camera || !renderer) return
    camera.aspect = window.innerWidth / 400
    camera.updateProjectionMatrix()
    renderer.setSize(window.innerWidth, 400)
  }
  window.addEventListener('resize', onWindowResize)

  const clock = new THREE.Clock()

  const animate = () => {
    animationId = requestAnimationFrame(animate)
    const t = clock.getElapsedTime()
    
    if (confettiMesh) {
      for(let i=0; i<confettiCount; i++) {
        const d = confettiData[i]
        d.y -= d.speed
        d.x += Math.sin(t + i) * 0.01
        if(d.y < 0) d.y = 12
        
        dummy.position.set(d.x, d.y, d.z)
        dummy.rotation.x += d.rx
        dummy.rotation.y += d.ry
        dummy.updateMatrix()
        confettiMesh.setMatrixAt(i, dummy.matrix)
      }
      confettiMesh.instanceMatrix.needsUpdate = true
    }

    characters.forEach((char) => {
      char.mesh.position.lerp(char.targetPos, 0.03)
      char.headMesh.lookAt(camera.position)

      if (char.isBirthdayPerson) {
        char.mesh.position.y = Math.abs(Math.sin(t * 4)) * 0.6
        char.mesh.rotation.y = Math.sin(t * 2) * 0.3
      } else {
        char.mesh.position.y = Math.abs(Math.sin(t * 6 + char.timeOffset)) * 0.1
        char.mesh.lookAt(0, char.mesh.position.y, 2.5)
      }
    })

    camera.position.x = Math.sin(t * 0.15) * 2
    camera.lookAt(0, 1.5, 0)

    renderer.render(scene, camera)
  }
  animate()
}

onMounted(() => {
  setTimeout(initScene, 100)
})

watch(() => props.faces, () => {
  if (props.faces.length > 0) {
    if (animationId) cancelAnimationFrame(animationId)
    characters.length = 0
    initScene()
  }
}, { deep: true })

onBeforeUnmount(() => {
  if (animationId) cancelAnimationFrame(animationId)
  if (renderer) renderer.dispose()
  window.removeEventListener('resize', () => {})
})
</script>

<style scoped>
.hero-canvas-container {
  width: 100%;
  height: 400px;
  position: absolute;
  top: 0;
  left: 0;
  z-index: 0;
  overflow: hidden;
  background-color: var(--bg-color);
}

.hero-gradient-overlay {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  height: 150px;
  background: linear-gradient(to bottom, rgba(250, 249, 246, 0) 0%, var(--bg-color) 100%);
  pointer-events: none;
  z-index: 1;
}
</style>