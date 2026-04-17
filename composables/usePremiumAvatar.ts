import { ref } from 'vue'

const avatarCache = new Map<string, string>()

export const usePremiumAvatar = () => {
  const processAvatar = async (url: string): Promise<string> => {
    if (!url) return '/main.png'
    if (avatarCache.has(url)) return avatarCache.get(url)!

    console.log(`[DEBUG-HHB] Vision API - Requesting analysis for URL: ${url}`);

    try {
      const response = await fetch('https://vision.casitaapps.com/api/detect', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url })
      }).catch(() => null)

      let cropBox = { x: 0, y: 0, width: 1, height: 1 } 
      let maskBase64 = null

      if (response && response.ok) {
        const data = await response.json()
        if (data.cropBox) cropBox = data.cropBox
        else if (data.faces && data.faces.length > 0) cropBox = data.faces[0].box || data.faces[0]

        // Extract segmentation mask
        maskBase64 = data.mask || data.alphaMask || data.segmentationMask || null
      }

      // Load original image safely
      const img = new Image()
      img.crossOrigin = 'anonymous'
      await new Promise((resolve, reject) => {
        img.onload = resolve
        img.onerror = reject
        img.src = url
      })

      // Try loading mask image if provided
      let maskImg: HTMLImageElement | null = null
      if (maskBase64) {
        const maskSrc = (maskBase64.startsWith('http') || maskBase64.startsWith('data:')) 
          ? maskBase64 
          : `data:image/png;base64,${maskBase64}`
        
        maskImg = new Image()
        maskImg.crossOrigin = 'anonymous'
        await new Promise((resolve, reject) => {
          maskImg!.onload = resolve
          maskImg!.onerror = reject
          maskImg!.src = maskSrc
        }).catch((e) => {
          console.warn(`[DEBUG-HHB] Vision API - Failed to decode mask image buffer`, e)
          maskImg = null
        })
      }

      const canvas = document.createElement('canvas')
      // Use willReadFrequently to optimize the pixel extraction process below
      const ctx = canvas.getContext('2d', { willReadFrequently: true })
      if (!ctx) return url

      canvas.width = 256
      canvas.height = 256

      // Math fix to entirely prevent stretching while applying paddings:
      const pad = 0.3
      let pxX = cropBox.x, pxY = cropBox.y, pxW = cropBox.width, pxH = cropBox.height;
      
      // If coordinates are normalized (0 to 1), map them to absolute pixels
      if (pxW <= 1.0 && pxH <= 1.0) {
        pxX *= img.width; pxY *= img.height; pxW *= img.width; pxH *= img.height;
      }

      const size = Math.max(pxW, pxH) * (1 + pad);
      const cx = pxX + pxW / 2;
      const cy = pxY + pxH / 2;

      const sx = cx - size / 2;
      const sy = cy - size / 2;

      // Transform context to handle cropping safely and purely without distortion
      ctx.clearRect(0, 0, canvas.width, canvas.height)
      ctx.save()
      const scale = canvas.width / size;
      ctx.scale(scale, scale);
      ctx.translate(-sx, -sy);
      ctx.drawImage(img, 0, 0);
      ctx.restore()

      // Step 2: Apply segmentation mask with robust alpha and luminance logic
      if (maskImg) {
        const maskCanvas = document.createElement('canvas');
        maskCanvas.width = canvas.width;
        maskCanvas.height = canvas.height;
        const mCtx = maskCanvas.getContext('2d', { willReadFrequently: true });
        
        mCtx.save();
        if (maskImg.width === img.width && maskImg.height === img.height) {
          mCtx.scale(scale, scale);
          mCtx.translate(-sx, -sy);
          mCtx.drawImage(maskImg, 0, 0);
        } else {
          mCtx.scale(scale, scale);
          mCtx.translate(-sx, -sy);
          mCtx.drawImage(maskImg, pxX, pxY, pxW, pxH);
        }
        mCtx.restore();

        const imgData = ctx.getImageData(0, 0, canvas.width, canvas.height);
        const maskData = mCtx.getImageData(0, 0, canvas.width, canvas.height);

        // Determine if the mask is purely grayscale (JPEG) or true Alpha (PNG)
        let hasTransparency = false;
        for (let j = 3; j < maskData.data.length; j += 4) {
          if (maskData.data[j] < 255) {
            hasTransparency = true;
            break;
          }
        }

        // Evaluate pixel by pixel to erase the background perfectly
        for (let i = 0; i < imgData.data.length; i += 4) {
          if (!hasTransparency) {
            // Mask is opaque (e.g. B&W JPEG), use Luminance (Red channel) as Alpha
            imgData.data[i+3] = maskData.data[i];
          } else {
            // Mask possesses transparent pixels, safely use intrinsic Alpha channel
            imgData.data[i+3] = maskData.data[i+3];
          }
        }
        ctx.putImageData(imgData, 0, 0);

      } else {
        // Fallback for when no mask is returned by Vision
        ctx.globalCompositeOperation = 'destination-in'
        ctx.beginPath()
        ctx.arc(canvas.width / 2, canvas.height / 2, canvas.width / 2, 0, Math.PI * 2, true)
        ctx.fill()
        ctx.globalCompositeOperation = 'source-over'
      }

      const base64 = canvas.toDataURL('image/png', 1.0)
      avatarCache.set(url, base64)
      return base64

    } catch (e) {
      console.warn('[DEBUG-HHB] PremiumAvatar pipeline failed for', url, 'returning original. Error:', e)
      avatarCache.set(url, url)
      return url
    }
  }

  return { processAvatar }
}