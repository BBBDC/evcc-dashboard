#!/usr/bin/env python3
"""
Generates high-res icons for EVCC & Solar-Log Dashboard:
- apple-touch-icon.png (180x180 px for iOS Home Screen)
- favicon.svg (crisp vector for browser tabs)
- favicon.ico (multi-size ICO 16x16, 32x32, 48x48)
- favicon-32x32.png
- favicon-16x16.png
- icon-192.png (PWA)
- icon-512.png (PWA)
- manifest.json
"""

import math
from pathlib import Path
from PIL import Image, ImageDraw, ImageFilter

OUTPUT_DIR = Path("/home/admin/evcc-dashboard")

# 1. Clean scalable SVG
SVG_CONTENT = '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" width="100%" height="100%">
  <defs>
    <!-- Background Gradient -->
    <linearGradient id="bgGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#0f172a"/>
      <stop offset="50%" stop-color="#1e293b"/>
      <stop offset="100%" stop-color="#090d16"/>
    </linearGradient>

    <!-- Solar Ring Gradient -->
    <linearGradient id="solarGrad" x1="0%" y1="100%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#f59e0b"/>
      <stop offset="50%" stop-color="#fbbf24"/>
      <stop offset="100%" stop-color="#f97316"/>
    </linearGradient>

    <!-- Bolt Electric Gradient -->
    <linearGradient id="boltGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#38bdf8"/>
      <stop offset="50%" stop-color="#0ea5e9"/>
      <stop offset="100%" stop-color="#06b6d4"/>
    </linearGradient>

    <!-- Subtle Glow Filter -->
    <filter id="solarGlow" x="-20%" y="-20%" width="140%" height="140%">
      <feGaussianBlur stdDeviation="12" result="blur" />
      <feComposite in="SourceGraphic" in2="blur" operator="over" />
    </filter>
  </defs>

  <!-- Squircle Base / Background -->
  <rect x="16" y="16" width="480" height="480" rx="108" fill="url(#bgGrad)"/>
  <rect x="16" y="16" width="480" height="480" rx="108" fill="none" stroke="rgba(255,255,255,0.12)" stroke-width="4"/>

  <!-- Solar Corona / Rays -->
  <g filter="url(#solarGlow)" opacity="0.95">
    <!-- Top Arc / Solar Rays -->
    <circle cx="256" cy="256" r="150" fill="none" stroke="url(#solarGrad)" stroke-width="14" stroke-dasharray="24 16" stroke-linecap="round"/>
  </g>

  <!-- Glowing Sun Core / Orbit -->
  <circle cx="256" cy="256" r="115" fill="none" stroke="rgba(245, 158, 11, 0.25)" stroke-width="24"/>
  <circle cx="256" cy="256" r="88" fill="rgba(245, 158, 11, 0.08)"/>

  <!-- Dynamic Lightning Bolt (EV Charging Zap) -->
  <g>
    <!-- Lightning Bolt Drop Shadow / Glow -->
    <path d="M280 96 L164 268 L248 268 L220 416 L348 236 L264 236 Z" 
          fill="none" stroke="rgba(6, 182, 212, 0.4)" stroke-width="28" stroke-linejoin="round"/>
    
    <!-- Lightning Bolt Body -->
    <path d="M280 96 L164 268 L248 268 L220 416 L348 236 L264 236 Z" 
          fill="url(#boltGrad)" stroke="#ffffff" stroke-width="6" stroke-linejoin="round"/>

    <!-- Bolt Inner Highlight -->
    <path d="M272 120 L188 252 L250 252 L232 376 L320 248 L256 248 Z" 
          fill="rgba(255, 255, 255, 0.4)" opacity="0.6"/>
  </g>

  <!-- Decorative Solar Flare Dots -->
  <circle cx="150" cy="150" r="10" fill="#fbbf24" opacity="0.8"/>
  <circle cx="362" cy="150" r="10" fill="#fbbf24" opacity="0.8"/>
  <circle cx="390" cy="256" r="12" fill="#38bdf8" opacity="0.9"/>
  <circle cx="122" cy="256" r="12" fill="#f59e0b" opacity="0.9"/>
</svg>
'''


def draw_master_image(size=512) -> Image.Image:
    """Renders a high-resolution 512x512 master icon with anti-aliasing via 4x supersampling."""
    ss = 4  # supersample factor
    w = size * ss
    h = size * ss

    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # 1. Background Rounded Rectangle
    pad = 16 * ss
    rx = 108 * ss
    bg_box = [pad, pad, w - pad, h - pad]

    # Draw gradient-like background using concentric/layered shapes
    draw.rounded_rectangle(bg_box, radius=rx, fill=(15, 23, 42, 255))
    
    # Inner subtle glow
    inner_box = [pad + 12 * ss, pad + 12 * ss, w - pad - 12 * ss, h - pad - 12 * ss]
    draw.rounded_rectangle(inner_box, radius=rx - 12 * ss, fill=(24, 34, 56, 255))
    
    center_box = [pad + 36 * ss, pad + 36 * ss, w - pad - 36 * ss, h - pad - 36 * ss]
    draw.rounded_rectangle(center_box, radius=rx - 32 * ss, fill=(15, 23, 42, 255))

    # Border
    draw.rounded_rectangle(bg_box, radius=rx, outline=(255, 255, 255, 40), width=4 * ss)

    # 2. Solar Ring / Corona
    cx, cy = w // 2, h // 2
    solar_radius = 150 * ss
    solar_width = 14 * ss

    # Draw dashed solar arc segments
    num_rays = 16
    for i in range(num_rays):
        angle_deg = i * (360.0 / num_rays)
        rad = math.radians(angle_deg)
        r1 = solar_radius - 8 * ss
        r2 = solar_radius + 8 * ss
        x1 = cx + r1 * math.cos(rad)
        y1 = cy + r1 * math.sin(rad)
        x2 = cx + r2 * math.cos(rad)
        y2 = cy + r2 * math.sin(rad)
        # Gold / orange color
        color = (245, 158, 11, 230) if i % 2 == 0 else (251, 191, 36, 230)
        draw.line([x1, y1, x2, y2], fill=color, width=solar_width, joint="curve")

    # Inner soft glowing ring
    draw.ellipse([cx - 100 * ss, cy - 100 * ss, cx + 100 * ss, cy + 100 * ss],
                 outline=(245, 158, 11, 60), width=18 * ss)
    draw.ellipse([cx - 75 * ss, cy - 75 * ss, cx + 75 * ss, cy + 75 * ss],
                 fill=(245, 158, 11, 20))

    # 3. High-Voltage Lightning Bolt (EV Charging Zap)
    # Coordinates scaled to 512 -> multiplied by ss
    bolt_coords = [
        (280 * ss, 96 * ss),
        (164 * ss, 268 * ss),
        (248 * ss, 268 * ss),
        (220 * ss, 416 * ss),
        (348 * ss, 236 * ss),
        (264 * ss, 236 * ss),
    ]

    # Outer cyan glow for bolt
    for glow_w, glow_alpha in [(24 * ss, 50), (14 * ss, 100), (6 * ss, 180)]:
        draw.polygon(bolt_coords, fill=(14, 165, 233, glow_alpha), outline=(56, 189, 248, glow_alpha))

    # Main bolt body (Cyan to Sky Blue)
    draw.polygon(bolt_coords, fill=(14, 165, 233, 255), outline=(255, 255, 255, 230))

    # Inner bright core
    inner_bolt = [
        (274 * ss, 118 * ss),
        (186 * ss, 256 * ss),
        (250 * ss, 256 * ss),
        (230 * ss, 380 * ss),
        (326 * ss, 244 * ss),
        (260 * ss, 244 * ss),
    ]
    draw.polygon(inner_bolt, fill=(224, 242, 254, 200))

    # Downsample with Lanczos filter for razor-sharp antialiasing
    final_img = img.resize((size, size), Image.Resampling.LANCZOS)
    return final_img


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Save SVG
    svg_path = OUTPUT_DIR / "favicon.svg"
    svg_path.write_text(SVG_CONTENT.strip(), encoding="utf-8")
    print(f"✅ Geschrieben: {svg_path}")

    # 2. Master Image (512x512)
    master_512 = draw_master_image(512)
    icon_512_path = OUTPUT_DIR / "icon-512.png"
    master_512.save(icon_512_path, "PNG", optimize=True)
    print(f"✅ Geschrieben: {icon_512_path}")

    # 3. PWA 192x192
    icon_192 = master_512.resize((192, 192), Image.Resampling.LANCZOS)
    icon_192_path = OUTPUT_DIR / "icon-192.png"
    icon_192.save(icon_192_path, "PNG", optimize=True)
    print(f"✅ Geschrieben: {icon_192_path}")

    # 4. Apple Touch Icon 180x180 (iPhone Homescreen App)
    apple_180 = master_512.resize((180, 180), Image.Resampling.LANCZOS)
    apple_path = OUTPUT_DIR / "apple-touch-icon.png"
    apple_180.save(apple_path, "PNG", optimize=True)
    print(f"✅ Geschrieben: {apple_path}")

    # 5. Favicons (32x32 and 16x16)
    fav_32 = master_512.resize((32, 32), Image.Resampling.LANCZOS)
    fav_32_path = OUTPUT_DIR / "favicon-32x32.png"
    fav_32.save(fav_32_path, "PNG", optimize=True)
    print(f"✅ Geschrieben: {fav_32_path}")

    fav_16 = master_512.resize((16, 16), Image.Resampling.LANCZOS)
    fav_16_path = OUTPUT_DIR / "favicon-16x16.png"
    fav_16.save(fav_16_path, "PNG", optimize=True)
    print(f"✅ Geschrieben: {fav_16_path}")

    # 6. Multi-resolution favicon.ico (16, 32, 48)
    fav_48 = master_512.resize((48, 48), Image.Resampling.LANCZOS)
    ico_path = OUTPUT_DIR / "favicon.ico"
    fav_48.save(ico_path, format="ICO", sizes=[(16, 16), (32, 32), (48, 48)])
    print(f"✅ Geschrieben: {ico_path}")

    # 7. Web App Manifest
    manifest_content = '''{
  "name": "EVCC & Solar-Log Zentrale",
  "short_name": "EVCC",
  "description": "Echtzeit-Dashboard für EVCC, Solar-Log & intelligentes PV-Laden",
  "start_url": "./",
  "display": "standalone",
  "background_color": "#0f172a",
  "theme_color": "#f59e0b",
  "orientation": "portrait-primary",
  "icons": [
    {
      "src": "icon-192.png",
      "sizes": "192x192",
      "type": "image/png",
      "purpose": "any maskable"
    },
    {
      "src": "icon-512.png",
      "sizes": "512x512",
      "type": "image/png",
      "purpose": "any maskable"
    },
    {
      "src": "apple-touch-icon.png",
      "sizes": "180x180",
      "type": "image/png"
    }
  ]
}
'''
    manifest_path = OUTPUT_DIR / "manifest.json"
    manifest_path.write_text(manifest_content.strip(), encoding="utf-8")
    print(f"✅ Geschrieben: {manifest_path}")


if __name__ == "__main__":
    main()
