param(
  [string]$OutIco = "static/app_icon.ico",
  [string]$OutPng = "static/app_icon_256.png",
  [string]$SvgSource = "static/app_icon_source.svg"
)

if (Test-Path $SvgSource) {
  $magick = Get-Command magick -ErrorAction SilentlyContinue
  if ($magick) {
    & $magick.Source $SvgSource -background none -resize 256x256 $OutPng
    if ($LASTEXITCODE -ne 0) { throw "Failed generating PNG from SVG via ImageMagick." }
    & $magick.Source $SvgSource -background none -define icon:auto-resize=256,128,64,48,32,16 $OutIco
    if ($LASTEXITCODE -ne 0) { throw "Failed generating ICO from SVG via ImageMagick." }
    Write-Host "Generated icon files from SVG:" $OutIco "and" $OutPng
    exit 0
  }
}

Add-Type -AssemblyName System.Drawing
$size = 256
$bmp = New-Object System.Drawing.Bitmap $size, $size
$g = [System.Drawing.Graphics]::FromImage($bmp)
$g.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias

$bgRect = New-Object System.Drawing.Rectangle 0,0,$size,$size
$bgBrush = New-Object System.Drawing.Drawing2D.LinearGradientBrush(
  (New-Object System.Drawing.Point 0,0),
  (New-Object System.Drawing.Point $size,$size),
  ([System.Drawing.Color]::FromArgb(255,52,78,65)),
  ([System.Drawing.Color]::FromArgb(255,88,129,87))
)
$g.FillRectangle($bgBrush, $bgRect)

$ringPen = New-Object System.Drawing.Pen ([System.Drawing.Color]::FromArgb(220,190,255,220)), 10
$g.DrawEllipse($ringPen, 18, 18, 220, 220)

$innerBrush = New-Object System.Drawing.SolidBrush ([System.Drawing.Color]::FromArgb(205,245,255,247))
$g.FillRectangle($innerBrush, 48, 58, 160, 140)

$font = New-Object System.Drawing.Font("Segoe UI", 66, [System.Drawing.FontStyle]::Bold, [System.Drawing.GraphicsUnit]::Pixel)
$txtBrush = New-Object System.Drawing.SolidBrush ([System.Drawing.Color]::FromArgb(255,44,77,55))
$fmt = New-Object System.Drawing.StringFormat
$fmt.Alignment = [System.Drawing.StringAlignment]::Center
$fmt.LineAlignment = [System.Drawing.StringAlignment]::Center
$g.DrawString("LE", $font, $txtBrush, (New-Object System.Drawing.RectangleF 48, 58, 160, 140), $fmt)

$shine = New-Object System.Drawing.SolidBrush ([System.Drawing.Color]::FromArgb(56,255,255,255))
$g.FillEllipse($shine, 50, 32, 160, 72)

if (-not (Test-Path (Split-Path $OutPng -Parent))) {
  New-Item -ItemType Directory -Path (Split-Path $OutPng -Parent) -Force | Out-Null
}
$bmp.Save($OutPng, [System.Drawing.Imaging.ImageFormat]::Png)

$hIcon = $bmp.GetHicon()
$icon = [System.Drawing.Icon]::FromHandle($hIcon)
$iconStream = [System.IO.File]::Open($OutIco, [System.IO.FileMode]::Create)
$icon.Save($iconStream)
$iconStream.Close()

[System.Runtime.InteropServices.Marshal]::Release($hIcon) | Out-Null
$g.Dispose(); $bgBrush.Dispose(); $ringPen.Dispose(); $innerBrush.Dispose(); $font.Dispose(); $txtBrush.Dispose(); $shine.Dispose(); $bmp.Dispose()

Write-Host "Generated fallback icon files:" $OutIco "and" $OutPng
