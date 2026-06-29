$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$docs = Join-Path $root "docs"
New-Item -ItemType Directory -Force -Path $docs | Out-Null

$svgPath = Join-Path $docs "backup_dashboard_architecture.svg"
$pptxPath = Join-Path $docs "backup_dashboard_architecture.pptx"

$svg = @'
<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="900" viewBox="0 0 1600 900">
  <defs>
    <style>
      .title { font: 700 42px "Segoe UI", Arial, sans-serif; fill: #172033; }
      .subtitle { font: 400 20px "Segoe UI", Arial, sans-serif; fill: #526070; }
      .box-title { font: 700 23px "Segoe UI", Arial, sans-serif; fill: #172033; }
      .box-text { font: 400 17px "Segoe UI", Arial, sans-serif; fill: #526070; }
      .small { font: 400 15px "Segoe UI", Arial, sans-serif; fill: #526070; }
      .lane { fill: #f5f7fb; stroke: #d6deea; stroke-width: 2; }
      .box { fill: #ffffff; stroke: #b9c6d8; stroke-width: 2; rx: 8; }
      .accent-blue { fill: #dfefff; stroke: #6aa6e8; }
      .accent-green { fill: #e1f6ea; stroke: #62ba83; }
      .accent-gold { fill: #fff2cf; stroke: #d9a92f; }
      .accent-red { fill: #ffe6e1; stroke: #db715f; }
      .arrow { stroke: #526070; stroke-width: 3; fill: none; marker-end: url(#arrowhead); }
      .dashed { stroke-dasharray: 8 8; }
      .tag { fill: #172033; }
      .tag-text { font: 700 14px "Segoe UI", Arial, sans-serif; fill: #ffffff; }
    </style>
    <marker id="arrowhead" markerWidth="12" markerHeight="8" refX="11" refY="4" orient="auto">
      <polygon points="0 0, 12 4, 0 8" fill="#526070" />
    </marker>
  </defs>

  <rect width="1600" height="900" fill="#fbfcfe"/>
  <text x="70" y="70" class="title">Backup Dashboard Collector Flow</text>
  <text x="72" y="105" class="subtitle">FastAPI loads YAML config, schedules SNMP/REST collectors, then publishes visibility data to Elasticsearch and Prometheus.</text>

  <rect x="55" y="145" width="320" height="640" class="lane" rx="12"/>
  <rect x="415" y="145" width="750" height="640" class="lane" rx="12"/>
  <rect x="1205" y="145" width="340" height="640" class="lane" rx="12"/>

  <text x="80" y="185" class="box-title">1. Runtime</text>
  <text x="440" y="185" class="box-title">2. Collection Pipeline</text>
  <text x="1230" y="185" class="box-title">3. Visibility</text>

  <rect x="90" y="230" width="250" height="110" class="box accent-blue" rx="8"/>
  <text x="115" y="265" class="box-title">Kubernetes Pod</text>
  <text x="115" y="294" class="box-text">Deployment starts container</text>
  <text x="115" y="321" class="box-text">APP_CONFIG=/app/config/collector.yaml</text>

  <rect x="90" y="410" width="250" height="110" class="box" rx="8"/>
  <text x="115" y="445" class="box-title">FastAPI App</text>
  <text x="115" y="474" class="box-text">lifespan initializes service</text>
  <text x="115" y="501" class="box-text">routes expose status APIs</text>

  <rect x="455" y="230" width="260" height="110" class="box accent-gold" rx="8"/>
  <text x="480" y="265" class="box-title">Config Loader</text>
  <text x="480" y="294" class="box-text">YAML to AppConfig</text>
  <text x="480" y="321" class="box-text">TO_BE_FILLED becomes skip reason</text>

  <rect x="820" y="230" width="260" height="110" class="box accent-blue" rx="8"/>
  <text x="845" y="265" class="box-title">Collector Factory</text>
  <text x="845" y="294" class="box-text">protocol=snmp -> SnmpCollector</text>
  <text x="845" y="321" class="box-text">protocol=rest -> RestCollector</text>

  <rect x="635" y="410" width="270" height="120" class="box accent-green" rx="8"/>
  <text x="660" y="445" class="box-title">Collector Scheduler</text>
  <text x="660" y="474" class="box-text">async task per collector</text>
  <text x="660" y="501" class="box-text">runs at configured second</text>

  <rect x="455" y="610" width="260" height="120" class="box" rx="8"/>
  <text x="480" y="645" class="box-title">SNMP Collectors</text>
  <text x="480" y="674" class="box-text">DXi, DD at :00</text>
  <text x="480" y="701" class="box-text">i6000 at :15</text>

  <rect x="820" y="610" width="260" height="120" class="box" rx="8"/>
  <text x="845" y="645" class="box-title">REST Collectors</text>
  <text x="845" y="674" class="box-text">Networker, ZFS at :30</text>
  <text x="845" y="701" class="box-text">JSON or text payload</text>

  <rect x="1235" y="230" width="275" height="115" class="box accent-green" rx="8"/>
  <text x="1260" y="265" class="box-title">Elasticsearch Writer</text>
  <text x="1260" y="294" class="box-text">bulk write CollectionResult</text>
  <text x="1260" y="321" class="box-text">backup-dashboard-YYYY.MM.DD</text>

  <rect x="1235" y="415" width="275" height="115" class="box accent-blue" rx="8"/>
  <text x="1260" y="450" class="box-title">FastAPI Endpoints</text>
  <text x="1260" y="479" class="box-text">/collectors, /run-once</text>
  <text x="1260" y="506" class="box-text">/healthz, /readyz</text>

  <rect x="1235" y="615" width="275" height="115" class="box accent-red" rx="8"/>
  <text x="1260" y="650" class="box-title">Prometheus Metrics</text>
  <text x="1260" y="679" class="box-text">collection_total</text>
  <text x="1260" y="706" class="box-text">duration, skipped, write_total</text>

  <path d="M215 340 L215 410" class="arrow"/>
  <path d="M340 465 L455 285" class="arrow"/>
  <path d="M715 285 L820 285" class="arrow"/>
  <path d="M950 340 L805 410" class="arrow"/>
  <path d="M715 530 L610 610" class="arrow"/>
  <path d="M905 530 L950 610" class="arrow"/>
  <path d="M1080 670 L1235 288" class="arrow"/>
  <path d="M905 470 L1235 472" class="arrow dashed"/>
  <path d="M770 530 L1235 672" class="arrow dashed"/>

  <rect x="618" y="768" width="385" height="54" class="tag" rx="8"/>
  <text x="643" y="802" class="tag-text">Result contract: success, error, and skipped all become CollectionResult documents</text>
</svg>
'@
Set-Content -Path $svgPath -Value $svg -Encoding UTF8

Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

function New-ZipTextEntry {
    param(
        [System.IO.Compression.ZipArchive] $Archive,
        [string] $Path,
        [string] $Content
    )
    $entry = $Archive.CreateEntry($Path)
    $stream = $entry.Open()
    $writer = New-Object System.IO.StreamWriter($stream, [System.Text.UTF8Encoding]::new($false))
    $writer.Write($Content)
    $writer.Dispose()
    $stream.Dispose()
}

function Emu { param([double] $Inch) [int64] [Math]::Round($Inch * 914400) }
function Escape-Xml { param([string] $Text) [System.Security.SecurityElement]::Escape($Text) }

function PptTextBox {
    param(
        [int] $Id,
        [string] $Name,
        [double] $X,
        [double] $Y,
        [double] $W,
        [double] $H,
        [string[]] $Lines,
        [string] $Fill = "FFFFFF",
        [string] $Line = "B9C6D8",
        [int] $TitleSize = 1800,
        [int] $BodySize = 1200,
        [string] $TextColor = "172033"
    )
    $paragraphs = @()
    for ($i = 0; $i -lt $Lines.Length; $i++) {
        $size = if ($i -eq 0) { $TitleSize } else { $BodySize }
        $bold = if ($i -eq 0) { '<a:b/>' } else { '' }
        $paragraphs += "<a:p><a:r><a:rPr lang=`"ko-KR`" sz=`"$size`">$bold<a:solidFill><a:srgbClr val=`"$TextColor`"/></a:solidFill></a:rPr><a:t>$(Escape-Xml $Lines[$i])</a:t></a:r></a:p>"
    }
    @"
<p:sp>
  <p:nvSpPr><p:cNvPr id="$Id" name="$Name"/><p:cNvSpPr/><p:nvPr/></p:nvSpPr>
  <p:spPr>
    <a:xfrm><a:off x="$(Emu $X)" y="$(Emu $Y)"/><a:ext cx="$(Emu $W)" cy="$(Emu $H)"/></a:xfrm>
    <a:prstGeom prst="roundRect"><a:avLst/></a:prstGeom>
    <a:solidFill><a:srgbClr val="$Fill"/></a:solidFill>
    <a:ln w="19050"><a:solidFill><a:srgbClr val="$Line"/></a:solidFill></a:ln>
  </p:spPr>
  <p:txBody><a:bodyPr lIns="182880" tIns="137160" rIns="137160" bIns="91440"/><a:lstStyle/>$($paragraphs -join '')</p:txBody>
</p:sp>
"@
}

function PptConnector {
    param(
        [int] $Id,
        [double] $X,
        [double] $Y,
        [double] $W,
        [double] $H,
        [bool] $Dashed = $false
    )
    $dash = if ($Dashed) { '<a:prstDash val="dash"/>' } else { '' }
    @"
<p:cxnSp>
  <p:nvCxnSpPr><p:cNvPr id="$Id" name="Arrow $Id"/><p:cNvCxnSpPr/><p:nvPr/></p:nvCxnSpPr>
  <p:spPr>
    <a:xfrm><a:off x="$(Emu $X)" y="$(Emu $Y)"/><a:ext cx="$(Emu $W)" cy="$(Emu $H)"/></a:xfrm>
    <a:prstGeom prst="line"><a:avLst/></a:prstGeom>
    <a:ln w="25400"><a:solidFill><a:srgbClr val="526070"/></a:solidFill>$dash<a:tailEnd type="triangle"/></a:ln>
  </p:spPr>
</p:cxnSp>
"@
}

$shapes = @()
$shapes += PptTextBox 10 "Title" 0.45 0.25 12.2 0.55 @("Backup Dashboard Collector Flow") "FBFCFE" "FBFCFE" 2800 1400
$shapes += PptTextBox 11 "Subtitle" 0.45 0.82 12.2 0.42 @("FastAPI loads YAML config, schedules SNMP/REST collectors, then publishes visibility data to Elasticsearch and Prometheus.") "FBFCFE" "FBFCFE" 950 950
$shapes += PptTextBox 20 "Kubernetes Pod" 0.65 1.55 2.15 0.9 @("Kubernetes Pod", "Deployment starts container", "APP_CONFIG points to YAML") "DFEFFF" "6AA6E8"
$shapes += PptTextBox 21 "FastAPI App" 0.65 3.35 2.15 0.9 @("FastAPI App", "lifespan initializes service", "status APIs exposed") "FFFFFF" "B9C6D8"
$shapes += PptTextBox 30 "Config Loader" 3.5 1.55 2.25 0.9 @("Config Loader", "YAML to AppConfig", "TO_BE_FILLED creates skip reason") "FFF2CF" "D9A92F"
$shapes += PptTextBox 31 "Collector Factory" 6.6 1.55 2.25 0.9 @("Collector Factory", "snmp -> SnmpCollector", "rest -> RestCollector") "DFEFFF" "6AA6E8"
$shapes += PptTextBox 32 "Scheduler" 5.05 3.35 2.35 1.0 @("Collector Scheduler", "async task per collector", "runs at configured second") "E1F6EA" "62BA83"
$shapes += PptTextBox 33 "SNMP Collectors" 3.5 5.1 2.25 0.95 @("SNMP Collectors", "DXi, DD at :00", "i6000 at :15") "FFFFFF" "B9C6D8"
$shapes += PptTextBox 34 "REST Collectors" 6.6 5.1 2.25 0.95 @("REST Collectors", "Networker, ZFS at :30", "JSON or text payload") "FFFFFF" "B9C6D8"
$shapes += PptTextBox 40 "Elasticsearch" 10.0 1.55 2.45 0.95 @("Elasticsearch Writer", "bulk write CollectionResult", "backup-dashboard-YYYY.MM.DD") "E1F6EA" "62BA83"
$shapes += PptTextBox 41 "Endpoints" 10.0 3.35 2.45 0.95 @("FastAPI Endpoints", "/collectors, /run-once", "/healthz, /readyz") "DFEFFF" "6AA6E8"
$shapes += PptTextBox 42 "Metrics" 10.0 5.1 2.45 0.95 @("Prometheus Metrics", "collection_total, duration", "skipped, write_total") "FFE6E1" "DB715F"
$shapes += PptTextBox 50 "Contract" 4.25 6.55 4.85 0.48 @("Result contract: success, error, and skipped all become CollectionResult documents") "172033" "172033" 850 850 "FFFFFF"
$shapes += PptConnector 60 1.72 2.45 0 0.9
$shapes += PptConnector 61 2.8 3.8 0.7 -1.8
$shapes += PptConnector 62 5.75 2.0 0.85 0
$shapes += PptConnector 63 7.72 2.45 -0.75 0.9
$shapes += PptConnector 64 5.6 4.35 -0.65 0.75
$shapes += PptConnector 65 6.8 4.35 0.65 0.75
$shapes += PptConnector 66 8.85 5.55 1.15 -3.55
$shapes += PptConnector 67 7.4 3.85 2.6 0 1
$shapes += PptConnector 68 6.2 4.35 3.8 1.25 1

$slideXml = @"
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<p:sld xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main">
  <p:cSld>
    <p:bg><p:bgPr><a:solidFill><a:srgbClr val="FBFCFE"/></a:solidFill></p:bgPr></p:bg>
    <p:spTree>
      <p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr>
      <p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/><a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr>
      $($shapes -join "`n")
    </p:spTree>
  </p:cSld>
  <p:clrMapOvr><a:masterClrMapping/></p:clrMapOvr>
</p:sld>
"@

$contentTypes = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/ppt/presentation.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml"/>
  <Override PartName="/ppt/slides/slide1.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.slide+xml"/>
  <Override PartName="/ppt/slideMasters/slideMaster1.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.slideMaster+xml"/>
  <Override PartName="/ppt/slideLayouts/slideLayout1.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.slideLayout+xml"/>
  <Override PartName="/ppt/theme/theme1.xml" ContentType="application/vnd.openxmlformats-officedocument.theme+xml"/>
</Types>
'@

$rootRels = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="ppt/presentation.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>
'@

$appXml = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>Codex</Application>
  <PresentationFormat>Widescreen</PresentationFormat>
  <Slides>1</Slides>
</Properties>
'@

$coreXml = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:title>Backup Dashboard Collector Architecture</dc:title>
  <dc:creator>Codex</dc:creator>
  <cp:lastModifiedBy>Codex</cp:lastModifiedBy>
</cp:coreProperties>
'@

$presentationXml = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<p:presentation xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main">
  <p:sldMasterIdLst><p:sldMasterId id="2147483648" r:id="rId1"/></p:sldMasterIdLst>
  <p:sldIdLst><p:sldId id="256" r:id="rId2"/></p:sldIdLst>
  <p:sldSz cx="12192000" cy="6858000" type="wide"/>
  <p:notesSz cx="6858000" cy="9144000"/>
</p:presentation>
'@

$presentationRels = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideMaster" Target="slideMasters/slideMaster1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide" Target="slides/slide1.xml"/>
</Relationships>
'@

$slideRels = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideLayout" Target="../slideLayouts/slideLayout1.xml"/>
</Relationships>
'@

$slideMasterXml = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<p:sldMaster xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main">
  <p:cSld><p:spTree><p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr><p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/><a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr></p:spTree></p:cSld>
  <p:clrMap bg1="lt1" tx1="dk1" bg2="lt2" tx2="dk2" accent1="accent1" accent2="accent2" accent3="accent3" accent4="accent4" accent5="accent5" accent6="accent6" hlink="hlink" folHlink="folHlink"/>
  <p:sldLayoutIdLst><p:sldLayoutId id="2147483649" r:id="rId1"/></p:sldLayoutIdLst>
</p:sldMaster>
'@

$slideMasterRels = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideLayout" Target="../slideLayouts/slideLayout1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme" Target="../theme/theme1.xml"/>
</Relationships>
'@

$slideLayoutXml = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<p:sldLayout xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" type="blank" preserve="1">
  <p:cSld name="Blank"><p:spTree><p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr><p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/><a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr></p:spTree></p:cSld>
  <p:clrMapOvr><a:masterClrMapping/></p:clrMapOvr>
</p:sldLayout>
'@

$slideLayoutRels = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideMaster" Target="../slideMasters/slideMaster1.xml"/>
</Relationships>
'@

$themeXml = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<a:theme xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" name="Codex Theme">
  <a:themeElements>
    <a:clrScheme name="Codex"><a:dk1><a:srgbClr val="172033"/></a:dk1><a:lt1><a:srgbClr val="FFFFFF"/></a:lt1><a:dk2><a:srgbClr val="526070"/></a:dk2><a:lt2><a:srgbClr val="FBFCFE"/></a:lt2><a:accent1><a:srgbClr val="6AA6E8"/></a:accent1><a:accent2><a:srgbClr val="62BA83"/></a:accent2><a:accent3><a:srgbClr val="D9A92F"/></a:accent3><a:accent4><a:srgbClr val="DB715F"/></a:accent4><a:accent5><a:srgbClr val="B9C6D8"/></a:accent5><a:accent6><a:srgbClr val="526070"/></a:accent6><a:hlink><a:srgbClr val="0563C1"/></a:hlink><a:folHlink><a:srgbClr val="954F72"/></a:folHlink></a:clrScheme>
    <a:fontScheme name="Codex"><a:majorFont><a:latin typeface="Segoe UI"/></a:majorFont><a:minorFont><a:latin typeface="Segoe UI"/></a:minorFont></a:fontScheme>
    <a:fmtScheme name="Codex"><a:fillStyleLst><a:solidFill><a:schemeClr val="phClr"/></a:solidFill></a:fillStyleLst><a:lnStyleLst><a:ln w="9525"><a:solidFill><a:schemeClr val="phClr"/></a:solidFill></a:ln></a:lnStyleLst><a:effectStyleLst><a:effectStyle><a:effectLst/></a:effectStyle></a:effectStyleLst><a:bgFillStyleLst><a:solidFill><a:schemeClr val="phClr"/></a:solidFill></a:bgFillStyleLst></a:fmtScheme>
  </a:themeElements>
</a:theme>
'@

if (Test-Path $pptxPath) {
    Remove-Item -LiteralPath $pptxPath -Force
}

$fileStream = [System.IO.File]::Open($pptxPath, [System.IO.FileMode]::CreateNew)
$archive = New-Object System.IO.Compression.ZipArchive($fileStream, [System.IO.Compression.ZipArchiveMode]::Create)
try {
    New-ZipTextEntry $archive "[Content_Types].xml" $contentTypes
    New-ZipTextEntry $archive "_rels/.rels" $rootRels
    New-ZipTextEntry $archive "docProps/app.xml" $appXml
    New-ZipTextEntry $archive "docProps/core.xml" $coreXml
    New-ZipTextEntry $archive "ppt/presentation.xml" $presentationXml
    New-ZipTextEntry $archive "ppt/_rels/presentation.xml.rels" $presentationRels
    New-ZipTextEntry $archive "ppt/slides/slide1.xml" $slideXml
    New-ZipTextEntry $archive "ppt/slides/_rels/slide1.xml.rels" $slideRels
    New-ZipTextEntry $archive "ppt/slideMasters/slideMaster1.xml" $slideMasterXml
    New-ZipTextEntry $archive "ppt/slideMasters/_rels/slideMaster1.xml.rels" $slideMasterRels
    New-ZipTextEntry $archive "ppt/slideLayouts/slideLayout1.xml" $slideLayoutXml
    New-ZipTextEntry $archive "ppt/slideLayouts/_rels/slideLayout1.xml.rels" $slideLayoutRels
    New-ZipTextEntry $archive "ppt/theme/theme1.xml" $themeXml
}
finally {
    $archive.Dispose()
    $fileStream.Dispose()
}

Write-Host "Created $svgPath"
Write-Host "Created $pptxPath"
