param(
    [string] $Output = "",
    [string] $WinFspRoot = "C:\Program Files (x86)\WinFsp"
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Resolve-Path (Join-Path $ScriptDir "..\..")
if ([string]::IsNullOrWhiteSpace($Output)) {
    $Output = Join-Path $RootDir "target\native\cxnfs-winfsp.exe"
}

$WinFspDll = Get-ChildItem -Path $WinFspRoot -Recurse -File -Filter "winfsp-msil.dll" |
    Sort-Object FullName -Descending |
    Select-Object -First 1
if ($null -eq $WinFspDll) {
    throw "missing winfsp-msil.dll under $WinFspRoot; install WinFsp first"
}

$Csc = "$env:WINDIR\Microsoft.NET\Framework64\v4.0.30319\csc.exe"
if (!(Test-Path $Csc)) {
    $Csc = "$env:WINDIR\Microsoft.NET\Framework\v4.0.30319\csc.exe"
}
if (!(Test-Path $Csc)) {
    throw "missing .NET Framework csc.exe"
}

$OutDir = Split-Path -Parent $Output
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

& $Csc /nologo /optimize+ /unsafe+ /platform:x64 /target:exe `
    "/out:$Output" `
    "/reference:$($WinFspDll.FullName)" `
    (Join-Path $ScriptDir "cxnfs_winfsp.cs")

Copy-Item -Force $WinFspDll.FullName (Join-Path $OutDir "winfsp-msil.dll")
Write-Output "Built $Output"
