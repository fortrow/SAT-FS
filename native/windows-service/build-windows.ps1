param(
    [string] $Output = ""
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Resolve-Path (Join-Path $ScriptDir "..\..")
if ([string]::IsNullOrWhiteSpace($Output)) {
    $Output = Join-Path $RootDir "target\native\cxnfs-service.exe"
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

& $Csc /nologo /optimize+ /platform:x64 /target:exe `
    "/out:$Output" `
    "/reference:System.ServiceProcess.dll" `
    (Join-Path $ScriptDir "cxnfs_service.cs")

Write-Output "Built $Output"
