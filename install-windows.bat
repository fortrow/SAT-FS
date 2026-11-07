@echo off
setlocal EnableExtensions

set "SCRIPT_DIR=%~dp0"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

set "JAR_PATH="
set "LIB_DIR="
set "JFLUX_CORE="
set "WINFSP_ADAPTER="
set "WINFSP_MSIL="
set "SERVICE_WRAPPER="
set "INSTALL_DIR=%ProgramFiles%\CXNFS"
set "CONFIG_DIR=%ProgramData%\CXNFS"
set "DATASET_DIR=%SystemDrive%\cxnfs\datasets"
set "OUTPUT_DIR=%SystemDrive%\cxnfs\outputs"

:parse
if "%~1"=="" goto after_parse
if "%~1"=="--jar" set "JAR_PATH=%~2" & shift & shift & goto parse
if "%~1"=="--lib-dir" set "LIB_DIR=%~2" & shift & shift & goto parse
if "%~1"=="--jflux-core" set "JFLUX_CORE=%~2" & shift & shift & goto parse
if "%~1"=="--winfsp-adapter" set "WINFSP_ADAPTER=%~2" & shift & shift & goto parse
if "%~1"=="--winfsp-msil" set "WINFSP_MSIL=%~2" & shift & shift & goto parse
if "%~1"=="--service-wrapper" set "SERVICE_WRAPPER=%~2" & shift & shift & goto parse
if "%~1"=="--install-dir" set "INSTALL_DIR=%~2" & shift & shift & goto parse
if "%~1"=="--config-dir" set "CONFIG_DIR=%~2" & shift & shift & goto parse
if "%~1"=="--dataset-dir" set "DATASET_DIR=%~2" & shift & shift & goto parse
if "%~1"=="--output-dir" set "OUTPUT_DIR=%~2" & shift & shift & goto parse
if "%~1"=="--help" goto usage
if "%~1"=="-h" goto usage
echo install-windows.bat: unknown option: %~1 1>&2
exit /b 1

:usage
echo Usage: install-windows.bat [options]
echo.
echo Installs pre-built CXNFS jars and packaging assets for Windows, and adds
echo the install directory to the machine PATH.
echo.
echo Options:
echo   --jar ^<path^>          Built CXNFS jar. Defaults to target\cxnfs-0.1.0.jar.
echo   --lib-dir ^<path^>      Directory containing runtime dependency jars.
echo                          Defaults to target\runtime-libs.
echo   --jflux-core ^<path^>   JFlux core jar. Defaults to lib\jflux-1.2.1-core.jar or %%USERPROFILE%%\STRATO\jflux\target\jflux-1.2.1-core.jar.
echo   --winfsp-adapter ^<path^> Pre-built cxnfs-winfsp.exe. Defaults to target\native\cxnfs-winfsp.exe if present.
echo   --winfsp-msil ^<path^>  WinFsp managed DLL. Defaults to target\native\winfsp-msil.dll if present.
echo   --service-wrapper ^<path^> Pre-built cxnfs-service.exe. Defaults to target\native\cxnfs-service.exe if present.
echo   --install-dir ^<path^>  Runtime install directory. Default: %%ProgramFiles%%\CXNFS.
echo   --config-dir ^<path^>   Config directory. Default: %%ProgramData%%\CXNFS.
echo   --dataset-dir ^<path^>  Example dataset jail. Default: %%SystemDrive%%\cxnfs\datasets.
echo   --output-dir ^<path^>   Example output jail. Default: %%SystemDrive%%\cxnfs\outputs.
exit /b 0

:after_parse
if "%JAR_PATH%"=="" set "JAR_PATH=%SCRIPT_DIR%\target\cxnfs-0.1.0.jar"
if "%LIB_DIR%"=="" set "LIB_DIR=%SCRIPT_DIR%\target\runtime-libs"
if "%JFLUX_CORE%"=="" (
  if exist "%SCRIPT_DIR%\lib\jflux-1.2.1-core.jar" (
    set "JFLUX_CORE=%SCRIPT_DIR%\lib\jflux-1.2.1-core.jar"
  ) else (
    set "JFLUX_CORE=%USERPROFILE%\STRATO\jflux\target\jflux-1.2.1-core.jar"
  )
)
if "%WINFSP_ADAPTER%"=="" set "WINFSP_ADAPTER=%SCRIPT_DIR%\target\native\cxnfs-winfsp.exe"
if "%WINFSP_MSIL%"=="" set "WINFSP_MSIL=%SCRIPT_DIR%\target\native\winfsp-msil.dll"
if "%SERVICE_WRAPPER%"=="" set "SERVICE_WRAPPER=%SCRIPT_DIR%\target\native\cxnfs-service.exe"

if not exist "%JAR_PATH%" (
  echo install-windows.bat: missing CXNFS jar: %JAR_PATH% 1>&2
  exit /b 1
)
if not exist "%LIB_DIR%\" (
  echo install-windows.bat: missing runtime lib directory: %LIB_DIR% 1>&2
  exit /b 1
)
if not exist "%SCRIPT_DIR%\packaging\bin\cxnfs.cmd" (
  echo install-windows.bat: missing packaging\bin\cxnfs.cmd 1>&2
  exit /b 1
)

if not exist "%INSTALL_DIR%" mkdir "%INSTALL_DIR%"
if not exist "%INSTALL_DIR%\lib" mkdir "%INSTALL_DIR%\lib"
if not exist "%CONFIG_DIR%" mkdir "%CONFIG_DIR%"
if not exist "%CONFIG_DIR%\repository" mkdir "%CONFIG_DIR%\repository"
if not exist "%CONFIG_DIR%\payload-staging" mkdir "%CONFIG_DIR%\payload-staging"
if not exist "%CONFIG_DIR%\run" mkdir "%CONFIG_DIR%\run"
if not exist "%CONFIG_DIR%\logs" mkdir "%CONFIG_DIR%\logs"
if not exist "%DATASET_DIR%" mkdir "%DATASET_DIR%"
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

copy /Y "%JAR_PATH%" "%INSTALL_DIR%\cxnfs.jar" >nul
copy /Y "%LIB_DIR%\*.jar" "%INSTALL_DIR%\lib\" >nul
if not exist "%INSTALL_DIR%\lib\jflux-*-core.jar" (
  if not exist "%JFLUX_CORE%" (
    echo install-windows.bat: missing JFlux core jar. Pass --jflux-core ^<path^>. 1>&2
    exit /b 1
  )
  copy /Y "%JFLUX_CORE%" "%INSTALL_DIR%\lib\" >nul
)
if exist "%SCRIPT_DIR%\lib\os-platform-finder*.jar" (
  copy /Y "%SCRIPT_DIR%\lib\os-platform-finder*.jar" "%INSTALL_DIR%\lib\" >nul
)
copy /Y "%SCRIPT_DIR%\packaging\bin\cxnfs.cmd" "%INSTALL_DIR%\cxnfs.cmd" >nul
if exist "%SCRIPT_DIR%\packaging\bin\sat-fs.cmd" (
  copy /Y "%SCRIPT_DIR%\packaging\bin\sat-fs.cmd" "%INSTALL_DIR%\sat-fs.cmd" >nul
  copy /Y "%SCRIPT_DIR%\packaging\bin\sat-fs.cmd" "%INSTALL_DIR%\satfs.cmd" >nul
)
if exist "%WINFSP_ADAPTER%" (
  copy /Y "%WINFSP_ADAPTER%" "%INSTALL_DIR%\cxnfs-winfsp.exe" >nul
)
if exist "%WINFSP_MSIL%" (
  copy /Y "%WINFSP_MSIL%" "%INSTALL_DIR%\winfsp-msil.dll" >nul
)
if exist "%SERVICE_WRAPPER%" (
  copy /Y "%SERVICE_WRAPPER%" "%INSTALL_DIR%\cxnfs-service.exe" >nul
  copy /Y "%SERVICE_WRAPPER%" "%CONFIG_DIR%\cxnfs-service.exe" >nul
)

call :add_to_machine_path "%INSTALL_DIR%"
if errorlevel 1 exit /b %ERRORLEVEL%

call :write_config "%CONFIG_DIR%\server.properties.example"
if not exist "%CONFIG_DIR%\server.properties" copy /Y "%CONFIG_DIR%\server.properties.example" "%CONFIG_DIR%\server.properties" >nul

del /F /Q "%CONFIG_DIR%\run\server.ready" "%CONFIG_DIR%\run\server.health" "%CONFIG_DIR%\run\server.stop" "%CONFIG_DIR%\run\server.lock" 2>nul
type nul > "%CONFIG_DIR%\logs\server.out.log"
type nul > "%CONFIG_DIR%\logs\server.err.log"

echo Installed CXNFS Windows files.
echo Jar: %INSTALL_DIR%\cxnfs.jar
echo Launcher: %INSTALL_DIR%\cxnfs.cmd
if exist "%INSTALL_DIR%\cxnfs-winfsp.exe" echo WinFsp adapter: %INSTALL_DIR%\cxnfs-winfsp.exe
if exist "%CONFIG_DIR%\cxnfs-service.exe" echo Service wrapper: %CONFIG_DIR%\cxnfs-service.exe
echo PATH: %INSTALL_DIR%
echo Config example: %CONFIG_DIR%\server.properties.example
echo Config: %CONFIG_DIR%\server.properties
echo Runtime markers are prepared under %CONFIG_DIR%\run and stale marker files were cleared.
echo Open a new terminal before running cxnfs from PATH.
exit /b 0

:add_to_machine_path
set "CXNFS_INSTALL_DIR_FOR_PATH=%~1"
powershell -NoProfile -ExecutionPolicy Bypass -Command "$install=$env:CXNFS_INSTALL_DIR_FOR_PATH; $machine=[Environment]::GetEnvironmentVariable('Path','Machine'); $parts=New-Object System.Collections.Generic.List[string]; if(-not [string]::IsNullOrWhiteSpace($machine)){ foreach($part in $machine.Split(';')){ if(-not [string]::IsNullOrWhiteSpace($part)){ [void]$parts.Add($part) } } }; $exists=$false; foreach($part in $parts){ if($part.TrimEnd('\') -ieq $install.TrimEnd('\')){ $exists=$true } }; if(-not $exists){ $parts.Add($install); [Environment]::SetEnvironmentVariable('Path', [string]::Join(';', $parts), 'Machine') }"
if errorlevel 1 (
  echo install-windows.bat: failed to add %CXNFS_INSTALL_DIR_FOR_PATH% to the machine PATH. Run this installer from an elevated shell. 1>&2
  exit /b 1
)
exit /b 0

:write_config
set "CONFIG_DIR_PROP=%CONFIG_DIR:\=/%"
set "DATASET_DIR_PROP=%DATASET_DIR:\=/%"
set "OUTPUT_DIR_PROP=%OUTPUT_DIR:\=/%"
> "%~1" (
  echo # Copy to %CONFIG_DIR_PROP%/server.properties and replace placeholders.
  echo serve.root.0=/datasets=%DATASET_DIR_PROP%
  echo serve.root.1=/outputs=%OUTPUT_DIR_PROP%
  echo serve.host=0.0.0.0
  echo serve.control-port=4509
  echo serve.transport-port=4510
  echo serve.bulk-transport=quic
  echo serve.initial-cords=1
  echo serve.max-cords=64
  echo serve.adaptive-cords=true
  echo serve.live-write-mode=positional
  echo serve.payload-staging-memory-bytes=536870912
  echo serve.payload-spill-dir=%CONFIG_DIR_PROP%/payload-staging
  echo serve.repository-root=%CONFIG_DIR_PROP%/repository
  echo serve.keystore=%CONFIG_DIR_PROP%/server.p12
  echo serve.keystore-password-file=%CONFIG_DIR_PROP%/server.p12.pass
  echo serve.ready-file=%CONFIG_DIR_PROP%/run/server.ready
  echo serve.health-file=%CONFIG_DIR_PROP%/run/server.health
  echo serve.health-interval-ms=1000
  echo serve.shutdown-file=%CONFIG_DIR_PROP%/run/server.stop
  echo serve.shutdown-poll-ms=250
  echo serve.daemon-lock-file=%CONFIG_DIR_PROP%/run/server.lock
  echo.
  echo auth.mechanisms=username_password
  echo auth.user.trainer.password=REPLACE_WITH_LONG_RANDOM_PASSWORD
  echo auth.user.eval.password=REPLACE_WITH_DIFFERENT_LONG_RANDOM_PASSWORD
  echo.
  echo # Optional per-user remote namespace jails. Omit to allow all served roots.
  echo # auth.user.trainer.directories=/datasets,/outputs
  echo # auth.user.eval.directories=/datasets
)
exit /b 0
