@echo off
setlocal

if "%CXNFS_HOME%"=="" set "CXNFS_HOME=%ProgramFiles%\CXNFS"
if "%CXNFS_CLASSPATH%"=="" set "CXNFS_CLASSPATH=%CXNFS_HOME%\cxnfs.jar;%CXNFS_HOME%\lib\*"
if "%JAVA_BIN%"=="" set "JAVA_BIN=java"

if "%CXNFS_JAVA_OPTS%"=="" (
  "%JAVA_BIN%" -cp "%CXNFS_CLASSPATH%" com.fortrow.cxnfs.Cxnfs %*
) else (
  "%JAVA_BIN%" %CXNFS_JAVA_OPTS% -cp "%CXNFS_CLASSPATH%" com.fortrow.cxnfs.Cxnfs %*
)
exit /b %ERRORLEVEL%
