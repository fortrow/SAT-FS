@echo off
setlocal

if "%SATFS_CLASSPATH%"=="" (
  if "%SATFS_HOME%"=="" set "SATFS_HOME=%~dp0.."
  set "SATFS_CLASSPATH=%SATFS_HOME%\sat-fs.jar;%SATFS_HOME%\cxnfs.jar;%SATFS_HOME%\lib\*"
)

if "%JAVA_BIN%"=="" set "JAVA_BIN=java"

"%JAVA_BIN%" %SATFS_JAVA_OPTS% -cp "%SATFS_CLASSPATH%" com.fortrow.satfs.Satfs %*
