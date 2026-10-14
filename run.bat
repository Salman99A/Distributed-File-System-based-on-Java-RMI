@echo off
setlocal
cd /d "%~dp0"
echo ==============================================
echo  Distributed File System (Java RMI) Runner
echo ==============================================

:: Free port 50900 if previously occupied by a stale JVM
for /f "tokens=5" %%a in ('netstat -aon 2^>nul ^| findstr :50900 ^| findstr LISTENING') do (
    echo [INFO] Freeing port 50900 held by process PID %%a...
    taskkill /F /PID %%a >nul 2>&1
)

if not exist bin mkdir bin

echo [1/2] Compiling Java source files...
javac -d bin src/*.java
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Compilation failed.
    pause
    exit /b %ERRORLEVEL%
)
echo Compilation successful.

echo [2/2] Launching Distributed File System (Main)...
java -cp bin Main %*

