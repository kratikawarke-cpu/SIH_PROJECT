@echo off
setlocal
cd /d "%~dp0"
cls

echo ========================================================
echo   CHAINTRACE - Starting Cyber Crime Command Center...
echo ========================================================
echo.

where py >nul 2>nul
if %ERRORLEVEL% equ 0 (
    py -3.12 run.py %*
    goto end
)

where python >nul 2>nul
if %ERRORLEVEL% equ 0 (
    python run.py %*
    goto end
)

if exist "C:\Users\Pratik\AppData\Local\Programs\Python\Python312\python.exe" (
    "C:\Users\Pratik\AppData\Local\Programs\Python\Python312\python.exe" run.py %*
    goto end
)

echo ERROR: Python is not found on PATH.
pause

:end
endlocal
