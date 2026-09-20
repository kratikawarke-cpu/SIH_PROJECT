# CHAINTRACE PowerShell Launcher
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

$PythonCandidates = @(
    "C:\Users\Pratik\AppData\Local\Programs\Python\Python312\python.exe",
    "py",
    "python"
)

$PyExec = $null
foreach ($cand in $PythonCandidates) {
    if (Get-Command $cand -ErrorAction SilentlyContinue) {
        $PyExec = $cand
        break
    }
}

if (-not $PyExec) {
    $PyExec = "python"
}

& $PyExec "$ScriptDir\run.py" $args
