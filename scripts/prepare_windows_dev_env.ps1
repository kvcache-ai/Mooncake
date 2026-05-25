$ErrorActionPreference = "Stop"

Write-Host "Checking local developer tools..." -ForegroundColor Cyan

$tools = @(
    "python",
    "pip",
    "cl"
)

foreach ($tool in $tools) {
    $cmd = Get-Command $tool -ErrorAction SilentlyContinue
    if ($null -eq $cmd) {
        Write-Host "Missing required tool: $tool" -ForegroundColor Red
        exit 1
    }
    Write-Host ("Found {0}: {1}" -f $tool, $cmd.Source) -ForegroundColor Green
}

Write-Host "Installing user-local cmake and ninja..." -ForegroundColor Cyan
python -m pip install --user cmake ninja

$pyScripts = Join-Path $env:APPDATA "Python\Python312\Scripts"
if (Test-Path $pyScripts) {
    Write-Host "Python user script directory:" $pyScripts -ForegroundColor Yellow
    Write-Host "Add it to PATH for interactive use if needed." -ForegroundColor Yellow
}

Write-Host "Checking WSL availability..." -ForegroundColor Cyan
try {
    wsl.exe --status | Out-Host
} catch {
    Write-Host "WSL is not available or not healthy on this machine." -ForegroundColor Yellow
    Write-Host "Mooncake Store C++ tests are primarily intended for Linux/WSL environments." -ForegroundColor Yellow
}

Write-Host "Done." -ForegroundColor Green
