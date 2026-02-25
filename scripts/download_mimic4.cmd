@echo off
setlocal

REM Usage option 1:
REM   set PHYSIONET_USER=your_username
REM   set PHYSIONET_PASS=your_password_or_token
REM   scripts\download_mimic4.cmd C:\datasets\mimic-iv-2.2
REM
REM Usage option 2:
REM   scripts\download_mimic4.cmd C:\datasets\mimic-iv-2.2 your_username your_password_or_token

if "%~1"=="" (
  echo Usage: scripts\download_mimic4.cmd ^<target_dir^> [user] [password]
  exit /b 1
)

if not "%~2"=="" set "PHYSIONET_USER=%~2"
if not "%~3"=="" set "PHYSIONET_PASS=%~3"

if "%PHYSIONET_USER%"=="" (
  echo Missing PHYSIONET_USER environment variable.
  exit /b 1
)

if "%PHYSIONET_PASS%"=="" (
  echo Missing PHYSIONET_PASS environment variable.
  exit /b 1
)

set TARGET=%~1
set ICU_DIR=%TARGET%\icu
set HOSP_DIR=%TARGET%\hosp

if not exist "%ICU_DIR%" mkdir "%ICU_DIR%"
if not exist "%HOSP_DIR%" mkdir "%HOSP_DIR%"

echo Downloading MIMIC-IV required files...

curl -f -L --user "%PHYSIONET_USER%:%PHYSIONET_PASS%" ^
  "https://physionet.org/files/mimiciv/2.2/icu/chartevents.csv.gz?download" ^
  -o "%ICU_DIR%\chartevents.csv.gz"
if errorlevel 1 exit /b 1

curl -f -L --user "%PHYSIONET_USER%:%PHYSIONET_PASS%" ^
  "https://physionet.org/files/mimiciv/2.2/hosp/labevents.csv.gz?download" ^
  -o "%HOSP_DIR%\labevents.csv.gz"
if errorlevel 1 exit /b 1

echo Done. Files saved under: %TARGET%
exit /b 0
