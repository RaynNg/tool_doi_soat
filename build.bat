@echo off
rem Dong goi cong cu thanh dist\DoiSoat.exe, chay tren may co internet roi chep file exe sang VDI.
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe python -m venv .venv
.venv\Scripts\python.exe -m pip install -q -r requirements.txt pyinstaller || exit /b 1
.venv\Scripts\python.exe -m PyInstaller --noconfirm --clean --onefile --name DoiSoat ^
  --add-data "static;static" --add-data "Bien.txt;." ^
  --hidden-import thrift_sasl --hidden-import puresasl --collect-submodules puresasl ^
  app.py
