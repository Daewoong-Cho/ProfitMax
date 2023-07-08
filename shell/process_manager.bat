@echo off
 
set PNAME=p_block_info_api.exe
set DIR=C:\ProfitMax\api\crypto
 
tasklist | findstr %PNAME% 
 
if %ERRORLEVEL% == 0 (
    echo %PNAME% is Running
) else (
    echo %PNAME% is not Running. Now Run…
    start /d"%DIR%" %PNAME% p_block_info_api.json
)

timeout 10