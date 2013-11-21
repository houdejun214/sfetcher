@echo off
set APP_HOME=%CD%
setlocal enabledelayedexpansion
cd %APP_HOME%
SET CLSNAME=com.sdata.crawl.DataCrawl

@set CLASSPATH=%CLASSPATH%

for /f "delims=" %%i in ('dir lib /b ') do set CLASSPATH=!CLASSPATH!;./lib/%%i

java -Xms256m -Xmx1024m -cp %CLASSPATH% %CLSNAME%  %1 %2
pause