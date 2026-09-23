@echo off
setlocal

set "JAVA_HOME=C:\Program Files\Java\jdk-17"
set "HADOOP_HOME=C:\hadoop"
set "PYSPARK_PYTHON=C:\PROGRA~1\PYTHON~1\python.exe"
set "PYSPARK_DRIVER_PYTHON=C:\PROGRA~1\PYTHON~1\python.exe"
set "PATH=%JAVA_HOME%\bin;%HADOOP_HOME%\bin;%PATH%"

cd /d "%~dp0\.."
python spark\spark_processing.py

endlocal
pause
