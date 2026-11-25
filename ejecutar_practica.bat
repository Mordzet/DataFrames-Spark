@echo off
chcp 65001 >nul
cd /d "%~dp0"
set "JAVA_HOME=C:\Users\saidg\.jdks\temurin-17"
set "HADOOP_HOME=C:\Users\saidg\.hadoop"
set "PYSPARK_PYTHON=C:\Users\saidg\AppData\Local\Python\pythoncore-3.14-64\python.exe"
set "PYSPARK_DRIVER_PYTHON=C:\Users\saidg\AppData\Local\Python\pythoncore-3.14-64\python.exe"
set "PATH=%JAVA_HOME%\bin;%HADOOP_HOME%\bin;C:\Users\saidg\AppData\Local\Python\pythoncore-3.14-64;C:\Users\saidg\AppData\Local\Python\pythoncore-3.14-64\Scripts;%PATH%"

echo ===========================================================================
echo Iniciando Practica de DataFrames en Apache Spark...
echo ===========================================================================
"C:\Users\saidg\AppData\Local\Python\pythoncore-3.14-64\python.exe" "%~dp0practica_dataframes.py"
echo.
echo ===========================================================================
echo Ejecucion completada.
echo ===========================================================================
pause
