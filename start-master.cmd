@echo off

rem
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem    http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.
rem

rem Starts the master on the machine this script is executed on.
setlocal enabledelayedexpansion

if not defined SPARK_HOME (
	set SPARK_HOME=%~dp0..
)

rem NOTE: This exact class name is matched downstream by SparkSubmit.
rem Any changes need to be reflected there.
set CLASS=org.apache.spark.deploy.master.Master

if ["%*"] == ["*--help"] 	set show_usage=1
if ["%*"] == ["*-h"] 		set show_usage=1
if defined show_usage (
	for /f "tokens=* delims= " %%p in ("Usage: ./sbin/start-master.cmd [options]") do echo %%~p
	set pattern="Usage:"
	set pattern+="\|Using Spark's default log4j profile:"
	set pattern+="\|Registered signal handlers for"

	call %SPARK_HOME%\bin\spark-class.cmd %CLASS% --help 2>&1 | findstr /v !pattern! 1>&2
	exit /b 1
)

set ORIGINAL_ARGS=%*

call %SPARK_HOME%\sbin\spark-config.cmd

call %SPARK_HOME%\bin\load-spark-env.cmd

if ["%SPARK_MASTER_PORT%"] == [""] (
	set SPARK_MASTER_PORT=7077
)

if ["%SPARK_MASTER_HOST%"] == [""] (
	set SPARK_MASTER_HOST=%COMPUTERNAME%
)

if ["%SPARK_MASTER_WEBUI_PORT%"] == [""] (
	set SPARK_MASTER_WEBUI_PORT=8080
)

call %SPARK_HOME%\sbin\spark-daemon.cmd start %CLASS% 1 --host %SPARK_MASTER_HOST% --port %SPARK_MASTER_PORT% --webui-port %SPARK_MASTER_WEBUI_PORT% %ORIGINAL_ARGS%
endlocal