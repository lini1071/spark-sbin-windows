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

rem Starts a slave on the machine this script is executed on.
rem
rem Environment Variables
rem
rem   SPARK_WORKER_INSTANCES  The number of worker instances to run on this
rem                           slave.  Default is 1.
rem   SPARK_WORKER_PORT       The base port number for the first worker. If set,
rem                           subsequent workers will increment this number.  If
rem                           unset, Spark will find a valid port number, but
rem                           with no guarantee of a predictable pattern.
rem   SPARK_WORKER_WEBUI_PORT The base port for the web interface of the first
rem                           worker.  Subsequent workers will increment this
rem                           number.  Default is 8081.
setlocal enabledelayedexpansion

if ["%SPARK_HOME%"] == [""] (
	set SPARK_HOME=%~dp0..
)

rem NOTE: This exact class name is matched downstream by SparkSubmit.
rem Any changes need to be reflected there.
set CLASS=org.apache.spark.deploy.worker.Worker

if ["%*"] == [""] set show_usage=1
if ["%*"] == ["*--help"] set show_usage=1
if ["%*"] == ["*-h"] set show_usage=1
if defined !show_usage! (
	for /f "tokens=* delims= " %%p in ("Usage: ./sbin/start-slave.cmd [options] <master>") do echo %%~p
	set pattern="Usage:"
	set pattern+="\|Using Spark's default log4j profile:"
	set pattern+="\|Registered signal handlers for"

	rem call %SPARK_HOME%\bin\spark-class.cmd %CLASS% --help 2>&1 | findstr /v !pattern! 1>&2
	exit /b 1
)

call %SPARK_HOME%\sbin\spark-config.cmd

call %SPARK_HOME%\bin\load-spark-env.cmd

rem First argument should be the master; we need to store it aside because we may
rem need to insert arguments between it and the other arguments
set MASTER=%1
shift

rem Determine desired worker port
if ["%SPARK_WORKER_WEBUI_PORT%"] == [""] (
	set SPARK_WORKER_WEBUI_PORT=8081
)

for /f "tokens=2* delims= " %%p in ("%*") do set ARGS=%%p
if not defined !SPARK_WORKER_INSTANCES! (
	call :start_instance 1 !ARGS!
) else (
	for /l %%i in (1,1,!SPARK_WORKER_INSTANCES!) do call :start_instance %%i !ARGS!
)
endlocal
goto :eof

rem Start up the appropriate number of workers on this machine.
rem quick local function to start a worker
:start_instance
	set WORKER_NUM=%1
	shift

	if ["%SPARK_WORKER_PORT%"] == [""] (
		set PORT_FLAG=
		set PORT_NUM=
	) else (
		set PORT_FLAG=--port
		set /a PORT_NUM=%SPARK_WORKER_PORT% + %WORKER_NUM% - 1
	)
	set /a WEBUI_PORT=%SPARK_WORKER_WEBUI_PORT% + %WORKER_NUM% - 1

	for /f "tokens=2* delims= " %%p in ("%*") do set INSTANCE_ARGS=%%p
	call %SPARK_HOME%\sbin\spark-daemon.cmd start %CLASS% !WORKER_NUM! --webui-port !WEBUI_PORT! !PORT_FLAG! !PORT_NUM! %MASTER% !INSTANCE_ARGS!
rem end of 'start_instance' function area