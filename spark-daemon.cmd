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

rem Runs a Spark command as a daemon.
rem
rem Environment Variables
rem
rem   SPARK_CONF_DIR  Alternate conf dir. Default is ${SPARK_HOME}/conf.
rem   SPARK_LOG_DIR   Where log files are stored. ${SPARK_HOME}/logs by default.
rem   SPARK_MASTER    host:path where spark code should be rsync'd from
rem   SPARK_PID_DIR   The pid files are stored. /tmp by default.
rem   SPARK_IDENT_STRING   A string representing this instance of spark. $USER by default
rem   SPARK_NICENESS The scheduling priority for daemons. Defaults to 0.
rem   SPARK_NO_DAEMONIZE   If set, will run the proposed command in the foreground. It will not output a PID file.
rem
setlocal enabledelayedexpansion

set usage="Usage: spark-daemon.cmd [--config <conf-dir>] (start|stop|submit|status) <spark-command> <spark-instance-number> <args...>"

rem if no args specified, show usage
if ["%*"] == [""] (
	for /f "tokens=* delims= " %%p in (%usage%) do echo %%~p
	exit /b 1
)

if [%SPARK_HOME%] == [] (
  set SPARK_HOME=%~dp0..
)

call !SPARK_HOME!\sbin\spark-config.cmd

rem get arguments

rem Check if --config is passed as an argument. It is an optional parameter.
rem Exit if the argument is not a directory.

if ["%1"] == ["--config"] (
	shift
	set conf_dir=%1
	if not exist !conf_dir!\NUL (
		echo ERROR : !conf_dir! is not a directory
		for /f "tokens=* delims= " %%p in (%usage%) do echo %%~p
		exit /b 1
	) else (
		set SPARK_CONF_DIR=!conf_dir!
	)
	shift
)

set option=%1
shift
set command=%1
shift
set instance=%1
shift

call !SPARK_HOME!\bin\load-spark-env.cmd

if ["%SPARK_IDENT_STRING%"] == [""] (
	set SPARK_IDENT_STRING=%USERNAME%
)


set SPARK_PRINT_LAUNCH_COMMAND=1

rem get log directory
if ["%SPARK_LOG_DIR%"] == [""] (
	set SPARK_LOG_DIR=%SPARK_HOME%\logs
)
set SPARK_LOG_DIR=!SPARK_LOG_DIR:/=\!

if not exist !SPARK_LOG_DIR!\NUL mkdir !SPARK_LOG_DIR!
copy /b !SPARK_LOG_DIR!\.spark_test +,, > NUL 2>&1
set TEST_LOG_DIR=%ERRORLEVEL%
if [!TEST_LOG_DIR!] == [0] (
	del /f !SPARK_LOG_DIR!\.spark_test
) else (
	rem chown %SPARK_IDENT_STRING% %SPARK_LOG_DIR%
	rem icacls 
)

if ["%SPARK_PID_DIR%"] == [""] (
	set SPARK_PID_DIR=\tmp
)
set SPARK_PID_DIR=!SPARK_PID_DIR:/=\!
rem for /f "tokens=* delims= " %i in ('%%') do set SPARK_PID_DIR=%i

rem some variables
set log=!SPARK_LOG_DIR!\spark-!SPARK_IDENT_STRING!-%command%-%instance%-%COMPUTERNAME%.out
set pid=!SPARK_PID_DIR!\spark-!SPARK_IDENT_STRING!-%command%-%instance%.pid

rem Set default scheduling priority
if [%SPARK_NICENESS%] == [] (
	set SPARK_NICENESS=0
)

rem previously executed shift count : 3
if ["!option!"] == ["submit"] (
	for /f "tokens=4* delims= " %%p in ("%*") do call :run_command submit %%p %%q
	exit /b %errorlevel%
)
if ["!option!"] == ["start"] (
	for /f "tokens=4* delims= " %%p in ("%*") do call :run_command class %%p %%q
	exit /b %errorlevel%
)
if ["!option!"] == ["stop"] (
	if exist %pid% (
		set /p TARGET_ID=< %pid%
		for /f "skip=1 tokens=2 delims=, usebackq" %%n in (`tasklist /fi "PID eq !TARGET_ID!" /fo csv`) do set pname=%%~n
		if ["!pname!"] == ["java.exe"] (
			echo stopping !command!
			taskkill /pid !TARGET_ID! && del /f %pid%
		) else (
			echo no !command! to stop
		)
	) else (
		echo no !command! to stop
	)
	exit /b %errorlevel%
)
if ["!option!"] == ["status"] (
    if exist %pid% (
		set /p TARGET_ID=< %pid%
		for /f "skip=1 tokens=2 delims=, usebackq" %%n in (`tasklist /fi "PID eq !TARGET_ID!" /fo csv`) do set pname=%%~n
		if ["!pname!"] == ["java.exe"]
			echo !command! is running.
			exit /b 0
		) else (
			echo %pid% file is present but !command! not running
			exit /b 1
		)
	) else (
		echo !command! not running.
		exit /b 2
	)
)
for /f "tokens=* delims= " %%p in (!usage!) do echo %%~p
exit /b 1
rem designed endpoint of batch script area

:run_command
	set mode=%1
	shift

	if not exist %SPARK_PID_DIR%\NUL mkdir %SPARK_PID_DIR%

	if exist [%pid%] (
		set /p TARGET_ID=<%pid%
		for /f "tokens=1 delims=, usebackq" %%n in (`tasklist /fi "PID eq !TARGET_ID!" /fo csv`) do set pname=%%n
		if ["!pname!"] == ["java.exe"] (
			echo %command% running as process !TARGET_ID!.  Stop it first.
			exit /b 1
		)
	)

	rem 1) This part(rsync) is not implemented for now.
	rem    I don't have idea this can be implemented without any external components.
	rem
	rem if [ "$SPARK_MASTER" != "" ]; then
	rem  echo rsync from "$SPARK_MASTER"
	rem  rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' "$SPARK_MASTER/" "${SPARK_HOME}"
	rem fi

	rem 2) Currently logging with using this script is disabled temporally
	rem	   because daemon processes are not intended to be opened in background.
	rem 
	rem call :spark_rotate_log %log%
	rem echo starting %command%, logging to %log%

	rem normalize SPARK_NICENESS for Windows
	set /a NICENESS_CAST=(19+(-%SPARK_NICENESS%))*6
	if !NICENESS_CAST! lss 40 (
		set PROCESS_PRIORITY=LOW
	)
	if !NICENESS_CAST! geq 40 if !NICENESS_CAST! lss 80 (
		set PROCESS_PRIORITY=BELOWNORMAL
	)
	if !NICENESS_CAST! geq 80 if !NICENESS_CAST! lss 120 (
		set PROCESS_PRIORITY=NORMAL
	)
	if !NICENESS_CAST! geq 120 if !NICENESS_CAST! lss 160 (
		set PROCESS_PRIORITY=ABOVENORMAL
	)
	if !NICENESS_CAST! geq 160 if !NICENESS_CAST! lss 200 (
		set PROCESS_PRIORITY=HIGH
	)
	if !NICENESS_CAST! geq 200 if !NICENESS_CAST! lss 240 (
		set PROCESS_PRIORITY=REALTIME
	)
	
	rem shift count in this label : 1
	if ["%mode%"] == ["class"] (
		for /f "tokens=2* delims= " %%p in ("%*") do call :execute_command %SPARK_HOME%\bin\spark-class.cmd %command% %%p %%q
		exit /b %errorlevel%
	)
	if ["%mode%"] == ["submit"] (
		for /f "tokens=2* delims= " %%p in ("%*") do call :execute_command %SPARK_HOME%\bin\spark-submit.cmd --class %command% %%p %%q
		exit /b %errorlevel%
	)
	
	echo unknown mode: %mode%
	exit /b 1
rem end of 'run_command' function area

:execute_command
	if defined SPARK_NO_DAEMONIZE (
		%*
		exit /b %errorlevel%
	)
	rem if [ -z ${SPARK_NO_DAEMONIZE+set} ]
	start "Apache Spark Distribution - %1 %2" /%PROCESS_PRIORITY% %* >> %log% 2>&1 < NUL
	for /f "skip=1 tokens=2 delims=, usebackq" %%p in (`tasklist /fo csv /fi "WINDOWTITLE eq Apache Spark Distribution - %1 %2" /fi "CPUTIME le 00:00:01"`) do set newpid=%%~p
	rem timeout /t 1 /nobreak > NUL
	rem for /f "skip=1 tokens=2 delims=, usebackq" %%p in (`tasklist /fo csv /fi "IMAGENAME eq java.exe" /fi "CPUTIME le 00:00:01"`) do set newpid=%%~p

	echo !newpid! > %pid%

	rem Poll for up to 5 seconds for the java process to start
	set idx=0
	:execute_command_wait
	set /a idx=!idx!+1
	if !idx! leq 5 (
		for /f "skip=1 tokens=1 delims=, usebackq" %%n in (`tasklist /fi "PID eq !newpid!" /fo csv`) do set pname=%%~n
		if ["!pname!"] == ["java.exe"] goto :execute_command_next
		timeout /t 1 /nobreak > NUL
		goto :execute_command_wait
	)

	:execute_command_next
	timeout /t 2 /nobreak > NUL
	rem Check if the process has died; in that case we'll tail the log so the user can see
	for /f "skip=1 tokens=1 delims=, usebackq" %%n in (`tasklist /fi "PID eq !newpid!" /fo csv`) do set pname=%%~n
	if not ["!pname!"] == ["java.exe"] (
		echo failed to launch: %*
		rem tail -2 "$log" | sed 's/^/  /'
		for /f "tokens=3 delims= usebackq" %%i in (`find /c /v "" %log%`) do set /a lines=%%i-2
		for /f "skip=!lines!" %%l in (`type %log%`) do echo   %%l
		echo full log in %log%
	)
	goto :eof
rem end of 'execute_command' function area

:spark_rotate_log
	rem log_no ; name only
	set log=%1
	set log_no=%~nx1
	set num=5
	if not ["%2"] == [""] (
		set "var="&for /f "delims=0123456789" %%i in ("%2") do set var=%%i
		if defined var set num=!var!
	)
	if exist !log! (
		rem rotate logs
		if !num! gtr 1 for /l %%i in (!num!,-1,2) do (
			set /a prev=%%i-1
			if exist !log!.!prev! rename "!log!.!prev!" "!log_no!.%%i"
			rem set /a num=%i
		)
		rename "!log!" "!log_no!.1"
	)
rem end of 'spark_rotate_log' function area
