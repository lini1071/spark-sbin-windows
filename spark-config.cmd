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

rem included in all the spark scripts with source command
rem should not be executable directly
rem also should not be passed any arguments, since we need original $*

rem symlink and absolute path should rely on SPARK_HOME to resolve
setlocal enabledelayedexpansion
if not defined SPARK_HOME (
	set SPARK_HOME=%~dp0..
)

if not defined SPARK_CONF_DIR (
	set SPARK_CONF_DIR=%SPARK_HOME%\conf
)
rem Add the PySpark classes to the PYTHONPATH:
if not defined PYSPARK_PYTHONPATH_SET (
	set PYTHONPATH=%SPARK_HOME%\python:%PYTHONPATH%
	set PYTHONPATH=%SPARK_HOME%\python\lib\py4j-0.10.4-src.zip:%PYTHONPATH%
	set PYSPARK_PYTHONPATH_SET=1
)