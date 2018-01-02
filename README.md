Batch scripts for running Apache Spark on Microsoft Windows

These files are "not official"(only for my personal interest),
so please understand that maybe codes are little dizzy and not cleaned.
You can use and modify it for your own but
please do not commit to official Apache Spark repository
because these scripts are not assured to work fine.

Tested essential and simple cases on:
Microsoft Windows 10 Pro 64bit
Apache Hadoop 3.0 GA
Apache Spark 2.2.1 (pre-built for Apache Hadoop 2.7 or later)

1) class (Master, Worker ; daemon)
start-master.cmd
start-slave.cmd
spark-daemon.cmd

2) submit (simple word-count application)
spark-daemon.cmd

You may add /b flag to 'start' command on :execute_command label in "spark-daemon.cmd"
to make the daemon running in background mode, but I don't recommend that.
(I get some problem on getting PID of running java processes...)

Maybe you need to add(or modify) the environment file "spark-env.cmd" on conf folder and
environment-loading file "load-spark-env.cmd" on bin folder.

Currently rsync-binding operation is not supported.
