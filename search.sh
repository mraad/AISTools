#!/bin/sh
hadoop jar\
 target/AISTools-1.0-SNAPSHOT-job.jar\
 -conf conf.xml\
 search\
 /ais-index/2009/01/01/*\
 output
