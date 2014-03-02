#!/bin/sh
hadoop fs -cat output/part* | awk -f output.awk > /mnt/hgfs/Share/join.csv
