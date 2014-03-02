#!/bin/sh
hadoop fs -cat output/part* | awk -f density.awk > /mnt/hgfs/Share/density.csv
