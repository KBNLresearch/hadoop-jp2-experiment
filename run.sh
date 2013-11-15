#!/bin/bash

hadoop fs -rm -r $2
hadoop jar target/hadoop-jp2-experiment-1.0-SNAPSHOT-jar-with-dependencies.jar eu.scape_project.hadoop.ConversionRunner $1 $2 $3
hadoop fs -cat $2/part-00000