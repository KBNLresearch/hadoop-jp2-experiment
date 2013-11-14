#!/bin/bash

hadoop fs -rm -r OUTPUT
hadoop jar target/jp2-workflow-1.0-SNAPSHOT.jar eu.scape_project.hadoop.ConversionRunner KEY OUTPUT /home/rar011/hadoop/tmp
hadoop fs -cat OUTPUT/part-00000
