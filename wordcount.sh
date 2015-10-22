#!/bin/bash 
rm -rf test/WC/outputFile


./bin/spark-submit --class "WordCount" --master local[4] test/WC/target/scala-2.10/wordcount-project_2.10-1.0.jar
