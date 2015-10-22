#!/bin/bash 
rm -rf test/WCandLC/outputFile


./bin/spark-submit --class "WCandLC" --master local[4] test/WCandLC/target/scala-2.10/wcandlc-project_2.10-1.0.jar
