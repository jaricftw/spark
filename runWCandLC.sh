#!/bin/bash 

for i in  1 2 3 4 5
do
    ./bin/spark-submit --class "WCandLC" --master local[4] test/WCandLC/target/scala-2.10/wcandlc-project_2.10-1.0.jar
    echo "i = $i"
done
