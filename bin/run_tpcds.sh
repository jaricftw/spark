#!/bin/bash

TPC_HOME=~/TPC-DS-1.4.0
TPC_JOB_HOME=~/Spark-TPC-DS

./spark-submit --master local --class BenchSQLDS $TPC_JOB_HOME/target/scala-2.10/spark-tpc-ds-assembly-0.0.1-SNAPSHOT.jar /tpcds/input 10 /tpcds/output 3 simple $TPC_HOME/tools

