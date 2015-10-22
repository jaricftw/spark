
//package com.oreilly.learningsparkexamples.mini.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel._

object WCandLC {
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("wordCount")

      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)

      // Load our input data.
      val input = sc.textFile("/home/jia/Benchmark/Marginal_ones_as_representatives/Spark_wordcount_grep_sort/data-MicroBenchmarks/in")
      //val input = sc.textFile("hdfs://128.111.57.77:9000/user/jia/spark-wordcount-wiki/in")
      //val input = sc.textFile("hdfs:///user/jia/spark-wordcount-wiki/in")
      //val input = sc.textFile("hdfs://localhost:9000/user/jia/spark-wordcount-wiki/in").cache()
        // sc.textFile("/home/jia/Benchmark/Marginal_ones_as_representatives/Spark_wordcount_grep_sort/data-MicroBenchmarks/in").persist(MEMORY_ONLY_SER)

      //input.cache()
      val filteredinput = input.filter(line=>line.contains("the")).cache()

      // Split up into words, and transform to word-count
      //val totallength = input.filter(line=>line.contains("the")).map(s=>s.length).reduce((a,b)=>a+b)
     // val words = input.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_).saveAsTextFile("/home/jia/SPARK/spark/test/WCandLC/outputFile")

      val ITERATIONS = 5

      for (i <- 1 to ITERATIONS) {
          // Only extract the lines that contains "the"
          //val totallength = input.filter(line=>line.contains("the")).map(s=>s.length).reduce((a,b)=>a+b)
          val totallength = filteredinput.map(s=>s.length).reduce((a,b)=>a+b)
      }

      sc.stop()
    }
}

