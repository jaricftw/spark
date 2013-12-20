package org.apache.spark.graph.algorithms

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graph._
import org.apache.spark.graph.util.GraphGenerators
import org.apache.spark.rdd._


class SvdppSuite extends FunSuite with LocalSparkContext {

  test("Test SVD++ with mean square error on training set") {
    withSpark { sc =>
      val SvdppErr = 0.01
      val edges = sc.textFile("mllib/data/als/test.data").map { line =>
        val fields = line.split(",")
        Edge(fields(0).toLong * 2, fields(1).toLong * 2 + 1, fields(2).toDouble)
      }
      val graph = Svdpp.run(edges)
      val err = graph.vertices.collect.map{ case (vid, vd) =>
        if (vid % 2 == 1) { vd.norm } else { 0.0 }
      }.reduce(_ + _) / graph.triplets.collect.size
      assert(err < SvdppErr)
    }
  }

}
