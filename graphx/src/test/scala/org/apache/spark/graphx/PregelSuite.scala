package org.apache.spark.graphx

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.rdd._

class PregelSuite extends FunSuite with LocalSparkContext {

  test("1 iteration") {
    withSpark { sc =>
      val n = 5
      val starEdges = (1 to n).map(x => (0: VertexID, x: VertexID))
      val star = Graph.fromEdgeTuples(sc.parallelize(starEdges, 3), "v").cache()
      val result = Pregel(star, 0)(
        (vid, attr, msg) => attr,
        et => Iterator.empty,
        (a: Int, b: Int) => throw new Exception("mergeMsg run unexpectedly"))
      assert(result.vertices.collect.toSet === star.vertices.collect.toSet)
    }
  }

  test("chain propagation") {
    withSpark { sc =>
      val n = 5
      val chain = Graph.fromEdgeTuples(
        sc.parallelize((1 until n).map(x => (x: VertexID, x + 1: VertexID)), 3),
        0).cache()
      assert(chain.vertices.collect.toSet === (1 to n).map(x => (x: VertexID, 0)).toSet)
      val chainWithSeed = chain.mapVertices { (vid, attr) => if (vid == 1) 1 else 0 }.cache()
      assert(chainWithSeed.vertices.collect.toSet ===
        Set((1: VertexID, 1)) ++ (2 to n).map(x => (x: VertexID, 0)).toSet)
      val result = Pregel(chainWithSeed, 0)(
        (vid, attr, msg) => math.max(msg, attr),
        et => if (et.dstAttr != et.srcAttr) Iterator((et.dstId, et.srcAttr)) else Iterator.empty,
        (a: Int, b: Int) => math.max(a, b))
      assert(result.vertices.collect.toSet ===
        chain.vertices.mapValues { (vid, attr) => attr + 1 }.collect.toSet)
    }
  }
}
