package org.apache.spark.graph

import scala.util.Random

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.graph.Graph._
import org.apache.spark.graph.LocalSparkContext._
import org.apache.spark.graph.impl.EdgePartition
import org.apache.spark.graph.impl.EdgePartitionBuilder
import org.apache.spark.rdd._

class GraphSuite extends FunSuite with LocalSparkContext {

  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", "org.apache.spark.graph.GraphKryoRegistrator")

  test("Graph Creation") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val rawEdges = (0L to 100L).zip((1L to 99L) :+ 0L)
      val edges = sc.parallelize(rawEdges)
      val graph = Graph.fromEdgeTuples(edges, 1.0F)
      assert(graph.edges.count() === rawEdges.size)
    }
  }

  test("Graph Creation with invalid vertices") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val rawEdges = (0L to 98L).zip((1L to 99L) :+ 0L)
      val edges: RDD[Edge[Int]] = sc.parallelize(rawEdges).map { case (s, t) => Edge(s, t, 1) }
      val vertices: RDD[(Vid, Boolean)] = sc.parallelize((0L until 10L).map(id => (id, true)))
      val graph = Graph(vertices, edges, false)
      assert( graph.edges.count() === rawEdges.size )
      assert( graph.vertices.count() === 100)
      graph.triplets.map { et =>
        assert((et.srcId < 10 && et.srcAttr) || (et.srcId >= 10 && !et.srcAttr))
        assert((et.dstId < 10 && et.dstAttr) || (et.dstId >= 10 && !et.dstAttr))
      }
    }
  }

  test("core operations") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 5
      val star = Graph.fromEdgeTuples(
        sc.parallelize((1 to n).map(x => (0: Vid, x: Vid)), 3), "v")
      // triplets
      assert(star.triplets.map(et => (et.srcId, et.dstId, et.srcAttr, et.dstAttr)).collect.toSet ===
        (1 to n).map(x => (0: Vid, x: Vid, "v", "v")).toSet)
      // reverse
      val reverseStar = star.reverse
      assert(reverseStar.outDegrees.collect.toSet === (1 to n).map(x => (x: Vid, 1)).toSet)
      // outerJoinVertices
      val reverseStarDegrees =
        reverseStar.outerJoinVertices(reverseStar.outDegrees) { (vid, a, bOpt) => bOpt.getOrElse(0) }
      val neighborDegreeSums = reverseStarDegrees.mapReduceTriplets(
        et => Iterator((et.srcId, et.dstAttr), (et.dstId, et.srcAttr)),
        (a: Int, b: Int) => a + b).collect.toSet
      assert(neighborDegreeSums === Set((0: Vid, n)) ++ (1 to n).map(x => (x: Vid, 0)))
      // mapVertices preserving type
      val mappedVAttrs = reverseStar.mapVertices((vid, attr) => attr + "2")
      assert(mappedVAttrs.vertices.collect.toSet === (0 to n).map(x => (x: Vid, "v2")).toSet)
      // mapVertices changing type
      val mappedVAttrs2 = reverseStar.mapVertices((vid, attr) => attr.length)
      assert(mappedVAttrs2.vertices.collect.toSet === (0 to n).map(x => (x: Vid, 1)).toSet)
      // groupEdges
      val doubleStar = Graph.fromEdgeTuples(
        sc.parallelize((1 to n).flatMap(x => List((0: Vid, x: Vid), (0: Vid, x: Vid))), 1), "v")
      val star2 = doubleStar.groupEdges { (a, b) => a}
      assert(star2.edges.collect.toArray.sorted(Edge.lexicographicOrdering[Int]) ===
        star.edges.collect.toArray.sorted(Edge.lexicographicOrdering[Int]))
      assert(star2.vertices.collect.toSet === star.vertices.collect.toSet)
    }
  }

  test("mapEdges") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 3
      val star = Graph.fromEdgeTuples(
        sc.parallelize((1 to n).map(x => (0: Vid, x: Vid))), "v")
      val starWithEdgeAttrs = star.mapEdges(e => e.dstId)

      // map(_.copy()) is a workaround for https://github.com/amplab/graphx/issues/25
      val edges = starWithEdgeAttrs.edges.map(_.copy()).collect()
      assert(edges.size === n)
      assert(edges.toSet === (1 to n).map(x => Edge(0, x, x)).toSet)
    }
  }

  test("mapReduceTriplets") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 5
      val star = Graph.fromEdgeTuples(sc.parallelize((1 to n).map(x => (0: Vid, x: Vid))), 0)
      val starDeg = star.joinVertices(star.degrees){ (vid, oldV, deg) => deg }
      val neighborDegreeSums = starDeg.mapReduceTriplets(
        edge => Iterator((edge.srcId, edge.dstAttr), (edge.dstId, edge.srcAttr)),
        (a: Int, b: Int) => a + b)
      assert(neighborDegreeSums.collect().toSet === (0 to n).map(x => (x, n)).toSet)

      // activeSetOpt
      val allPairs = for (x <- 1 to n; y <- 1 to n) yield (x: Vid, y: Vid)
      val complete = Graph.fromEdgeTuples(sc.parallelize(allPairs, 3), 0)
      val vids = complete.mapVertices((vid, attr) => vid).cache()
      val active = vids.vertices.filter { case (vid, attr) => attr % 2 == 0 }
      val numEvenNeighbors = vids.mapReduceTriplets(et => {
        // Map function should only run on edges with destination in the active set
        if (et.dstId % 2 != 0) {
          throw new Exception("map ran on edge with dst vid %d, which is odd".format(et.dstId))
        }
        Iterator((et.srcId, 1))
      }, (a: Int, b: Int) => a + b, Some((active, EdgeDirection.In))).collect.toSet
      assert(numEvenNeighbors === (1 to n).map(x => (x: Vid, n / 2)).toSet)

      // outerJoinVertices followed by mapReduceTriplets(activeSetOpt)
      val ring = Graph.fromEdgeTuples(sc.parallelize((0 until n).map(x => (x: Vid, (x+1) % n: Vid)), 3), 0)
        .mapVertices((vid, attr) => vid).cache()
      val changed = ring.vertices.filter { case (vid, attr) => attr % 2 == 1 }.mapValues(-_)
      val changedGraph = ring.outerJoinVertices(changed) { (vid, old, newOpt) => newOpt.getOrElse(old) }
      val numOddNeighbors = changedGraph.mapReduceTriplets(et => {
        // Map function should only run on edges with source in the active set
        if (et.srcId % 2 != 1) {
          throw new Exception("map ran on edge with src vid %d, which is even".format(et.dstId))
        }
        Iterator((et.dstId, 1))
      }, (a: Int, b: Int) => a + b, Some(changed, EdgeDirection.Out)).collect.toSet
      assert(numOddNeighbors === (2 to n by 2).map(x => (x: Vid, 1)).toSet)

    }
  }

  test("aggregateNeighbors") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 3
      val star = Graph.fromEdgeTuples(sc.parallelize((1 to n).map(x => (0: Vid, x: Vid))), 1)

      val indegrees = star.aggregateNeighbors(
        (vid, edge) => Some(1),
        (a: Int, b: Int) => a + b,
        EdgeDirection.In)
      assert(indegrees.collect().toSet === (1 to n).map(x => (x, 1)).toSet)

      val outdegrees = star.aggregateNeighbors(
        (vid, edge) => Some(1),
        (a: Int, b: Int) => a + b,
        EdgeDirection.Out)
      assert(outdegrees.collect().toSet === Set((0, n)))

      val noVertexValues = star.aggregateNeighbors[Int](
        (vid: Vid, edge: EdgeTriplet[Int, Int]) => None,
        (a: Int, b: Int) => throw new Exception("reduceFunc called unexpectedly"),
        EdgeDirection.In)
      assert(noVertexValues.collect().toSet === Set.empty[(Vid, Int)])
    }
  }

  test("joinVertices") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val vertices = sc.parallelize(Seq[(Vid, String)]((1, "one"), (2, "two"), (3, "three")), 2)
      val edges = sc.parallelize((Seq(Edge(1, 2, "onetwo"))))
      val g: Graph[String, String] = Graph(vertices, edges)

      val tbl = sc.parallelize(Seq[(Vid, Int)]((1, 10), (2, 20)))
      val g1 = g.joinVertices(tbl) { (vid: Vid, attr: String, u: Int) => attr + u }

      val v = g1.vertices.collect().toSet
      assert(v === Set((1, "one10"), (2, "two20"), (3, "three")))
    }
  }

  test("collectNeighborIds") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val chain = (0 until 100).map(x => (x, (x+1)%100) )
      val rawEdges = sc.parallelize(chain, 3).map { case (s,d) => (s.toLong, d.toLong) }
      val graph = Graph.fromEdgeTuples(rawEdges, 1.0)
      val nbrs = graph.collectNeighborIds(EdgeDirection.Both)
      assert(nbrs.count === chain.size)
      assert(graph.numVertices === nbrs.count)
      nbrs.collect.foreach { case (vid, nbrs) => assert(nbrs.size === 2) }
      nbrs.collect.foreach { case (vid, nbrs) =>
        val s = nbrs.toSet
        assert(s.contains((vid + 1) % 100))
        assert(s.contains(if (vid > 0) vid - 1 else 99 ))
      }
    }
  }

  test("mask") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 5
      val vertices = sc.parallelize((0 to n).map(x => (x:Vid, x)))
      val edges = sc.parallelize((1 to n).map(x => Edge(0, x, x)))
      val graph: Graph[Int, Int] = Graph(vertices, edges)

      val subgraph = graph.subgraph(
        e => e.dstId != 4L,
        (vid, vdata) => vid != 3L
      ).mapVertices((vid, vdata) => -1).mapEdges(e => -1)

      val projectedGraph = graph.mask(subgraph)

      val v = projectedGraph.vertices.collect().toSet
      assert(v === Set((0,0), (1,1), (2,2), (4,4), (5,5)))

      // the map is necessary because of object-reuse in the edge iterator
      val e = projectedGraph.edges.map(e => Edge(e.srcId, e.dstId, e.attr)).collect().toSet
      assert(e === Set(Edge(0,1,1), Edge(0,2,2), Edge(0,5,5)))

    }
  }

  test ("filter") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 5
      val vertices = sc.parallelize((0 to n).map(x => (x:Vid, x)))
      val edges = sc.parallelize((1 to n).map(x => Edge(0, x, x)))
      val graph: Graph[Int, Int] = Graph(vertices, edges)
      val filteredGraph = graph.filter(
        graph => {
          val degrees: VertexRDD[Int] = graph.outDegrees
          graph.outerJoinVertices(degrees) {(vid, data, deg) => deg.getOrElse(0)}
        },
        vpred = (vid: Vid, deg:Int) => deg > 0
      )

      val v = filteredGraph.vertices.collect().toSet
      assert(v === Set((0,0)))

      // the map is necessary because of object-reuse in the edge iterator
      val e = filteredGraph.edges.map(e => Edge(e.srcId, e.dstId, e.attr)).collect().toSet
      assert(e.isEmpty)
    }
  }

  test("VertexSetRDD") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 100
      val a = sc.parallelize((0 to n).map(x => (x.toLong, x.toLong)), 5)
      val b = VertexRDD(a).mapValues(x => -x).cache() // Allow joining b with a derived RDD of b
      assert(b.count === n + 1)
      assert(b.leftJoin(a){ (id, a, bOpt) => a + bOpt.get }.map(x=> x._2).reduce(_+_) === 0)
      val c = b.aggregateUsingIndex[Long](a, (x, y) => x)
      assert(b.leftJoin(c){ (id, b, cOpt) => b + cOpt.get }.map(x=> x._2).reduce(_+_) === 0)
      val d = c.filter(q => ((q._2 % 2) == 0))
      val e = a.filter(q => ((q._2 % 2) == 0))
      assert(d.count === e.count)
      assert(b.zipJoin(c)((id, b, c) => b + c).map(x => x._2).reduce(_+_) === 0)
      val f = b.mapValues(x => if (x % 2 == 0) -x else x)
      assert(b.diff(f).collect().toSet === (2 to n by 2).map(x => (x.toLong, x.toLong)).toSet)
    }
  }

  test("subgraph") {
    withSpark(new SparkContext("local", "test")) { sc =>
      // Create a star graph of 10 veritces.
      val n = 10
      val star = Graph.fromEdgeTuples(sc.parallelize((1 to n).map(x => (0: Vid, x: Vid))), "v")
      // Take only vertices whose vids are even
      val subgraph = star.subgraph(vpred = (vid, attr) => vid % 2 == 0)

      // We should have 5 vertices.
      assert(subgraph.vertices.collect().toSet === (0 to n by 2).map(x => (x, "v")).toSet)

      // And 4 edges.
      assert(subgraph.edges.map(_.copy()).collect().toSet === (2 to n by 2).map(x => Edge(0, x, 1)).toSet)
    }
  }

  test("EdgePartition.sort") {
    val edgesFrom0 = List(Edge(0, 1, 0))
    val edgesFrom1 = List(Edge(1, 0, 0), Edge(1, 2, 0))
    val sortedEdges = edgesFrom0 ++ edgesFrom1
    val builder = new EdgePartitionBuilder[Int]
    for (e <- Random.shuffle(sortedEdges)) {
      builder.add(e.srcId, e.dstId, e.attr)
    }

    val edgePartition = builder.toEdgePartition
    assert(edgePartition.iterator.map(_.copy()).toList === sortedEdges)
    assert(edgePartition.indexIterator(_ == 0).map(_.copy()).toList === edgesFrom0)
    assert(edgePartition.indexIterator(_ == 1).map(_.copy()).toList === edgesFrom1)
  }

  test("EdgePartition.innerJoin") {
    def makeEdgePartition[A: ClassManifest](xs: Iterable[(Int, Int, A)]): EdgePartition[A] = {
      val builder = new EdgePartitionBuilder[A]
      for ((src, dst, attr) <- xs) { builder.add(src: Vid, dst: Vid, attr) }
      builder.toEdgePartition
    }
    val aList = List((0, 1, 0), (1, 0, 0), (1, 2, 0), (5, 4, 0), (5, 5, 0))
    val bList = List((0, 1, 0), (1, 0, 0), (1, 1, 0), (3, 4, 0), (5, 5, 0))
    val a = makeEdgePartition(aList)
    val b = makeEdgePartition(bList)

    assert(a.innerJoin(b) { (src, dst, a, b) => a }.iterator.map(_.copy()).toList ===
      List(Edge(0, 1, 0), Edge(1, 0, 0), Edge(5, 5, 0)))
  }
}
