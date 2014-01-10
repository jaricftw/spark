package org.apache.spark.graphx

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.SparkException


/**
 * Contains additional functionality for [[Graph]]. All operations are expressed in terms of the
 * efficient GraphX API. This class  is implicitly constructed for each Graph object.
 *
 * @tparam VD the vertex attribute type
 * @tparam ED the edge attribute type
 */
class GraphOps[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) {

  /** The number of edges in the graph. */
  lazy val numEdges: Long = graph.edges.count()


  /** The number of vertices in the graph. */
  lazy val numVertices: Long = graph.vertices.count()


  /**
   * The in-degree of each vertex in the graph.
   * @note Vertices with no in-edges are not returned in the resulting RDD.
   */
  lazy val inDegrees: VertexRDD[Int] = degreesRDD(EdgeDirection.In)


  /**
   * The out-degree of each vertex in the graph.
   * @note Vertices with no out-edges are not returned in the resulting RDD.
   */
  lazy val outDegrees: VertexRDD[Int] = degreesRDD(EdgeDirection.Out)


  /**
   * The degree of each vertex in the graph.
   * @note Vertices with no edges are not returned in the resulting RDD.
   */
  lazy val degrees: VertexRDD[Int] = degreesRDD(EdgeDirection.Both)


  /**
   * Computes the neighboring vertex degrees.
   *
   * @param edgeDirection the direction along which to collect neighboring vertex attributes
   */
  private def degreesRDD(edgeDirection: EdgeDirection): VertexRDD[Int] = {
    if (edgeDirection == EdgeDirection.In) {
      graph.mapReduceTriplets(et => Iterator((et.dstId,1)), _ + _)
    } else if (edgeDirection == EdgeDirection.Out) {
      graph.mapReduceTriplets(et => Iterator((et.srcId,1)), _ + _)
    } else { // EdgeDirection.both
      graph.mapReduceTriplets(et => Iterator((et.srcId,1), (et.dstId,1)), _ + _)
    }
  }

  /**
   * Computes a statistic for the neighborhood of each vertex.
   *
   * @param mapFunc the function applied to each edge adjacent to each vertex. The mapFunc can
   * optionally return `None`, in which case it does not contribute to the final sum.
   * @param reduceFunc the function used to merge the results of each map operation
   * @param direction the direction of edges to consider (e.g., In, Out, Both).
   * @tparam A the aggregation type
   *
   * @return an RDD containing tuples of vertex identifiers and
   * their resulting value. Vertices with no neighbors will not appear in the RDD.
   *
   * @example We can use this function to compute the average follower
   * age for each user:
   *
   * {{{
   * val graph: Graph[Int,Int] = loadGraph()
   * val averageFollowerAge: RDD[(Int, Int)] =
   *   graph.aggregateNeighbors[(Int,Double)](
   *     (vid, edge) => (edge.otherVertex(vid).data, 1),
   *     (a, b) => (a._1 + b._1, a._2 + b._2),
   *     -1,
   *     EdgeDirection.In)
   *     .mapValues{ case (sum,followers) => sum.toDouble / followers}
   * }}}
   */
  def aggregateNeighbors[A: ClassTag](
      mapFunc: (VertexID, EdgeTriplet[VD, ED]) => Option[A],
      reduceFunc: (A, A) => A,
      dir: EdgeDirection)
    : VertexRDD[A] = {
    // Define a new map function over edge triplets
    val mf = (et: EdgeTriplet[VD,ED]) => {
      // Compute the message to the dst vertex
      val dst =
        if (dir == EdgeDirection.In || dir == EdgeDirection.Both) {
          mapFunc(et.dstId, et)
        } else { Option.empty[A] }
      // Compute the message to the source vertex
      val src =
        if (dir == EdgeDirection.Out || dir == EdgeDirection.Both) {
          mapFunc(et.srcId, et)
        } else { Option.empty[A] }
      // construct the return array
      (src, dst) match {
        case (None, None) => Iterator.empty
        case (Some(srcA),None) => Iterator((et.srcId, srcA))
        case (None, Some(dstA)) => Iterator((et.dstId, dstA))
        case (Some(srcA), Some(dstA)) => Iterator((et.srcId, srcA), (et.dstId, dstA))
      }
    }

    graph.mapReduceTriplets(mf, reduceFunc)
  } // end of aggregateNeighbors


  /**
   * Collect the neighbor vertex ids for each vertex.
   *
   * @param edgeDirection the direction along which to collect
   * neighboring vertices
   *
   * @return the set of neighboring ids for each vertex
   */
  def collectNeighborIds(edgeDirection: EdgeDirection): VertexRDD[Array[VertexID]] = {
    val nbrs =
      if (edgeDirection == EdgeDirection.Both) {
        graph.mapReduceTriplets[Array[VertexID]](
          mapFunc = et => Iterator((et.srcId, Array(et.dstId)), (et.dstId, Array(et.srcId))),
          reduceFunc = _ ++ _
        )
      } else if (edgeDirection == EdgeDirection.Out) {
        graph.mapReduceTriplets[Array[VertexID]](
          mapFunc = et => Iterator((et.srcId, Array(et.dstId))),
          reduceFunc = _ ++ _)
      } else if (edgeDirection == EdgeDirection.In) {
        graph.mapReduceTriplets[Array[VertexID]](
          mapFunc = et => Iterator((et.dstId, Array(et.srcId))),
          reduceFunc = _ ++ _)
      } else {
        throw new SparkException("It doesn't make sense to collect neighbor ids without a direction.")
      }
    graph.vertices.leftZipJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Array.empty[VertexID])
    }
  } // end of collectNeighborIds


  /**
   * Collect the neighbor vertex attributes for each vertex.
   *
   * @note This function could be highly inefficient on power-law
   * graphs where high degree vertices may force a large ammount of
   * information to be collected to a single location.
   *
   * @param edgeDirection the direction along which to collect
   * neighboring vertices
   *
   * @return the vertex set of neighboring vertex attributes for each vertex
   */
  def collectNeighbors(edgeDirection: EdgeDirection) :
    VertexRDD[ Array[(VertexID, VD)] ] = {
    val nbrs = graph.aggregateNeighbors[Array[(VertexID,VD)]](
      (vid, edge) =>
        Some(Array( (edge.otherVertexId(vid), edge.otherVertexAttr(vid)) )),
      (a, b) => a ++ b,
      edgeDirection)

    graph.vertices.leftZipJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Array.empty[(VertexID, VD)])
    }
  } // end of collectNeighbor


  /**
   * Join the vertices with an RDD and then apply a function from the
   * the vertex and RDD entry to a new vertex value.  The input table
   * should contain at most one entry for each vertex.  If no entry is
   * provided the map function is skipped and the old value is used.
   *
   * @tparam U the type of entry in the table of updates
   * @param table the table to join with the vertices in the graph.
   * The table should contain at most one entry for each vertex.
   * @param mapFunc the function used to compute the new vertex
   * values.  The map function is invoked only for vertices with a
   * corresponding entry in the table otherwise the old vertex value
   * is used.
   *
   * @note for small tables this function can be much more efficient
   * than leftJoinVertices
   *
   * @example This function is used to update the vertices with new
   * values based on external data.  For example we could add the out
   * degree to each vertex record
   *
   * {{{
   * val rawGraph: Graph[Int,()] = Graph.textFile("webgraph")
   *   .mapVertices(v => 0)
   * val outDeg: RDD[(Int, Int)] = rawGraph.outDegrees()
   * val graph = rawGraph.leftJoinVertices[Int,Int](outDeg,
   *   (v, deg) => deg )
   * }}}
   *
   */
  def joinVertices[U: ClassTag](table: RDD[(VertexID, U)])(mapFunc: (VertexID, VD, U) => VD)
    : Graph[VD, ED] = {
    val uf = (id: VertexID, data: VD, o: Option[U]) => {
      o match {
        case Some(u) => mapFunc(id, data, u)
        case None => data
      }
    }
    graph.outerJoinVertices(table)(uf)
  }

  /**
   * Filter the graph by computing some values to filter on, and applying the predicates.
   *
   * @param preprocess a function to compute new vertex and edge data before filtering
   * @param epred edge pred to filter on after preprocess, see more details under Graph#subgraph
   * @param vpred vertex pred to filter on after prerocess, see more details under Graph#subgraph
   * @tparam VD2 vertex type the vpred operates on
   * @tparam ED2 edge type the epred operates on
   * @return a subgraph of the orginal graph, with its data unchanged
   *
   * @example This function can be used to filter the graph based on some property, without
   * changing the vertex and edge values in your program. For example, we could remove the vertices
   * in a graph with 0 outdegree
   *
   * {{{
   * graph.filter(
   *   graph => {
   *     val degrees: VertexRDD[Int] = graph.outDegrees
   *     graph.outerJoinVertices(degrees) {(vid, data, deg) => deg.getOrElse(0)}
   *   },
   *   vpred = (vid: VertexID, deg:Int) => deg > 0
   * )
   * }}}
   *
   */
  def filter[VD2: ClassTag, ED2: ClassTag](
      preprocess: Graph[VD, ED] => Graph[VD2, ED2],
      epred: (EdgeTriplet[VD2, ED2]) => Boolean = (x: EdgeTriplet[VD2, ED2]) => true,
      vpred: (VertexID, VD2) => Boolean = (v:VertexID, d:VD2) => true): Graph[VD, ED] = {
    graph.mask(preprocess(graph).subgraph(epred, vpred))
  }
} // end of GraphOps
