package org.apache.spark.graph.impl

import org.apache.spark.util.collection.PrimitiveVector
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.graph._
import org.apache.spark.graph.impl.GraphImpl._
import org.apache.spark.graph.impl.MsgRDDFunctions._
import org.apache.spark.graph.util.BytecodeUtils
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ClosureCleaner


/**
 * A Graph RDD that supports computation on graphs.
 *
 * Graphs are represented using two classes of data: vertex-partitioned and
 * edge-partitioned. `vTable` contains vertex attributes, which are
 * vertex-partitioned. `eTable` contains edge attributes, which are
 * edge-partitioned. For operations on vertex neighborhoods, vertex attributes
 * are replicated to the edge partitions where they appear as sources or
 * destinations. `vertexPlacement` specifies where each vertex will be
 * replicated. `vTableReplicated` stores the replicated vertex attributes, which
 * are co-partitioned with the relevant edges.
 *
 * mask in vertices means filter
 * mask in vTableReplicated means active
 */
class GraphImpl[VD: ClassManifest, ED: ClassManifest] protected (
    @transient val vertices: VertexRDD[VD],
    @transient val edges: EdgeRDD[ED],
    @transient val vertexPlacement: VertexPlacement,
    @transient val vTableReplicated: VTableReplicated[VD])
  extends Graph[VD, ED] {

  def this(
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED],
      vertexPlacement: VertexPlacement) = {
    this(vertices, edges, vertexPlacement, new VTableReplicated(vertices, edges, vertexPlacement))
  }

  def this(
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED]) = {
    this(vertices, edges, new VertexPlacement(edges, vertices))
  }

  /** Return a RDD that brings edges together with their source and destination vertices. */
  @transient override val triplets: RDD[EdgeTriplet[VD, ED]] = {
    val vdManifest = classManifest[VD]
    val edManifest = classManifest[ED]

    edges.zipEdgePartitions(vTableReplicated.get(true, true)) { (ePart, vPartIter) =>
      val (_, vPart) = vPartIter.next()
      new EdgeTripletIterator(vPart.index, vPart.values, ePart)(vdManifest, edManifest)
    }
  }

  override def persist(newLevel: StorageLevel): Graph[VD, ED] = {
    vertices.persist(newLevel)
    edges.persist(newLevel)
    this
  }

  override def cache(): Graph[VD, ED] = persist(StorageLevel.MEMORY_ONLY)

  override def partitionBy(partitionStrategy: PartitionStrategy): Graph[VD, ED] = {
    val numPartitions = edges.partitions.size
    val edManifest = classManifest[ED]
    val newEdges = new EdgeRDD(edges.map { e =>
      val part: Pid = partitionStrategy.getPartition(e.srcId, e.dstId, numPartitions)

      // Should we be using 3-tuple or an optimized class
      new MessageToPartition(part, (e.srcId, e.dstId, e.attr))
    }
      .partitionBy(new HashPartitioner(numPartitions))
      .mapPartitionsWithIndex( { (pid, iter) =>
        val builder = new EdgePartitionBuilder[ED]()(edManifest)
        iter.foreach { message =>
          val data = message.data
          builder.add(data._1, data._2, data._3)
        }
        val edgePartition = builder.toEdgePartition
        Iterator((pid, edgePartition))
      }, preservesPartitioning = true).cache())
    new GraphImpl(vertices, newEdges)
  }

  override def statistics: Map[String, Any] = {
    // Get the total number of vertices after replication, used to compute the replication ratio.
    def numReplicatedVertices(vid2pids: RDD[Array[Array[Vid]]]): Double = {
      vid2pids.map(_.map(_.size).sum.toLong).reduce(_ + _).toDouble
    }

    val numVertices = this.ops.numVertices
    val numEdges = this.ops.numEdges
    val replicationRatioBoth = numReplicatedVertices(vertexPlacement.bothAttrs) / numVertices
    val replicationRatioSrcOnly = numReplicatedVertices(vertexPlacement.srcAttrOnly) / numVertices
    val replicationRatioDstOnly = numReplicatedVertices(vertexPlacement.dstAttrOnly) / numVertices
    // One entry for each partition, indicate the total number of edges on that partition.
    val loadArray = edges.partitionsRDD.map(_._2.size).collect().map(_.toDouble / numEdges)
    val minLoad = loadArray.min
    val maxLoad = loadArray.max
    Map(
      "Num Vertices" -> numVertices,
      "Num Edges" -> numEdges,
      "Replication (both)" -> replicationRatioBoth,
      "Replication (src only)" -> replicationRatioSrcOnly,
      "Replication (dest only)" -> replicationRatioDstOnly,
      "Load Array" -> loadArray,
      "Min Load" -> minLoad,
      "Max Load" -> maxLoad)
  }

  /**
   * Display the lineage information for this graph.
   */
  def printLineage() = {
    def traverseLineage(
        rdd: RDD[_],
        indent: String = "",
        visited: Map[Int, String] = Map.empty[Int, String]) {
      if (visited.contains(rdd.id)) {
        println(indent + visited(rdd.id))
        println(indent)
      } else {
        val locs = rdd.partitions.map( p => rdd.preferredLocations(p) )
        val cacheLevel = rdd.getStorageLevel
        val name = rdd.id
        val deps = rdd.dependencies
        val partitioner = rdd.partitioner
        val numparts = partitioner match { case Some(p) => p.numPartitions; case None => 0}
        println(indent + name + ": " + cacheLevel.description + " (partitioner: " + partitioner +
          ", " + numparts +")")
        println(indent + " |--->  Deps:    " + deps.map(d => (d, d.rdd.id) ).toString)
        println(indent + " |--->  PrefLoc: " + locs.map(x=> x.toString).mkString(", "))
        deps.foreach(d => traverseLineage(d.rdd, indent + " | ", visited))
      }
    }
    println("eTable ------------------------------------------")
    traverseLineage(edges, "  ")
    var visited = Map(edges.id -> "eTable")
    println("\n\nvTable ------------------------------------------")
    traverseLineage(vertices, "  ", visited)
    visited += (vertices.id -> "vTable")
    println("\n\nvertexPlacement.bothAttrs -------------------------------")
    traverseLineage(vertexPlacement.bothAttrs, "  ", visited)
    visited += (vertexPlacement.bothAttrs.id -> "vertexPlacement.bothAttrs")
    println("\n\ntriplets ----------------------------------------")
    traverseLineage(triplets, "  ", visited)
    println(visited)
  } // end of printLineage

  override def reverse: Graph[VD, ED] =
    new GraphImpl(vertices, edges.mapEdgePartitions(_.reverse), vertexPlacement, vTableReplicated)

  override def mapVertices[VD2: ClassManifest](f: (Vid, VD) => VD2): Graph[VD2, ED] = {
    if (classManifest[VD] equals classManifest[VD2]) {
      // The map preserves type, so we can use incremental replication
      val newVerts = vertices.mapVertexPartitions(_.map(f))
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newVTableReplicated = new VTableReplicated[VD2](
        changedVerts, edges, vertexPlacement,
        Some(vTableReplicated.asInstanceOf[VTableReplicated[VD2]]))
      new GraphImpl(newVerts, edges, vertexPlacement, newVTableReplicated)
    } else {
      // The map does not preserve type, so we must re-replicate all vertices
      new GraphImpl(vertices.mapVertexPartitions(_.map(f)), edges, vertexPlacement)
    }
  }

  override def mapEdges[ED2: ClassManifest](f: Edge[ED] => ED2): Graph[VD, ED2] =
    new GraphImpl(vertices, edges.mapEdgePartitions(_.map(f)), vertexPlacement, vTableReplicated)

  override def mapTriplets[ED2: ClassManifest](f: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2] = {
    // Use an explicit manifest in PrimitiveKeyOpenHashMap init so we don't pull in the implicit
    // manifest from GraphImpl (which would require serializing GraphImpl).
    val vdManifest = classManifest[VD]
    val newETable =
      edges.zipEdgePartitions(vTableReplicated.get(true, true)) { (edgePartition, vTableReplicatedIter) =>
        val (pid, vPart) = vTableReplicatedIter.next()
        val et = new EdgeTriplet[VD, ED]
        val newEdgePartition = edgePartition.map { e =>
          et.set(e)
          et.srcAttr = vPart(e.srcId)
          et.dstAttr = vPart(e.dstId)
          f(et)
        }
        Iterator((pid, newEdgePartition))
    }
    new GraphImpl(vertices, new EdgeRDD(newETable), vertexPlacement, vTableReplicated)
  }

  override def subgraph(
      epred: EdgeTriplet[VD, ED] => Boolean = x => true,
      vpred: (Vid, VD) => Boolean = (a, b) => true): Graph[VD, ED] = {
    // Filter the vertices, reusing the partitioner and the index from this graph
    val newVerts = vertices.mapVertexPartitions(_.filter(vpred))

    // Filter the edges
    val edManifest = classManifest[ED]
    val newEdges = new EdgeRDD[ED](triplets.filter { et =>
      vpred(et.srcId, et.srcAttr) && vpred(et.dstId, et.dstAttr) && epred(et)
    }.mapPartitionsWithIndex( { (pid, iter) =>
      val builder = new EdgePartitionBuilder[ED]()(edManifest)
      iter.foreach { et => builder.add(et.srcId, et.dstId, et.attr) }
      val edgePartition = builder.toEdgePartition
      Iterator((pid, edgePartition))
    }, preservesPartitioning = true)).cache()

    // Reuse the previous VTableReplicated unmodified. It will contain extra vertices, which is
    // fine.
    new GraphImpl(newVerts, newEdges, new VertexPlacement(newEdges, newVerts), vTableReplicated)
  } // end of subgraph

  override def mask[VD2: ClassManifest, ED2: ClassManifest] (
      other: Graph[VD2, ED2]): Graph[VD, ED] = {
    val newVerts = vertices.innerJoin(other.vertices) { (vid, v, w) => v }
    val newEdges = edges.innerJoin(other.edges) { (src, dst, v, w) => v }
    new GraphImpl(newVerts, newEdges)

  }

  override def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED] = {
    ClosureCleaner.clean(merge)
    val newETable = edges.mapEdgePartitions(_.groupEdges(merge))
    new GraphImpl(vertices, newETable, vertexPlacement, vTableReplicated)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Lower level transformation methods
  //////////////////////////////////////////////////////////////////////////////////////////////////

  override def mapReduceTriplets[A: ClassManifest](
      mapFunc: EdgeTriplet[VD, ED] => Iterator[(Vid, A)],
      reduceFunc: (A, A) => A,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)] = None) = {

    ClosureCleaner.clean(mapFunc)
    ClosureCleaner.clean(reduceFunc)

    // For each vertex, replicate its attribute only to partitions where it is
    // in the relevant position in an edge.
    val mapUsesSrcAttr = accessesVertexAttr[VD, ED](mapFunc, "srcAttr")
    val mapUsesDstAttr = accessesVertexAttr[VD, ED](mapFunc, "dstAttr")
    val vs = activeSetOpt match {
      case Some((activeSet, _)) => vTableReplicated.get(mapUsesSrcAttr, mapUsesDstAttr, activeSet)
      case None => vTableReplicated.get(mapUsesSrcAttr, mapUsesDstAttr)
    }
    val activeDirectionOpt = activeSetOpt.map(_._2)

    // Map and combine.
    val preAgg = edges.zipEdgePartitions(vs) { (edgePartition, vTableReplicatedIter) =>
      val (_, vPart) = vTableReplicatedIter.next()

      // Choose scan method
      val activeFraction = vPart.numActives.getOrElse(0) / edgePartition.indexSize.toFloat
      val edgeIter = activeDirectionOpt match {
        case Some(EdgeDirection.Both) =>
          if (activeFraction < 0.8) {
            edgePartition.indexIterator(srcVid => vPart.isActive(srcVid))
              .filter(e => vPart.isActive(e.dstId))
          } else {
            edgePartition.iterator.filter(e => vPart.isActive(e.srcId) && vPart.isActive(e.dstId))
          }
        case Some(EdgeDirection.Out) =>
          if (activeFraction < 0.8) {
            edgePartition.indexIterator(srcVid => vPart.isActive(srcVid))
          } else {
            edgePartition.iterator.filter(e => vPart.isActive(e.srcId))
          }
        case Some(EdgeDirection.In) =>
          edgePartition.iterator.filter(e => vPart.isActive(e.dstId))
        case None =>
          edgePartition.iterator
      }

      // Scan edges and run the map function
      val et = new EdgeTriplet[VD, ED]
      val mapOutputs = edgeIter.flatMap { e =>
        et.set(e)
        if (mapUsesSrcAttr) {
          et.srcAttr = vPart(e.srcId)
        }
        if (mapUsesDstAttr) {
          et.dstAttr = vPart(e.dstId)
        }
        mapFunc(et)
      }
      // Note: This doesn't allow users to send messages to arbitrary vertices.
      vPart.aggregateUsingIndex(mapOutputs, reduceFunc).iterator
    }

    // do the final reduction reusing the index map
    vertices.aggregateUsingIndex(preAgg, reduceFunc)
  } // end of mapReduceTriplets

  override def outerJoinVertices[U: ClassManifest, VD2: ClassManifest]
      (updates: RDD[(Vid, U)])(updateF: (Vid, VD, Option[U]) => VD2): Graph[VD2, ED] = {
    if (classManifest[VD] equals classManifest[VD2]) {
      // updateF preserves type, so we can use incremental replication
      val newVerts = vertices.leftJoin(updates)(updateF)
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newVTableReplicated = new VTableReplicated[VD2](
        changedVerts, edges, vertexPlacement,
        Some(vTableReplicated.asInstanceOf[VTableReplicated[VD2]]))
      new GraphImpl(newVerts, edges, vertexPlacement, newVTableReplicated)
    } else {
      // updateF does not preserve type, so we must re-replicate all vertices
      val newVerts = vertices.leftJoin(updates)(updateF)
      new GraphImpl(newVerts, edges, vertexPlacement)
    }
  }

  private def accessesVertexAttr[VD, ED](closure: AnyRef, attrName: String): Boolean = {
    try {
      BytecodeUtils.invokedMethod(closure, classOf[EdgeTriplet[VD, ED]], attrName)
    } catch {
      case _: ClassNotFoundException => true // if we don't know, be conservative
    }
  }
} // end of class GraphImpl


object GraphImpl {

  def apply[VD: ClassManifest, ED: ClassManifest](
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD): GraphImpl[VD, ED] =
  {
    fromEdgeRDD(createETable(edges), defaultVertexAttr)
  }

  def fromEdgePartitions[VD: ClassManifest, ED: ClassManifest](
      edges: RDD[(Pid, EdgePartition[ED])],
      defaultVertexAttr: VD): GraphImpl[VD, ED] = {
    fromEdgeRDD(createETableFromEdgePartitions(edges), defaultVertexAttr)
  }

  def apply[VD: ClassManifest, ED: ClassManifest](
      vertices: RDD[(Vid, VD)],
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD): GraphImpl[VD, ED] =
  {
    val etable = createETable(edges).cache()

    // Get the set of all vids
    val partitioner = Partitioner.defaultPartitioner(vertices)
    val vPartitioned = vertices.partitionBy(partitioner)
    val vidsFromEdges = collectVidsFromEdges(etable, partitioner)
    val vids = vPartitioned.zipPartitions(vidsFromEdges) { (vertexIter, vidsFromEdgesIter) =>
      vertexIter.map(_._1) ++ vidsFromEdgesIter.map(_._1)
    }

    val vtable = VertexRDD(vids, vPartitioned, defaultVertexAttr)

    new GraphImpl(vtable, etable)
  }

  /**
   * Create the edge table RDD, which is much more efficient for Java heap storage than the
   * normal edges data structure (RDD[(Vid, Vid, ED)]).
   *
   * The edge table contains multiple partitions, and each partition contains only one RDD
   * key-value pair: the key is the partition id, and the value is an EdgePartition object
   * containing all the edges in a partition.
   */
  private def createETable[ED: ClassManifest](
      edges: RDD[Edge[ED]]): EdgeRDD[ED] = {
    val eTable = edges.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[ED]
      iter.foreach { e =>
        builder.add(e.srcId, e.dstId, e.attr)
      }
      Iterator((pid, builder.toEdgePartition))
    }
    new EdgeRDD(eTable)
  }

  private def createETableFromEdgePartitions[ED: ClassManifest](
      edges: RDD[(Pid, EdgePartition[ED])]): EdgeRDD[ED] = {
    new EdgeRDD(edges)
  }

  private def fromEdgeRDD[VD: ClassManifest, ED: ClassManifest](
      edges: EdgeRDD[ED],
      defaultVertexAttr: VD): GraphImpl[VD, ED] = {
    edges.cache()
    // Get the set of all vids
    val vids = collectVidsFromEdges(edges, new HashPartitioner(edges.partitions.size))
    // Create the VertexRDD.
    val vtable = VertexRDD(vids.mapValues(x => defaultVertexAttr))
    new GraphImpl(vtable, edges)
  }

  /** Collects all vids mentioned in edges and partitions them by partitioner. */
  private def collectVidsFromEdges(
      edges: EdgeRDD[_],
      partitioner: Partitioner): RDD[(Vid, Int)] = {
    // TODO: Consider doing map side distinct before shuffle.
    new ShuffledRDD[Vid, Int, (Vid, Int)](
      edges.collectVids.map(vid => (vid, 0)), partitioner)
      .setSerializer(classOf[VidMsgSerializer].getName)
  }
} // end of object GraphImpl
