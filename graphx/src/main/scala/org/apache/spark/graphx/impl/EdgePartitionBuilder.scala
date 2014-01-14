package org.apache.spark.graphx.impl

import scala.reflect.ClassTag
import scala.util.Sorting

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.PrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.PrimitiveVector

private[graphx]
class EdgePartitionBuilder[@specialized(Long, Int, Double) ED: ClassTag](size: Int = 64) {
  var edges = new PrimitiveVector[Edge[ED]](size)

  /** Add a new edge to the partition. */
  def add(src: VertexID, dst: VertexID, d: ED) {
    edges += Edge(src, dst, d)
  }

  def toEdgePartition: EdgePartition[ED] = {
    val edgeArray = edges.trim().array
    Sorting.quickSort(edgeArray)(Edge.lexicographicOrdering)
    val srcIds = new Array[VertexID](edgeArray.size)
    val dstIds = new Array[VertexID](edgeArray.size)
    val data = new Array[ED](edgeArray.size)
    val index = new PrimitiveKeyOpenHashMap[VertexID, Int]
    // Copy edges into columnar structures, tracking the beginnings of source vertex id clusters and
    // adding them to the index
    if (edgeArray.length > 0) {
      index.update(srcIds(0), 0)
      var currSrcId: VertexID = srcIds(0)
      var i = 0
      while (i < edgeArray.size) {
        srcIds(i) = edgeArray(i).srcId
        dstIds(i) = edgeArray(i).dstId
        data(i) = edgeArray(i).attr
        if (edgeArray(i).srcId != currSrcId) {
          currSrcId = edgeArray(i).srcId
          index.update(currSrcId, i)
        }
        i += 1
      }
    }
    new EdgePartition(srcIds, dstIds, data, index)
  }
}
