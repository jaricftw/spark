package spark.api.python

import spark.Partitioner

import java.util.Arrays

/**
 * A [[spark.Partitioner]] that performs handling of byte arrays, for use by the Python API.
 */
class PythonPartitioner(override val numPartitions: Int) extends Partitioner {

  override def getPartition(key: Any): Int = {
    if (key == null) {
      return 0
    }
    else {
      val hashCode = {
        if (key.isInstanceOf[Array[Byte]]) {
          System.err.println("Dumping a byte array!" +           Arrays.hashCode(key.asInstanceOf[Array[Byte]])
          )
          Arrays.hashCode(key.asInstanceOf[Array[Byte]])
        }
        else
          key.hashCode()
      }
      val mod = hashCode % numPartitions
      if (mod < 0) {
        mod + numPartitions
      } else {
        mod // Guard against negative hash codes
      }
    }
  }

  override def equals(other: Any): Boolean = other match {
    case h: PythonPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
