package spark.rdd

import scala.reflect.ClassTag
import spark.{RDD, Partition, TaskContext}

private[spark] class GlommedRDD[T: ClassTag](prev: RDD[T])
  extends RDD[Array[T]](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) =
    Array(firstParent[T].iterator(split, context).toArray).iterator
}
