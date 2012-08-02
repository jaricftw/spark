package spark.streaming

import spark.RDD
import spark.UnionRDD

import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer

class QueueInputRDS[T: ClassManifest](
    ssc: SparkStreamContext,
    val queue: Queue[RDD[T]],
    oneAtATime: Boolean,
    defaultRDD: RDD[T]
  ) extends InputRDS[T](ssc) {
  
  override def start() { }
  
  override def stop() { }
  
  override def compute(validTime: Time): Option[RDD[T]] = {
    val buffer = new ArrayBuffer[RDD[T]]()
    if (oneAtATime && queue.size > 0) {
      buffer += queue.dequeue()
    } else {
      buffer ++= queue
    }
    if (buffer.size > 0) {
      Some(new UnionRDD(ssc.sc, buffer.toSeq))
    } else if (defaultRDD != null) {
      Some(defaultRDD)
    } else {
      None
    }
  }
  
}