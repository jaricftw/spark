package spark.streaming.dstream

import spark.streaming.{Duration, DStream, Time}
import spark.RDD
import spark.SparkContext._

import scala.reflect.ClassTag

private[streaming]
class FlatMapValuedDStream[K: ClassTag, V: ClassTag, U: ClassTag](
    parent: DStream[(K, V)],
    flatMapValueFunc: V => TraversableOnce[U]
  ) extends DStream[(K, U)](parent.ssc) {

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[(K, U)]] = {
    parent.getOrCompute(validTime).map(_.flatMapValues[U](flatMapValueFunc))
  }
}
