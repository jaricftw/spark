package spark.streaming.dstream

import spark.RDD
import spark.rdd.UnionRDD
import spark.storage.StorageLevel
import spark.streaming.{Interval, Time, DStream}

private[streaming]
class WindowedDStream[T: ClassManifest](
    parent: DStream[T],
    _windowTime: Time,
    _slideTime: Time) 
  extends DStream[T](parent.ssc) {

  if (!_windowTime.isMultipleOf(parent.slideTime))
    throw new Exception("The window duration of WindowedDStream (" + _slideTime + ") " +
    "must be multiple of the slide duration of parent DStream (" + parent.slideTime + ")")

  if (!_slideTime.isMultipleOf(parent.slideTime))
    throw new Exception("The slide duration of WindowedDStream (" + _slideTime + ") " +
    "must be multiple of the slide duration of parent DStream (" + parent.slideTime + ")")

  parent.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowTime: Time =  _windowTime

  override def dependencies = List(parent)

  override def slideTime: Time = _slideTime

  override def parentRememberDuration: Time = rememberDuration + windowTime

  override def compute(validTime: Time): Option[RDD[T]] = {
    val currentWindow = Interval(validTime - windowTime + parent.slideTime, validTime)
    Some(new UnionRDD(ssc.sc, parent.slice(currentWindow)))
  }
}



