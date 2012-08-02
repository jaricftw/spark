package spark.streaming

import spark.RDD

/**
 * An input stream that always returns the same RDD on each timestep. Useful for testing.
 */
class ConstantInputRDS[T: ClassManifest](ssc: SparkStreamContext, rdd: RDD[T])
  extends InputRDS[T](ssc) {

  override def start() {}

  override def stop() {}

  override def compute(validTime: Time): Option[RDD[T]] = {
    Some(rdd)
  }
}