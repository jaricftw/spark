package org.apache.spark.graphx

import org.scalatest.Suite
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Provides a method to run tests against a {@link SparkContext} variable that is correctly stopped
 * after each test.
*/
trait LocalSparkContext {
  /** Runs `f` on a new SparkContext and ensures that it is stopped afterwards. */
  def withSpark[T](f: SparkContext => T) = {
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")
    val sc = new SparkContext("local", "test", conf)
    try {
      f(sc)
    } finally {
      sc.stop()
      // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")
    }
  }
}
