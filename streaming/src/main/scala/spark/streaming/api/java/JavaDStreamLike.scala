package spark.streaming.api.java

import java.util.{List => JList}
import java.lang.{Long => JLong}

import scala.collection.JavaConversions._

import spark.streaming._
import spark.api.java.JavaRDD
import spark.api.java.function.{Function2 => JFunction2, Function => JFunction, _}
import java.util
import spark.RDD
import JavaDStream._

trait JavaDStreamLike[T, This <: JavaDStreamLike[T, This]] extends Serializable {
  implicit val classManifest: ClassManifest[T]

  def dstream: DStream[T]

  implicit def scalaIntToJavaLong(in: DStream[Long]): JavaDStream[JLong] = {
    in.map(new JLong(_))
  }

  /**
   * Print the first ten elements of each RDD generated in this DStream. This is an output
   * operator, so this DStream will be registered as an output stream and there materialized.
   */
  def print() = dstream.print()

  /**
   * Return a new DStream in which each RDD has a single element generated by counting each RDD
   * of this DStream.
   */
  def count(): JavaDStream[JLong] = dstream.count()

  /**
   * Return a new DStream in which each RDD has a single element generated by counting the number
   * of elements in a window over this DStream. windowDuration and slideDuration are as defined in the
   * window() operation. This is equivalent to window(windowDuration, slideDuration).count()
   */
  def countByWindow(windowDuration: Duration, slideDuration: Duration) : JavaDStream[JLong] = {
    dstream.countByWindow(windowDuration, slideDuration)
  }

  /**
   * Return a new DStream in which each RDD is generated by applying glom() to each RDD of
   * this DStream. Applying glom() to an RDD coalesces all elements within each partition into
   * an array.
   */
  def glom(): JavaDStream[JList[T]] =
    new JavaDStream(dstream.glom().map(x => new java.util.ArrayList[T](x.toSeq)))

  /** Return the StreamingContext associated with this DStream */
  def context(): StreamingContext = dstream.context()

  /** Return a new DStream by applying a function to all elements of this DStream. */
  def map[R](f: JFunction[T, R]): JavaDStream[R] = {
    new JavaDStream(dstream.map(f)(f.returnType()))(f.returnType())
  }

  /** Return a new DStream by applying a function to all elements of this DStream. */
  def map[K2, V2](f: PairFunction[T, K2, V2]): JavaPairDStream[K2, V2] = {
    def cm = implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[Tuple2[K2, V2]]]
    new JavaPairDStream(dstream.map(f)(cm))(f.keyType(), f.valueType())
  }

  /**
   * Return a new DStream by applying a function to all elements of this DStream,
   * and then flattening the results
   */
  def flatMap[U](f: FlatMapFunction[T, U]): JavaDStream[U] = {
    import scala.collection.JavaConverters._
    def fn = (x: T) => f.apply(x).asScala
    new JavaDStream(dstream.flatMap(fn)(f.elementType()))(f.elementType())
  }

  /**
   * Return a new DStream by applying a function to all elements of this DStream,
   * and then flattening the results
   */
  def flatMap[K2, V2](f: PairFlatMapFunction[T, K2, V2]): JavaPairDStream[K2, V2] = {
    import scala.collection.JavaConverters._
    def fn = (x: T) => f.apply(x).asScala
    def cm = implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[Tuple2[K2, V2]]]
    new JavaPairDStream(dstream.flatMap(fn)(cm))(f.keyType(), f.valueType())
  }

    /**
   * Return a new DStream in which each RDD is generated by applying mapPartitions() to each RDDs
   * of this DStream. Applying mapPartitions() to an RDD applies a function to each partition
   * of the RDD.
   */
  def mapPartitions[U](f: FlatMapFunction[java.util.Iterator[T], U]): JavaDStream[U] = {
    def fn = (x: Iterator[T]) => asScalaIterator(f.apply(asJavaIterator(x)).iterator())
    new JavaDStream(dstream.mapPartitions(fn)(f.elementType()))(f.elementType())
  }

  /**
   * Return a new DStream in which each RDD is generated by applying mapPartitions() to each RDDs
   * of this DStream. Applying mapPartitions() to an RDD applies a function to each partition
   * of the RDD.
   */
  def mapPartitions[K, V](f: PairFlatMapFunction[java.util.Iterator[T], K, V])
  : JavaPairDStream[K, V] = {
    def fn = (x: Iterator[T]) => asScalaIterator(f.apply(asJavaIterator(x)).iterator())
    new JavaPairDStream(dstream.mapPartitions(fn))(f.keyType(), f.valueType())
  }

  /**
   * Return a new DStream in which each RDD has a single element generated by reducing each RDD
   * of this DStream.
   */
  def reduce(f: JFunction2[T, T, T]): JavaDStream[T] = dstream.reduce(f)

  /**
   * Return a new DStream in which each RDD has a single element generated by reducing all
   * elements in a window over this DStream. windowDuration and slideDuration are as defined in the
   * window() operation. This is equivalent to window(windowDuration, slideDuration).reduce(reduceFunc)
   */
  def reduceByWindow(
      reduceFunc: JFunction2[T, T, T],
      invReduceFunc: JFunction2[T, T, T],
      windowDuration: Duration,
      slideDuration: Duration
    ): JavaDStream[T] = {
    dstream.reduceByWindow(reduceFunc, invReduceFunc, windowDuration, slideDuration)
  }

  /**
   * Return all the RDDs between 'fromDuration' to 'toDuration' (both included)
   */
  def slice(fromDuration: Duration, toDuration: Duration): JList[JavaRDD[T]] = {
    new util.ArrayList(dstream.slice(fromDuration, toDuration).map(new JavaRDD(_)).toSeq)
  }

  /**
   * Apply a function to each RDD in this DStream. This is an output operator, so
   * this DStream will be registered as an output stream and therefore materialized.
   */
  def foreach(foreachFunc: JFunction[JavaRDD[T], Void]) {
    dstream.foreach(rdd => foreachFunc.call(new JavaRDD(rdd)))
  }

  /**
   * Apply a function to each RDD in this DStream. This is an output operator, so
   * this DStream will be registered as an output stream and therefore materialized.
   */
  def foreach(foreachFunc: JFunction2[JavaRDD[T], Time, Void]) {
    dstream.foreach((rdd, time) => foreachFunc.call(new JavaRDD(rdd), time))
  }

  /**
   * Return a new DStream in which each RDD is generated by applying a function
   * on each RDD of this DStream.
   */
  def transform[U](transformFunc: JFunction[JavaRDD[T], JavaRDD[U]]): JavaDStream[U] = {
    implicit val cm: ClassManifest[U] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[U]]
    def scalaTransform (in: RDD[T]): RDD[U] =
      transformFunc.call(new JavaRDD[T](in)).rdd
    dstream.transform(scalaTransform(_))
  }

  /**
   * Return a new DStream in which each RDD is generated by applying a function
   * on each RDD of this DStream.
   */
  def transform[U](transformFunc: JFunction2[JavaRDD[T], Time, JavaRDD[U]]): JavaDStream[U] = {
    implicit val cm: ClassManifest[U] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[U]]
    def scalaTransform (in: RDD[T], time: Time): RDD[U] =
      transformFunc.call(new JavaRDD[T](in), time).rdd
    dstream.transform(scalaTransform(_, _))
  }

  /**
   * Enable periodic checkpointing of RDDs of this DStream
   * @param interval Time interval after which generated RDD will be checkpointed
   */
  def checkpoint(interval: Duration) = {
    dstream.checkpoint(interval)
  }
}