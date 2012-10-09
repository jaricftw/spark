package spark.rdd

import spark.Aggregator
import spark.Partitioner
import spark.RDD
import spark.ShuffleDependency
import spark.SparkEnv
import spark.Split

private[spark] class ShuffledRDDSplit(val idx: Int) extends Split {
  override val index = idx
  override def hashCode(): Int = idx
}

/**
 * The resulting RDD from a shuffle (e.g. repartitioning of data).
 * @param parent the parent RDD.
 * @param aggregator if provided, this aggregator will be used to perform map-side combining.
 * @param part the partitioner used to partition the RDD
 * @tparam K the key class.
 * @tparam V the value class.
 * @tparam C if map side combiners are used, then this is the combiner type; otherwise,
 *           this is the same as V.
 */
class ShuffledRDD[K, V, C](
    @transient parent: RDD[(K, V)],
    aggregator: Option[Aggregator[K, V, C]],
    part: Partitioner) extends RDD[(K, C)](parent.context) {

  override val partitioner = Some(part)

  @transient
  val splits_ = Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))

  override def splits = splits_

  override def preferredLocations(split: Split) = Nil

  val dep = new ShuffleDependency(context.newShuffleId, parent, aggregator, part)
  override val dependencies = List(dep)

  override def compute(split: Split): Iterator[(K, C)] = {
    SparkEnv.get.shuffleFetcher.fetch[K, C](dep.shuffleId, split.index)
  }
}