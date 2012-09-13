---
layout: global
title: Spark Programming Guide
---
At a high level, every Spark application consists of a *driver program* that runs the user's `main` function and executes various *parallel operations* on a cluster. The main abstraction Spark provides is a *distributed dataset*, which is a collection of elements partitioned across the nodes of the cluster that can be operated on in parallel. Distributed datasets are created by starting with a file in the Hadoop file system (HDFS) or an existing collection in the driver program and possibly transforming it. Users may also ask Spark to *cache* a dataset in memory, allowing it to be reused efficiently across parallel operations. Finally, distributed datasets automatically recover from node failures.

A second abstraction in Spark is *shared variables* that can be used in parallel operations. By default, when Spark runs a function in parallel as a set of tasks on different nodes, it ships a copy of each variable used in the function to each task. Sometimes, a variable needs to be shared across tasks, or between tasks and the driver program. Spark supports two types of shared variables: *broadcast variables*, which can be used to cache a value in memory on all nodes, and *accumulators*, which are variables that are only "added" to, such as counters and sums.

This guide shows each of these features and walks through some samples. It assumes some familiarity with Scala, especially with the syntax for [closures](http://www.scala-lang.org/node/133). Note that Spark can be run interactively through the `spark-shell` interpreter. You might want to do that to follow along!

# Linking with Spark

To write a Spark application, you will need to add both Spark and its dependencies to your CLASSPATH. The easiest way to do this is to run `sbt/sbt assembly` to build both Spark and its dependencies into one JAR (`core/target/spark-core-assembly-0.5.0.jar`), then add this to your CLASSPATH. Alternatively, you can publish Spark to the Maven cache on your machine using `sbt/sbt publish-local`. It will be an artifact called `spark-core` under the organization `org.spark-project`.

In addition, you'll need to import some Spark classes and implicit conversions. Add the following lines at the top of your program:

{% highlight scala %}
import spark.SparkContext
import SparkContext._
{% endhighlight %}

# Initializing Spark

The first thing a Spark program must do is to create a `SparkContext` object, which tells Spark how to access a cluster.
This is done through the following constructor:

{% highlight scala %}
new SparkContext(master, jobName, [sparkHome], [jars])
{% endhighlight %}

The `master` parameter is a string specifying a [Mesos]({{HOME_PATH}}running-on-mesos.html) cluster to connect to, or a special "local" string to run in local mode, as described below. `jobName` is a name for your job, which will be shown in the Mesos web UI when running on a cluster. Finally, the last two parameters are needed to deploy your code to a cluster if running on Mesos, as described later.

In the Spark interpreter, a special interpreter-aware SparkContext is already created for you, in the variable called `sc`. Making your own SparkContext will not work. You can set which master the context connects to using the `MASTER` environment variable. For example, run `MASTER=local[4] ./spark-shell` to run locally with four cores.

### Master Names

The master name can be in one of three formats:

<table class="table">
<tr><th>Master Name</th><th>Meaning</th></tr>
<tr><td> local </td><td> Run Spark locally with one worker thread (i.e. no parallelism at all). </td></tr>
<tr><td> local[K] </td><td> Run Spark locally with K worker threads (which should be set to the number of cores on your machine). </td></tr>
<tr><td> HOST:PORT </td><td> Connect Spark to the given (Mesos)({{HOME_PATH}}running-on-mesos.html) master to run on a cluster. The host parameter is the hostname of the Mesos master. The port must be whichever one the master is configured to use, which is 5050 by default. 
<br /><br />
<strong>NOTE:</strong> In earlier versions of Mesos (the <code>old-mesos</code> branch of Spark), you need to use master@HOST:PORT.
</td></tr>
</table>

### Deploying to a Cluster

If you want to run your job on a cluster, you will need to specify the two optional parameters:

* `sparkHome`: The path at which Spark is installed on your worker machines (it should be the same on all of them).
* `jars`: A list of JAR files on the local machine containing your job's code and any dependencies, which Spark will deploy to all the worker nodes. You'll need to package your job into a set of JARs using your build system. For example, if you're using SBT, the [sbt-assembly](https://github.com/sbt/sbt-assembly) plugin is a good way to make a single JAR with your code and dependencies.

If some classes will be shared across _all_ your jobs, it's also possible to copy them to the workers manually and set the `SPARK_CLASSPATH` environment variable in `conf/spark-env.sh` to point to them; see [Configuration]({{HOME_PATH}}configuration.html) for details.


# Distributed Datasets

Spark revolves around the concept of a _resilient distributed dataset_ (RDD), which is a fault-tolerant collection of elements that can be operated on in parallel. There are currently two types of RDDs: *parallelized collections*, which take an existing Scala collection and run functions on it in parallel, and *Hadoop datasets*, which run functions on each record of a file in Hadoop distributed file system or any other storage system supported by Hadoop. Both types of RDDs can be operated on through the same methods.

## Parallelized Collections

Parallelized collections are created by calling `SparkContext`'s `parallelize` method on an existing Scala collection (a `Seq` object). The elements of the collection are copied to form a distributed dataset that can be operated on in parallel. For example, here is some interpreter output showing how to create a parallel collection from an array:

{% highlight scala %}
scala> val data = Array(1, 2, 3, 4, 5)
data: Array[Int] = Array(1, 2, 3, 4, 5)

scala> val distData = sc.parallelize(data)
distData: spark.RDD[Int] = spark.ParallelCollection@10d13e3e
{% endhighlight %}

Once created, the distributed dataset (`distData` here) can be operated on in parallel. For example, we might call `distData.reduce(_ + _)` to add up the elements of the array. We describe operations on distributed datasets later on.

One important parameter for parallel collections is the number of *slices* to cut the dataset into. Spark will run one task for each slice of the cluster. Typically you want 2-4 slices for each CPU in your cluster. Normally, Spark tries to set the number of slices automatically based on your cluster. However, you can also set it manually by passing it as a second parameter to `parallelize` (e.g. `sc.parallelize(data, 10)`).

## Hadoop Datasets

Spark can create distributed datasets from any file stored in the Hadoop distributed file system (HDFS) or other storage systems supported by Hadoop (including your local file system, [Amazon S3](http://wiki.apache.org/hadoop/AmazonS3), Hypertable, HBase, etc). Spark supports text files, [SequenceFiles](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/mapred/SequenceFileInputFormat.html), and any other Hadoop InputFormat.

Text file RDDs can be created using `SparkContext`'s `textFile` method. This method takes an URI for the file (either a local path on the machine, or a `hdfs://`, `s3n://`, `kfs://`, etc URI). Here is an example invocation:

{% highlight scala %}
scala> val distFile = sc.textFile("data.txt")
distFile: spark.RDD[String] = spark.HadoopRDD@1d4cee08
{% endhighlight %}

Once created, `distFile` can be acted on by dataset operations. For example, we can add up the sizes of all the lines using the `map` and `reduce` operations as follows: `distFile.map(_.size).reduce(_ + _)`.

The `textFile` method also takes an optional second argument for controlling the number of slices of the file. By default, Spark creates one slice for each block of the file (blocks being 64MB by default in HDFS), but you can also ask for a higher number of slices by passing a larger value. Note that you cannot have fewer slices than blocks.

For [SequenceFiles](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/mapred/SequenceFileInputFormat.html), use SparkContext's `sequenceFile[K, V]` method where `K` and `V` are the types of key and values in the file. These should be subclasses of Hadoop's [Writable](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/Writable.html) interface, like [IntWritable](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/IntWritable.html) and [Text](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/Text.html). In addition, Spark allows you to specify native types for a few common Writables; for example, `sequenceFile[Int, String]` will automatically read IntWritables and Texts.

Finally, for other Hadoop InputFormats, you can use the `SparkContext.hadoopRDD` method, which takes an arbitrary `JobConf` and input format class, key class and value class. Set these the same way you would for a Hadoop job with your input source.

## Distributed Dataset Operations

Distributed datasets support two types of operations: *transformations*, which create a new dataset from an existing one, and *actions*, which return a value to the driver program after running a computation on the dataset. For example, `map` is a transformation that passes each dataset element through a function and returns a new distributed dataset representing the results. On the other hand, `reduce` is an action that aggregates all the elements of the dataset using some function and returns the final result to the driver program (although there is also a parallel `reduceByKey` that returns a distributed dataset).

All transformations in Spark are <i>lazy</i>, in that they do not compute their results right away. Instead, they just remember the transformations applied to some base dataset (e.g. a file). The transformations are only computed when an action requires a result to be returned to the driver program. This design enables Spark to run more efficiently -- for example, we can realize that a dataset created through `map` will be used in a `reduce` and return only the result of the `reduce` to the driver, rather than the larger mapped dataset.

One important transformation provided by Spark is *caching*. When you cache a distributed dataset, each node stores any slices of it that it computes in memory and reuses them in other actions on that dataset (or datasets derived from it). This allows future actions to be much faster (often by more than 10x). Caching is a key tool for building iterative algorithms with Spark and for interactive use from the interpreter.

The following tables list the transformations and actions supported:

### Transformations

<table class="table">
<tr><th>Transformation</th><th>Meaning</th></tr>
<tr><td> map(<i>func</i>) </td><td> Return a new distributed dataset formed by passing each element of the source through a function <i>func</i>. </td></tr>
<tr><td> filter(<i>func</i>) </td><td> Return a new dataset formed by selecting those elements of the source on which <i>func</i> returns true. </td></tr>
<tr><td> flatMap(<i>func</i>) </td><td> Similar to map, but each input item can be mapped to 0 or more output items (so <i>func</i> should return a Seq rather than a single item). </td></tr>
<tr><td> sample(<i>withReplacement</i>, <i>frac</i>, <i>seed</i>) </td><td> Sample a fraction <i>frac</i> of the data, with or without replacement, using a given random number seed. </td></tr>
<tr><td> union(otherDataset) </td><td> Return a new dataset that contains the union of the elements in the source dataset and the argument. </td></tr>
<tr><td> groupByKey([numTasks]) </td><td> When called on a dataset of (K, V) pairs, returns a dataset of (K, Seq[V]) pairs. 
<strong>Note:</strong> By default, this uses only 8 parallel tasks to do the grouping. You can pass an optional <code>numTasks</code> argument to set a different number of tasks.
</td></tr>
<tr><td> reduceByKey(func, [numTasks]) </td><td> When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function. Like in <code>groupByKey</code>, the number of reduce tasks is configurable through an optional second argument. </td></tr>
<tr><td> join(otherDataset, [numTasks]) </td><td> When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. </td></tr>
<tr><td> groupWith(otherDataset, [numTasks]) </td><td> When called on datasets of type (K, V) and (K, W), returns a dataset of (K, Seq[V], Seq[W]) tuples. This operation is also called CoGroup in other frameworks. </td></tr>
<tr><td> cartesian(otherDataset) </td><td> When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements). </td></tr>
<tr><td> sortByKey([ascendingOrder]) </td><td> When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified in the boolean <code>ascendingOrder</code> argument.</td></tr>
</table>

### Actions
<table class="table">
<tr><th>Action</th><th>Meaning</th></tr>
<tr><td> reduce(<i>func</i>) </td><td> Aggregate the elements of the dataset using a function <i>func</i> (which takes two arguments and returns one). The function should be associative so that it can be computed correctly in parallel. </td></tr>
<tr><td> collect() </td><td> Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data. </td></tr>
<tr><td> count() </td><td> Return the number of elements in the dataset. </td></tr>
<tr><td> take(<i>n</i>) </td><td> Return an array with the first <i>n</i> elements of the dataset. Note that this is currently not executed in parallel. Instead, the driver program computes all the elements. </td></tr>
<tr><td> first() </td><td> Return the first element of the dataset (similar to take(1)). </td></tr>
<tr><td> saveAsTextFile(path) </td><td> Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file. </td></tr>
<tr><td> saveAsSequenceFile(path) </td><td> Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, HDFS or any other Hadoop-supported file system. This is only available on RDDs of key-value pairs that either implement Hadoop's Writable interface or are implicitly convertible to Writable (Spark includes conversions for basic types like Int, Double, String, etc). </td></tr>
<tr><td> foreach(<i>func</i>) </td><td> Run a function <i>func</i> on each element of the dataset. This is usually done for side effects such as updating an accumulator variable (see below) or interacting with external storage systems. </td></tr>
</table>

### Caching
Calling `cache()` on an RDD asks that it be stored in memory after the first time it is computed. Different partitions of the dataset will be stored on the different cluster nodes that computed them, making subsequent uses of the dataset faster. The cache is fault-tolerant -- if any partition of an RDD is lost, it will be recomputed using the transformations that originally created it.

# Shared Variables

Normally, when a function passed to a Spark operation (such as `map` or `reduce`) is executed on a remote cluster node, it works on separate copies of all the variables used in the function. These variables are copied to each machine, and no updates to the variables on the remote machine are propagated back to the driver program. Supporting general, read-write shared variables across tasks would be inefficient. However, Spark does provide two limited types of *shared variables* for two common usage patterns: broadcast variables and accumulators.

## Broadcast Variables

Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a large input dataset in an efficient manner. Spark also attempts to distribute broadcast variables using efficient broadcast algorithms to reduce communication cost.

Broadcast variables are created from a variable `v` by calling `SparkContext.broadcast(v)`. The broadcast variable is a wrapper around `v`, and its value can be accessed by calling the `value` method. The interpreter session below shows this:

{% highlight scala %}
scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
broadcastVar: spark.Broadcast[Array[Int]] = spark.Broadcast(b5c40191-a864-4c7d-b9bf-d87e1a4e787c)

scala> broadcastVar.value
res0: Array[Int] = Array(1, 2, 3)
{% endhighlight %}

After the broadcast variable is created, it should be used instead of the value `v` in any functions run on the cluster so that `v` is not shipped to the nodes more than once. In addition, the object `v` should not be modified after it is broadcast in order to ensure that all nodes get the same value of the broadcast variable (e.g. if the variable is shipped to a new node later).

## Accumulators

Accumulators are variables that are only "added" to through an associative operation and can therefore be efficiently supported in parallel. They can be used to implement counters (as in MapReduce) or sums. Spark natively supports accumulators of type Int and Double, and programmers can add support for new types.

An accumulator is created from an initial value `v` by calling `SparkContext.accumulator(v)`. Tasks running on the cluster can then add to it using the `+=` operator. However, they cannot read its value. Only the driver program can read the accumulator's value, using its `value` method.

The interpreter session below shows an accumulator being used to add up the elements of an array:

{% highlight scala %}
scala> val accum = sc.accumulator(0)
accum: spark.Accumulator[Int] = 0

scala> sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x)
...
10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s

scala> accum.value
res2: Int = 10
{% endhighlight %}

# Where to Go from Here

You can see some [example Spark programs](http://www.spark-project.org/examples.html) on the Spark website.

In addition, Spark includes several sample jobs in `examples/src/main/scala`. Some of them have both Spark versions and local (non-parallel) versions, allowing you to see what had to be changed to make the program run on a cluster. You can run them using by passing the class name to the `run` script included in Spark -- for example, `./run spark.examples.SparkPi`. Each example program prints usage help when run without any arguments.
