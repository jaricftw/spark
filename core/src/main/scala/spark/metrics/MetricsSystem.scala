package spark.metrics

import com.codahale.metrics.{JmxReporter, MetricSet, MetricRegistry}

import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import spark.Logging
import spark.metrics.sink.Sink
import spark.metrics.source.Source

/**
 * Spark Metrics System, created by specific "instance", combined by source,
 * sink, periodically poll source metrics data to sink destinations.
 *
 * "instance" specify "who" (the role) use metrics system. In spark there are several roles
 * like master, worker, executor, client driver, these roles will create metrics system
 * for monitoring. So instance represents these roles. Currently in Spark, several instances
 * have already implemented: master, worker, executor, driver.
 *
 * "source" specify "where" (source) to collect metrics data. In metrics system, there exists
 * two kinds of source:
 *   1. Spark internal source, like MasterSource, WorkerSource, etc, which will collect
 *   Spark component's internal state, these sources are related to instance and will be
 *   added after specific metrics system is created.
 *   2. Common source, like JvmSource, which will collect low level state, is configured by
 *   configuration and loaded through reflection.
 *
 * "sink" specify "where" (destination) to output metrics data to. Several sinks can be
 * coexisted and flush metrics to all these sinks.
 *
 * Metrics configuration format is like below:
 * [instance].[sink|source].[name].[options] = xxxx
 *
 * [instance] can be "master", "worker", "executor", "driver", which means only the specified
 * instance has this property.
 * wild card "*" can be used to replace instance name, which means all the instances will have
 * this property.
 *
 * [sink|source] means this property belongs to source or sink. This field can only be source or sink.
 *
 * [name] specify the name of sink or source, it is custom defined.
 *
 * [options] is the specific property of this source or sink.
 */
private[spark] class MetricsSystem private (val instance: String) extends Logging {
  initLogging()

  val confFile = System.getProperty("spark.metrics.conf")
  val metricsConfig = new MetricsConfig(Option(confFile))

  val sinks = new mutable.ArrayBuffer[Sink]
  val sources = new mutable.ArrayBuffer[Source]
  val registry = new MetricRegistry()

  metricsConfig.initialize()
  registerSources()
  registerSinks()

  def start() {
    sinks.foreach(_.start)
  }

  def stop() {
    sinks.foreach(_.stop)
  }

  def registerSource(source: Source) {
    sources += source
    try {
      registry.register(source.sourceName, source.metricRegistry)
    } catch {
      case e: IllegalArgumentException => logInfo("Metrics already registered", e)
    }
  }

  def registerSources() {
    val instConfig = metricsConfig.getInstance(instance)
    val sourceConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SOURCE_REGEX)

    // Register all the sources related to instance
    sourceConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      try {
        val source = Class.forName(classPath).newInstance()
        registerSource(source.asInstanceOf[Source])
      } catch {
        case e: Exception => logError("Source class " + classPath + " cannot be instantialized", e)
      }
    }
  }

  def registerSinks() {
    val instConfig = metricsConfig.getInstance(instance)
    val sinkConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SINK_REGEX)

    sinkConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      try {
        val sink = Class.forName(classPath)
          .getConstructor(classOf[Properties], classOf[MetricRegistry])
          .newInstance(kv._2, registry)
        sinks += sink.asInstanceOf[Sink]
      } catch {
        case e: Exception => logError("Sink class " + classPath + " cannot be instantialized", e)
      }
    }
  }
}

private[spark] object MetricsSystem {
  val SINK_REGEX = "^sink\\.(.+)\\.(.+)".r
  val SOURCE_REGEX = "^source\\.(.+)\\.(.+)".r

  val MINIMAL_POLL_UNIT = TimeUnit.SECONDS
  val MINIMAL_POLL_PERIOD = 1

  def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int) {
    val period = MINIMAL_POLL_UNIT.convert(pollPeriod, pollUnit)
    if (period < MINIMAL_POLL_PERIOD) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
        " below than minimal polling period ")
    }
  }

  def createMetricsSystem(instance: String): MetricsSystem = new MetricsSystem(instance)
}
