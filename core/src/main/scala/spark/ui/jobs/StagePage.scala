package spark.ui.jobs

import java.util.Date

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import spark.ui.UIUtils._
import spark.util.Distribution
import spark.scheduler.cluster.TaskInfo
import spark.executor.TaskMetrics

/** Page showing statistics and task list for a given stage */
class StagePage(parent: JobProgressUI) {
  val listener = parent.listener
  val dateFmt = parent.dateFmt

  def render(request: HttpServletRequest): Seq[Node] = {
    val stageId = request.getParameter("id").toInt
    val tasks = listener.stageToTaskInfos(stageId)

    val shuffleRead = listener.hasShuffleRead(stageId)
    val shuffleWrite = listener.hasShuffleWrite(stageId)

    val taskHeaders: Seq[String] =
      Seq("Task ID", "Service Time (ms)", "Locality Level", "Worker", "Launch Time") ++
        {if (shuffleRead) Seq("Shuffle Read (bytes)")  else Nil} ++
        {if (shuffleWrite) Seq("Shuffle Write (bytes)") else Nil}

    val taskTable = listingTable(taskHeaders, taskRow, tasks)

    // TODO(pwendell): Consider factoring this more nicely with the functions in SparkListener
    val serviceTimes = tasks.map{case (info, metrics) => metrics.executorRunTime.toDouble}
    val serviceQuantiles = "Service Time" +: Distribution(serviceTimes).get.getQuantiles().map(_.toString)

    val shuffleReadSizes = tasks.map{
      case(info, metrics) => metrics.shuffleReadMetrics.map(_.remoteBytesRead).getOrElse(0L).toDouble}
    val shuffleReadQuantiles = "Shuffle Read" +: Distribution(shuffleReadSizes).get.getQuantiles().map(_.toString)

    val shuffleWriteSizes = tasks.map{
      case(info, metrics) => metrics.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L).toDouble}
    val shuffleWriteQuantiles = "Shuffle Write" +: Distribution(shuffleWriteSizes).get.getQuantiles().map(_.toString)


    val listings: Seq[Seq[String]] = Seq(serviceQuantiles,
      if (shuffleRead) shuffleReadQuantiles else Nil,
      if (shuffleWrite) shuffleWriteQuantiles else Nil)

    val quantileHeaders = Seq("Metric", "Min", "25%", "50%", "75%", "Max")
    val quantileTable = listingTable(quantileHeaders, quantileRow, listings)

    val content =
      <h2>Summary Metrics</h2> ++ quantileTable ++ <h2>Tasks</h2> ++ taskTable;

    headerSparkPage(content, "Stage Details: %s".format(stageId))
  }

  def quantileRow(data: Seq[String]): Seq[Node] = <tr> {data.map(d => <td>{d}</td>)} </tr>

  def taskRow(taskData: (TaskInfo, TaskMetrics)): Seq[Node] = {
    val (info, metrics) = taskData
    <tr>
      <td>{info.taskId}</td>
      <td>{metrics.executorRunTime}</td>
      <td>{info.taskLocality}</td>
      <td>{info.hostPort}</td>
      <td>{dateFmt.format(new Date(info.launchTime))}</td>
      {metrics.shuffleReadMetrics.map{m => <td>{m.remoteBytesRead}</td>}.getOrElse("") }
      {metrics.shuffleWriteMetrics.map{m => <td>{m.shuffleBytesWritten}</td>}.getOrElse("") }
    </tr>
  }
}
