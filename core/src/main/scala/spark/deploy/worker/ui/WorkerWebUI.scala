package spark.deploy.worker.ui

import akka.actor.ActorRef
import akka.util.{Duration, Timeout}

import java.io.{FileInputStream, File}

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.{Handler, Server}

import spark.deploy.worker.Worker
import spark.{Utils, Logging}
import spark.ui.JettyUtils
import spark.ui.JettyUtils._
import spark.ui.UIUtils

/**
 * Web UI server for the standalone worker.
 */
private[spark]
class WorkerWebUI(val worker: Worker, val workDir: File, requestedPort: Option[Int] = None)
    extends Logging {
  implicit val timeout = Timeout(
    Duration.create(System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds"))
  val host = Utils.localHostName()
  val port = requestedPort.getOrElse(
    System.getProperty("worker.ui.port", WorkerWebUI.DEFAULT_PORT).toInt)

  var server: Option[Server] = None
  var boundPort: Option[Int] = None

  val indexPage = new IndexPage(this)

  val handlers = Array[(String, Handler)](
    ("/static", createStaticHandler(WorkerWebUI.STATIC_RESOURCE_DIR)),
    ("/log", (request: HttpServletRequest) => log(request)),
    ("/logPage", (request: HttpServletRequest) => logPage(request)),
    ("/json", (request: HttpServletRequest) => indexPage.renderJson(request)),
    ("*", (request: HttpServletRequest) => indexPage.render(request))
  )

  def start() {
    try {
      val (srv, bPort) = JettyUtils.startJettyServer("0.0.0.0", port, handlers)
      server = Some(srv)
      boundPort = Some(bPort)
      logInfo("Started Worker web UI at http://%s:%d".format(host, bPort))
    } catch {
      case e: Exception =>
        logError("Failed to create Worker JettyUtils", e)
        System.exit(1)
    }
  }

  def log(request: HttpServletRequest): String = {
    val defaultBytes = 10000
    val appId = request.getParameter("appId")
    val executorId = request.getParameter("executorId")
    val logType = request.getParameter("logType")
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt).getOrElse(defaultBytes)
    val path = "%s/%s/%s/%s".format(workDir.getPath, appId, executorId, logType)

    val (startByte, endByte) = getByteRange(path, offset, byteLength)
    val file = new File(path)
    val logLength = file.length

    val pre = "==== Bytes %s-%s of %s of %s/%s/%s ====\n"
      .format(startByte, endByte, logLength, appId, executorId, logType)
    pre + Utils.offsetBytes(path, startByte, endByte)
  }

  def logPage(request: HttpServletRequest): Seq[scala.xml.Node] = {
    val defaultBytes = 10000
    val appId = request.getParameter("appId")
    val executorId = request.getParameter("executorId")
    val logType = request.getParameter("logType")
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt).getOrElse(defaultBytes)
    val path = "%s/%s/%s/%s".format(workDir.getPath, appId, executorId, logType)

    val (startByte, endByte) = getByteRange(path, offset, byteLength)
    val file = new File(path)
    val logLength = file.length
    //val logPageLength = endByte-startByte

    val logText = <node>{Utils.offsetBytes(path, startByte, endByte)}</node>

    val linkToMaster = <p><a href={worker.masterWebUiUrl}>Back to Master</a></p>

    val range = <span>Bytes {startByte.toString} - {endByte.toString} of {logLength}</span>

    val backButton =
      if (startByte > 0) {
        <a href={"?appId=%s&executorId=%s&logType=%s&offset=%s&byteLength=%s"
          .format(appId, executorId, logType, math.max(startByte-byteLength, 0),
            byteLength)}>
          <button>Previous {math.min(byteLength, startByte)} Bytes</button>
        </a>
      }
      else {
        <button disabled="disabled">Previous 0 Bytes</button>
      }

    val nextButton =
      if (endByte < logLength) {
        <a href={"?appId=%s&executorId=%s&logType=%s&offset=%s&byteLength=%s".
          format(appId, executorId, logType, endByte, byteLength)}>
          <button>Next {math.min(byteLength, logLength-endByte)} Bytes</button>
        </a>
      }
      else {
        <button disabled="disabled">Next 0 Bytes</button>
      }

    val content =
      <html>
        <body>
          {linkToMaster}
          <hr />
          <div>
            <div style="float:left;width:40%">{backButton}</div>
            <div style="float:left;">{range}</div>
            <div style="float:right;">{nextButton}</div>
          </div>
          <br />
          <div style="height:500px;overflow:auto;padding:5px;">
            <pre>{logText}</pre>
          </div>
        </body>
      </html>
    UIUtils.basicSparkPage(content, logType + " log page for " + appId)
  }

  /** Determine the byte range for a log or log page. */
  def getByteRange(path: String, offset: Option[Long], byteLength: Int)
  : (Long, Long) = {
    val defaultBytes = 10000
    val maxBytes = 1024 * 1024

    val file = new File(path)
    val logLength = file.length()
    val getOffset = offset.getOrElse(logLength-defaultBytes)

    val startByte =
      if (getOffset < 0) 0L
      else if (getOffset > logLength) logLength
      else getOffset

    val logPageLength = math.min(byteLength, maxBytes)

    val endByte = math.min(startByte+logPageLength, logLength)

    (startByte, endByte)
  }

  def stop() {
    server.foreach(_.stop())
  }
}

private[spark] object WorkerWebUI {
  val STATIC_RESOURCE_DIR = "spark/ui/static"
  val DEFAULT_PORT="8081"
}
