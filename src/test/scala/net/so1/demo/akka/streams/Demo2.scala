package net.so1.demo.akka.streams

import akka.actor.ActorSystem
import akka.stream.Supervision.Directive
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Random


class Demo2
    extends WordSpec
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Matchers {

  implicit val system = ActorSystem("demo2")
  implicit val ec = system.dispatcher
  // runs transformation steps on actors and its thread pool - @see MaterializationSettings
  implicit val mat = ActorMaterializer(
    // set async stage buffer size
    ActorMaterializerSettings(system).withInputBuffer(initialSize = 64, maxSize = 64)
  )

  // allows to use is ready within and whenReady {}
  implicit val defaultPatience = PatienceConfig(timeout = 2.seconds, interval = 50.millis)

  override def beforeEach(): Unit = {}

  override def afterEach(): Unit = {}

  override def afterAll(): Unit = Await.result(system.terminate(), 500.seconds)


  "Demo2" must {
    case class FileWithContent(fileName: String, content: String)
    case class FileWithResponse(fileName: String, content: String)
    val fileContents = Seq(
      """Here we used MqttConnectionSettings
        |factory to set the address of the server, client ID,
        |which needs to be unique for every client, and client persistence implementation
        | (MemoryPersistence) which allows to control reliability guarantees.
        |
        |Then let’s create a source that is going to connect to the
        |MQTT server upon materialization and receive messages that are sent to the subscribed topics.""".stripMargin,
      """The FileTailSource starts at a given offset in a file and emits chunks of bytes
        |until reaching the end of the file, it will then poll the file for changes
        |and emit new changes as they are written to the file (unless there is backpressure).
        |
        |A very common use case is combining reading bytes with parsing the bytes into lines,
        |therefore FileTailSource contains a few factory methods to create a source that parses
        |the bytes into lines and emits those.
        |
        |""".stripMargin)


    def startProcessingFile(fileName: String): Future[FileWithContent] = Future {
      println(s"Starting file: $fileName")
      Thread.sleep(100)
      FileWithContent(fileName, fileContents(Random.nextInt(fileContents.length)))
    }

    def splitFileContentToHttpRequest(file: String, fileContents: String): List[FileWithContent] = {
      // Produce pair of file, line
      println(s"Splitting file $file")
      fileContents.split("\\n").map { line =>
        FileWithContent(file, line)
      }.toList
    }

    def callHttpService(file: String, body: String): Future[String] = {
      Future {
        val sleepTime = 37 * body.length
        println(s"HTTP($sleepTime) from file: $file request for: $body")
        Thread.sleep(sleepTime)
        "OK"
      }
    }

    val rtiExceptionDecider: Function[Throwable, Directive] = new Function[Throwable, Directive] {
      @scala.throws[Exception](classOf[Exception])
      override def apply(param: Throwable): Directive = {
        println(s"Report error: $param")
        akka.stream.Supervision.Resume
      }
    }

    def checkIfFailure(responseList: List[FileWithResponse]): String = {
      if (responseList.exists(_.content != "OK")) "NOK"
      else "OK"
    }

    "Sample 1: Throttle async HTTP calls and split to substreams" in {
      val fileNames = Seq("RTI_requests1.xml", "RTI_requests2.xml")
      val parallelism = 2

      val graph = Source(fileNames)
        .mapAsyncUnordered(parallelism)(fileName => startProcessingFile(fileName))
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
        .mapConcat(s => splitFileContentToHttpRequest(s.fileName, s.content))
        .throttle(10, 1.second, 10, ThrottleMode.shaping)
        .buffer(2, OverflowStrategy.backpressure )
        .mapAsyncUnordered(parallelism) { case FileWithContent(file, line) =>
          // HTTP call
          callHttpService(file, line).map { resp =>
            FileWithResponse(file, resp)
          }
        }
        //.withAttributes(ActorAttributes.withSupervisionStrategy(rtiExceptionDecider))
        .groupBy(10000, _.fileName)
        .fold(List.empty[FileWithResponse]) { case (a, v) => v :: a }
        .collect { case responseList if responseList.nonEmpty =>
          val fileName: String = responseList.head.fileName
          checkIfFailure(responseList) match {
            case "OK" =>
              println(s"File processed OK: $fileName")
            case _ =>
              println(s"File not proceed $fileName!")
          }
        }
        .async
        .mergeSubstreams
        .toMat(Sink.last[Unit])(Keep.right) // force return of last result wrapped in future

      // Await for graph end
      Await.ready(graph.run().map { v => println("Done Sample 1!"); v }, 20.second)
    }
  }
}
