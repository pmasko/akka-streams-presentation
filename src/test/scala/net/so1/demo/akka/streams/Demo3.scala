package net.so1.demo.akka.streams

import akka.actor.{ActorSystem, Cancellable}
import akka.event.Logging
import akka.stream._
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Try


class Demo3
  extends WordSpec
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Matchers {

  implicit val system = ActorSystem()


  // runs transformation steps on actors and its thread pool - @see MaterializationSettings
  implicit val mat = ActorMaterializer(
    // set async stage buffer size
    ActorMaterializerSettings(system).withInputBuffer(initialSize = 64, maxSize = 64)
  )

  // allows to use is ready within and whenReady {}
  implicit val defaultPatience = PatienceConfig(timeout = 2.seconds, interval = 50.millis)
  implicit val ec = system.dispatcher
  override def beforeEach(): Unit = {}

  override def afterEach(): Unit = {}

  override def afterAll(): Unit = Await.result(system.terminate(), 20.seconds)

  val eventsSource: Source[Int, Cancellable] =
    Source.tick(20.millis, 120.millis, 1)
      .map { e => println(e); e }

  "Practical samples" must {
    //https://softwaremill.com/windowing-in-big-data-streams-spark-flink-kafka-akka/
    //https://softwaremill.com/windowing-data-in-akka-streams/


  }
}
