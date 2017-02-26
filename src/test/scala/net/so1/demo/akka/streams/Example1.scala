package net.so1.demo.akka.streams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.JavaConverters._


class Example1
    extends WordSpec
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with MustMatchers {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  implicit val defaultPatience =
    PatienceConfig(timeout = 2.seconds, interval = 50.millis)

  override def beforeEach(): Unit = {}

  override def afterEach(): Unit = {}

  override def afterAll(): Unit =
    Await.result(system.terminate(), 5.seconds)


  "Example1" must {

    "first example" in {

    }
  }
}
