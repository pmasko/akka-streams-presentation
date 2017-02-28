package net.so1.demo.akka.streams

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.Fusing.FusedGraph
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, Framing, GraphDSL, Keep, Merge, RunnableGraph, Sink, Source}
import akka.stream._
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Random


class Demo1
    extends WordSpec
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Matchers {

  implicit val system = ActorSystem()

  // runs transformation steps on actors and its thread pool - @see MaterializationSettings
  implicit val mat = ActorMaterializer()
  // allows to use is ready within and whenReady {}
  implicit val defaultPatience = PatienceConfig(timeout = 2.seconds, interval = 50.millis)

  override def beforeEach(): Unit = {}

  override def afterEach(): Unit = {}

  override def afterAll(): Unit = Await.result(system.terminate(), 5.seconds)


  "Simple examples of akka streams" must {

    "Example 1: generate, map, filter, print some random numbers" in {
      val n = 100

      // Source object
      val source: Source[Int, NotUsed] = Source(1 to n)
      // transformations
      val generateRandomFlow: Flow[Int, Double, NotUsed] = Flow[Int].map(v => Random.nextGaussian() * v)
      val filterPositive = Flow[Double].filter(v => v > 0.0)

      // Sink - process result numbers
      val printAllSing: Sink[Double, Future[Done]] = Sink.foreach(v => println(v))

      // create graph with void return type = NotUsed
      val graph: RunnableGraph[NotUsed] = source
        .via(generateRandomFlow)
        .via(filterPositive)
        .filter(_ < 15.0)      // yet again filter
        .to(printAllSing)   // connect - to sink

      println("Run the flow 1")
      graph.run()
    }


    // we can resuse the flow
    def positiveGaussianNumbers(numGenerated: Int): Source[Double, NotUsed] =
      Source(1 to numGenerated)
        .map(_ * Random.nextGaussian())
        .filter(_ > 0)
        .named("PositiveRandomNumbers")

    "Example 2: generate, map, filter and materialize final value!" in {
      // Source of positive random numbers below 25
      val source: Source[Double, NotUsed] = positiveGaussianNumbers(100).filter(_ < 25)

      // sink with materialized value

      val foldSink: Sink[Double, Future[Double]] = Sink.fold(0.0)((acc, v) => acc + v)
      // create graph with Double return type wrapped in Future
      val graph: RunnableGraph[Future[Double]] =
        source
          .take(20) // take 20 first values
          .alsoTo(Sink.foreach(println)) // send to first sink and print
          .map(_ * -1.0).named("negateStage")
          .toMat(foldSink)(Keep.right) // simplified runWith(sink) would create & run & return last stage value

      // graph is an object and can be printed for representation
      // println("graph: " + graph)

      // a single graph can be run multiple times and should produce distinct instances
      val result1: Future[Double] = graph.run()
      val result2: Future[Double] = graph.run()

      println(s"Result value from example 2: ${result1.futureValue}, ${result2.futureValue}")

      result1.futureValue should not be result2.futureValue
    }

    "Example 3: Build simple split and merge flow" in {

      // force to run all source steps on single actor and cpu different then the following steps.
      // otherwise akka will try to fuse steps and execute them on a single actor to spare on data transfer cost (since v2.0)

      val asyncSource: Source[Double, NotUsed] = positiveGaussianNumbers(100).async
      val sink: Sink[Double, Future[Int]] = Sink.fold(0){ case (acc, v) =>  println(s"Processing: $acc"); acc +  1 }

      val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        // broadcast step -
        val bcast = b.add(Broadcast[Double](2))
        // merge step
        val merge = b.add(Merge[Double](2))

        asyncSource ~> bcast.in

        // negate and throttle and send to sink
        bcast.out(0) ~> Flow[Double].map(_ * -1.0) ~> merge ~> sink
        bcast.out(1) ~> Flow[Double].sliding(10) ~>
          Flow[Seq[Double]].map { vals =>
            val v = vals.sum
            println(v)
            v
          } ~> merge
        ClosedShape
      })
      graph.run()
    }

    "Example 4: grouping and concatenating" in {
      val text = """In the previous section we explored the possibility of composition, and hierarchy, but we stayed away from non-linear, generalized graph components. There is nothing in Akka Streams though that enforces that stream processing layouts can only be linear. The DSL for Source and friends is optimized for creating such linear chains, as they are the most common in practice. There is a more advanced DSL for building complex graphs, that can be used if more flexibility is needed. We will see that the difference between the two DSLs is only on the surface: the concepts they operate on are uniform across all DSLs and fit together nicely."""
      //http://doc.akka.io/docs/akka/2.4.17/scala/stream/stream-rate.html
      //http://doc.akka.io/docs/akka/2.4.17/scala/stream/stream-parallelism.html
      //http://doc.akka.io/docs/akka/2.4.17/scala/stream/stream-graphs.html#graph-matvalue-scala
    }

    def lineSink(filename: String): Sink[String, Future[IOResult]] =
      Flow[String]
        .map(s => ByteString(s + "\n"))
        .toMat(FileIO.toPath(Paths.get(filename)))(Keep.right)
  }
}
