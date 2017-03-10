package net.so1.demo.akka.streams

import akka.actor.{ActorRef, ActorSystem, Cancellable}
import akka.stream._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Merge, RunnableGraph, Sink, Source}
import akka.{Done, NotUsed}
import org.reactivestreams.{Subscriber, Subscription}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Random, Try}


class Demo1
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


    // we can re-use the flow
    def positiveGaussianNumbers(numGenerated: Int): Source[Double, NotUsed] =
      Source(1 to numGenerated)
        .map(_ * Random.nextGaussian())
        .filter(_ > 0)
        .named("PositiveRandomNumbers")

    def slowDBQuery(word: String) : Future[BigDecimal] = {
      Future.successful {
        Thread.sleep(50 * word.length)
        Random.nextGaussian()
      }
    }

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

    "Example 4: integration with actors, reactive streams and async processing" in {
      val text = """In the previous section we explored the possibility of composition, and hierarchy, but we stayed away from non-linear, generalized graph components. There is nothing in Akka Streams though that enforces that stream processing layouts can only be linear. The DSL for Source and friends is optimized for creating such linear chains, as they are the most common in practice. There is a more advanced DSL for building complex graphs, that can be used if more flexibility is needed. We will see that the difference between the two DSLs is only on the surface: the concepts they operate on are uniform across all DSLs and fit together nicely."""

      val sinkSubscriber = Sink.fromSubscriber(new Subscriber[(BigDecimal, String)] {
        var subscription: Subscription = _

        override def onError(t: Throwable): Unit = println(s"Error ?: $t")

        override def onSubscribe(s: Subscription): Unit = {
          println("Got subscription object")
          subscription = s
          subscription.request(100)
        }

        override def onComplete(): Unit = {
          println("Stream completed for Example 4")
        }

        override def onNext(t: (BigDecimal, String)): Unit = {
          println(s"Got new data value=${t._1} word: ${t._2}")
          subscription.request(1)
        }
      })

      val sourceRef: Source[String, ActorRef] = Source.actorRef[String](5, OverflowStrategy.dropHead)
      // Second type is materialized value
      val asyncWordsSource: Source[String, ActorRef] =
          sourceRef
            .map(_.split("\\s").toList)
          .mapConcat(identity) // flatMap List[List[String]] => List[String]
            .map(_.trim)
          .async

      val graph = asyncWordsSource
        .mapAsync(2) { word: String =>
          slowDBQuery(word).map { v =>
            (v, word)
          }
        }
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
        .toMat(sinkSubscriber)(Keep.left)

      // run graph and materialize the left most value i.e ActorRef which accepts string as a parameter.
      graph.run() ! text
      Thread.sleep(10000)
    }

    val eventsSource: Source[Int, Cancellable] =
        Source.tick(20.millis, 120.millis, 1)
          .map { e => println(e); e }
          .async

    "Example 5: Group upstream data in batches with timeout" in {

      val graph =
        eventsSource
        .map { e => Thread.sleep(10 + Random.nextInt(200)); e }.async // slow down the source and add jitter
        .groupedWithin(10, 1.second)
        .runForeach { el =>
          println(s"Data: #=${el.size} $el")
        }

      Try(Await.ready(graph, 20.second))
    }


    "Example 6: Merge upstream data when downstream is slower" in {
      val graph =
        eventsSource
          .conflate(_ + _)
          .mapAsync(1) { e =>
            Future {
              val sleepTime = 100 + Random.nextInt(200)
              Thread.sleep(sleepTime)
              e
            }
          }
          .runForeach { el =>
            println(s"Got result = $el")
          }

      Try(Await.ready(graph, 20.second))
    }



  }
}
