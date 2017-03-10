package net.so1.demo.akka.streams

import java.nio.file.FileSystems

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.cassandra.scaladsl.CassandraSink
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.alpakka.file.scaladsl.DirectoryChangesSource
import akka.stream.scaladsl.{FileIO, Framing}
import akka.util.ByteString
import com.datastax.driver.core.{Cluster, ConsistencyLevel, PreparedStatement}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent._
import scala.concurrent.duration._


class Demo3
  extends WordSpec
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Matchers {

  implicit val system = ActorSystem()
  implicit val session = Cluster.builder.addContactPoint("127.0.0.1").withPort(9042).build.connect()

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

  override def afterAll(): Unit = Await.result(system.terminate(), 200.seconds)


  "Alpakka demo - parse master product price" must {

    "Observe product prices and import to cassandra" in {
      val path = "/tmp/products"
      val fs = FileSystems.getDefault
      case class ProductPrice(storeId: Int, productId: Long, shelfPrice: BigDecimal)

      // listen to directory changes
      val changes = DirectoryChangesSource(fs.getPath(path), pollInterval = 1.second, maxBufferSize = 1000)

      val sourceOfProductPrices = changes
        .filter {
          case (p, change) =>
            change == DirectoryChange.Modification
        }
        .flatMapConcat { case (newFilePath, _) =>
          FileIO.fromPath(newFilePath)
        }.via(Framing.delimiter(
        ByteString("\n"),
        maximumFrameLength = 256,
        allowTruncation = true)
      )
        .map(_.utf8String)
        .map { s =>
          // create product price
          val fields = s.split(";")
          val pp = ProductPrice(fields(0).toInt, fields(1).toLong, BigDecimal(fields(2)))
          println(s"$pp")
          pp
        }


      val preparedStatement = session.prepare(
        "INSERT INTO so1.master_product_price (store_id, product_id, shelf_price, valid_from, created_at) " +
          "VALUES (?, ?,1.0, toTimestamp(now()), toTimestamp(now())) IF NOT EXISTS"
      ).setConsistencyLevel(ConsistencyLevel.ONE)

      val statementBinder = (productPrice: ProductPrice, statement: PreparedStatement) =>
        statement.bind(productPrice.storeId.asInstanceOf[java.lang.Integer], productPrice.productId.toInt.asInstanceOf[java.lang.Integer])

      val sink = CassandraSink[ProductPrice](parallelism = 8, preparedStatement, statementBinder)
      val result =
        sourceOfProductPrices
        .buffer(100, OverflowStrategy.backpressure)
        .runWith(sink)

      Await.result(result, 100.second)
    }
  }
}
