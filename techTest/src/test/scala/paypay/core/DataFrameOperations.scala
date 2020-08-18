package paypay.core

import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import paypay.core.DataFrameOperations._
import paypay.core.DataFrameOperationsOps._
import org.apache.spark.SparkConf
import org.scalatest.wordspec.AnyWordSpec
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.scalatest.wordspec.AsyncWordSpec
import scala.concurrent.Future
import org.apache.spark.SparkContext
import org.scalatest.compatible.Assertion

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

trait SparkSessionExtras {

  val sparkConf =
    new SparkConf()
      .setMaster("local")
      .setAppName("spark test")
      .set("spark.network.timeout", "10000000")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.executor.memory", "500g")
}

class DataFrameOperationsSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with DataFrameSuiteBase {

  override lazy val spark: SparkSession =
    SparkSession
      .builder
      .master("local")
      .appName("sparkTest")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate

  override def beforeAll(): Unit = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  import spark.implicits._

  describe("withIp") {

    it("should generate a coulmn containing the ip address form client:port") {

      val actualDf = List(
        "0.0.0.0:8000",
        "0.0.0.0:8050",
        "0.0.0.0:9000",
        "0.0.0.0:9050",
        "0.0.0.0:10000"
      )
        .toDF("client:port")
        .withIp
        .select("ip")

      val expectedDf =
        List(
          "0.0.0.0",
          "0.0.0.0",
          "0.0.0.0",
          "0.0.0.0",
          "0.0.0.0"
        ).toDF("ip")

      actualDf.collect shouldEqual expectedDf.collect
    }

  }

  describe("withTimestampOffset") {

    it("should return timestamp_offset to be null when no previous column exists") {

      val actualDf =
        List(
          ("0.0.0.0", "2015-07-22T09:00:28.019143Z")
        )
          .toDF("ip", "time")
          .withColumn("time", to_timestamp(col("time")))
          .withTimestampOffset

      val expectedDf =
        List(
          ("0.0.0.0", "2015-07-22T09:00:28.019143Z", null)
        )
          .toDF("ip", "time", "timestamp_offset")
          .withColumn("time", to_timestamp(col("time")))

      actualDf.collect shouldEqual expectedDf.collect
    }

    it("should set timestamp_offset to be null when no previous column exists for both windows") {
      val actualDf =
        List(
          ("0.0.0.0", "2015-07-22T09:00:28.019143Z"),
          ("0.0.0.10", "2015-07-22T09:00:28.019143Z")
        )
          .toDF("ip", "time")
          .withColumn("time", to_timestamp(col("time")))
          .withTimestampOffset

      val expectedDf =
        List(
          ("0.0.0.0", "2015-07-22T09:00:28.019143Z", null),
          ("0.0.0.10", "2015-07-22T09:00:28.019143Z", null)
        )
          .toDF("ip", "time", "timestamp_offset")
          .withColumn("time", to_timestamp(col("time")))

      actualDf.collect shouldEqual expectedDf.collect
    }

    it("generate the correct offset timestamp") {
      val actualDf =
        List(
          ("0.0.0.0", "2015-07-22T09:00:28.019143Z"),
          ("0.0.0.0", "2015-07-22T09:00:28.500000Z"),
          ("0.0.0.10", "2015-07-22T09:00:28.019143Z")
        )
          .toDF("ip", "time")
          .withColumn("time", to_timestamp(col("time")))
          .withTimestampOffset

      val expectedDf =
        List(
          ("0.0.0.0", "2015-07-22T09:00:28.019143Z", null),
          ("0.0.0.0", "2015-07-22T09:00:28.500000Z", "2015-07-22T09:00:28.019143Z"),
          ("0.0.0.10", "2015-07-22T09:00:28.019143Z", null)
        )
          .toDF("ip", "time", "timestamp_offset")
          .withColumn("time", to_timestamp(col("time")))
          .withColumn("timestamp_offset", to_timestamp(col("timestamp_offset")))

      actualDf.collect shouldEqual expectedDf.collect
    }

  }

  describe("withUrl") {
    it("should transform a reqest into a url string") {

      val actualDf =
        List(
          "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1"
        )
          .toDF("request")
          .withUrl
          .select("url")

      val expectedDf =
        List(
          "https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null"
        ).toDF("url")

      actualDf.collect shouldEqual expectedDf.collect
    }

  }

  describe("withSessionStart") {

    it("should generate start for each for each new session") {

      val actualDf =
        List(
          ("2015-07-22T09:00:28.019143Z", null),
          ("2015-07-22T09:00:28.500000Z", "2015-07-22T09:00:28.019143Z"),
          ("2015-07-22T09:00:59.000000Z", "2015-07-22T09:00:28.500000Z"),
          ("2015-07-22T10:00:00.000000Z", "2015-07-22T09:00:59.000000Z")
        )
          .toDF("time", "timestamp_offset")
          .withColumn("time", to_timestamp(col("time")))
          .withSessionStart(60 * 15)

      val rowArray =
        actualDf
          .select("session_start")
          .collect

      val p0 =
        Array(rowArray(1), rowArray(2)) sameElements Array(Row(null), Row(null))

      val p1 =
        !(Array(rowArray(0), rowArray(3)) sameElements Array(Row(null), Row(null)))

      (p0 && p1) shouldBe true
    }

  }

  describe("withSessionId") {

    it("should generate a filled in column of sessionId's based on the sessionid in the start column") {

      val actualRowArray =
        List(
          ("0.0.0.0", "2015-07-22T09:00:27.000000Z", "xxxx"),
          ("0.0.0.0", "2015-07-22T09:00:28.000000Z", null),
          ("0.0.0.0", "2015-07-22T09:00:29.500000Z", null),
          ("0.0.0.0", "2015-07-22T09:00:30.500000Z", null),
          ("0.0.0.0", "2015-07-22T10:00:28.000000Z", "yyyy"),
          ("0.0.0.0", "2015-07-22T10:00:30.000000Z", null)
        )
          .toDF("ip", "time", "session_start")
          .withColumn("time", to_timestamp(col("time")))
          .withSessionId
          .select("session_id")
          .collect

      val expectedRowArray =
        List("xxxx", "xxxx", "xxxx", "xxxx", "yyyy", "yyyy")
          .toDF("session_id")
          .collect

      actualRowArray shouldEqual expectedRowArray

    }

  }

}
