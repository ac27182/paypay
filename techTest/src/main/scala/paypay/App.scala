package paypay

import paypay.domain._
import paypay.core.PayPayUDF
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.catalyst.expressions._
import java.util.UUID
import org.apache.log4j.Logger
import org.apache.log4j.Level

import paypay.core.implicits._

object Main {

  private[this] def readAccessLogs(
      path: String
  )(implicit spark: SparkSession): DataFrame =
    spark
      .read
      .format("csv")
      .option("delimiter", " ")
      .option("quote", "\"")
      .option("header", "true")
      .schema(Schema.accessLogSchema)
      .load(path)

  private[this] val sparkConf: SparkConf =
    new SparkConf()
      .setMaster("local")
      .setAppName("pay-pay-data-engineering-challange-app")
      .set("spark.executor.memory", "4g")

  private[this] implicit val spark: SparkSession =
    SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate

  def main(args: Array[String]): Unit =
    try {
      import spark.implicits._

      // supressing spark logs for clean runtime
      Logger.getRootLogger.setLevel(Level.ERROR)

      // reading in access logs from predefined path
      val accessLogsDf: DataFrame =
        readAccessLogs(s"$linuxBasePath/logs")(spark)

      // accessLogs => sessionizedLogs => aggregatedSessionizedLogs
      val aggregates: DataFrame = accessLogsDf.sessionizeLogs.generateAggregates

      // reduces the aggregate logs into a report containing:
      // => total bytes recieved
      // => total bytes sent
      // => mean session time
      val reportDf: DataFrame = aggregates.generateReportAggregates

      // reduces the aggregate logs into to top 5 percent of ip's ranked by session time
      val activeIpsDf: DataFrame = aggregates.generateActiveIpAggregates

      // persist the aggregates in memory
      aggregates.persist(StorageLevel.MEMORY_ONLY)

      // write the respective dataframes to disk

      aggregates
        .coalesce(1)
        .write
        .option("header", "true")
        .csv(s"$linuxBasePath/output/aggregates")

      activeIpsDf
        .coalesce(1)
        .write
        .option("header", "true")
        .csv(s"$linuxBasePath/output/activeIps")

      reportDf
        .coalesce(1)
        .write
        .json(s"$linuxBasePath/output/report")

      // un persist the aggregates out of habbit
      aggregates.unpersist

    } finally {
      spark.stop
    }

}
