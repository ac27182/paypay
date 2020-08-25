package paypay.core

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import paypay.core.implicits._

object DataFrameOperations {

  // simple data frame operations

  def selectSessionizedLogsColumns(dataFrame: DataFrame): DataFrame =
    dataFrame
      .select(
        "session_id",
        "client:port",
        "ip",
        "time",
        "received_bytes",
        "sent_bytes",
        "url"
      )

  def withIp(dataFrame: DataFrame): DataFrame =
    dataFrame.withColumn(
      "ip",
      ColumnOperations.genIp
    )

  def withTimestampOffset(dataFrame: DataFrame): DataFrame =
    dataFrame
      .withColumn(
        "timestamp_offset",
        ColumnOperations.genTimestampOffset
      )

  def withUrl(dataFrame: DataFrame): DataFrame =
    dataFrame
      .withColumn(
        "url",
        ColumnOperations.genUrl(col("request"))
      )

  def withSessionStart(dataFrame: DataFrame, epsilon: Int): DataFrame =
    dataFrame
      .withColumn(
        "session_start",
        ColumnOperations.genSessionStart(epsilon)
      )

  def withSessionId(dataFrame: DataFrame): DataFrame =
    dataFrame
      .withColumn(
        "session_id",
        ColumnOperations.genSessionId
      )

  def withPercentile(dataFrame: DataFrame): DataFrame =
    dataFrame
      .withColumn(
        "percentile",
        ColumnOperations.genPercentile
      )

  // composite dataframe operations

  def sessionizeLogs(accessLogsDf: DataFrame): DataFrame =
    accessLogsDf
      .withIp
      .withTimestampOffset
      .withUrl
      .withSessionStart(60 * 10) // 10 minute session window
      .withSessionId
      .selectSessionizedLogsColumns

  def generateAggregates(dataFrame: DataFrame): DataFrame =
    dataFrame
      .groupBy(
        col("ip"),
        col("session_id")
      )
      .agg(
        ColumnOperations.aggSessionTimeStart,
        ColumnOperations.aggSessionTimeEnd,
        ColumnOperations.aggSessionTimeTotal,
        ColumnOperations.aggTotalRecievedBytes,
        ColumnOperations.aggTotalSentBytes,
        ColumnOperations.aggUniqueUrlVisits
      )

  def generateReportAggregates(dataFrame: DataFrame): DataFrame =
    dataFrame
      .agg(
        ColumnOperations.aggMeanSessionTime,
        ColumnOperations.aggAllRecievedBytes,
        ColumnOperations.aggAllSentBytes
      )

  def generateActiveIpAggregates(dataFrame: DataFrame): DataFrame =
    dataFrame
      .groupBy(
        col("ip")
      )
      .agg(
        ColumnOperations.aggIpTimeTotal
      )
      .withPercentile
      .filter(col("percentile") === 1)
      .select(
        "ip",
        "ip_time_total_s"
      )

}
