package paypay.core

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ColumnOperations {

  // basic column operations

  def genIp: Column =
    split(col("client:port"), ":").getItem(0)

  def genUrl(requestColumn: Column): Column =
    split(requestColumn, " ").getItem(1)

  private[this] def predicate: (Int, Column, Column) => Column =
    (e, time, timestampOffsetColumn) => (unix_timestamp(time) - unix_timestamp(timestampOffsetColumn)) > lit(e)

  def genSessionStart(epsilon: Int): Column =
    when(
      col("timestamp_offset").isNull || predicate(
        epsilon,
        col("time"),
        col("timestamp_offset")
      ),
      PayPayUDF.uuid()
    ).otherwise(null)

  // window functions

  def genTimestampOffset: Column =
    lag(col("time"), 1)
      .over(WindowSpecs.ipByTime)

  def genSessionId: Column =
    last(col("session_start"), true)
      .over(WindowSpecs.ipByTime)

  def genPercentile: Column =
    ntile(20)
      .over(WindowSpecs.byIpTimeTotal)

  // aggregate column Operations

  def aggSessionTimeStart: Column =
    max("time").alias("session_time_start")

  def aggSessionTimeEnd: Column =
    min("time").alias("session_time_end")

  def aggSessionTimeTotal: Column =
    (unix_timestamp(max("time")) - unix_timestamp(min("time")))
      .alias("session_time_total_s")

  def aggTotalRecievedBytes: Column =
    sum("received_bytes").alias("total_recieved_bytes")

  def aggTotalSentBytes: Column =
    sum("sent_bytes").alias("total_sent_bytes")

  def aggUniqueUrlVisits: Column =
    countDistinct("url").alias("unique_url_visits")

  def aggMeanSessionTime: Column =
    mean("session_time_total_s").alias("mean_session_time_s")

  def aggAllRecievedBytes: Column =
    sum("total_recieved_bytes").alias("all_recieved_bytes")

  def aggAllSentBytes: Column =
    sum("total_sent_bytes").alias("all_sent_bytes")

  def aggIpTimeTotal: Column =
    sum("session_time_total_s").alias("ip_time_total_s")

}
