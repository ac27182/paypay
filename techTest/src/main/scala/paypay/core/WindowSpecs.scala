package paypay.core

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object WindowSpecs {

  val ipByTime: WindowSpec =
    Window
      .partitionBy(col("ip"))
      .orderBy(col("time"))

  val sessionIdByTime: WindowSpec =
    Window
      .partitionBy(col("session_id"))
      .orderBy(col("time"))

  val byIpTimeTotal: WindowSpec =
    Window
      .orderBy(col("ip_time_total_s").desc)

}
