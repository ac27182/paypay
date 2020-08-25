package paypay.core

import org.apache.spark.sql._

private[core] final class DataFrameOperationsOps(val dataFrame: DataFrame) extends AnyVal {

  def withIp =
    DataFrameOperations
      .withIp(dataFrame)

  def selectSessionizedLogsColumns =
    DataFrameOperations
      .selectSessionizedLogsColumns(dataFrame)

  def withTimestampOffset =
    DataFrameOperations
      .withTimestampOffset(dataFrame)

  def withUrl =
    DataFrameOperations
      .withUrl(dataFrame)

  def withSessionStart(epsilon: Int) =
    DataFrameOperations
      .withSessionStart(dataFrame, epsilon)

  def withSessionId =
    DataFrameOperations
      .withSessionId(dataFrame)

  def withPercentile =
    DataFrameOperations
      .withPercentile(dataFrame)

  def sessionizeLogs =
    DataFrameOperations
      .sessionizeLogs(dataFrame)

  def generateAggregates =
    DataFrameOperations
      .generateAggregates(dataFrame)

  def generateReportAggregates =
    DataFrameOperations
      .generateReportAggregates(dataFrame)

  def generateActiveIpAggregates =
    DataFrameOperations
      .generateActiveIpAggregates(dataFrame)

}

object implicits {
  implicit def dataFrameOperationsOps(self: DataFrame): DataFrameOperationsOps =
    new DataFrameOperationsOps(self)
}
