package paypay.core

import java.util.UUID
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object PayPayUDF {

  def uuid: UserDefinedFunction =
    udf(() => UUID.randomUUID.toString)

}
