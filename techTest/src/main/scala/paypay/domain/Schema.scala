package paypay.domain

import org.apache.spark.sql.types._

object Schema {

  val accessLogSchema: StructType =
    new StructType()
      .add("time", TimestampType)
      .add("elb", StringType)
      .add("client:port", StringType)
      .add("backend:port", StringType)
      .add("request_processing_time", FloatType)
      .add("backend_processing_time", FloatType)
      .add("response_processing_time", FloatType)
      .add("elb_status_code", IntegerType)
      .add("backend_status_code", IntegerType)
      .add("received_bytes", IntegerType)
      .add("sent_bytes", IntegerType)
      .add("request", StringType)
      .add("user_agent", StringType)
      .add("ssl_cipher", StringType)
      .add("ssl_protocol", StringType)

}
