package paypay.domain

case class AccessLog(
    timestamp: String,
    elb: String,
    `client:port`: String,
    `backend:port`: String,
    request_processing_time: String,
    backend_processing_time: String,
    response_processing_time: String,
    elb_status_code: String,
    backend_status_code: String,
    received_bytes: String,
    sent_bytes: String,
    request: String,
    user_agent: String,
    ssl_cipher: String,
    ssl_protocol: String
)
