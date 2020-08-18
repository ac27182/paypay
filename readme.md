# PayPay Data Engineering Challenge

## Core Processing and Analytical Goals

- Sessionize the logs by `ip address`

- Derive the number of `unique url visits` per session

- Derive the `average session time`

- find the `top 10 percent` of users by total session time

# local setup

## prerequisites

- openjdk8
- docker >= 19.03.11
- scala >= 2.12.11
- sbt >= 1.3.11

## testing steps

```sh
# clone the reposiory
$ git clone https://github.com/ac27182/paypay.git

$ cd paypay

# build the tech test jar
$ sbt -mem 4096 techTest/clean techTest/assembly

# mount the techtest directory inside the spark container
$ docker run -v `pwd`:/paypay -it bde2020/spark-master:2.4.5-hadoop2.7 /bin/bash

# run the spark job inside the container
$ spark/bin/spark-submit --class paypay.Main /paypay/techTest/target/scala-2.12/techTest.jar

# exit the spark container
$ exit

# run cleanup
$ sbt clean

# remove spark image image
$ docker rmi -f bde2020/spark-master:2.4.5-hadoop2.7
```

- **nb:** output files will be be written to `paypay/data/ouput/*`

## schemas

### access log schema

```
root
 |-- time: timestamp (nullable = true)
 |-- elb: string (nullable = true)
 |-- client:port: string (nullable = true)
 |-- backend:port: string (nullable = true)
 |-- request_processing_time: float (nullable = true)
 |-- backend_processing_time: float (nullable = true)
 |-- response_processing_time: float (nullable = true)
 |-- elb_status_code: integer (nullable = true)
 |-- backend_status_code: integer (nullable = true)
 |-- received_bytes: integer (nullable = true)
 |-- sent_bytes: integer (nullable = true)
 |-- request: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- ssl_cipher: string (nullable = true)
 |-- ssl_protocol: string (nullable = true)
```

# references

- https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html#access-log-entry-format
- https://github.com/Pay-Baymax/DataEngineerChallenge
- https://mode.com/sql-tutorial/sql-window-functions/
