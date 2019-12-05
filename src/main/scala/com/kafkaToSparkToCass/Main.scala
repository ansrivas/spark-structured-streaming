package com.kafkaToSparkToCass

/**
  * Created by ankur on 18.12.16.
  */
import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}

import com.datastax.driver.core.Session

import collection.JavaConversions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession

object Main {

  private val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("akka").setLevel(Level.INFO)
    Logger.getLogger("com.datastax").setLevel(Level.INFO)
    Logger.getLogger("kafka").setLevel(Level.WARN)


    val sparkJob = new SparkJob()
    try {
      sparkJob.runJob()
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage)
        logger.error(ex.getStackTrace.toString)
    }
  }
}

class SparkJob extends Serializable {
  @transient lazy val logger = Logger.getLogger(this.getClass)

  logger.setLevel(Level.INFO)
  val sparkSession =
    SparkSession.builder
      .master("local[*]")
      .appName("kafka2Spark2Cassandra")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()

  val connector = CassandraConnector.apply(sparkSession.sparkContext.getConf)

  // Create keyspace and tables here, NOT in prod
  connector.withSessionDo { session =>
    Statements.createKeySpaceAndTable(session, true)
  }

  private def processRow(value: Commons.UserEvent) = {
    connector.withSessionDo { session =>
      session.execute(Statements.cql(value.user_id, value.time, value.event))
    }
  }

  def runJob() = {

    logger.info("Execution started with following configuration")
    val cols = List("user_id", "time", "event")

    import sparkSession.implicits._
    val lines = sparkSession.readStream
      .format("kafka")
      .option("subscribe", "test.1")
      .option("kafka.bootstrap.servers", "localhost:9092")
      //.option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)",
                  "CAST(topic as STRING)",
                  "CAST(partition as INTEGER)")
      .as[(String, String, Integer)]

    val df =
      lines.map { line =>
        val columns = line._1.split(";") // value being sent out as a comma separated value "userid_1;2015-05-01T00:00:00;some_value"
        (columns(0), Commons.getTimeStamp(columns(1)), columns(2))
      }.toDF(cols: _*)

    df.printSchema()

    // Run your business logic here
    val ds = df.select($"user_id", $"time", $"event").as[Commons.UserEvent]

    // This Foreach sink writer writes the output to cassandra.
    import org.apache.spark.sql.ForeachWriter
    val writer = new ForeachWriter[Commons.UserEvent] {
      override def open(partitionId: Long, version: Long) = true
      override def process(value: Commons.UserEvent) = {
        processRow(value)
      }
      override def close(errorOrNull: Throwable) = {}
    }

    val query =
      ds.writeStream.queryName("kafka2Spark2Cassandra").foreach(writer).start

    query.awaitTermination()
    sparkSession.stop()
  }
}
