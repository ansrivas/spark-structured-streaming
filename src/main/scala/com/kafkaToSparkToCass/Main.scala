package com.kafkaToSparkToCass

/**
  * Created by ankur on 18.12.16.
  */
import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}

import com.datastax.driver.core.Session
import java.io.ByteArrayInputStream

import collection.JavaConversions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.CassandraRowWriter
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.explode
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  StringDeserializer
}
import org.apache.spark

import scala.io.Source

object Main {

  private val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("akka").setLevel(Level.INFO)
    Logger.getLogger("com.datastax").setLevel(Level.INFO)
    Logger.getLogger("kafka").setLevel(Level.INFO)

    logger.setLevel(Level.INFO)

    val sparkJob = new SparkJob()
    try {
      sparkJob.runJob()
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage)
        logger.error(ex.printStackTrace())
    }
  }
}

class SparkJob extends Serializable {
  @transient lazy val logger = Logger.getLogger(this.getClass)

  logger.setLevel(Level.INFO)
  val sparkSession =
    SparkSession.builder
      .master("local[2]")
      .appName("kafka2Spark2Cassandra")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()

  // Check this class thoroughly, it does some initializations which shouldn't be in PRODUCTION
  // WARNING: go through this class properly.
  @transient val cassWriter = new CassandraWriter(sparkSession)

  def runJob() = {

    logger.info("Execution started with following configuration")

    import sparkSession.implicits._

    val lines = sparkSession.readStream
      .format("kafka")
      .option("subscribe", "test.1")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("value",
                  "CAST(topic as STRING)",
                  "CAST(partition as INTEGER)")

    lines.printSchema()

    val df = lines
      .select($"value")
      .withColumn("deserialized", Deserializer.deser($"value"))
      .select($"deserialized")

    df.printSchema()

    val ds = df
      .select($"deserialized.user_id",
              $"deserialized.time",
              $"deserialized.event")
      .as[Commons.UserEvent]

    val query =
      ds.writeStream
        .queryName("kafka2Spark2Cassandra")
        .foreach(cassWriter.writer)
        .start

    query.awaitTermination()
    sparkSession.stop()
  }
}
