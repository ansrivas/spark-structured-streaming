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

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("com.datastax").setLevel(Level.WARN)
    Logger.getLogger("kafka").setLevel(Level.WARN)

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
  //read avro schema file
  @transient lazy val schemaString =
    Source.fromURL(getClass.getResource("/message.avsc")).mkString
  // Initialize schema
  @transient lazy val schema: Schema =
    new Schema.Parser().parse(schemaString)
  @transient lazy val reader = new SpecificDatumReader[GenericRecord](schema)

  logger.setLevel(Level.INFO)
  val sparkSession =
    SparkSession.builder
      .master("local[2]")
      .appName("kafka2Spark2Cassandra")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()
  println("Schema string, ", schemaString)

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

  val deserializer = udf { (input: Array[Byte]) =>
    deserializeMessage(input)
  }
  def deserializeMessage(input: Array[Byte]): Commons.UserEvent = {
    try {

      val in = new ByteArrayInputStream(input)
      val decoder = DecoderFactory.get.directBinaryDecoder(in, null)
      val msg = reader.read(null, decoder)

      Commons.UserEvent(msg.get("user_id").toString,
                        Commons.getTimeStamp(msg.get("timestamp").toString),
                        msg.get("event_id").toString)

    } catch {
      case e: Exception => null
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
      .option("startingOffsets", "latest")
      .load()
//      .selectExpr("""deserializer($value) AS message""")
//      .select($"value", $"topic")
    //      .selectExpr("value",
//                  "CAST(topic as STRING)",
//                  "CAST(partition as INTEGER)")
//      .selectExpr("CAST(value AS STRING)",
//                  "CAST(topic as STRING)",
//                  "CAST(partition as INTEGER)")
//      .as[(String, String, Integer)]

    lines.printSchema()
    val df = lines
      .select($"value")
      .withColumn("deserialized", deserializer($"value"))
      .select($"deserialized")

    df.printSchema()

    val ds = df
      .select($"deserialized.user_id",
              $"deserialized.time",
              $"deserialized.event")
      .as[Commons.UserEvent]

//     This Foreach sink writer writes the output to cassandra.
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
