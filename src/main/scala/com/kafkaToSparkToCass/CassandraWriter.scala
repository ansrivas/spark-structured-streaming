package com.kafkaToSparkToCass

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession

class CassandraWriter(sparkSession: SparkSession) extends Serializable {
  val connector = CassandraConnector.apply(sparkSession.sparkContext.getConf)

  // Create keyspace and tables here, NOT in prod
  def createKeySpace(): Unit = {
    connector.withSessionDo { session =>
      Statements.createKeySpaceAndTable(session, true)
    }
  }

  private def processRow(value: Commons.UserEvent) = {
    connector.withSessionDo { session =>
      session.execute(Statements.cql(value.user_id, value.time, value.event))
    }
  }

  //     This Foreach sink writer writes the output to cassandra.
  import org.apache.spark.sql.ForeachWriter
  val writer = new ForeachWriter[Commons.UserEvent] {
    override def open(partitionId: Long, version: Long) = true
    override def process(value: Commons.UserEvent) = {
      processRow(value)
    }
    override def close(errorOrNull: Throwable) = {}
  }
}
