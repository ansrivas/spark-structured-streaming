package com.kafkaToSparkToCass

import java.sql.Timestamp

import com.datastax.driver.core.Session

object Statements extends Serializable {

  def cql(id: String, time: Timestamp, ename: String): String = s"""
       INSERT INTO my_keyspace.test_table (user_id,time,event)
       VALUES('$id', '$time', '$ename event')"""

  def createKeySpaceAndTable(session: Session, dropTable: Boolean = false) = {
    session.execute(
      """CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };""")
    if (dropTable)
      session.execute("""DROP TABLE IF EXISTS my_keyspace.test_table""")

    session.execute(
      """CREATE TABLE IF NOT EXISTS my_keyspace.test_table ( user_id  text, time timestamp, event text, primary key((user_id), time) ) WITH CLUSTERING ORDER BY (time DESC)""")
  }
}
