package com.kafkaToSparkToCass

import java.sql.Timestamp

import com.datastax.driver.core.Session

object Statements extends Serializable {

  def cql(id: String, time: Timestamp, ename: String): String = s"""
       insert into my_keyspace.test_table (user_id,time,event)
       values('$id', '$time', '$ename event')"""

  def createKeySpaceAndTable(session: Session, dropTable: Boolean = false) = {
    session.execute(
      """CREATE KEYSPACE  if not exists  my_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };""")
    if (dropTable)
      session.execute("""drop table if exists my_keyspace.test_table""")

    session.execute(
      """create table if not exists my_keyspace.test_table ( user_id  text, time timestamp, event text, primary key((user_id), time) ) WITH CLUSTERING ORDER BY (time DESC)""")
  }
}
