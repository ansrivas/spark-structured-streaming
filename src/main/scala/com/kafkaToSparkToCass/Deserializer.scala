/**
  * Copyright (C) Recogizer Group GmbH - All Rights Reserved
  * Unauthorized copying of this file, via any medium is strictly prohibited
  * Proprietary and confidential
  * Created on 15.07.17.
  */
package com.kafkaToSparkToCass

import java.io.ByteArrayInputStream

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.sql.functions.udf

import scala.io.Source

object Deserializer extends Serializable {

  val deser = udf { (input: Array[Byte]) =>
    deserializeMessage(input)
  }
  //read avro schema file
  @transient lazy val schemaString =
    Source.fromURL(getClass.getResource("/message.avsc")).mkString
  // Initialize schema
  @transient lazy val schema: Schema =
    new Schema.Parser().parse(schemaString)
  @transient lazy val reader = new SpecificDatumReader[GenericRecord](schema)

  private def deserializeMessage(input: Array[Byte]): Commons.UserEvent = {
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
}
