package com.kafkaToSparkToCass

/**
  * Created by ankur on 19.12.16.
  */
import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}

object Commons {

  case class UserEvent(user_id: String, time: Timestamp, event: String)
      extends Serializable

  def getTimeStamp(timeStr: String): Timestamp = {
    val dateFormat1: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormat2: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    val date: Option[Timestamp] = {
      try {
        Some(new Timestamp(dateFormat1.parse(timeStr).getTime))
      } catch {
        case e: java.text.ParseException =>
          Some(new Timestamp(dateFormat2.parse(timeStr).getTime))
      }
    }
    date.getOrElse(Timestamp.valueOf(timeStr))
  }

}
