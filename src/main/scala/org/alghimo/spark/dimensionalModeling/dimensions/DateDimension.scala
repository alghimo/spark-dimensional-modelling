package org.alghimo.spark.dimensionalModeling.dimensions

import org.joda.time.DateTime
import java.sql.{Date => SqlDate}
import java.sql.Timestamp

/**
  * Created by alghimo on 5/31/2017.
  */
case class DateDimension(
  date_sk:          Long,
  date:             SqlDate,
  day:              Byte,
  month:            Byte,
  quarter:          Byte,
  semester:         Byte,
  year:             Short,
  day_of_week:      Byte,
  day_of_week_name: String,
  day_of_year:      Short,
  is_weekend:       Boolean,
  week_of_year:     Byte,
  biweek_of_year:   Byte,
  month_name:       String,
  month_name_short: String,
  day_seq:          Int,
  week_seq:         Int,
  biweek_seq:       Int,
  month_seq:        Int,
  quarter_seq:      Int,
  semester_seq:     Int,
  dim_timestamp:    Timestamp,
  dim_is_current:   Boolean = true,
  dim_start_ts:     Option[Timestamp] = None,
  dim_end_ts:       Option[Timestamp] = None
)

object DateDimension {
  import DateUtils._

  def apply(key: Long, date: DateTime): DateDimension = {
    val month = date.getMonthOfYear

    DateDimension(
      key,
      date             = new SqlDate(date.getMillis),
      day              = date.getDayOfMonth.toByte,
      month            = month.toByte,
      quarter          = quarter(month).toByte,
      semester         = semester(month).toByte,
      year             = date.getYear.toShort,
      day_of_week      = date.getDayOfWeek.toByte,
      day_of_week_name = date.dayOfWeek.getAsText,
      day_of_year      = date.getDayOfYear.toShort,
      is_weekend       = weekendDays.contains(date.getDayOfWeek),
      week_of_year     = date.getWeekOfWeekyear.toByte,
      biweek_of_year   = ((date.getWeekOfWeekyear + 1) / 2).toByte,
      month_name       = date.toString("MMMM"),
      month_name_short = date.toString("MMM"),
      day_seq          = daySeq(date),
      week_seq         = weekSeq(date),
      biweek_seq       = biweekSeq(date),
      month_seq        = monthSeq(date),
      quarter_seq      = quarterSeq(date),
      semester_seq     = semesterSeq(date),
      dim_timestamp    = new Timestamp(date.getMillis),
      dim_is_current   = true,
      dim_start_ts     = Some(new Timestamp(date.getMillis)),
      dim_end_ts       = None
    )
  }

  def apply(key: Long, sqlDate: SqlDate): DateDimension = apply(key, new DateTime(sqlDate))
}
