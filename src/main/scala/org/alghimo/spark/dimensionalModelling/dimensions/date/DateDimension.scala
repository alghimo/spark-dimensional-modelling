package org.alghimo.spark.dimensionalModelling.dimensions.date

import java.sql.{Timestamp, Date => SqlDate}

import org.joda.time.DateTime

/**
  * Created by alghimo on 5/31/2017.
  */
case class DateDimension(
  date_sk:                Long,
  date:                   SqlDate,
  day:                    Byte,
  month:                  Byte,
  quarter:                Byte,
  semester:               Byte,
  year:                   Short,
  day_of_week:            Byte,
  day_of_week_name:       String,
  day_of_week_name_short: String,
  day_of_year:            Short,
  is_weekend:             Boolean,
  week_of_year:           Byte,
  biweek_of_year:         Byte,
  month_name:             String,
  month_name_short:       String,
  day_seq:                Int,
  week_seq:               Int,
  biweek_seq:             Int,
  month_seq:              Int,
  quarter_seq:            Int,
  semester_seq:           Int,
  _dim_timestamp:         Timestamp,
  _dim_is_current:        Boolean = true,
  _dim_start_ts:          Option[Timestamp] = None,
  _dim_end_ts:            Option[Timestamp] = None
)

object DateDimension {
  import DateUtils._

  def apply(key: Long, date: DateTime): DateDimension = {
    val month = date.getMonthOfYear

    DateDimension(
      key,
      date                   = new SqlDate(date.getMillis),
      day                    = date.getDayOfMonth.toByte,
      month                  = month.toByte,
      quarter                = quarter(month).toByte,
      semester               = semester(month).toByte,
      year                   = date.getYear.toShort,
      day_of_week            = date.getDayOfWeek.toByte,
      day_of_week_name       = date.dayOfWeek.getAsText,
      day_of_week_name_short = date.dayOfWeek.getAsText,
      day_of_year            = date.getDayOfYear.toShort,
      is_weekend             = weekendDays.contains(date.getDayOfWeek),
      week_of_year           = date.getWeekOfWeekyear.toByte,
      biweek_of_year         = ((date.getWeekOfWeekyear + 1) / 2).toByte,
      month_name             = date.toString("MMMM"),
      month_name_short       = date.toString("MMM"),
      day_seq                = daySeq(date),
      week_seq               = weekSeq(date),
      biweek_seq             = biweekSeq(date),
      month_seq              = monthSeq(date),
      quarter_seq            = quarterSeq(date),
      semester_seq           = semesterSeq(date),
      _dim_timestamp         = new Timestamp(date.getMillis),
      _dim_is_current        = true,
      _dim_start_ts          = Some(new Timestamp(date.getMillis)),
      _dim_end_ts            = None
    )
  }

  def apply(key: Long, sqlDate: SqlDate): DateDimension = apply(key, new DateTime(sqlDate))
}
