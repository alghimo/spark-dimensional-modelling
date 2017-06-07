package org.alghimo.spark.dimensionalModeling.dimensions

import org.joda.time.DateTime

/**
  * Created by alghimo on 5/31/2017.
  */
object DateUtils extends Serializable {
    val millisInDay = 1000 * 60 * 60 * 24
    val daysInWeek = 7
    val daysInBiweek = 14
    val monthsInYear = 12
    val quartersInYear = 4
    val semestersInYear = 2
    val firstEpochYear = 1970
    val weekendDays = Seq(6, 7)

    def quarter(month: Int) = 1 + (month - 1) / 3
    def semester(month: Int) = 1 + (month - 1) / 6
    def daySeq(date: DateTime) = (date.getMillis / millisInDay).toInt
    def weekSeq(date: DateTime) = (date.getMillis / (millisInDay * daysInWeek)).toInt
    def biweekSeq(date: DateTime) = (date.getMillis / (millisInDay * daysInBiweek)).toInt
    def monthSeq(date: DateTime) = ((date.getYear - firstEpochYear) * monthsInYear + date.getMonthOfYear).toInt
    def quarterSeq(date: DateTime) = ((date.getYear - firstEpochYear) * quartersInYear + quarter(date.getMonthOfYear)).toInt
    def semesterSeq(date: DateTime) = ((date.getYear - firstEpochYear) * semestersInYear + semester(date.getMonthOfYear)).toInt
}
