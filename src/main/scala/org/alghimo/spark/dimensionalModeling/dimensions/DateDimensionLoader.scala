package org.alghimo.spark.dimensionalModeling.dimensions

import org.alghimo.spark.dimensionalModeling.DimensionLoader
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

/**
  * Created by alghimo on 6/6/2017.
  */
class DateDimensionLoader(override val dimensionTableName: String, @transient override val spark: SparkSession)
  extends DimensionLoader[NaturalDateDimension, EnrichedDateDimension, DateDimension]
  with Serializable
{
  override def surrogateKeyColumnName: String = "date_sk"

  override def naturalKeyColumns: Seq[String] = Seq("date")

  override def naturalToEnrichedDimension(n: NaturalDateDimension): EnrichedDateDimension = EnrichedDateDimension(n.date)

  implicit def naturalDimensionEncoder = Encoders.product[NaturalDateDimension]
  implicit def enrichedDimensionEncoder = Encoders.product[EnrichedDateDimension]
  implicit def dimensionEncoder = Encoders.product[DateDimension]
  implicit def enrichedWithDimensionEncoder = Encoders.product[(EnrichedDateDimension, DateDimension)]
  implicit def dimensionWithEnrichedEncoder = Encoders.product[(DateDimension, EnrichedDateDimension)]

  override def areEqual(enrichedDim: EnrichedDateDimension, dim: DateDimension): Boolean = {
    enrichedDim.year  == dim.year &&
    enrichedDim.month == dim.month &&
    enrichedDim.day   == dim.day
  }

  def areEqualSql: String = {
    "(_1.year = _2.year AND _1.month = _2.month AND _1.day = _2.day)"
  }

  /**
    * This method takes a tuple (ENRICHED_DIM, DIM) and returns a Dimension.
    * Usually, to get the resulting dimension, it takes all the values from the enriched dimension, and the special columns
    * from the existing dimension: surrogate key, is_current, start and end timestamps. With sql it could look like:
    * allUpdatedDims
    *   .selectExpr(s"_2.${skColumnName}", s"_2.${isCurrentVersionColumnName}", s"_2.${startTimestampColumnName}", s"_2.${endTimestampColumnName}", "_1.*")
    *   .selectExpr(currentDims.columns:_*)
    *   .as[D_WITH_KEY]
    * @param enrichedWithDimension Tuple (ENRICHED_DIM, DIM)
    * @return
    */
  override def type1Change(enrichedWithDimension: (EnrichedDateDimension, DateDimension)): DateDimension = {
    val d: DateDimension         = enrichedWithDimension._2
    val e: EnrichedDateDimension = enrichedWithDimension._1

    d.copy(
      date             = e.date,
      day              = e.day,
      month            = e.month,
      quarter          = e.quarter,
      semester         = e.semester,
      year             = e.year,
      day_of_week      = e.day_of_week,
      day_of_week_name = e.day_of_week_name,
      day_of_year      = e.day_of_year,
      is_weekend       = e.is_weekend,
      week_of_year     = e.week_of_year,
      biweek_of_year   = e.biweek_of_year,
      month_name       = e.month_name,
      month_name_short = e.month_name_short,
      day_seq          = e.day_seq,
      week_seq         = e.week_seq,
      biweek_seq       = e.biweek_seq,
      month_seq        = e.month_seq,
      quarter_seq      = e.quarter_seq,
      semester_seq     = e.semester_seq
    )
  }

  override def hasType2Changes(joinedDim: EnrichedWithDimension): Boolean = false

  /**
    * This method takes a tuple (ENRICHED_DIM, DIM) and returns another tuple (ENRICHED_DIM, DIM).
    * The first element of the tuple is the new (current) version of the dimension, and the second one
    * is the "old" version. An old version has the "is_current" column set to false and an end timestamp.
    * @param enrichedWithDimension Tuple (ENRICHED_DIM, DIM)
    * @return
    */
  override def type2Changes(enrichedWithDimension: (EnrichedDateDimension, DateDimension)): (EnrichedDateDimension, DateDimension) = {
    val d: DateDimension         = enrichedWithDimension._2
    val e: EnrichedDateDimension = enrichedWithDimension._1

    (e, d.copy(dim_is_current = false, dim_end_ts = Some(e.dim_timestamp)))
  }
}
