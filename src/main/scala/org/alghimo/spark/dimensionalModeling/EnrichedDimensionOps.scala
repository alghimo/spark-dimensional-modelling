package org.alghimo.spark.dimensionalModeling

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, max, row_number}

/**
  * Created by alghimo on 6/6/2017.
  */
trait EnrichedDimensionOps[ENRICHED_DIM <: (Product with Serializable), DIM <: (Product with Serializable)]
  extends DimensionTableProvider[DIM]
  with SurrogateKeyColumn
  with TimestampColumn
  with StartTimestampColumn
  with EndTimestampColumn
  with IsCurrentColumn
  with EnrichedDimensionProvider[ENRICHED_DIM]
  with DimensionProvider[DIM]
{
  import spark.implicits._

  /**
    * Defines the condition to join two datasets based on the natural key.
    * This is used to join datasets without key with datasets with key.
    * @return
    */
  def naturalKeyColumns: Seq[String]

  def enrichedDimensionsToDimensions(enrichedDims: EnrichedDimensions, refreshDimensionTable: Boolean = false): Dimensions = {
    val dimensions = dimensionTable(refreshDimensionTable)
    val maxSk = dimensions.select(max(surrogateKeyColumn)).as[Long].collect().head
    val rankWindow = Window.partitionBy().orderBy(naturalKeyColumns.head, naturalKeyColumns.tail:_*)

    enrichedDims
      .withColumn(surrogateKeyColumnName, lit(maxSk) + (row_number() over rankWindow))
      .withColumn(startTimestampColumnName, timestampColumn)
      .withColumn(endTimestampColumnName, lit(null).cast("timestamp"))
      .withColumn(isCurrentColumnName, lit(true))
      .selectExpr(dimensions.columns:_*)
      .as[DIM]
  }

  def keepOnlyMostRecentEvents(enrichedDimensions: EnrichedDimensions): EnrichedDimensions = {
    val naturalKeyWindow = Window.partitionBy(naturalKeyColumns.map(new Column(_)):_*).orderBy(timestampColumn.desc)

    enrichedDimensions
      .withColumn("row_num", row_number() over naturalKeyWindow)
      .filter("row_num = 1")
      .drop("row_num")
      .as[ENRICHED_DIM]
  }
}
