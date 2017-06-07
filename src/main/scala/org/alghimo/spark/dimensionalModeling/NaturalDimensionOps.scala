package org.alghimo.spark.dimensionalModeling

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, max, row_number}

/**
  * Created by alghimo on 6/6/2017.
  */
trait NaturalDimensionOps[NATURAL_DIM <: (Product with Serializable), ENRICHED_DIM <: (Product with Serializable)]
  extends NaturalDimensionProvider[NATURAL_DIM]
  with EnrichedDimensionProvider[ENRICHED_DIM]
  with Serializable
{
  def dfToNaturalDimensions(df: DataFrame, naturalKeyExpressions: Seq[String]*): NaturalDimensions = {
    naturalKeyExpressions
      .map(df.selectExpr(_:_*))
      .reduce(_.union(_))
      .distinct
      .as[NATURAL_DIM]
  }

  def naturalToEnrichedDimension(n: NATURAL_DIM): ENRICHED_DIM

  def dfToEnrichedDimensions(df: DataFrame, naturalKeyExpressions: Seq[String]*): EnrichedDimensions = {
    val naturalToEnrichedDimension = this.naturalToEnrichedDimension _
    dfToNaturalDimensions(df, naturalKeyExpressions:_*).map(naturalToEnrichedDimension)
  }

  def naturalToEnrichedDimensions(naturalDims: NaturalDimensions): EnrichedDimensions = {
    val naturalToEnrichedDimension = this.naturalToEnrichedDimension _
    naturalDims.map(naturalToEnrichedDimension)
  }
}
