package org.alghimo.spark.dimensionalModelling

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
    println("Creating natural dimensionions from dataframe")
    naturalKeyExpressions
      .map(df.selectExpr(_:_*))
      .reduce(_.union(_))
      .distinct
      .as[NATURAL_DIM]
  }

  def naturalToEnrichedDimension(n: NATURAL_DIM): ENRICHED_DIM

  /**
    * Transforms a DataFrame into Enriched dimensions.
    * @param df The dataframe to extract the enriched dimensions from.
    * @param naturalKeyExpressions Multiple sequences of strings that define how to extract dimensions.
    * For instance, let's say that your Dataframe contains dates in the "created_date" and "updated_date" columns,
    * The call to this method would be like:
    * {{{
    *   dfToEnrichedDimensions(myDf, Seq("created_date"), Seq("updated_date")
    * }}}
    * @return
    */
  def dfToEnrichedDimensions(df: DataFrame, naturalKeyExpressions: Seq[String]*): EnrichedDimensions = {
    println("Creating enriched dimensionions from dataframe")
    val naturalToEnrichedDimension = this.naturalToEnrichedDimension _
    dfToNaturalDimensions(df, naturalKeyExpressions:_*).map(naturalToEnrichedDimension)
  }

  def naturalToEnrichedDimensions(naturalDims: NaturalDimensions): EnrichedDimensions = {
    println("Creating enriched dimensionions from natural dimensions")
    val naturalToEnrichedDimension = this.naturalToEnrichedDimension _
    naturalDims.map(naturalToEnrichedDimension)
  }
}
