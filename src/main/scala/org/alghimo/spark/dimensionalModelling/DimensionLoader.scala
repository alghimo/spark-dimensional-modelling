package org.alghimo.spark.dimensionalModelling

/**
  * Created by alghimo on 6/6/2017.
  */

import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

/**
  * Created by alghimo on 6/4/2017.
  * @todo Add support other slow change types.
  */
trait DimensionLoader[NATURAL_DIM <: (Product with Serializable), ENRICHED_DIM <: (Product with Serializable), DIM <: (Product with Serializable)]
  extends DimensionTableProvider[DIM]
  with NaturalDimensionOps[NATURAL_DIM, ENRICHED_DIM]
  with EnrichedDimensionOps[ENRICHED_DIM, DIM]
  with DimensionOps[ENRICHED_DIM, DIM]
  with SurrogateKeyColumn
  with TimestampColumn
  with StartTimestampColumn
  with EndTimestampColumn
  with IsCurrentColumn
  with Serializable
{
  /**
    * Extracts dimensions from enriched dimensions and returns the new dimension table (without saving it)
    * @param enrichedDimensions
    * @param refreshDimensionTable
    * @return The new dimension table
    */
  def extractDimensions(enrichedDimensions: EnrichedDimensions, refreshDimensionTable: Boolean = false): Dimensions = {
    val columns = dimensionTable(refreshDimensionTable).columns

    val notCurrentDims               = notCurrentDimensions(refreshDimensionTable).selectExpr(columns:_*).as[DIM]
    val unchangedDims                = unchangedDimensions(enrichedDimensions).selectExpr(columns:_*).as[DIM]
    val newDims                      = newDimensions(enrichedDimensions)
    val (newType2Dims, dimsToUpdate) = updatedDimensions(enrichedDimensions)
    val updatedDims                  = dimsToUpdate.selectExpr(columns:_*).as[DIM]
    val allNewDims                   = enrichedDimensionsToDimensions(newDims.union(newType2Dims)).selectExpr(columns:_*).as[DIM]

    notCurrentDims
      .union(unchangedDims)
      .union(updatedDims)
      .union(allNewDims)
  }

  /**
    * Returns the rows in the dimension table that either don't exist in the new dataset, or are unchanged.
    * High level, this code shoud be:
    * unchanged = dimensionTable.as('d)
    *   .join(ds.as('n), naturalKeyColumns("n"), "left")
    *   .filter(rightSideIsNull("n") || bothSidesAreTheSame("d", "n", ds.columns))
    *   .selectExpr("d.*")
    *   .as[D_WITH_KEY]
    * @param allEnrichedDimensions
    * @param refreshDimensionTable
    * @return
    */
  def unchangedDimensions(allEnrichedDimensions: EnrichedDimensions, refreshDimensionTable: Boolean = false): Dimensions = {
    val enrichedDimensions = keepOnlyMostRecentEvents(allEnrichedDimensions)
    val currentDims = currentDimensions(refreshDimensionTable)
    keepUnchanged(joinOnNaturalKeys(currentDims, enrichedDimensions, "left"))
  }

  /**
    * Extracts new dimensions from a dataset (ie: Dimensions that didn't exist before), adding a surrogate key.
    * High level, this should look like:
    * newDimsWithoutKey = ds.as('n)
    *   .join(dimensionTable.as('d), naturalKeyColumns("d"), "left")
    *   .filter(s"d.\${skColumnName} IS NULL")
    *   .selectExpr("n.*")
    *   .as[D_WITHOUT_KEY]
    * maxSk = dimensionTable.select(max(skColumn)).as[Long].collect
    * val rankWindow = Window.partitionBy()
    * newDimsWithoutKey
    *   .withColumn(skColumnName, row_number() over rankWindow)
    *   .withColumn(skColumnName, skColumn + lit(maxSk))
    *   .as[D_WITH_KEY]
    * @param allEnrichedDimensions
    * @param refreshDimensionTable
    * @return
    */
  def newDimensions(allEnrichedDimensions: EnrichedDimensions, refreshDimensionTable: Boolean = false): EnrichedDimensions = {
    val enrichedDimensions = keepOnlyMostRecentEvents(allEnrichedDimensions)
    val currentDims = currentDimensions(refreshDimensionTable)

    keepNew(joinOnNaturalKeys(enrichedDimensions, currentDims, "left"))
  }

  /**
    * Extracts updated dimensions from a dataset of enriched dimensions.
    * This method returns two datasets: one with enriched dimensions and one with dimensions.
    * The first one can happen only if there are type2 changes, since in that case we'll have new dimensions to insert.
    * The second one (updated dimensions) happens when there are type1 and / or type2 changes.
    * @param allEnrichedDimensions
    * @param refreshDimensionTable
    * @return
    */
  def updatedDimensions(allEnrichedDimensions: EnrichedDimensions, refreshDimensionTable: Boolean = false): (EnrichedDimensions, Dimensions) = {
    val enrichedDimensions = keepOnlyMostRecentEvents(allEnrichedDimensions)
    val currentDims = currentDimensions(refreshDimensionTable)
    val enrichedWithCurrentDims = joinOnNaturalKeys(enrichedDimensions, currentDims, "inner")

    val allUpdatedDims = enrichedWithCurrentDims.filter(e => areDifferent(e._1, e._2))

    val type1Updates = allUpdatedDims
      .filter(d => !hasType2Changes(d))
      .map(type1Change)

    val rawType2Updates = allUpdatedDims
      .filter(hasType2Changes _)
      .map(type2Changes)

    val type2OldDimensions = rawType2Updates.map(_._2)
    val type2NewDimensions = rawType2Updates.map(_._1)

    val updatedDimensions = type1Updates.union(type2OldDimensions)

    (type2NewDimensions, updatedDimensions)
  }

  /**
    * Extracts dimensions from enriched dimensions and returns the new dimension table (saving it)
    * @param enrichedDimensions
    * @param refreshDimensionTable
    * @return
    */
  def extractDimensionsAndSave(enrichedDimensions: EnrichedDimensions, refreshDimensionTable: Boolean = false): Dimensions =
    save(extractDimensions(enrichedDimensions, refreshDimensionTable))

  /**
    * Extracts dimensions from natural dimensions and returns the new dimension table (saving it)
    * @param naturalDimensions
    * @param refreshDimensionTable
    * @return
    */
  def extractDimensionsAndSave[X: ClassTag](naturalDimensions: NaturalDimensions, refreshDimensionTable: Boolean): Dimensions =
    extractDimensionsAndSave(naturalDimensions.map(naturalToEnrichedDimension), refreshDimensionTable)

  /**
    * Extracts dimensions from a dataframe and returns the new dimension table (saving it)
    * @param df
    * @param refreshDimensionTable
    * @return
    */
  def extractDimensionsAndSave[X: ClassTag, Y: ClassTag](df: DataFrame, refreshDimensionTable: Boolean)(naturalKeyExpressions: Seq[String]*): Dimensions =
    extractDimensionsAndSave(dfToEnrichedDimensions(df, naturalKeyExpressions:_*), refreshDimensionTable)

  /**
    * Extracts dimensions from natural dimensions and returns the new dimension table (without saving it)
    * @param naturalDimensions
    * @param refreshDimensionTable
    * @return The new dimension table
    */
  def extractDimensions[X: ClassTag](naturalDimensions: NaturalDimensions, refreshDimensionTable: Boolean): Dimensions =
    extractDimensions(naturalDimensions.map(naturalToEnrichedDimension), refreshDimensionTable)

  /**
    * Extracts dimensions from a dataframe and returns the new dimension table (without saving it)
    * @param df
    * @param refreshDimensionTable
    * @return The new dimension table
    */
  def extractDimensions[X: ClassTag, Y: ClassTag](df: DataFrame, refreshDimensionTable: Boolean)(naturalKeyExpressions: Seq[String]*): Dimensions =
    extractDimensions(dfToEnrichedDimensions(df, naturalKeyExpressions:_*), refreshDimensionTable)
}

