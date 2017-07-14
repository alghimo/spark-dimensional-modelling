package org.alghimo.spark.dimensionalModelling

import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.apache.spark.sql.functions.expr

import scala.reflect.ClassTag
/**
  * Created by alghimo on 6/6/2017.
  */
trait DimensionOps[ENRICHED_DIM <: (Product with Serializable), DIM <: (Product with Serializable)]
  extends DimensionTableProvider[DIM]
  with SurrogateKeyColumn
  with TimestampColumn
  with StartTimestampColumn
  with EndTimestampColumn
  with IsCurrentColumn
  with EnrichedDimensionProvider[ENRICHED_DIM]
  with DimensionProvider[DIM]
  with EnrichedDimensionWithDimensionProvider[ENRICHED_DIM,DIM]
  with Serializable
{
  /**
    * Columns that trigger a type 2 change when updated (type2 = history is kept).
    * By default, a type1 change is applied.
    */
  def type2ChangeColumns: Seq[String] = Seq.empty

  /**
    * Defines the condition to join two datasets based on the natural key.
    * This is used to join datasets without key with datasets with key.
    * @return
    */
  def naturalKeyColumns: Seq[String]

  /**
    * This method takes a tuple (ENRICHED_DIM, DIM) and returns a Dimension.
    * Usually, to get the resulting dimension, it takes all the values from the enriched dimension, and the special columns
    * from the existing dimension: surrogate key, is_current, start and end timestamps. With sql it could look like:
    * allUpdatedDims
    *   .selectExpr(s"_2.\${skColumnName}", s"_2.\${isCurrentVersionColumnName}", s"_2.\${startTimestampColumnName}", s"_2.\${endTimestampColumnName}", "_1.*")
    *   .selectExpr(currentDims.columns:_*)
    *   .as[D_WITH_KEY]
    * @param enrichedWithDimension Tuple (ENRICHED_DIM, DIM)
    * @return
    */
  def type1Change(enrichedWithDimension: EnrichedWithDimension): DIM


  /**
    * This method takes a tuple (ENRICHED_DIM, DIM) and returns another tuple (ENRICHED_DIM, DIM).
    * The first element of the tuple is the new (current) version of the dimension, and the second one
    * is the "old" version. An old version has the "is_current" column set to false and an end timestamp.
    * @param enrichedWithDimension Tuple (ENRICHED_DIM, DIM)
    * @return
    */
  def type2Changes(enrichedWithDimension: EnrichedWithDimension): EnrichedWithDimension

  /**
    * Checks whether the give tuple (ENRICHED_DIM, DIM) has type2 changes
    * @param joinedDim
    * @return
    */
  def hasType2Changes(joinedDim: EnrichedWithDimension): Boolean

  def areEqual(enrichedDim: ENRICHED_DIM, dim: DIM): Boolean
  def areEqualSql: String
  def areDifferent(enrichedDim: ENRICHED_DIM, dim: DIM): Boolean = !areEqual(enrichedDim, dim)

  def keepWhenBothAreDifferent(joinedDims: JoinEnrichedWithDimension): JoinEnrichedWithDimension = joinedDims.filter(j => areDifferent(j._1, j._2))

  def enrichedIsNullOrEqual(joinedDim: EnrichedWithDimension): Boolean = joinedDim._1 == null || areEqual(joinedDim._1, joinedDim._2)
  def enrichedIsNullOrEqualSql(enrichedAlias: String): String = s"${enrichedAlias} IS NULL OR $areEqualSql"

  def keepUnchanged(joinedDims: JoinEnrichedWithDimension): Dimensions = joinedDims.filter(enrichedIsNullOrEqualSql("_1")).map(_._2)
  def keepUnchanged[X: ClassTag](joinedDims: JoinDimensionWithEnriched): Dimensions = joinedDims.filter(enrichedIsNullOrEqualSql("_2")).map(_._1)

  def keepNew(joinedDims: JoinEnrichedWithDimension): EnrichedDimensions = joinedDims.filter("_2 IS NULL").map(_._1)
  def keepNew[X: ClassTag](joinedDims: JoinDimensionWithEnriched): EnrichedDimensions = joinedDims.filter("_1 IS NULL").map(_._2)

  def joinOnNaturalKeys(enrichedDimensions: EnrichedDimensions, dimensions: Dimensions, joinType: String): JoinEnrichedWithDimension = {
    val joinCondition = naturalKeyColumns
      .map(c => (enrichedDimensions.col(c).isNull && dimensions.col(c).isNull) || enrichedDimensions.col(c)===dimensions.col(c))
      .reduceLeft(_ && _)

    enrichedDimensions.joinWith(dimensions, joinCondition, joinType)
  }

  def joinOnNaturalKeys[X: ClassTag](dimensions: Dimensions, enrichedDimensions: EnrichedDimensions, joinType: String): JoinDimensionWithEnriched = {
    val joinCondition = naturalKeyColumns
      .map(c => (enrichedDimensions.col(c).isNull && dimensions.col(c).isNull) || enrichedDimensions.col(c)===dimensions.col(c))
      .reduceLeft(_ && _)

    dimensions.joinWith(enrichedDimensions, joinCondition, joinType)
  }
}
