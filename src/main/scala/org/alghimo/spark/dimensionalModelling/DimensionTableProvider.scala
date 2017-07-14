package org.alghimo.spark.dimensionalModelling

import org.apache.spark.sql.{Column, Dataset, Encoder, Encoders}

/**
  * Created by alghimo on 6/4/2017.
  */
trait DimensionTableProvider[DIM <: (Product with Serializable)]
  extends SparkSessionProvider
  with IsCurrentColumn
  with DimensionProvider[DIM]
{
  /**
    * Name of the table where the dimension data is.
    * @return
    */
  def dimensionTableName: String

  def tmpDimensionTableName: String = {
    val Array(dbName, tableName) = dimensionTableName.split('.')

    s"${dbName}.tmp_${tableName}"
  }

  def maxPartitionsInDimensionTable: Int = 400

  def dimensionTable(refresh: Boolean = false): Dataset[DIM] = {
    if (refresh) {
      spark.catalog.refreshTable(dimensionTableName)
    }

    spark.table(dimensionTableName).as[DIM]
  }

  def currentDimensions(refresh: Boolean = false): Dataset[DIM] = dimensionTable(refresh).filter(s"${isCurrentColumnName}")
  def notCurrentDimensions(refresh: Boolean = false): Dataset[DIM] = dimensionTable(refresh).filter(s"NOT ${isCurrentColumnName}")

  def save(ds: Dataset[DIM], useTempTable: Boolean = true): Dataset[DIM] = {
    val toSave = if (useTempTable) {
      ds
        .coalesce(maxPartitionsInDimensionTable)
        .write
        .mode("overwrite")
        .saveAsTable(tmpDimensionTableName)

      spark.table(tmpDimensionTableName)
    } else {
      ds
    }

    toSave
      .coalesce(maxPartitionsInDimensionTable)
      .write
      .mode("overwrite")
      .saveAsTable(dimensionTableName)

    dimensionTable(refresh = true)
  }
}
