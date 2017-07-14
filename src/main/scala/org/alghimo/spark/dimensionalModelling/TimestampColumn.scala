package org.alghimo.spark.dimensionalModelling

import org.apache.spark.sql.Column

/**
  * Created by alghimo on 6/6/2017.
  */
trait TimestampColumn extends Serializable {
  def timestampColumnName: String = "_dim_timestamp"
  def timestampColumn = new Column(timestampColumnName)
}
