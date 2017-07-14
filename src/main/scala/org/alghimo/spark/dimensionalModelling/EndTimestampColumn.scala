package org.alghimo.spark.dimensionalModelling

import org.apache.spark.sql.Column

/**
  * Created by alghimo on 6/6/2017.
  */
trait EndTimestampColumn extends Serializable {
  def endTimestampColumnName: String = "_dim_end_ts"
  def endTimestampColumn = new Column(endTimestampColumnName)
}
