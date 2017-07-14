package org.alghimo.spark.dimensionalModelling

import org.apache.spark.sql.Column

/**
  * Created by alghimo on 6/6/2017.
  */
trait StartTimestampColumn extends Serializable {
  def startTimestampColumnName: String = "_dim_start_ts"
  def startTimestampColumn = new Column(startTimestampColumnName)
}
