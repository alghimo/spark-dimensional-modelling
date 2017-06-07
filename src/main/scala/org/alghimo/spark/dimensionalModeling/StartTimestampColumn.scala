package org.alghimo.spark.dimensionalModeling

import org.apache.spark.sql.Column

/**
  * Created by alghimo on 6/6/2017.
  */
trait StartTimestampColumn extends Serializable {
  def startTimestampColumnName: String = "dim_start_ts"
  def startTimestampColumn = new Column(startTimestampColumnName)
}
