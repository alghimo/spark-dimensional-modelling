package org.alghimo.spark.dimensionalModelling

import org.apache.spark.sql.Column

/**
  * Created by alghimo on 6/6/2017.
  */
trait IsCurrentColumn extends Serializable {
  def isCurrentColumnName: String = "_dim_is_current"
  def isCurrentColumn = new Column(isCurrentColumnName)
}
