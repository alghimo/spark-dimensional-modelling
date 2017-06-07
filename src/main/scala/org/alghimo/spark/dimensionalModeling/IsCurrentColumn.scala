package org.alghimo.spark.dimensionalModeling

import org.apache.spark.sql.Column

/**
  * Created by alghimo on 6/6/2017.
  */
trait IsCurrentColumn extends Serializable {
  def isCurrentColumnName: String = "dim_is_current"
  def isCurrentColumn = new Column(isCurrentColumnName)
}
