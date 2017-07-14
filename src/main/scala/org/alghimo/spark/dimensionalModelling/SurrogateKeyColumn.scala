package org.alghimo.spark.dimensionalModelling

import org.apache.spark.sql.Column

/**
  * Created by alghimo on 6/6/2017.
  */
trait SurrogateKeyColumn extends Serializable {
  def surrogateKeyColumnName: String
  def surrogateKeyColumn = new Column(surrogateKeyColumnName)
}
