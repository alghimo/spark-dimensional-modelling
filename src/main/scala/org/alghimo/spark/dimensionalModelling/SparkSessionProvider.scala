package org.alghimo.spark.dimensionalModelling

import org.apache.spark.sql.SparkSession

/**
  * Created by alghimo on 6/4/2017.
  */
trait SparkSessionProvider extends Serializable {
  @transient val spark: SparkSession
}
