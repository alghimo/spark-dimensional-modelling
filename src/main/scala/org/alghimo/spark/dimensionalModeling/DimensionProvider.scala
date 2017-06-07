package org.alghimo.spark.dimensionalModeling

import org.apache.spark.sql.{Dataset, Encoder}

/**
  * Created by alghimo on 6/6/2017.
  */
trait DimensionProvider[DIM <: (Product with Serializable)] extends Serializable {
  type Dimensions = Dataset[DIM]
  implicit def dimensionEncoder: Encoder[DIM]
}
