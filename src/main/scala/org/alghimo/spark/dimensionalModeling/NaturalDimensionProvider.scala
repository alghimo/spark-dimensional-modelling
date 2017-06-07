package org.alghimo.spark.dimensionalModeling

import org.apache.spark.sql.{Dataset, Encoder}

/**
  * Created by alghimo on 6/7/2017.
  */
trait NaturalDimensionProvider[NATURAL_DIM <: (Product with Serializable)] extends Serializable {
  type NaturalDimensions = Dataset[NATURAL_DIM]
  implicit def naturalDimensionEncoder: Encoder[NATURAL_DIM]
}
