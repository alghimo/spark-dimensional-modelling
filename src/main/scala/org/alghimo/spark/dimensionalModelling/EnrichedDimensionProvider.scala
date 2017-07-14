package org.alghimo.spark.dimensionalModelling

import org.apache.spark.sql.{Dataset, Encoder}

/**
  * Created by alghimo on 6/7/2017.
  */
trait EnrichedDimensionProvider[ENRICHED_DIM <: (Product with Serializable)] extends Serializable {
  type EnrichedDimensions = Dataset[ENRICHED_DIM]
  implicit def enrichedDimensionEncoder: Encoder[ENRICHED_DIM]
}
