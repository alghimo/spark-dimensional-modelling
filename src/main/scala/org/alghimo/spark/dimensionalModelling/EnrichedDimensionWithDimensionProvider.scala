package org.alghimo.spark.dimensionalModelling

import org.apache.spark.sql.{Dataset, Encoder}

/**
  * Created by alghimo on 6/7/2017.
  */
trait EnrichedDimensionWithDimensionProvider[ENRICHED_DIM <: (Product with Serializable), DIM <: (Product with Serializable)] {
  type EnrichedWithDimension = (ENRICHED_DIM, DIM)
  type DimensionWithEnriched = (DIM, ENRICHED_DIM)
  type JoinEnrichedWithDimension = Dataset[EnrichedWithDimension]
  type JoinDimensionWithEnriched = Dataset[DimensionWithEnriched]

  implicit def enrichedWithDimensionEncoder: Encoder[(ENRICHED_DIM, DIM)]
  implicit def dimensionWithEnrichedEncoder: Encoder[(DIM, ENRICHED_DIM)]

  implicit def enrichedWithDimensionToDimensionWithEnriched(e: EnrichedWithDimension): DimensionWithEnriched = e.swap
  implicit def dimensionWithEnrichedToEnrichedWithDimension(d: EnrichedWithDimension): DimensionWithEnriched = d.swap
  implicit def joinEnrichedWithDimensionToJoinDimensionWithEnriched(e: JoinEnrichedWithDimension): JoinDimensionWithEnriched = e.map(_.swap)
  implicit def joinDimensionWithEnrichedToJoinEnrichedWithDimension(d: JoinEnrichedWithDimension): JoinDimensionWithEnriched = d.map(_.swap)
}
