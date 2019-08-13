package org.apache.spark.ml

import org.apache.spark.ml.classification.OneVsRestParams
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol, HasRawPredictionCol}
import org.apache.spark.ml.util.{DefaultParamsReader, DefaultParamsWriter, SchemaUtils}


object Hacks {

  val _DefaultParamsReader = DefaultParamsReader
  val _DefaultParamsWriter = DefaultParamsWriter
  val _SchemaUtils = SchemaUtils
  val OneVsRestMultiParams = OneVsRestParams

  trait _HasPredictionCol extends HasPredictionCol
  trait _HasLabelCol extends HasLabelCol
  trait _HasRawPredictionCol extends HasRawPredictionCol
  trait _PredictorParams extends PredictorParams

}


