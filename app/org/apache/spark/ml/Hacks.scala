package org.apache.spark.ml

import org.apache.spark.ml.classification.{ClassifierParams, OneVsRestParams}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.util.{DefaultParamsReader, DefaultParamsWriter, SchemaUtils}


object Hacks {

  val _DefaultParamsReader = DefaultParamsReader
  val _DefaultParamsWriter = DefaultParamsWriter
  val _SchemaUtils = SchemaUtils
  val OneVsRestMultiParams = OneVsRestParams

  trait _ClassifierParams extends ClassifierParams
  trait _HasPredictionCol extends HasPredictionCol
  trait _HasLabelCol extends HasLabelCol
}


