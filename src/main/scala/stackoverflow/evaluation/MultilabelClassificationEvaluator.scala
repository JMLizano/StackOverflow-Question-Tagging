package stackoverflow.evaluation

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.mllib.evaluation.MultilabelMetrics
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DoubleType}
import stackoverflow.classification.{HasLabelCol, HasPredictionCol}


/**
  * Totally copy pasta from org.spark.ml.evaluation.MulticlassClassificationEvaluator
  *
  * Evaluator for multilabel classification, which expects two input columns: prediction and label.
  */
class MultilabelClassificationEvaluator(override val uid: String)
  extends Evaluator with HasPredictionCol with HasLabelCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("mlEval"))

  /**
    * param for metric name in stackoverflow.evaluation (supports `"f1"` (default), `"precision"`,
    * `"recall"`, `"accuracy"`, `"hammingLoss"`)
    *
    * @group param
    */
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("f1", "precision",
      "recall", "accuracy", "hammingLoss"))
    new Param(this, "metricName", "metric name in stackoverflow.evaluation " +
      "(f1|precision|recall|accuracy|hammingLoss)", allowedParams)
  }

  /** @group getParam */
  def getMetricName: String = $(metricName)

  /** @group setParam */
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricName -> "f1")

  def getMetrics(dataset: Dataset[_]): MultilabelMetrics = {
    val schema = dataset.schema
    // TODO: Fix the type checking (SchemaUtils is private)
    //    SchemaUtils.checkColumnType(schema, $(predictionCol), DoubleType)
    //    SchemaUtils.checkNumericType(schema, $(labelCol))

    val predictionAndLabels =
      dataset.select(col($(predictionCol)), col($(labelCol)).cast(ArrayType(DoubleType))).rdd.map {
        case Row(prediction: Seq[Double], label: Seq[Double]) => (prediction.toArray, label.toArray)
      }
    new MultilabelMetrics(predictionAndLabels)
  }

  override def evaluate(dataset: Dataset[_]): Double = {
    val metrics = getMetrics(dataset)
    val metric = $(metricName) match {
      case "f1" => metrics.f1Measure
      case "precision" => metrics.precision
      case "recall" => metrics.recall
      case "accuracy" => metrics.accuracy
      case "hammingLoss" => metrics.hammingLoss
    }
    metric
  }

  override def isLargerBetter: Boolean = $(metricName) != "hammingLoss"

  override def copy(extra: ParamMap): MultilabelClassificationEvaluator = defaultCopy(extra)
}

object MultilabelClassificationEvaluator
  extends DefaultParamsReadable[MultilabelClassificationEvaluator] {

  override def load(path: String): MultilabelClassificationEvaluator = super.load(path)
}