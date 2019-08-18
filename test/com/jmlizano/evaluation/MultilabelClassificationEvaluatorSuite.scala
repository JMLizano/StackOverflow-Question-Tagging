package com.jmlizano.evaluation


import org.apache.spark.sql.DataFrame
import com.jmlizano.Base
import stackoverflow.evaluation.MultilabelClassificationEvaluator

class MultilabelClassificationEvaluatorSuite  extends Base {

  import testImplicits._

  def evaluateMetric(df:DataFrame, evaluator: MultilabelClassificationEvaluator,
                     metric: String, expectedValue: Double, delta: Double) = {

    val specificEvaluator = evaluator.setMetricName(metric)
    assert(math.abs(specificEvaluator.evaluate(df) - expectedValue) < delta)
  }

  test("evaluate") {
    val delta = 0.00001
    val evaluator = new MultilabelClassificationEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("label")
    val scoreAndLabels: DataFrame =
      Seq((Seq(0.0, 1.0),      Seq(0.0, 2.0)),
          (Seq(0.0, 2.0),      Seq(0.0, 1.0)),
          (Seq(),              Seq(0.0)),
          (Seq(2.0),           Seq(2.0)),
          (Seq(2.0, 0.0),      Seq(2.0, 0.0)),
          (Seq(0.0, 1.0, 2.0), Seq(0.0, 1.0)),
          (Seq(1.0),           Seq(1.0, 2.0))).toDF("prediction", "label")

    val hammingLoss = (1.0 / (7 * 3)) * (2 + 2 + 1 + 0 + 0 + 1 + 1)
    val precision = 1.0 / 7 * (1.0 / 2 + 1.0 / 2 + 0 + 1.0 / 1 + 2.0 / 2 + 2.0 / 3 + 1.0 / 1.0)
    val recall = 1.0 / 7 * (1.0 / 2 + 1.0 / 2 + 0 / 1 + 1.0 / 1 + 2.0 / 2 + 2.0 / 2 + 1.0 / 2)
    val accuracy = 1.0 / 7 * (1.0 / 3 + 1.0 /3 + 0 + 1.0 / 1 + 2.0 / 2 + 2.0 / 3 + 1.0 / 2)
    val f1 = (1.0 / 7) *
      2 * ( 1.0 / (2 + 2) + 1.0 / (2 + 2) + 0 + 1.0 / (1 + 1) +
      2.0 / (2 + 2) + 2.0 / (3 + 2) + 1.0 / (1 + 2) )

    evaluateMetric(scoreAndLabels, evaluator, "hammingLoss", hammingLoss, delta)
    evaluateMetric(scoreAndLabels, evaluator, "precision", precision, delta)
    evaluateMetric(scoreAndLabels, evaluator, "recall", recall, delta)
    evaluateMetric(scoreAndLabels, evaluator, "f1", f1, delta)
    evaluateMetric(scoreAndLabels, evaluator, "accuracy", accuracy, delta)
  }

  test("isLargerBetter param") {
    val evaluatorHamming = new MultilabelClassificationEvaluator().setMetricName("hammingLoss")
    val evaluatorPrecision = new MultilabelClassificationEvaluator().setMetricName("precision")

    assert(!evaluatorHamming.isLargerBetter)
    assert(evaluatorPrecision.isLargerBetter)
  }

}
