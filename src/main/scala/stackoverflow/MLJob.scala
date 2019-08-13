package stackoverflow


import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql._
import stackoverflow.evaluation.MultilabelClassificationEvaluator

// Abstraction to train a ML model
trait MLJob extends SparkSessionWrapper {

  val pipeline: Pipeline
  val hyperparameters: Array[ParamMap]
  val evaluator: MultilabelClassificationEvaluator = new MultilabelClassificationEvaluator()

  def getData(spark: SparkSession, path: String): Dataset[_]

  /**
    * Randomly splits this Dataset into train and test, with the provided train
    * ratio. The test ratio is computed as 1 - train ratio
    *
    * @param df dataset to split
    * @param trainRatio ratio to use for train split
    */
  def testTrainSplit(df: Dataset[_], trainRatio: Double = 0.8): Array[Dataset[_]] = {
    val testRatio: Double = 1 - trainRatio
    df.randomSplit(Array(trainRatio, testRatio)).asInstanceOf[Array[Dataset[_]]]
  }


  def trainWithValidation(data: Dataset[_], params: Array[ParamMap]): TrainValidationSplitModel = {
    //TODO: Hyperparameter tunning as a whole or for each binary classifier?
    val tv = new TrainValidationSplit()
      .setTrainRatio(0.8)
      .setEstimator(pipeline)
      .setEstimatorParamMaps(params)
      .setEvaluator(evaluator.setMetricName("hammingLoss"))

    tv.fit(data)
  }

  def main(args: Array[String]): Unit = {

    require(args.length > 0, "You must provide a path to read the data from")

    val Array(train, test) = testTrainSplit(getData(spark, args(0)))
    val model = trainWithValidation(train, hyperparameters)

    // Output model params
    model.explainParams()
    // Score the model against test data
    val predictions = model.transform(test)
    val metrics = evaluator.getMetrics(predictions)

    println(s"Hamming Loss = ${metrics.hammingLoss}")
    println(s"f1 score     = ${metrics.f1Measure}")
    println(s"Precision    = ${metrics.precision}")
    println(s"Recall       = ${metrics.recall}")

    spark.stop()
  }
}


