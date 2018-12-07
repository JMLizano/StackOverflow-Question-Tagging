package stackoverflow


import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.sql._
import org.apache.spark.mllib.evaluation.MultilabelMetrics
import org.apache.spark.sql.functions._


trait MLExperiment {

  val manipulationSteps: Array[PipelineStage]

  val featureEngineeringSteps: Array[PipelineStage]

  val trainingSteps: Array[PipelineStage]

  def getData(spark: SparkSession): Dataset[_]

  def train(spark: SparkSession): PipelineModel = {
    val experimentData: Dataset[_] = getData(spark)
    train(spark, experimentData)
  }

  def train(spark: SparkSession, data: Dataset[_]): PipelineModel = {
    val pipeline = new Pipeline().setStages(manipulationSteps ++ featureEngineeringSteps ++ trainingSteps)
    pipeline.fit(data)
  }

  def evaluate(pred: Dataset[(Array[Double], Array[Double])]) : MultilabelMetrics = new MultilabelMetrics(pred.rdd)
}


