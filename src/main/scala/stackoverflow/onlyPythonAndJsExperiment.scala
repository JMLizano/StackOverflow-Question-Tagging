package stackoverflow

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import stackoverflow.classification.OneVsRestMulti

// TODO: This can be generalized to a class that accept the labels to use as a constructor parameter
object onlyPythonAndJsExperiment extends MLJob {

  val binaryModel: LogisticRegression = new LogisticRegression()
                                          .setRawPredictionCol("rawPrediction")
                                          .setThreshold(0.8)
  val multiLabelModel: OneVsRestMulti = new OneVsRestMulti("OnevsRestLogistic")
                                          .setClassifier(binaryModel)
                                          .setPredictionCol("prediction")

  override val hyperparameters: Array[ParamMap] = new ParamGridBuilder()
    .addGrid(binaryModel.maxIter, Array(100))
    .addGrid(binaryModel.tol, Array(0.01, 0.1))
    .addGrid(binaryModel.threshold, Array(0.5, 0.65, 0.8))
    .build()

  override val pipeline: Pipeline = new Pipeline().setStages(Array(
    Feature.titleTokenizer(),
    Feature.stopWordsRemover(),
    Feature.tf(1000, "TitleStop", "TFOut"),
    Feature.idf(2, "TFOut", "features"),
    multiLabelModel
  ))

  override def getData(spark: SparkSession, path: String): Dataset[_] = {
    val data = spark.read.parquet(path).select("Id", "label", "Title")
    filterTags(data)
  }

  def filterTags(dataset: Dataset[_]): Dataset[_] = {
    val selectedTags = Array("python", "javascript")
    val selectedTagsUdf = udf { (tags: Seq[String]) => tags.filter(selectedTags.contains(_)) }
    dataset
      .withColumn("label", selectedTagsUdf(col("label")))
      .where("size(label) > 0")
  }

}