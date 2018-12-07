package stackoverflow

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import stackoverflow.classification.OneVsRestMulti


object onlyPythonAndJsExperiment extends MLExperiment {

  override def getData(spark: SparkSession): Dataset[_] = {
    val data = spark.read.parquet("/Users/chemalizano/Work/stackoverflowQuestions/src/test/resources/questions")
      .select("Id", "label", "Title")
    filterTags(data)
  }

  override val manipulationSteps: Array[PipelineStage] = Array(
    Feature.titleTokenizer(),
    Feature.stopWordsRemover()
  )

  override val featureEngineeringSteps: Array[PipelineStage] = Array(
    Feature.tf(1000, "TitleStop", "TFOut"),
    Feature.idf(2, "TFOut", "features")
  )

  val binaryModel = new LogisticRegression().setMaxIter(100).setTol(0.1)
  val multiLabelModel = new OneVsRestMulti("test").setClassifier(binaryModel).setPredictionCol("PredictionMulti")

  override val trainingSteps: Array[PipelineStage] = Array(multiLabelModel)

  def filterTags(dataset: Dataset[_]): Dataset[_] = {
    val selectedTags = Array("python", "javascript")
    val selectedTagsUdf = udf { (tags: Seq[String]) => tags.filter(selectedTags.contains(_)) }
    dataset
      .withColumn("label", selectedTagsUdf(col("label")))
      .where("size(label) > 0")
  }

  def filter(spark: SparkSession): SQLTransformer = {
    val top20 = Array("python", "javascript")
    spark.udf.register("filterTags", (tags: Seq[String]) => tags.filter(top20.contains(_)))
    new SQLTransformer().setStatement("Select filterTags(Tags) as label from __THIS__")
  }
}