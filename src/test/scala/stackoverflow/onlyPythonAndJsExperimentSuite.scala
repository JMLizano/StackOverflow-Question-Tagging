package stackoverflow

import classification.OneVsRestMultiModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

class onlyPythonAndJsExperimentSuite extends Base {

  import testImplicits._

  test("filterTags") {

    val df = spark.createDataFrame(Seq(
      (Seq("python", "no-python"), 1),
      (Seq("empty"), 2),
      (Seq("javascript", "jquery"), 3),
      (Seq("javascript"), 4)
    )).toDF("label", "test")

    val expectedDf = Seq(
      (Seq("python"), 1),
      (Seq("javascript"), 3),
      (Seq("javascript"), 4)
    )

    val outputDf = onlyPythonAndJsExperiment.filterTags(df).as[(Seq[String], Int)].collect().toSeq
    assert(outputDf ==  expectedDf)
  }

//  test("filter") {
//
//    val df = spark.createDataFrame(Seq(
//      (Seq("python", "no-python"), 1),
//      (Seq("empty"), 2),
//      (Seq("javascript", "jquery"), 3),
//      (Seq("javascript"), 4)
//    )).toDF("label", "test")
//
//    val expectedDf = Seq(
//      (Seq("python"), 1),
//      (Seq("javascript"), 3),
//      (Seq("javascript"), 4)
//    )
//
//    val outputDf = onlyPythonAndJsExperiment.filter(df, spark).as[(Seq[String], Int)].collect().toSeq
//    assert(outputDf ==  expectedDf)
//  }

  test("get data") {
    val data = onlyPythonAndJsExperiment.getData(spark, "/Users/chemalizano/Work/stackoverflowQuestions/src/test/resources/questions")
    val labels = data.select($"label").distinct().as[Seq[String]].collect().toSet.flatten
    assert(labels == Set("python", "javascript"))
  }

  test("set model") {
    assert(onlyPythonAndJsExperiment.multiLabelModel.getClassifier != null)
  }

  test("train and evaluate") {
    val data = onlyPythonAndJsExperiment.getData(spark, "/Users/chemalizano/Work/stackoverflowQuestions/src/test/resources/questions")
    val Array(train, test) = onlyPythonAndJsExperiment.testTrainSplit(data)
    val model = onlyPythonAndJsExperiment.trainWithValidation(train, onlyPythonAndJsExperiment.hyperparameters)
    val predictions = model.transform(test)
      .withColumn("labelNumeric", when(array_contains($"label", "python"), Array(1.0)).otherwise(Array(0.0)))
    val metrics = onlyPythonAndJsExperiment.evaluator.setLabelCol("labelNumeric").getMetrics(predictions)

    predictions.select("labelNumeric", "prediction").show()

    println(s"Hamming Loss = ${metrics.hammingLoss}")
    println(s"f1 score     = ${metrics.f1Measure}")
    println(s"Precision    = ${metrics.precision}")
    println(s"Recall       = ${metrics.recall}")
  }
}
