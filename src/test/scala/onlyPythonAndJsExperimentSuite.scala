package stackoverflow

import classification.OneVsRestMultiModel

class onlyPythonAndJsExperimentSuite extends Base {

  import testImplicits._

//  test("filterTags") {
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
//    val outputDf = onlyPythonAndJsExperiment.filterTags(df).as[(Seq[String], Int)].collect().toSeq
//    assert(outputDf ==  expectedDf)
//  }
//
//  test("get data") {
//    val data = onlyPythonAndJsExperiment.getData(spark)
//    val labels = data.select($"label").distinct().as[Seq[String]].collect().toSet.flatten
//    assert(labels == Set("python", "javascript"))
//  }
//
//  test("set model") {
//    assert(onlyPythonAndJsExperiment.multiLabelModel.getClassifier != null)
//  }

  test("train") {
    val model = onlyPythonAndJsExperiment.train(spark)
    assert(model.stages.last.asInstanceOf[OneVsRestMultiModel].models.length == 2)
    val transformed = model.transform(onlyPythonAndJsExperiment.getData(spark))
    transformed.show()
  }

//  test("evaluate") {
//    val model = onlyPythonAndJsExperiment.train(spark)
//    val transformed = model.transform(onlyPythonAndJsExperiment.getData(spark))
//    val metrics = onlyPythonAndJsExperiment.evaluate(transformed)
//  }



}
