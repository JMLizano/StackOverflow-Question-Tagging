package stackoverflow.classification

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.DataFrame
import stackoverflow.Base


class OnevsRestMultiLabelSuite  extends Base {

  val binaryModel: LogisticRegression = new LogisticRegression()
  val data: DataFrame = spark.createDataFrame(Seq(
    (Seq("python", "no-python"), 1),
    (Seq("empty"), 2),
    (Seq("javascript", "jquery"), 3),
    (Seq("javascript"), 4)
  )).toDF("label", "test")

//  test("Model persistence") = {
//    val multiLabelModel: OneVsRestMulti = new OneVsRestMulti("OnevsRestLogistic")
//      .setClassifier(binaryModel)
//      .setPredictionCol("prediction")
//  }

}
