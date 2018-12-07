//package stackoverflow
//
//import com.holdenkarau.spark.testing.DataFrameSuiteBase
//import org.apache.spark.ml.linalg.Vectors
//import org.apache.spark.ml.classification.LogisticRegression
//import org.scalatest.FunSuite
//import org.apache.spark.sql._
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//import org.apache.spark.ml._
//import org.apache.spark.ml.classification.LogisticRegression
//import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
//import org.apache.spark.ml.feature.{SQLTransformer, RegexTokenizer, Tokenizer, StopWordsRemover, HashingTF, IDF}
//
//
//
//class MultiLabelTest extends FunSuite with DataFrameSuiteBase {
//
////  test("Fitting model"){
////§
////    val df = spark.createDataFrame(Seq(
////      (Array("laravel"), Vectors.sparse(10000, Array(1312,6542,6680,9610), Array(0.0,0.0,0.0,0.0))),
////      (Array("c#", "mysql", "sql"), Vectors.sparse(10000, Array(1053,5536,9384,9443), Array(0.0,0.0,0.0,0.0))),
////      (Array("node.js","express"),   Vectors.sparse(10000, Array(1695,3857,4835,8392,9641), Array(0.0,0.0,0.0,0.0,0.0)))
////    )).toDF("label", "features")
////
////    val lr = new LogisticRegression()
////    val MultiLabelClassifier= new OneVsRestMulti("test").setClassifier(lr)
////    val MultiLabelModel = MultiLabelClassifier.fit(df)
////
////    assert(MultiLabelClassifier.computeNumClasses(df) == 6)
////    assert(MultiLabelModel.models.length == 6)
////    MultiLabelModel.transform(df).show()
////  }
//
//  test("Real training"){
//    spark.conf.set("fs.s3a.access.key", "AKIAJQQRGAOEISQJDFTA")
//    spark.conf.set("fs.s3a.secret.key", "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")
//
//
//    val questions = spark.read.parquet("s3a://tmp.brandcrumb.com/chema/data/parquet/Posts/Questions").where("Year=2018 AND Month=9").select("Id", "Tags", "Title")
//
//    val tokenizeTagsTransform = new RegexTokenizer().setInputCol("Tags").setOutputCol("label").setGaps(false).setPattern("[^<>]+")
//
//    val tokenized = tokenizeTagsTransform.transform(questions)
//    val filteredTokenized = tokenized.where("(array_contains(label, 'python') AND size(label) <= 1) OR (array_contains(label, 'javascript') AND size(label) <= 1)")
//
//    val tokenizeTitleTransform = new Tokenizer().setInputCol("Title").setOutputCol("TitleSplitted")
//
//    val englishStopWords = StopWordsRemover.loadDefaultStopWords("english")
//    val rmStopWordsTransform = new StopWordsRemover().setInputCol("TitleSplitted").setOutputCol("TitleStop").setStopWords(englishStopWords)
//
//    val tf = new HashingTF().setInputCol("TitleStop").setOutputCol("TFOut").setNumFeatures(10000)
//    val idf = new IDF().setInputCol("TFOut").setOutputCol("IDFOut").setMinDocFreq(2)
//
//    val stages = Array(tokenizeTitleTransform, rmStopWordsTransform, tf, idf)
//    val pipeline = new Pipeline().setStages(stages)
//    val pipelineFitted = pipeline.fit(filteredTokenized)
//    val train = pipelineFitted.transform(filteredTokenized).select(col("label"), col("IDFOut").as("features"))
//
//    val lr = new LogisticRegression().setMaxIter(100).setTol(0.1)
//    val MultiLabelClassifier= new OneVsRestMulti("test").setClassifier(lr)
//    val MultiLabelModel = MultiLabelClassifier.fit(train)
//
//    assert(MultiLabelClassifier.computeNumClasses(train) == 2)
//    assert(MultiLabelModel.models.length == 2)
//    MultiLabelModel.transform(train).show()
//
//  }
//}