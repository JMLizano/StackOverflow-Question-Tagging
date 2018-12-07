//package stackoverflow.classification
//
//import org.apache.spark.ml.classification._
//import org.apache.spark.ml.linalg.{Vector, Vectors}
//import org.apache.spark.ml.param.{Param, ParamMap}
//import org.apache.spark.sql.Dataset
//import org.apache.spark.sql.functions._
//import stackoverflow.classification
//
///**
//  * Implements a multilabel classificator
//  *
//  * Receives a column Label with an array of labels for each observation
//  *
//  */
////abstract class MultiLabelClassifier extends  ProbabilisticClassifier[Vector, MultiLabelClassifier, MultiLabelClassifierModel]{
////
//////  lazy val labels: Array[String] = (dataset: Dataset[_]) => dataset.select(explode(dataset($(labelCol)))).as[String].distinct().collect()
////
//////  override def getNumClasses(dataset: Dataset[_], maxNumClasses: Int): Int = labels.length
////}
////
////
////abstract class MultiLabelClassifierModel extends ProbabilisticClassificationModel[Vector, MultiLabelClassifierModel]
//
//
//
//trait ClassifierTypeTrait {
//  // scalastyle:off structural.type
//  type ClassifierType = Classifier[Vector, E, M] forSome {
//    type M <: ClassificationModel[Vector, M]
//    type E <: Classifier[Vector, E, M]
//  }
//  // scalastyle:on structural.type
//}
//
//
//class BinaryMultiLabelClassifier (
//    override val uid: String)
//  extends ProbabilisticClassifier[Vector, BinaryMultiLabelClassifier, BinaryMultiLabelClassifierModel]
//    with classification.ClassifierTypeTrait {
//
//  def getLabels(dataset: Dataset[_]): Array[String] = {
//    import dataset.sparkSession.implicits._
//    dataset.select(explode(dataset($(labelCol)))).distinct().as[String].collect()
//  }
//
//  override def getNumClasses(dataset: Dataset[_], maxNumClasses: Int): Int = getLabels(dataset).length
//
//  val classifier: Param[ClassifierType] = new Param(this, "classifier", "base binary classifier")
//
//  def getClassifier: ClassifierType = $(classifier)
//
//  def setClassifier(value: Classifier[_, _, _]): this.type = set(classifier, value.asInstanceOf[ClassifierType])
//
//  override protected def train(dataset: Dataset[_]): BinaryMultiLabelClassifierModel = {
//    // For each label train a binary model
//    val labels = getLabels(dataset)
//    val models = labels.map { label =>
//      val trainingDataset = dataset.withColumn(label, when(col($(labelCol)).contains(label), 1.0).otherwise(0.0))
//      val classifier = getClassifier
//      val paramMap = new ParamMap()
//      paramMap.put(classifier.labelCol -> label)
//      paramMap.put(classifier.featuresCol -> getFeaturesCol)
//      paramMap.put(classifier.predictionCol -> getPredictionCol)
//      classifier.fit(trainingDataset, paramMap)
//    }
//
//    val model = new BinaryMultiLabelClassifierModel(uid, models)
//    copyValues(model)
//  }
//
//  override def copy(extra: ParamMap): BinaryMultiLabelClassifier = defaultCopy(extra)
//
//}
//
//
//class BinaryMultiLabelClassifierModel (
//    override val uid: String,
//    val models: Array[_ <: ClassificationModel[Vector, _]])
//  extends ClassificationModel[Vector, BinaryMultiLabelClassifierModel]
//    with classification.ClassifierTypeTrait {
//
//  val numClasses = 0
//
////  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
////    // TODO: rawPrediction should be same size of models
////    Vectors.dense(models.zip(rawPrediction.toArray).map{
////      case (model, prediction) =>
////        model.raw2prediction(Vectors.dense(1 - prediction, prediction))
////    })
////  }
//
//  override protected def predictRaw(features: Vector): Vector = {
//    // Transform features to a dataset
//    // Iterate over each model, calling transform on the features_dataset
//    // Concatenate all outputs
//    Vectors.dense(models.map{ _.predictRaw(features).toDense.values(1) })
//  }
//
//
////  override protected def raw2prediction(rawPrediction: Vector): Vector = {
////    // TODO: rawPrediction should be same size of models
////    val predictions = rawPrediction.toArray.zipWithIndex.filter{ case (rawPred, _) => rawPred >= 0.5 }
////    Vectors.dense(predictions.map(_._2.toDouble))
////  }
//
//  override def copy(extra: ParamMap): BinaryMultiLabelClassifierModel = {
//    val newModel = copyValues(new BinaryMultiLabelClassifierModel(uid, models), extra)
//    newModel.setParent(parent)
//  }
//
//}
