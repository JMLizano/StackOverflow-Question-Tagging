/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stackoverflow.classification

import java.util.UUID

import scala.language.existentials
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml._
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.classification.{ClassificationModel, Classifier}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{Param, ParamMap, ParamPair, Params}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.Hacks._
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, JObject, _}
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization


trait ClassifierTypeTrait {
  // scalastyle:off structural.type
  type ClassifierType = Classifier[F, E, M] forSome {
    type F
    type M <: ClassificationModel[F, M]
    type E <: Classifier[F, E, M]
  }
  // scalastyle:on structural.type
}

/**
  * Params for [[stackoverflow.classification.OneVsRestMulti]].
  */
trait OneVsRestMultiParams extends _PredictorParams
  with  _HasRawPredictionCol with ClassifierTypeTrait {

  /**
    * param for the base binary classifier that we reduce multiclass stackoverflow.classification into.
    * The base classifier input and output columns are ignored in favor of
    * the ones specified in [[stackoverflow.classification.OneVsRestMulti]].
    * @group param
    */
  val classifier: Param[ClassifierType] = new Param(this, "classifier", "base binary classifier")

  /** @group getParam */
  def getClassifier: ClassifierType = $(classifier)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)
}


object OneVsRestMultiParams extends ClassifierTypeTrait {

  def validateParams(instance: OneVsRestMultiParams): Unit = {
    def checkElement(elem: Params, name: String): Unit = elem match {
      case stage: MLWritable => // good
      case other =>
        throw new UnsupportedOperationException("OneVsRestMulti write will fail " +
          s" because it contains $name which does not implement MLWritable." +
          s" Non-Writable $name: ${other.uid} of type ${other.getClass}")
    }

    instance match {
      case ovrModel: OneVsRestMultiModel => ovrModel.models.foreach(checkElement(_, "model"))
      case _ => // no need to check OneVsRest here
    }

    checkElement(instance.getClassifier, "classifier")
  }

  def saveImpl(
                path: String,
                instance: OneVsRestMultiParams,
                sc: SparkContext,
                extraMetadata: Option[JObject] = None): Unit = {

    val params = instance.extractParamMap().toSeq
    val jsonParams = render(params
      .filter { case ParamPair(p, v) => p.name != "classifier" }
      .map { case ParamPair(p, v) => p.name -> parse(p.jsonEncode(v)) }
      .toList)

    _DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata, Some(jsonParams))

    val classifierPath = new Path(path, "classifier").toString
    instance.getClassifier.asInstanceOf[MLWritable].save(classifierPath)
  }

  def loadImpl(
                path: String,
                sc: SparkContext,
                expectedClassName: String): (_DefaultParamsReader.Metadata, ClassifierType) = {

    val metadata = _DefaultParamsReader.loadMetadata(path, sc, expectedClassName)
    val classifierPath = new Path(path, "classifier").toString
    val estimator = _DefaultParamsReader.loadParamsInstance[ClassifierType](classifierPath, sc)
    (metadata, estimator)
  }
}

/**
  * Model produced by [[OneVsRestMulti]].
  * This stores the models resulting from training k binary classifiers: one for each class.
  * Each example is scored against all k models, and all the models with confidence above
  * `threshold` are picked to label the example.
  *
  * @param models The binary classification models for the reduction.
  *               The i-th model is produced by testing the i-th class (taking label 1) vs the rest
  *               (taking label 0).
  */
final class OneVsRestMultiModel(
    override val uid: String,
    val labelsMapping: Map[Int, String],
    val models: Array[_ <: ClassificationModel[_, _]])
  extends Model[OneVsRestMultiModel]
    with OneVsRestMultiParams  with MLWritable {

  override def transformSchema(schema: StructType): StructType = {
//    validateAndTransformSchema(schema, fitting = false, getClassifier.featuresDataType)
    schema.add(StructField($(predictionCol), DoubleType, false))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    // Check schema
    transformSchema(dataset.schema, logging = true)

    // determine the input columns: these need to be passed through
    val origCols = dataset.schema.map(f => col(f.name))

    // add an accumulator column to store predictions of all the models
    val accColName = "mbc$acc" + UUID.randomUUID().toString
    val rawAccColName = "mbc$rawAcc" + UUID.randomUUID().toString
    val initUDF = udf { () => Map[Int, Double]() }
    val rawInitUDF = udf { () => Map[Int, Vector]() }
    val newDataset = dataset.withColumn(accColName, initUDF()).withColumn(rawAccColName,rawInitUDF())

    // persist if underlying dataset is not persistent.
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      newDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // update the accumulator column with the result of prediction of models
    val aggregatedDataset = models.zipWithIndex.foldLeft[DataFrame](newDataset) {
      case (df, (model, index)) =>
        val predictionCol = model.getPredictionCol
        val rawPredictionCol = model.getRawPredictionCol
        // TODO: Add probability col if exists
        val columns = origCols ++ List(col(predictionCol), col(rawPredictionCol), col(accColName), col(rawAccColName))

        // add temporary column to store intermediate scores and update
        val tmpColName = "mbc$tmp" + UUID.randomUUID().toString
        val rawTmpColName = "mbc$rawTmp" //+ UUID.randomUUID().toString

        val updateUDF = udf { (predictions: Map[Int, Double], prediction: Double) =>
          predictions + ((index, prediction))
        }
        val rawUpdateUDF = udf { (predictions: Map[Int, Vector], prediction: Vector) =>
          predictions + ((index, prediction))
        }
        model.setFeaturesCol(getFeaturesCol)
        model.setRawPredictionCol(getRawPredictionCol)
        model.setPredictionCol(getPredictionCol)

        val transformedDataset = model.transform(df).select(columns: _*)
        val updatedDataset = transformedDataset
          .withColumn(tmpColName, updateUDF(col(accColName), col(predictionCol)))
          .withColumn(rawTmpColName, rawUpdateUDF(col(rawAccColName), col(rawPredictionCol)))
        val newColumns = origCols ++ List(col(tmpColName), col(rawTmpColName))

        // switch out the intermediate column with the accumulator column
        updatedDataset.select(newColumns: _*)
          .withColumnRenamed(tmpColName, accColName)
          .withColumnRenamed(rawTmpColName, rawAccColName)
    }

    if (handlePersistence) {
      newDataset.unpersist()
    }

    // output the index of all the classifiers with confidence above threshold
    // as prediction
    val doubleLabelUDF = udf { predictions: Map[Int, Double] =>
      // TODO: Indices to labels
      // TODO: Configurable threshold, not 1
      // Selecting only prediction for positive class
      predictions.collect { case pred if pred._2 == 1 => pred._1.toDouble }.toSeq
    }

    val labelUDF = udf { predictions: Map[Int, Double] =>
      // TODO: Indices to labels
      // TODO: Configurable threshold, not 1
      // Selecting only prediction for positive class
      predictions.collect { case pred if pred._2 == 1 => labelsMapping(pred._1) }.toSeq
    }

    val rawProbUDF = udf { rawPredictions: Map[Int, Vector] =>
      // TODO: Indices to labels
      // TODO: Configurable threshold, not 1
      // Selecting only prediction for positive class
      rawPredictions.collect { case pred => pred._2(0) }.toSeq
    }

    // output label and label metadata as prediction
    // Refactored line due to method withColumn(name, col, metadata) being private
    aggregatedDataset
      .withColumn($(predictionCol), doubleLabelUDF(col(accColName)))
      .withColumn("predictionLabel", labelUDF(col(accColName)))
      .withColumn($(rawPredictionCol), rawProbUDF(col(rawAccColName)))
//      .drop(accColName)
  }

  override def copy(extra: ParamMap): OneVsRestMultiModel = {
    val copied = new OneVsRestMultiModel(
      uid, labelsMapping, models.map(_.copy(extra).asInstanceOf[ClassificationModel[_, _]]))
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new OneVsRestMultiModel.OneVsRestMultiModelWriter(this)
}

object OneVsRestMultiModel extends MLReadable[OneVsRestMultiModel] {

  override def read: MLReader[OneVsRestMultiModel] = new OneVsRestModelReader

  override def load(path: String): OneVsRestMultiModel = super.load(path)

  /** [[MLWriter]] instance for [[OneVsRestMultiModel]] */
  class OneVsRestMultiModelWriter(instance: OneVsRestMultiModel) extends MLWriter {

    OneVsRestMultiParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      implicit val formats = org.json4s.DefaultFormats
      val extraJson = ("numClasses" -> instance.models.length) ~ ("labelMappings" -> Serialization.write(instance.labelsMapping))
      OneVsRestMultiParams.saveImpl(path, instance, sc, Some(extraJson))
      instance.models.zipWithIndex.foreach { case (model: MLWritable, idx) =>
        val modelPath = new Path(path, s"model_$idx").toString
        model.save(modelPath)
      }
    }
  }

  private class OneVsRestModelReader extends MLReader[OneVsRestMultiModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[OneVsRestMultiModel].getName

    override def load(path: String): OneVsRestMultiModel = {
      implicit val format = DefaultFormats
      val (metadata, classifier) = OneVsRestMultiParams.loadImpl(path, sc, className)
//      val labelMetadata = Metadata.fromJson((metadata.metadata \ "labelMetadata").extract[String])
      val numClasses = (metadata.metadata \ "numClasses").extract[Int]
      val labelMappings =  parse((metadata.metadata \ "labelMappings").extract[String]).extract[Map[Int,String]]

      val models = Range(0, numClasses).toArray.map { idx =>
        val modelPath = new Path(path, s"model_$idx").toString
        _DefaultParamsReader.loadParamsInstance[ClassificationModel[_, _]](modelPath, sc)
      }
      val ovrModel = new OneVsRestMultiModel(metadata.uid, labelMappings.asInstanceOf[Map[Int,String]], models)
      _DefaultParamsReader.getAndSetParams(ovrModel, metadata)
      ovrModel.set("classifier", classifier)
      ovrModel
    }
  }
}

/**
  * Reduction of Multiclass Classification to Binary Classification.
  * Performs reduction using one against all strategy.
  * For a multiclass stackoverflow.classification with k classes, train k models (one per class).
  * Each example is scored against all k models and the model with highest score
  * is picked to label the example.
  */
final class OneVsRestMulti(
    override val uid: String)
  extends Estimator[OneVsRestMultiModel]
    with OneVsRestMultiParams  with MLWritable {

  def this() = this(Identifiable.randomUID("oneVsRest"))

  /** @group setParam */
  def setClassifier(value: Classifier[_, _, _]): this.type = {
    set(classifier, value.asInstanceOf[ClassifierType])
  }

  override def transformSchema(schema: StructType): StructType = {
    //TODO: Improve this
//    validateAndTransformSchema(schema, fitting = true, getClassifier.featuresDataType)
    schema.add(StructField($(predictionCol), DoubleType, false))
  }

  def getLabels(dataset: Dataset[_]): Array[String] = {
    import dataset.sparkSession.implicits._
    dataset.select(explode(dataset($(labelCol)))).distinct().as[String].collect()
  }

  def computeNumClasses(dataset: Dataset[_]): Int = getLabels(dataset).length

  /**
    * Examine a schema to identify the number of classes in a label column.
    * Returns None if the number of labels is not specified, or if the label column is continuous.
    */
//  def getNumClasses(labelSchema: StructField): Option[Int] = {
//    Attribute.fromStructField(labelSchema) match {
//      case binAttr: BinaryAttribute => Some(2)
//      case nomAttr: NominalAttribute => nomAttr.getNumValues
//      case _: NumericAttribute | UnresolvedAttribute => None
//    }
//  }

  var labelsMapping =  Map[Int, String]()

  override def fit(dataset: Dataset[_]): OneVsRestMultiModel = {
    transformSchema(dataset.schema)

//    val instr = Instrumentation.create(this, dataset)
//    instr.logParams(labelCol, featuresCol, predictionCol)
//    instr.logNamedValue("classifier", $(classifier).getClass.getCanonicalName)

    // determine number of classes either from metadata if provided, or via computation.
    val labelSchema = dataset.schema($(labelCol))
//    val computeNumClasses: () => Int = () => {
//      val Row(maxLabelIndex: Double) = dataset.agg(max(col($(labelCol)).cast(DoubleType))).head()
//      // classes are assumed to be numbered from 0,...,maxLabelIndex
//      maxLabelIndex.toInt + 1
//    }
//    val numClasses = getNumClasses(labelSchema).fold(computeNumClasses(dataset))(identity)
      val numClasses = computeNumClasses(dataset)
//    instr.logNumClasses(numClasses)

    val multiclassLabeled = dataset.select($(labelCol), $(featuresCol))

    // persist if underlying dataset is not persistent.
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      multiclassLabeled.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // create k columns, one for each binary classifier.
//    val models = Range(0, numClasses).par.map { index =>
    val labels = getLabels(dataset)
    val models = labels.zipWithIndex.map { case (label, index) =>
      labelsMapping = labelsMapping + ((index, label))
      // generate new label metadata for the binary problem.
      val newLabelMeta = BinaryAttribute.defaultAttr.withName("label").toMetadata()
      // TODO: Handle column names, they should not contain any special characters (like dots)
      // TODO: better to transform them to double instead of using strings
      val labelColName = "mc2b$" + index
      // Refactored this line due to method withColumn(name, col, metadata) being private
      val trainingDataset = multiclassLabeled.withColumn(
        labelColName, when(array_contains(col($(labelCol)), label), 1.0).otherwise(0.0))
      val classifier = getClassifier
      val paramMap = new ParamMap()
      paramMap.put(classifier.labelCol -> labelColName)
      paramMap.put(classifier.featuresCol -> getFeaturesCol)
      classifier.fit(trainingDataset, paramMap)
    }.toArray[ClassificationModel[_, _]]
//    instr.logNumFeatures(models.head.numFeatures)

    if (handlePersistence) {
      multiclassLabeled.unpersist()
    }

    // extract label metadata from label column if present, or create a nominal attribute
    // to output the number of labels
//    val labelAttribute = Attribute.fromStructField(labelSchema) match {
//      case _: NumericAttribute | UnresolvedAttribute =>
//        NominalAttribute.defaultAttr.withName("label").withNumValues(numClasses)
//      case attr: Attribute => attr
//    }
    val model = new OneVsRestMultiModel(uid, labelsMapping, models).setParent(this)
//    instr.logSuccess(model)
    copyValues(model)
  }

  override def copy(extra: ParamMap): OneVsRestMulti = {
    val copied = defaultCopy(extra).asInstanceOf[OneVsRestMulti]
    if (isDefined(classifier)) {
      copied.setClassifier($(classifier).copy(extra))
    }
    copied
  }

  override def write: MLWriter = new OneVsRestMulti.OneVsRestMultiWriter(this)
}

object OneVsRestMulti extends MLReadable[OneVsRestMulti] {

  override def read: MLReader[OneVsRestMulti] = new OneVsRestMultiReader

  override def load(path: String): OneVsRestMulti = super.load(path)

  /** [[MLWriter]] instance for [[OneVsRestMulti]] */
  class OneVsRestMultiWriter(instance: OneVsRestMulti) extends MLWriter {

    OneVsRestMultiParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      OneVsRestMultiParams.saveImpl(path, instance, sc)
    }
  }

  private class OneVsRestMultiReader extends MLReader[OneVsRestMulti] {

    /** Checked against metadata when loading model */
    private val className = classOf[OneVsRestMulti].getName

    override def load(path: String): OneVsRestMulti = {
      val (metadata, classifier) = OneVsRestMultiParams.loadImpl(path, sc, className)
      val ovr = new OneVsRestMulti(metadata.uid)
      _DefaultParamsReader.getAndSetParams(ovr, metadata)
      ovr.setClassifier(classifier)
    }
  }
}
