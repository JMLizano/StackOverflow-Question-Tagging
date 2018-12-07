package stackoverflow

import org.apache.spark.ml.feature._


object Feature {

  def tf(numFeatures: Int, input: String, output: String): HashingTF = {
    new HashingTF()
      .setInputCol(input)
      .setOutputCol(output)
      .setNumFeatures(numFeatures)
  }

  def idf(MinDocFreq: Int, input: String, output: String) = {
    new IDF()
      .setInputCol("TFOut")
      .setOutputCol(output)
      .setMinDocFreq(MinDocFreq)
  }

  def splitTags(inputCol: String = "Tags", outputCol: String = "label"): RegexTokenizer = {
    new RegexTokenizer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setGaps(false)
      .setPattern("[^<>]+")
  }

  def  titleTokenizer(inputCol: String = "Title", outputCol: String = "TitleSplitted"): Tokenizer = {
    new Tokenizer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
  }

  def  stopWordsRemover(inputCol: String = "TitleSplitted", outputCol: String = "TitleStop") : StopWordsRemover = {
    val englishStopWords = StopWordsRemover.loadDefaultStopWords("english")
    new StopWordsRemover().setInputCol(inputCol).setOutputCol(outputCol).setStopWords(englishStopWords)
  }

}
