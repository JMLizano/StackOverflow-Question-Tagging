package stackoverflow

import java.nio.file.Paths

import org.scalatest._
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}


class Base extends FunSuite with BeforeAndAfterAll { self =>

  var spark: SparkSession = _

  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }

  /**
    * Initialize the SparkSession.
    */
  override protected def beforeAll(): Unit = {
   super.beforeAll()

   spark = SparkSession
     .builder()
     .appName("stackOverflowTesting")
     .master("local")
     .getOrCreate()

   spark.sparkContext.setLogLevel("ERROR")
  }

  /**
    * Stop the underlying SparkSession, if any.
    */
  override protected def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
        spark = null
      }
    } finally {
      super.afterAll()
    }
  }

  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String = Paths.get(getClass.getResource(resource).toURI).toString

}