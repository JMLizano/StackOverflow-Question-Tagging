package stackoverflow

import java.nio.file.Paths
import extractQuestions._


class extractQuestionsSuite extends Base {

  import testImplicits._

  test("readXml") {
    val posts = readXml(fsPath("/Posts.xml"), "posts", "row",  extractQuestions.postSchema, spark)
    assert(posts.count() == 29998)
    assert(posts.columns.size == extractQuestions.postSchema.fields.size)
  }

  test("getQuestions") {
    val questions = getQuestions(fsPath("/Posts.xml"), spark)
    assert(questions.count() == 6075)
    assert(questions.columns.toSeq == extractQuestions.postColumnNames)
  }

  test("saveQuestions") {
    val questions = getQuestions(fsPath("/Posts.xml"), spark)
    saveQuestions(questions, "src/test/resources/questions")
  }

}
