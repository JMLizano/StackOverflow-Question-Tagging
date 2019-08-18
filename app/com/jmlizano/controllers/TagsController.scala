package com.jmlizano.controllers


import scala.concurrent.ExecutionContext
import javax.inject._
import play.api.data.Form
import play.api.mvc.{MessagesRequest, _}
import play.api.Logger
import play.api.libs.json.Json
import com.jmlizano.serving.TagsModel
import com.jmlizano.serving.TagsModel.tagsResult


case class PostFormInput(Title: String, Body: String)

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class TagsController @Inject()(
    val controllerComponents: ControllerComponents,
    messagesAction: MessagesActionBuilder
  )(implicit ec: ExecutionContext)
  extends BaseController {

  private val logger = Logger(getClass)

  private val form: Form[PostFormInput] = {
    import play.api.data.Forms._

    Form(
      mapping(
        "Title" -> nonEmptyText,
        "Body" -> text
      )(PostFormInput.apply)(PostFormInput.unapply)
    )
  }

  def index() = messagesAction { implicit request: MessagesRequest[AnyContent] =>
    Ok(com.jmlizano.views.html.postSubmit(form, tagsResult(Seq.empty)))
  }

  def tags(api: Boolean = true) = messagesAction {
    implicit request: MessagesRequest[AnyContent] =>
      logger.trace("process: ")
      processJsonPost(api)
  }

  private def processJsonPost[A](api: Boolean)(implicit request: MessagesRequest[A]): Result = {
    def failure(badForm: Form[PostFormInput]): Result = {
      BadRequest(badForm.errorsAsJson)
    }

    def success(input: PostFormInput): Result = {
      val predictions = TagsModel.predict(input)
      if(api)
        Ok(Json.toJson(predictions))
      else
        Ok(com.jmlizano.views.html.postSubmit(form.fill(input), predictions))
    }

    form.bindFromRequest().fold(failure, success)
  }
}
