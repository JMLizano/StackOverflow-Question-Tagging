package com.jmlizano.controllers

import scala.concurrent.{ExecutionContext, Future}
import javax.inject._

import play.api.data.Form
import play.api.mvc._
import play.api.Logger


case class PostFormInput(title: String, body: String)

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
        "title" -> nonEmptyText,
        "body" -> text
      )(PostFormInput.apply)(PostFormInput.unapply)
    )
  }

  def tags() = messagesAction { implicit request: MessagesRequest[AnyContent] =>
    logger.trace("process: ")
    processJsonPost()
  }

  private def processJsonPost[A]()(implicit request: MessagesRequest[A]): Result = {
    def failure(badForm: Form[PostFormInput]): Result = {
      BadRequest(badForm.errorsAsJson)
    }

    def success(input: PostFormInput): Result = {
      Ok(input.title)
    }

    form.bindFromRequest().fold(failure, success)
  }
}
