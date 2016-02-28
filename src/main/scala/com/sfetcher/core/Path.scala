package com.sfetcher.core

import java.net.URI

import com.lakeside.core.utils.StringUtils

import scala.collection.mutable
import scala.language.postfixOps

object Path {

  private val HttpPathPattern = "^((\\w+) )?https?:\\/\\/.*$" r
  private val FilePathPattern = "file://.*" r

  def apply(input: String): Path = {
    if (input matches HttpPathPattern.regex) {
      new HttpPath(input)
    } else if (input matches FilePathPattern.regex) {
      new FilePath(input)
    } else {
      throw new RuntimeException("Could resolve the path string")
    }
  }
}

/**
  * Created by dejun on 28/2/16.
  */
trait Path extends Serializable

trait Parameterize {
  def withParam(param: (String, String)): Parameterize

  def withParams(params: Seq[(String, String)]): Parameterize

  def withParams(params: String): Parameterize
}

/**
  * object represent a local file path
  *
  * @param input
  */
case class FilePath(input: String) extends Path {
  var path = StringUtils.trim(input, "/")
  var uri = new URI(path)
}

/**
  * object represent http path
  *
  * @param url
  */
case class HttpPath(private val url: String) extends Path with Parameterize {
  var method = "GET"
  var path = StringUtils.trim(url, "/")
  if (url.startsWith("POST ") || url.startsWith("GET ")) {
    val strings: Array[String] = url.split(" ", 2)
    method = strings(0)
    path = strings(1)
  }

  val parameters = mutable.ListBuffer[(String, String)]()

  def withParam(param: (String, String)): HttpPath = {
    parameters += param
    this
  }

  def withParams(params: Seq[(String, String)]): HttpPath = {
    parameters ++= params
    this
  }

  def withParams(params: String): HttpPath = {
    val list = params.split("&").toList.map(_.trim).filter(_.length > 0).flatMap(x => {
      val s = x.split('=')
      Map[String, String](s(0) -> s(1))
    })

    parameters ++= list
    this
  }
}
