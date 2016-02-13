package io.sdata.http

import java.util.Locale

import org.apache.http.impl.EnglishReasonPhraseCatalog
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpResponse, HttpStatus}

/**
 * Created by dejun on 13/2/16.
 */
trait Downloader {

  def download(url: String): Response
}


class Response(val _url: String, val response: Option[HttpResponse] = None) {

  def url = _url

  def status = response match {
    case Some(res) => res.getStatusLine.getReasonPhrase
    case _ => EnglishReasonPhraseCatalog.INSTANCE.getReason(HttpStatus.SC_BAD_REQUEST, Locale.getDefault)
  }

  /**
   * get content of the response
   * @return
   */
  def content = response match {
    case Some(res) => EntityUtils.toString(res.getEntity)
    case _ => ""
  }

  def success = response match {
    case Some(res) => true
    case _ => false
  }
}

class StaticResponse(url: String, htmlContent: Option[String]=None) extends Response(url) {
  override def status = {
    htmlContent match {
      case Some(i)=>EnglishReasonPhraseCatalog.INSTANCE.getReason(HttpStatus.SC_OK, Locale.getDefault)
      case _=>EnglishReasonPhraseCatalog.INSTANCE.getReason(HttpStatus.SC_BAD_REQUEST, Locale.getDefault)
    }
  }

  override def content = {
    htmlContent match {
      case Some(s)=>s
      case _=>""
    }
  }

  override def success = !htmlContent.isEmpty
}