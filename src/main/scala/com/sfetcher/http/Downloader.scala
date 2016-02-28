package com.sfetcher.http

import java.util.Locale

import com.sfetcher.core.HttpPath
import org.apache.http.impl.EnglishReasonPhraseCatalog
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpStatus, HttpResponse}

/**
 * Created by dejun on 13/2/16.
 */
trait Downloader {

  def download(path: HttpPath): Response
}


class Response(val _url: String, val response: Option[HttpResponse] = None) {

  def url = _url

  /**
    * get status phrase
    * @return
    */
  def status = response match {
    case Some(res) => res.getStatusLine.getReasonPhrase
    case _ => EnglishReasonPhraseCatalog.INSTANCE.getReason(HttpStatus.SC_BAD_REQUEST, Locale.getDefault)
  }

  /**
   * get content of the response
    *
    * @return
   */
  def content = response match {
    case Some(res) => EntityUtils.toString(res.getEntity)
    case _ => ""
  }

  /**
    * check if success for this response
    * @return
    */
  def success = response match {
    case Some(res) => res.getStatusLine.getStatusCode == HttpStatus.SC_OK
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