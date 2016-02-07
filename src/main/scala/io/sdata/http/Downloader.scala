package io.sdata.http

import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.nio.client.{CloseableHttpAsyncClient, HttpAsyncClients}
import org.apache.http.util.EntityUtils

/**
 * Created by dejun on 31/1/16.
 */

object Downloader {

  private val client: CloseableHttpAsyncClient = HttpAsyncClients.createDefault
  client.start


  /**
   * Downloads given url via GET and returns response entity.
   *
   */
  def download(url: String): Response = {
    val get: HttpGet = new HttpGet(url)
    val response: HttpResponse = client.execute(get, null).get
    new Response(response)
  }


  class Response(httpResponse: HttpResponse) {

    def status = httpResponse.getStatusLine.getReasonPhrase

    /**
     * get content of the response
     * @param response
     * @return
     */
    def content(response:HttpResponse):String = {
      EntityUtils.toString(response.getEntity)
    }
  }
}
