package io.sdata.http

import org.apache.http.HttpResponse
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.nio.client.{CloseableHttpAsyncClient, HttpAsyncClientBuilder}
import org.apache.http.util.EntityUtils

/**
 * Created by dejun on 31/1/16.
 */

object Downloader {

  private val requestConfig = RequestConfig.custom()
    .setCircularRedirectsAllowed(true)
    .setConnectTimeout(20 * 1000)
    .setSocketTimeout(20 * 1000)
    .build()

  private val clientBuilder = HttpAsyncClientBuilder.create()
  // set default request config
  clientBuilder.setDefaultRequestConfig(requestConfig)
  private val client: CloseableHttpAsyncClient = clientBuilder.build()
  client.start()


  /**
   * Downloads given url via GET and returns response entity.
   *
   */
  def download(url: String): Response = {
    val get: HttpGet = new HttpGet(url)
    val response: HttpResponse = client.execute(get, null).get
    new Response(url, response)
  }


  class Response(val _url: String, val httpResponse: HttpResponse) {

    def url = _url

    def status = httpResponse.getStatusLine.getReasonPhrase

    /**
     * get content of the response
     * @return
     */
    def content = {
      EntityUtils.toString(httpResponse.getEntity)
    }
  }

}

