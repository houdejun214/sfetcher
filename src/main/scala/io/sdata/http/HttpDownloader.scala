package io.sdata.http

import org.apache.http.HttpResponse
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.nio.client.{CloseableHttpAsyncClient, HttpAsyncClientBuilder}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by dejun on 31/1/16.
 */

object HttpDownloader extends Downloader {

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  private val requestConfig = RequestConfig.custom()
    .setCircularRedirectsAllowed(true)
    .setConnectTimeout(20 * 1000)
    .setSocketTimeout(10 * 1000)
    //    .setMaxRedirects(3)
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
    try {
      val response: HttpResponse = client.execute(get, null).get
      new Response(url, Option(response))
    } catch {
      case ex: Exception =>
        log.info(s"fail to download page as {$ex}, <= $url")
        new Response(url)
    }
  }

}

