package com.sfetcher.http

import java.nio.charset.CodingErrorAction
import java.security.cert.X509Certificate
import java.util
import javax.net.ssl.SSLContext

import com.sfetcher.core.HttpPath
import org.apache.http.client.config.{AuthSchemes, CookieSpecs, RequestConfig}
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.config.{ConnectionConfig, RegistryBuilder}
import org.apache.http.conn.ssl.NoopHostnameVerifier
import org.apache.http.impl.client.LaxRedirectStrategy
import org.apache.http.impl.conn.SystemDefaultDnsResolver
import org.apache.http.impl.nio.client.{CloseableHttpAsyncClient, HttpAsyncClients}
import org.apache.http.impl.nio.codecs.{DefaultHttpRequestWriterFactory, DefaultHttpResponseParserFactory}
import org.apache.http.impl.nio.conn.{ManagedNHttpClientConnectionFactory, PoolingNHttpClientConnectionManager}
import org.apache.http.impl.nio.reactor.{DefaultConnectingIOReactor, IOReactorConfig}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy
import org.apache.http.nio.conn.{NoopIOSessionStrategy, SchemeIOSessionStrategy}
import org.apache.http.nio.util.HeapByteBufferAllocator
import org.apache.http.ssl.{SSLContexts, TrustStrategy}
import org.apache.http.{Consts, HttpResponse, NameValuePair}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by dejun on 31/1/16.
 */

object HttpDownloader extends Downloader {

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  private val client: CloseableHttpAsyncClient = getClient()

  def getClient() = {
    // Use custom message parser / writer to customize the way HTTP
    // messages are parsed from and written out to the data stream.
    val responseParserFactory = new DefaultHttpResponseParserFactory();


    val requestWriterFactory = new DefaultHttpRequestWriterFactory();

    // Use a custom connection factory to customize the process of
    // initialization of outgoing HTTP connections. Beside standard connection
    // configuration parameters HTTP connection factory can define message
    // parser / writer routines to be employed by individual connections.
    val connFactory = new ManagedNHttpClientConnectionFactory(
      requestWriterFactory, responseParserFactory, HeapByteBufferAllocator.INSTANCE);

    // Client HTTP connection objects when fully initialized can be bound to
    // an arbitrary network socket. The process of network socket initialization,
    // its connection to a remote address and binding to a local one is controlled
    // by a connection socket factory.

    // SSL context for secure connections can be created either based on
    // system or application specific properties.
    val acceptingTrustStrategy = new TrustStrategy() {
      override def isTrusted(chain: Array[X509Certificate], authType: String): Boolean = true
    }

    var sslContext: SSLContext = null;
    try {
      sslContext = SSLContexts.custom()
        .loadTrustMaterial(null, acceptingTrustStrategy).build();
    } catch {
      // Handle error
      case _: Throwable =>
    }
    // Use custom hostname verifier to customize SSL hostname verification.
    val hostnameVerifier = NoopHostnameVerifier.INSTANCE;

    // Create a registry of custom connection session strategies for supported
    // protocol schemes.
    val sessionStrategyRegistry = RegistryBuilder.create[SchemeIOSessionStrategy]()
      .register("http", NoopIOSessionStrategy.INSTANCE)
      .register("https", new SSLIOSessionStrategy(sslContext, hostnameVerifier))
      .build();

    // Use custom DNS resolver to override the system DNS resolution.
    val dnsResolver = new SystemDefaultDnsResolver();

    // Create I/O reactor configuration
    val ioReactorConfig = IOReactorConfig.custom()
      .setIoThreadCount(Runtime.getRuntime().availableProcessors())
      .setSoKeepAlive(true)
      .setConnectTimeout(20 * 1000)
      .setSoReuseAddress(true)
      .setSelectInterval(20)
      .build();

    // Create a custom I/O reactor
    val ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);

    // Create a connection manager with custom configuration.
    val connManager = new PoolingNHttpClientConnectionManager(
      ioReactor, connFactory, sessionStrategyRegistry, dnsResolver);

    // Create connection configuration
    val connectionConfig = ConnectionConfig.custom()
      .setMalformedInputAction(CodingErrorAction.IGNORE)
      .setUnmappableInputAction(CodingErrorAction.IGNORE)
      .setCharset(Consts.UTF_8)
      .build();
    // Configure the connection manager to use connection configuration either
    // by default or for a specific host.
    connManager.setDefaultConnectionConfig(connectionConfig);

    // Configure total max or per route limits for persistent connections
    // that can be kept in the pool or leased by the connection manager.
    connManager.setMaxTotal(200);
    connManager.setDefaultMaxPerRoute(100);

    // Create global request configuration
    val defaultRequestConfig = RequestConfig.custom()
      .setCookieSpec(CookieSpecs.DEFAULT)
      .setExpectContinueEnabled(true)
      .setConnectTimeout(20 * 1000)
      .setSocketTimeout(10 * 1000)
      .setRedirectsEnabled(true)
      .setTargetPreferredAuthSchemes(util.Arrays.asList(AuthSchemes.NTLM, AuthSchemes.DIGEST))
      .setProxyPreferredAuthSchemes(util.Arrays.asList(AuthSchemes.BASIC))
      .build();

    // Create an HttpClient with the given custom dependencies and configuration.
    val httpclient = HttpAsyncClients.custom()
      .setConnectionManager(connManager)
      .setRedirectStrategy(new LaxRedirectStrategy)
      .setDefaultRequestConfig(defaultRequestConfig)
      .build()
    httpclient.start()
    httpclient
  }

  /**
   * Downloads given url via GET and returns response entity.
   *
   */
  def download(httpPath: HttpPath): Response = {
    val url = httpPath.path
    var request: HttpRequestBase = null
    httpPath.method match {
      case "POST"=> val post = new HttpPost(url)
        if(httpPath.parameters.nonEmpty){
          addPostParameters(post,httpPath.parameters)
        }
        request = post
      case "GET" => request=new HttpGet(url)
      case _=>
    }
    try {
      val response: HttpResponse = client.execute(request, null).get
      new Response(url, Option(response))
    } catch {
      case ex: Exception =>
        log.info(s"fail to download page as {$ex}, <= $url")
        new Response(url)
    }
  }

  private def addPostParameters(post: HttpPost, parameters:Seq[(String, String)]) = {
    val urlParameters = new util.ArrayList[NameValuePair]()
    parameters.foreach(pair=>urlParameters.add(new BasicNameValuePair(pair._1, pair._2)))
    post.setEntity(new UrlEncodedFormEntity(urlParameters))
  }
}

