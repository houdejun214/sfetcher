package com.sfetcher.http

import java.util.concurrent.TimeUnit

import com.lakeside.core.utils.{PathUtils, StringUtils}
import com.sfetcher.core.HttpPath

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
 * Created by dejun on 13/2/16.
 */
object PhantomJSRender extends Downloader {

  private val CommandPattern = "{0} --ignore-ssl-errors=true " +
    "--load-images=false " +
    "--web-security=false {2} {3}"

  private lazy val sdataHome: String = {
    var home: String = ""
    if (!sys.env.contains("SDATA_HOME")) {
      println("Please specify env `SDATA_HOME` first.")
      home = sys.props("SDATA_HOME")
    } else {
      home = sys.env("SDATA_HOME")
    }
    home
  }

  private lazy val phantomjsHome = PathUtils.join(sdataHome, "phantomjs")
  private lazy val phantomjsPath = PathUtils.join(phantomjsHome, "/bin/phantomjs")
  private lazy val configPath = PathUtils.join(phantomjsHome, "config.json")
  private lazy val renderPath = PathUtils.join(phantomjsHome, "phantom-loader.js")

  /**
   * render html page.
    *
    * @param url
   */
  def render(url: String): String = {
    import scala.concurrent.ExecutionContext.Implicits.global
    import sys.process._
    val command: String = StringUtils.format(CommandPattern, phantomjsPath, configPath, renderPath, url)
    var retry=1
    while (retry <= 3){
      val buffer = new StringBuffer
      val process = command.run(BasicIO(false, buffer, None))
      val f:Future[Int] = Future {
        process.exitValue()
      }
      try {
        Await.result(f, Duration(60,TimeUnit.SECONDS))
        return buffer.toString
      } catch {
        case ex:Exception=>
          process.destroy()
          retry+=1
          println("Proceed html render timeout, try it again.")
      }
    }
    "" //empty result
  }

  override def download(httpPath:HttpPath): Response = {
    val url = httpPath.path
    val content = render(url)
    new StaticResponse(url, Option(content))
  }
}


