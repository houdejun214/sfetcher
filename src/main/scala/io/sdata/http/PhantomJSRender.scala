package io.sdata.http

import com.lakeside.core.utils.{PathUtils, StringUtils}

/**
 * Created by dejun on 13/2/16.
 */
object PhantomJSRender extends Downloader {

  import sys.process._

  private val CommandPattern = "{0} --ignore-ssl-errors=true " +
    "--load-images=false " +
    "--web-security=false {2} {3}"

  private lazy val sdataHome: String = {
    var home: String = "";
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
   * @param url
   */
  def render(url: String): String = {
    val command: String = StringUtils.format(CommandPattern, phantomjsPath, configPath, renderPath, url)
    //println(s"phantomjs=> $command")
    val result = (command !!)
    result
  }

  override def download(url: String): Response = {
    val content = render(url)
    new StaticResponse(url, Option(content))
  }
}


