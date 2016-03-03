package com.sfetcher

import com.sfetcher.core.CrawlDSL._
import com.sfetcher.core._

/**
  *
  */
object ShangbiaoCrawler extends AbstractApp {

  val layer0 = Pattern("http://sbcx.saic.gov.cn:9080/tmois/wsggcx_getGgaoMainlate.xhtml")
    .links(Seq(
      link("table.import_photo td img |links"),
      link("{$url}|")
    )).entry()

  val layer1 = Pattern("http://sbcx.saic.gov.cn:9080/tmois/wsggcx_getImageInputSterem.xhtml*")
    .datum("shangbiao", Seq(
      "img".on("{$url}")
    )).entry()

  var entry = Entry("POST http://sbcx.saic.gov.cn:9080/tmois/wsggcx_getGgaoMainlate.xhtml")
    .withParam("gmBean.anNum" -> "1476")
    .withParam("pagenum" -> "1")
    .withParam("pagesize" -> "15")
    .->(layer0)

  start(layer0, layer1, entry)
}
