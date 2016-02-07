package io.sdata

import io.sdata.core.CrawlDSL._
import io.sdata.core.{Entry, Pattern, Pipeline}
import io.sdata.store.DummyStore

import scala.collection._

/**
 *
 */
object AmazonCrawler extends App {

  var entry = Entry("http://www.amazon.com/s/ref=lp_7147441011_ex_n_1?rh=n%3A7141123011%2Cn%3A10445813011&bbn=10445813011&ie=UTF8&qid=1454943074")

  val layer1 = Pattern("http://www.amazon.com//BCBGMax-Womens-Kyndal-Cascade-X-Small/dp/B019NI8O70?**")
    .datum()
    .links(Seq(
    link() on ("#productTitle|txt")
  )).entry()


  lazy val layer2 = Pattern("http://www.amazon.com//BCBGMax-Womens-Kyndal-Cascade-X-Small/dp/B019NI8O70?**")
    .datum("product")
    .fields(Seq(
    field[String]("name") on ("#productTitle|txt"),
    field[String]("url") on ("omit()|url"),
    field[String]("price") on ("#priceblock_ourprice|txt"),
    field[String]("image") on ("#imgTagWrapperId img|link")
  )).entry()

  lazy val store = new DummyStore()

  Pipeline(entry, layer1, layer2)
    .start

}
