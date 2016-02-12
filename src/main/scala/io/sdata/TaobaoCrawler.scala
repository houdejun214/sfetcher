package io.sdata

import io.sdata.core.CrawlDSL._
import io.sdata.core.{Entry, Pattern, Pipeline}
import io.sdata.store.DummyStore

import scala.collection._

/**
 *
 */
object TaobaoCrawler extends App {

  val layer0 = Pattern("https://hanxierka.tmall.com/index.htm?*")
  .datum()
  .links(Seq(
    link(".tshop-pbsm-shop-nav-ch .menu-list .menu a|links")
  )).entry()

  val layer2 = Pattern("https://hanxierka.tmall.com/category-*.htm?*")
    .datum()
    .links(Seq(
    link(".tshop-pbsm-shop-item-cates li.cat.fst-cat|links ")
  )).entry()

  val layer3 = Pattern("https://detail.tmall.com/item.htm?*")
    .datum("product")
    .fields(Seq(
    field[String]("name") on ("#productTitle|txt"),
    field[String]("url") on ("omit()|url"),
    field[String]("price") on ("#priceblock_ourprice|txt"),
    field[String]("image") on ("#imgTagWrapperId img|link")
  )).entry()

  var entry = Entry("https://hanxierka.tmall.com/?spm=a312a.7700718.0.0.BqrR7F")
    .-> (layer0)

  val layer1 = Pattern("https://hanxierka.tmall.com/search.htm?*")
    .-> (layer2)

  lazy val store = new DummyStore()

  Pipeline(entry,layer0, layer1, layer2, layer3)
    .start

}
