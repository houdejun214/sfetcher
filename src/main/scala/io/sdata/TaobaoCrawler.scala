package io.sdata

import io.sdata.core.CrawlDSL._
import io.sdata.core.{Entry, Pattern, Pipeline}
import io.sdata.store.DummyStore

import scala.collection._

/**
 *
 */
object TaobaoCrawler extends App {

  var entry = Entry("https://hanxierka.world.tmall.com/?spm=a312a.7700718.0.0.BqrR7F")

  val layer0 = Pattern("https://hanxierka.world.tmall.com/index.htm?:*")
  .datum()
  .links(Seq(
    link(".tshop-pbsm-shop-nav-ch .menu-list|link")
  )).entry()


  val layer1 = Pattern("https://hanxierka.world.tmall.com/search.htm?:*")
    .pointTo(layer2)

  val layer2 = Pattern("https://hanxierka.world.tmall.com/category-:id.htm?:*")
    .datum()
    .links(Seq(
    link(".tshop-pbsm-shop-item-cates li.cat.fst-cat|link ")
  )).entry()

  val layer3 = Pattern("https://detail.tmall.com/item.htm?:*")
    .datum("product")
    .fields(Seq(
    field[String]("name") on ("#productTitle|txt"),
    field[String]("url") on ("omit()|url"),
    field[String]("price") on ("#priceblock_ourprice|txt"),
    field[String]("image") on ("#imgTagWrapperId img|link")
  )).entry()

  lazy val store = new DummyStore()

  Pipeline(entry,layer0, layer1, layer2, layer3)
    .start

}
