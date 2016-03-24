package com.sfetcher

import com.sfetcher.core.CrawlDSL._
import com.sfetcher.core.{AbstractApp, Entry, Pattern}

/**
  *
  */
object TaobaoCrawler extends AbstractApp {

  val layer0 =
    Pattern("http*://hanxierka.tmall.com/index.htm?*")
    .links(Seq(
      link(".tshop-pbsm-shop-nav-ch .menu-list .menu a|links")
    )).entry()

  val layer2 =
    Pattern("http?://hanxierka.*.tmall.com/category-*.htm?*")
    .links(Seq(
      link(".tshop-pbsm-shop-item-cates li.cat.fst-cat a|links "),
      link(".tshop-pbsm-tmall-srch-list .J_TItems .item .detail a|links "),
      link(".tshop-pbsm-tmall-srch-list .J_TItems .pagination a|links ")
    )).entry()

  val layer3 =
    Pattern("http*://detail.tmall.com/item.htm?*")
    .datum("product",Seq(
      "name".on("#detail .tm-detail-meta .tb-detail-hd h1|txt"),
      "url".on("{$url}"),
      "price".on("#detail .tm-detail-meta .tm-price-panel .tm-price|html"),
      "image".on("#detail .tb-gallery .tb-booth a img|link"),
      "shop_name".on(".sea-header-bd .hd-shop-info .hd-shop-name a|text")
    )).entry()


  var entry =
    Entry("file://Users/dejun/working/temp/category.html")
    .->(layer2)

  val layer1 =
    Pattern("http*://hanxierka.tmall.com/search.htm?*")
    .->(layer2)

  start(entry, layer0, layer1, layer2, layer3)
}
