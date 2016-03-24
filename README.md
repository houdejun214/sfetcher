# sfetcher
============

Yet, another scala based crawler framework for extracting the data you need from websites.
In a fast, simple, yet extensible way.

## Define crawler with DSL.

A sample case on how to define a crawler to fetcher data from taobao.com.
```scala

object TaobaoCrawler extends AbstractApp {

  // define the parser for first layer. 
  // parse the index(home) page of a shop
  val layer0 = 
    Pattern("http*://hanxierka.tmall.com/index.htm?*")
    .links(Seq(
      link(".tshop-pbsm-shop-nav-ch .menu-list .menu a|links")
    )).entry()

  // define the parser for second layer
  // parse product list page of a shop
  val layer2 = 
    Pattern("http?://hanxierka.*.tmall.com/category-*.htm?*")
    .links(Seq(
      link(".tshop-pbsm-shop-item-cates li.cat.fst-cat a|links "),
      link(".tshop-pbsm-tmall-srch-list .J_TItems .item .detail a|links "),
      link(".tshop-pbsm-tmall-srch-list .J_TItems .pagination a|links ")
    )).entry()

  // define the parser for third layer
  // parse the page details.
  val layer3 = 
    Pattern("http*://detail.tmall.com/item.htm?*")
    .datum("product",Seq(
      "name".on("#detail .tm-detail-meta .tb-detail-hd h1|txt"),
      "url".on("{$url}"),
      "price".on("#detail .tm-detail-meta .tm-price-panel .tm-price|html"),
      "image".on("#detail .tb-gallery .tb-booth a img|link"),
      "shop_name".on(".sea-header-bd .hd-shop-info .hd-shop-name a|text")
    )).entry()

  // define the entry for this crawler
  var entry = 
    Entry("file://Users/dejun/working/temp/category.html")
    .->(layer2)

  val layer1 = 
    Pattern("http*://hanxierka.tmall.com/search.htm?*")
    .->(layer2)

  start(entry, layer0, layer1, layer2, layer3)
}

```


## Roadmap

- Extends Http path
    + Support HTTP Method (GET, POST)
    + Support external parameters in Pattern match. 
* Use akka [routing-dsl/path-matchers.html](http://doc.akka.io/docs/akka-stream-and-http-experimental/2.0.3/scala/http/routing-dsl/path-matchers.html)
* Persistent the crawl link queue.
* Distributed fetcher system.
