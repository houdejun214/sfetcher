package com.sfetcher.core.crawldb

/**
 * Created by dejun on 12/2/16.
 */
trait CrawlDB {

  def exists(url: String): Boolean

  /**
    * append the urls into the crawlDB
    * @param urls
    */
  def append(urls: String*): Unit

  /**
    * append the url if it's not exists in the crawlDB
    *
    * @param url
    * @return
    *         Option(true) if append success.
    *         Option(false) if already exists.
    */
  def appendIfNotExists(url: String): Option[Boolean]

  /**
    * size of crawlDB
    * @return
    */
  def size():Int
}


