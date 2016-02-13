package io.sdata.core.crawldb

/**
 * Created by dejun on 12/2/16.
 */
trait CrawlDB {

  def exists(url:String):Boolean

  def append(urls:String*):Unit

  def appendIfNotExists(urls:String):Option[Boolean]
}


