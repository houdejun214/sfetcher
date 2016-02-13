package io.sdata.core.crawldb

import org.mapdb.{Serializer, HTreeMap, DBMaker}

/**
 * Created by dejun on 13/2/16.
 */
object CrawlDBOfMapDB {

  lazy val instance= new CrawlDBOfMapDB("./.crawl.links.mdb")

  def apply() = instance
}

class CrawlDBOfMapDB(path:String) extends CrawlDB{
  // runtime database
  lazy val crawlDB = DBMaker
    .fileDB(path)
    //    .memoryDB()
    .make()

  lazy val memDb  = DBMaker
    .memoryDB()
    .make()

  lazy val linkQueue: HTreeMap[String, Integer] = crawlDB.hashMap("counters")
    .keySerializer(Serializer.STRING)
    .valueSerializer(Serializer.INTEGER)
    .createOrOpen()

  def exists(url:String) = {
    linkQueue.containsKey(url)
  }

  def append(urls:String*) = {
    urls.foreach {
      url =>
        if (!linkQueue.containsKey(url)) {
          linkQueue.put(url, 1)
        }
    }

  }

  override def appendIfNotExists(urls: String): Option[Boolean] = ???
}