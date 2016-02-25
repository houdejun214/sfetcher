package com.sfetcher.core.crawldb

import java.io.File

import com.lakeside.core.utils.FileUtils
import org.mapdb.{DBMaker, HTreeMap, Serializer}

/**
  * Created by dejun on 13/2/16.
  */
object CrawlDBOfMapDB {

  lazy val instance = new CrawlDBOfMapDB("/Users/dejun/working/sdatacrawler/.crawl.links.mdb")

  def apply() = instance

  def apply(path:String) = {
    new CrawlDBOfMapDB(path)
  }
}

class CrawlDBOfMapDB(path: String) extends CrawlDB {
  // runtime database
  val db = DBMaker
    .newFileDB(new File(path))
    .mmapFileEnable()
    .closeOnJvmShutdown()
    .make()

  val linkQueue: HTreeMap[String, Integer] = db.createHashMap("links")
    .keySerializer(Serializer.STRING)
    .valueSerializer(Serializer.INTEGER)
    .makeOrGet()

  def exists(url: String) = {
    linkQueue.containsKey(url)
  }

  def append(urls: String*) = {
    urls.foreach {
      url =>
        if (!linkQueue.containsKey(url)) {
          linkQueue.put(url, 1)
          db.commit()
        }
    }
  }

  def size():Int = {
    linkQueue.size()
  }

  override def appendIfNotExists(link: String): Option[Boolean] = {
    try {
      exists(link) match {
        case false => append(link)
          return Option(true)
      }
      Option(false)
    } catch {
      case ex: Exception => Option(false)
    }
  }

  def close(deleteFile:Boolean = false) = {
    db.commit()
    db.close()
    if(deleteFile){
      FileUtils.delete(path)
      FileUtils.delete(Seq(path,"p").mkString("."))
    }
  }
}