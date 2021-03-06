package com.sfetcher.core

import com.google.inject.Provider
import com.lakeside.config.Configuration
import com.sfetcher.core.crawldb.{CrawlDB, CrawlDBOfH2}
import com.sfetcher.store.{DBStore, FileCsvStore}

/**
 * Created by dejun on 3/2/16.
 */

object CrawlContext extends Provider[CrawlContext] {

  val context: CrawlContext = new CrawlContext

  def settings = context.settings

  def settings(key: String): String = {
    context.settings.get(key)
  }

  def settings(key: String, value: String): Unit = {
    context.settings.put(key, value)
  }

  def apply(): CrawlContext = {
    context
  }

  override def get(): CrawlContext = context


  object Implicits {
    implicit lazy val crawDB: CrawlDB = CrawlDBOfH2
    implicit lazy val store: DBStore = FileCsvStore
    //implicit lazy val downloader: Downloader = HttpDownloader //HttpDownloader //PhantomJSRender
  }

}

class CrawlContext {

  val settings = new Configuration()

  // runtime database
  //  val runtime = new CrawlRuntime

  var router: route.Router[EntryRef] = null

}
