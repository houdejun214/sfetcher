package io.sdata.core

import com.google.inject.Provider
import com.lakeside.config.Configuration

/**
 * Created by dejun on 3/2/16.
 */

object CrawlContext extends Provider[CrawlContext] {

  val context:CrawlContext = new CrawlContext

  def settings(key:String): String = {
    context.settings.get(key)
  }

  def settings(key:String, value:String): Unit = {
    context.settings.put(key, value)
  }

  def apply():CrawlContext = {
    context
  }

  override def get(): CrawlContext = context
}

class CrawlContext {

  val settings = new Configuration()

  // runtime database
//  val runtime = new CrawlRuntime

  var router:route.Router[Entry] = null
}
