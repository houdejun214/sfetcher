package com.sdata.actors

import akka.actor.Actor
import com.google.inject.Inject
import com.sdata.actors.CrawlActor.CrawlPage
import com.sdata.core.CrawlContext
import com.sdata.http.Downloader

/**
 * Created by dejun on 3/2/16.
 */

object CrawlActor {

  case class CrawlPage(url:String)

}

class CrawlActor @Inject()(crawlContext:CrawlContext) extends Actor {
  override def receive: Receive = {
    case CrawlPage(url) => {
      val content = Downloader.download(url)
    }
  }
}
