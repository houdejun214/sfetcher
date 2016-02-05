package com.sdata.actors

import akka.actor.{Actor, ActorRef, Props}
import com.sdata.core.parser.select.PageContext
import com.sdata.fetcher.CatalogLinkTask
import com.sdata.http.Downloader

/**
 * Created by dejun on 31/1/16.
 */
class LinkActor extends Actor{
  def receive: Receive = {
    case CatalogLinkTask(url, context:PageContext) => {
      val content = Downloader.download(url)

    }
  }
}


object LinkActor {
  lazy val linkActor: ActorRef = CrawlActors.system.actorOf(Props(classOf[LinkActor]))
}

