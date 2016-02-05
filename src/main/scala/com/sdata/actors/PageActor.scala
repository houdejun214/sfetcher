package com.sdata.actors

import akka.actor.{Actor, ActorRef, Props}
import com.sdata.fetcher.PageDetailTask
import com.sdata.http.Downloader

/**
 * Created by dejun on 31/1/16.
 */
class PageActor extends Actor{
  def receive: Receive = {
    case PageDetailTask(url) => {
      val content = Downloader.download(url)
    }
  }
}

object PageActor {
  lazy val linkActor: ActorRef = CrawlActors.system.actorOf(Props(classOf[PageActor]))
}
