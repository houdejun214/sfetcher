package com.sdata.actors

import akka.actor.Actor
import com.sdata.fetcher.CatalogLinkTask
import com.sdata.net.Downloader

/**
 * Created by dejun on 31/1/16.
 */
class LinkActor extends Actor{
  def receive: Receive = {
    case CatalogLinkTask(url) => {
      val content = Downloader.download(url)
    }
  }
}
