package com.sdata.actors

import akka.actor.Actor
import com.sdata.fetcher.PageDetailTask
import com.sdata.net.Downloader

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
