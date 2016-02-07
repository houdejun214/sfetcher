package io.sdata.actors

import akka.actor.Actor
import io.sdata.actors.PageActor.PageDetailTask
import io.sdata.http.Downloader

/**
 * Created by dejun on 31/1/16.
 */
object PageActor extends NamedActor{

  override def name = "PageActor"

  case class PageDetailTask(url:String)

}

class PageActor extends Actor{
  def receive: Receive = {
    case PageDetailTask(url) => {
      val content = Downloader.download(url)
    }
  }
}


