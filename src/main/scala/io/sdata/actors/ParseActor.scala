package io.sdata.actors

import akka.actor.Actor
import com.google.inject.Inject
import io.sdata.actors.ParseActor.PageContent
import io.sdata.core.CrawlContext

/**
 * Created by dejun on 3/2/16.
 */

object ParseActor extends NamedActor{
  override final val name = "ParseActor"
  case class PageContent(content:String)
}

class ParseActor @Inject()(crawlContext: CrawlContext) extends Actor{
  override def receive: Receive = {
    case PageContent(content)=>

  }


}
