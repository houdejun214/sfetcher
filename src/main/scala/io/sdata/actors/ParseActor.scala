package io.sdata.actors

import com.google.inject.{Inject, Injector}
import io.sdata.actors.ParseActor.PageContent
import io.sdata.core.CrawlContext
import io.sdata.modules.ActorInject

/**
 * Created by dejun on 3/2/16.
 */

object ParseActor {
  case class PageContent(content:String)
}

class ParseActor @Inject()(inject: Injector,
                           crawlContext:CrawlContext) extends BaseActor(inject) with ActorInject{

  override def receive: Receive = {
    case PageContent(content)=> {

    }
  }
}
