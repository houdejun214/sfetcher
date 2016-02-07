package io.sdata.actors

import akka.actor.Actor
import akka.event.Logging
import com.google.inject.{Inject, Injector}
import io.sdata.actors.CrawlActor.{CrawlPage, PageDatum}
import io.sdata.core.{CrawlContext, DatumSchema}
import io.sdata.http.Downloader

/**
 * Created by dejun on 3/2/16.
 */

object CrawlActor extends NamedActor{

  override final val name = "CrawlActor"

  case class Page(url:String)
  case class CrawlPage(url:String)
  case class PageDatum(datum:DatumSchema)

}

class CrawlActor @Inject()(crawlContext:CrawlContext,
                           val inject: Injector) extends Actor {

  val log = Logging(context.system, this)

  override def receive: Receive = {
    case CrawlPage(url) => {
//      val parseActor = inject.instance[ParseActor]
      val response = Downloader.download(url)
//      parseActor ! PageContent(content)
      log.info("Download {}, with status:{}", url, response.status)
    }
    case PageDatum(datum) => {

    }
  }
}
