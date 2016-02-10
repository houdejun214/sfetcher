package io.sdata.actors

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.google.inject.{Inject, Injector}
import io.sdata.actors.CrawlActor.{CrawlPage, PageDatum}
import io.sdata.actors.ParseActor.PageContent
import io.sdata.core.{CrawlContext, DatumSchema}
import io.sdata.http.Downloader
import io.sdata.modules.ActorInject

/**
 * Created by dejun on 3/2/16.
 */

object CrawlActor {

  case class Page(url:String)
  case class CrawlPage(url:String)
  case class PageDatum(datum:DatumSchema)

}

class CrawlActor @Inject()(inject: Injector,
                           crawlContext:CrawlContext) extends BaseActor(inject) with ActorInject{
  val parseActor = injectActor[ParseActor]

  override def receive: Receive = {
    case CrawlPage(url) => {
      val watch: Stopwatch = new Stopwatch().start()
      val response = Downloader.download(url)
      log.info("Download [{}] in [{}ms] <= {}", response.status, watch.elapsed(TimeUnit.MILLISECONDS), url)
      parseActor ! PageContent(response.content)
    }
    case PageDatum(datum) => {

    }
  }

//
}
