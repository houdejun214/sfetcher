package io.sdata.actors

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.google.inject.{Inject, Injector}
import io.sdata.actors.CrawlActor.{CrawlPage, EntryPage}
import io.sdata.actors.ParseActor.PageContent
import io.sdata.core.{CrawlContext, DatumSchema, Entry}
import io.sdata.http.Downloader
import io.sdata.modules.ActorInject

/**
 * Created by dejun on 3/2/16.
 */

object CrawlActor {

  case class EntryPage(from: Entry, url: String)

  case class Page(url: String)

  case class CrawlPage(from: Entry, url: String)

  case class PageDatum(datum: DatumSchema)

}

class CrawlActor @Inject()(inject: Injector,
                           crawlContext: CrawlContext) extends BaseActor(inject) with ActorInject {


  override def receive: Receive = {
    case EntryPage(from, url) => {
      val response = download(url)
      val parseActor = injectActor[ParseActor]
      parseActor ! PageContent(from, response)
    }
    case CrawlPage(from, url) => {
      val response = download(url)
      val parseActor = injectActor[ParseActor]
      parseActor ! PageContent(from, response)
    }
  }

  def download(url: String) = {
    val watch: Stopwatch = new Stopwatch().start()
    val response = Downloader.download(url)
    log.info("Download [{}] in [{}ms] <= {}", response.status, watch.elapsed(TimeUnit.MILLISECONDS), url)
    response
  }
}
