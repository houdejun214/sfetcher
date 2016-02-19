package io.sdata.actors

import java.io.File
import java.net.URI
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.google.inject.{Inject, Injector}
import io.sdata.actors.CrawlActor.CrawlPage
import io.sdata.actors.ParseActor.PageContent
import io.sdata.core.{CrawlContext, DatumSchema, Entry}
import io.sdata.http.{Downloader, Response, StaticResponse}
import io.sdata.modules.ActorInject
import org.apache.commons.io.FileUtils

/**
 * Created by dejun on 3/2/16.
 */

object CrawlActor {

  //case class EntryPage(from: Entry, url: String)

  case class Page(url: String)

  case class CrawlPage(from: Entry, url: String)

  case class PageDatum(datum: DatumSchema)

}

class CrawlActor @Inject()(inject: Injector,
                           crawlContext: CrawlContext)
  extends BaseActor(inject)
//    with PersistentActor
    with ActorInject {

  import CrawlContext.Implicits.downloader

  override def receive: Receive = {
    case CrawlPage(from, url) => {
      val response = download(url)
      if (response.success) {
        val parseActor = injectActor[ParseActor]("parse-dispatcher")
        parseActor ! PageContent(from, response)
      }
    }
  }

  def download(url: String)(implicit downloader: Downloader) = {
    val watch: Stopwatch = new Stopwatch().start()
    log.info("Downloading => {}", url)
    val uri = new URI(url)
    if(uri.getScheme.equals("file")){
      readFile(uri)
    }else{
      val response = downloader.download(url)
      if (response.success) {
        log.info("Download [{}] in [{}ms] <= {}", response.status, watch.elapsed(TimeUnit.MILLISECONDS), url)
      }
      response
    }
  }

  def readFile(uri:URI): Response = {
      try{
        val content: String =  FileUtils.readFileToString(new File(uri.getSchemeSpecificPart), "utf-8")
        new StaticResponse(uri.toString, Option(content))
      }catch {
        case e:Exception=> {
          e.printStackTrace()
          new StaticResponse(uri.toString())
        }
      }
  }

//  override def receiveRecover: Receive = ???
//
//  override def receiveCommand: Receive = ???
//
//  override def persistenceId: String = "CrawlActor"
}
