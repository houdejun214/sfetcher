package io.sdata.actors

import java.io.File
import java.net.URI
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.google.inject.{Inject, Injector}
import com.sfetcher.core._
import _root_.io.sdata.actors.CrawlActor.{CrawlPath, CrawlURL}
import io.sdata.actors.ParseActor.PageContent
import com.sfetcher.http.{Downloader, Response, StaticResponse}
import com.sfetcher.modules.ActorInject
import org.apache.commons.io.FileUtils

/**
 * Created by dejun on 3/2/16.
 */

object CrawlActor {

  //indicate crawl task.
  trait Crawl
  case class Page(url: String)
  case class CrawlURL(from: EntryRef, url: String) extends Crawl
  case class CrawlPath(from: EntryRef, path: Path) extends Crawl
  case class PageDatum(datum: DatumSchema)
}

class CrawlActor @Inject()(inject: Injector,
                           crawlContext: CrawlContext)
  extends BaseActor(inject)
    with ActorInject {

  import CrawlContext.Implicits.downloader

  override def receive: Receive = {
    case CrawlPath(from, path:Path) => {
      val response = download(path)
      if (response.success) {
        val parseActor = injectActor[ParseActor]("parse-dispatcher")
        parseActor ! PageContent(from, response)
      }
    }
    case CrawlURL(from, url) => {
      val response = download(Path(url))
      if (response.success) {
        val parseActor = injectActor[ParseActor]("parse-dispatcher")
        parseActor ! PageContent(from, response)
      }
    }
  }

  def download(path: Path)(implicit downloader: Downloader): Response = {
    val watch: Stopwatch = new Stopwatch().start()
    log.info("Downloading => {}", path)
    path match {
      case file: FilePath =>
        readFile(file.uri)
      case http: HttpPath =>
        val response = downloader.download(http)
        if (response.success) {
          log.info("Download [{}] in [{}ms] <= {}", response.status, watch.elapsed(TimeUnit.MILLISECONDS), path)
        }
        response
      case _ =>
        throw new FetchException("Can not resolve downloader")
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
}
