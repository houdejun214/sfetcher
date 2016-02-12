package io.sdata.actors

import java.util

import com.google.inject.{Inject, Injector}
import io.sdata.actors.CrawlActor.CrawlPage
import io.sdata.actors.DatumEmitorActor.EmitDatum
import io.sdata.actors.ParseActor.PageContent
import io.sdata.core.route.RouteResult
import io.sdata.core.{CrawlContext, Entry}
import io.sdata.http.Downloader.Response
import io.sdata.modules.ActorInject
import org.jsoup.Jsoup

import scala.collection.{JavaConversions, mutable}

/**
 * Created by dejun on 3/2/16.
 */

object ParseActor {

  case class PageContent(from: Entry, res: Response)

}

class ParseActor @Inject()(inject: Injector,
                           crawlContext: CrawlContext) extends BaseActor(inject) with ActorInject {


  override def receive: Receive = {
    case PageContent(from, response) => {
      if (from.hashPoint){
        parseWithPattern(response, from.point)
      }
      else {
        parseWithPattern(response, from)
      }
    }
  }

  def parseWithPattern(response: Response, from: Entry): Unit = {
      if (from.isDatumPage) {
        val emitorActor = injectActor[DatumEmitorActor]
        val datum = resolveDatum(from, response)
        emitorActor ! EmitDatum(datum)
      } else {
        val links = resolveLinks(from, response)
        val crawlActor = injectActor[CrawlActor]
        links foreach {
          case l => {
            val routeResult: RouteResult[Entry] = crawlContext.router.route(l)
             Option(routeResult) match {
              case Some(i) =>
                val target: Entry = routeResult.target()
                crawlActor ! CrawlPage(target, l)
              case None => //do nothing
             }

          }
        }
      }
  }

  def resolveDatum(from: Entry, res: Response):mutable.Map[String, AnyRef] = {
    val doc = Jsoup.parse(res.content)
    val datum: mutable.Map[String, AnyRef] = mutable.Map[String, AnyRef]()
    from.schema.fields foreach {
      case (name, f) => {
        val data = f.selector.select(doc)
        datum += (name -> data)
      }
    }
    datum
  }


  def resolveLinks(from: Entry, res: Response):mutable.ListBuffer[String] = {
    val doc = Jsoup.parse(res.content, res.url)
    val links = mutable.ListBuffer[String]()
    from.schema.links foreach {
      case f =>
        val data = f.selector.select(doc)

        if(data.isInstanceOf[util.List[_]]){
          val list:util.List[_] = data.asInstanceOf[util.List[String]]
          for(l <- JavaConversions.asScalaBuffer(list)) {
            links+=l.toString
          }
        }else{
          links+=data.toString
        }
    }
    links
  }
}
