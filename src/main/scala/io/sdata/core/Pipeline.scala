package io.sdata.core

import akka.actor.ActorSystem
import com.google.inject.Guice
import io.sdata.actors.CrawlActor.EntryPage
import io.sdata.actors.{CrawlActor, CrawlModule}
import io.sdata.modules.{AkkaModule, ConfigModule, GuiceAkkaExtension}
import net.codingwell.scalaguice.InjectorExtensions._

import scala.collection.mutable

/**
 * Created by dejun on 9/2/16.
 */

object Pipeline {
  def apply(entries: Entry*) = {
    val pipeline: Pipeline = new Pipeline()
    pipeline.append(entries)
    pipeline
  }
}

class Pipeline {

  private var _entries = mutable.ListBuffer[Entry]()

  def append(entries: Seq[Entry]): Unit = {
    for (e: Entry <- entries) {
      _entries += e
    }
  }

  def start() = {
    val injector = Guice.createInjector(
      new AkkaModule,
      new ConfigModule,
      new CrawlModule
    )
    val system = injector.instance[ActorSystem]
//    val log = Logging(system, Pipeline)
    val crawlActor = system.actorOf(GuiceAkkaExtension(system).props[CrawlActor])
    val router: route.Router[Entry] = new route.Router[Entry]()
    CrawlContext().router = router
    var entry: ConstEntry = null
    for (e <- _entries) {
      if (e.isInstanceOf[ConstEntry]) {
        entry = e.asInstanceOf[ConstEntry]
      } else if (e.isInstanceOf[Pattern]) {
        val p = e.asInstanceOf[Pattern]
        router.addRoute(p.pattern, p)
      }
    }
    println("###########################################")
    println("#")
    println("#  Start the crawling task.")
    println("#")
    println("###########################################")
    crawlActor ! EntryPage(entry, entry.entryUrl)
  }
}
