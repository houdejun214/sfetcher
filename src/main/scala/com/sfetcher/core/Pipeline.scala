package com.sfetcher.core

import _root_.io.sdata.actors.CrawlActor.CrawlPath
import com.google.inject.Guice
import com.sfetcher.modules.{AkkaModule, ConfigModule}
import io.sdata.actors.{CrawlActorDispatcher, CrawlModule}
import net.codingwell.scalaguice.InjectorExtensions._

import scala.collection.mutable

/**
 * Created by dejun on 9/2/16.
 */

object Pipeline {
  def apply(entries: EntryRef*) = {
    val pipeline: Pipeline = new Pipeline()
    pipeline.append(entries)
    pipeline
  }
}

class Pipeline {

  private var _entries = mutable.ListBuffer[EntryRef]()

  def append(entries: Seq[EntryRef]): Unit = {
    for (e: EntryRef <- entries) {
      _entries += e
    }
  }

  def start() = {
    val injector = Guice.createInjector(
      new AkkaModule,
      new ConfigModule,
      new CrawlModule
    )
    val dispatcher = injector.instance[CrawlActorDispatcher]
    //    val log = Logging(system, Pipeline)

    //val crawlActor = system.actorOf(GuiceAkkaExtension(system).props[CrawlActor])
    val router: route.Router[EntryRef] = new route.Router[EntryRef]()
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
    dispatcher ! CrawlPath(entry, entry.path)
  }
}
