package io.sdata.actors

import javax.inject.Inject

import akka.actor.ActorSystem
import com.sfetcher.modules.GuiceAkkaExtension

/**
 * use this dispatcher to dispatch new page task
 * Created by dejun on 15/2/16.
 */
class CrawlActorDispatcher @Inject()(system:ActorSystem) {

  def tell(page: CrawlActor.Crawl)= {
    val crawlActor = system.actorOf(GuiceAkkaExtension(system)
      .props[CrawlActor]
      .withDispatcher("crawl-dispatcher"))
    crawlActor ! page
  }

  def !(page: CrawlActor.Crawl)=tell(page)
}
