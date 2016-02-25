package io.sdata.actors

import com.google.inject.{AbstractModule, Provider}
import com.sfetcher.core.CrawlContext
import net.codingwell.scalaguice.ScalaModule


/**
 * Created by dejun on 7/2/16.
 */

object CrawlModule {

  class CrawlContextProvider extends Provider[CrawlContext] {
    override def get() = CrawlContext()
  }

}

class CrawlModule extends AbstractModule with ScalaModule {
  override def configure() {
    bind[CrawlContext].toInstance(CrawlContext())
  }
}



