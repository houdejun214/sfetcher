package io.sdata.actors

import com.google.inject.AbstractModule
import net.codingwell.scalaguice.ScalaModule


/**
 * Created by dejun on 7/2/16.
 */
class CrawlModule extends AbstractModule with ScalaModule {
  override def configure() {
//    bind[Actor].annotatedWith(Names.named(CrawlActor.name)).to[CrawlActor]
//    bind[Actor].annotatedWith(Names.named(ParseActor.name)).to[ParseActor]
//    bind[Actor].annotatedWith(Names.named(PageActor.name)).to[PageActor]
  }
}
