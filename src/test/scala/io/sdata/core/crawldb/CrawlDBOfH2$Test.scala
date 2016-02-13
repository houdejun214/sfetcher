package io.sdata.core.crawldb

import java.util.UUID

import org.specs2.mutable.Specification

/**
 * Created by dejun on 13/2/16.
 */
class CrawlDBOfH2$Test extends Specification {

  "This is a specification for the 'CrawlDBOfH2' ".txt

  "CrawlDBOfH2 should appendIfNotExists" >> {
    CrawlDBOfH2.appendIfNotExists("http://google.com?"+UUID.randomUUID().toString) must beSome(true)
  }

  "CrawlDBOfH2 should exists" >> {
    CrawlDBOfH2.appendIfNotExists("http://google.com")
    CrawlDBOfH2.exists("http://google.com") must be_==(true)
  }

}
