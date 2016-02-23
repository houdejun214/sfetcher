package io.sdata.core.crawldb

import org.specs2.mutable.Specification

/**
  * Created by dejun on 20/2/16.
  */
class CrawlDBOfMapDB$Test extends Specification {

  "CrawlDBOfMapDB$Test" should {
    "instance" in {
      val db = CrawlDBOfMapDB()
      db.appendIfNotExists("http://google.com")
      db.exists("http://google.com") must be_==(true)
    }

    "apply" in {
      ok
    }

    "Size" in {
      ok
    }
  }
}
