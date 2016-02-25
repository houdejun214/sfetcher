package com.sfetcher.core.crawldb

import java.util.UUID

import com.lakeside.core.utils.time.StopWatch

/**
  * Created by dejun on 23/2/16.
  */
object CrawlDBOfH2Performance$Test extends App{

  val watch: StopWatch = StopWatch.newWatch()
  for (i <- 1 to 1000){
    CrawlDBOfH2.appendIfNotExists("http://google.com?"+ UUID.randomUUID().toString)
  }
  println(s"Finished in ${watch.getElapsedTime}ms")
  println(s"Size in ${CrawlDBOfH2.size()}")
}
