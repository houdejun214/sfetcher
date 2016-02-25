package com.sfetcher.core.crawldb

import java.io.File
import java.util.UUID

import com.lakeside.core.utils.time.StopWatch

/**
  * Created by dejun on 23/2/16.
  */
object CrawlDBOfMapDBPerformance$Test extends App{

  val watch: StopWatch = StopWatch.newWatch()
  val tempFile: String = File.createTempFile("mapdb-temp", "db").toString
  var db = CrawlDBOfMapDB(tempFile)
  for (i <- 1 to 1000){
    db.appendIfNotExists("http://google.com?"+ UUID.randomUUID().toString)
  }
  println(s"Finished in ${watch.getElapsedTime}ms")
  println(s"Size in ${db.size()}")
  db.close()

  db = CrawlDBOfMapDB(tempFile)
  println(s"Size in ${db.size()}")
  db.close(true)
}
