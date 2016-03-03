package com.sfetcher.core

import com.sfetcher.http.{Downloader, HttpDownloader}
import com.sfetcher.store.FileCsvStore

/**
 * Created by dejun on 3/2/16.
 */
trait AbstractApp extends App {

  implicit val defaultStore = FileCsvStore
  implicit val defaultDownloader = HttpDownloader


  /**
    * start the sfetcher App.
    */
  def start(refs:EntryRef*)(implicit downloader:Downloader): Unit ={
    Pipeline(refs).start(downloader)
  }
}
