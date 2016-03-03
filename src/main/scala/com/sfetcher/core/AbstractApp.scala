package com.sfetcher.core

/**
 * Created by dejun on 3/2/16.
 */
trait AbstractApp extends App {


  /**
    * start the sfetcher App.
    */
  def start(refs:EntryRef*): Unit ={
    Pipeline(refs).start()
  }
}
