package com.sdata

import com.sdata.fetcher.{Fetcher, Datum}

/**
 * Hello world!
 *
 */


class Amazon extends Datum("product") {

  def name = field[String]("name") select("#productTitle|txt")

  def url = field[String]("url") select("omit()|url")

  def price = field[String]("price") select("#priceblock_ourprice|txt")

  def image = field[String]("image") select("#imgTagWrapperId img|link")
}

object AmazonApp extends App{
  val fetcher = Fetcher("Amazone")
  fetcher links {

  }

  fetcher filter {

  }

  fetcher.start
}
