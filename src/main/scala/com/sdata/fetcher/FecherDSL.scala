package com.sdata.fetcher

import akka.actor.ActorSystem
import com.sdata.core.parser.select.{PageContext, DataSelector, DataSelectorPipleBuilder}

/**
 * Created by dejun on 25/1/16.
 */

case class Task(implicit context: PageContext);

case class CatalogLinkTask(url:String) extends Task;

case class PageDetailTask(url:String) extends Task;

object Selector {

  def build(syntax:String): DataSelector = {
    DataSelectorPipleBuilder.build(syntax);
  }

  def apply(syntax:String): DataSelector = build(syntax)

}

class Fetcher {
  def start() = {
    val system = ActorSystem("Sdatacrawler")

  }

  def filter(unit: Unit) = {

  }

  def links(unit: Unit) = {

  }

  private var entry: String = null
}

object Fetcher {
  def apply(name:String) = new Fetcher()
}