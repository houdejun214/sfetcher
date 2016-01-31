package com.sdata.fetcher

import com.sdata.core.parser.select.DataSelector

import scala.collection._;

/**
 * Created by dejun on 25/1/16.
 */
abstract class Datum(tableName:String) {

  private val fields = mutable.Map[String,Field[_]]()

  def field[C](name:String)= {
    val f = new Field[C](name)
    fields += (name->f)
    f
  }

}

class Field[C](name:String) {

  var selector:DataSelector=null;

  def select(selector:String) = {
    this.selector = Selector(selector)
  }

  def on(selector:String) = select(selector);

}

case class ListField(val name:String) extends Field(name);

case class ObjectField(val name:String) extends Field(name);


