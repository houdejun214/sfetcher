package com.sfetcher.core

import com.sfetcher.core.parser.select.DataSelector
import com.sfetcher.core.parser.select.DataSelectorPipleBuilder

import scala.collection._
import scala.reflect.ClassTag

/**
 * Created by dejun on 25/1/16.
 */

object DatumSchema {
  def apply(name: String, entry: EntryRef) = new DatumSchema(name, entry)

}

class DatumSchema(tableName: String, entryRef: EntryRef) {
  private val _fields = mutable.Map[String, Selectable]()
  private val _links = mutable.ListBuffer[Link]()

  /**
   * create a new field description
 *
   * @param name
   * @tparam C
   * @return
   */
  def field[C](name: String) = {
    val f = new Field[C](name)
    _fields += (name -> f)
    f
  }

  /**
   * create list of fields
 *
   * @param list
   * @return
   */
  def fields(list: Seq[Selectable]) = {
    for (s: Selectable <- list) {
      val f = s.asInstanceOf[Field[_]]
      _fields += (f.name -> f)
    }
    this
  }

  def fields = _fields

  def links = _links

  def entry() = {
    entryRef
  }

  /**
   * create a new link description
 *
   * @return
   */
  def link() = {
    val f = new Link
    _links += f
    f
  }

  /**
   * create a new link description
 *
   * @return
   */
  def links(list: Seq[Selectable]) = {
    for (s: Selectable <- list) {
      val l = s.asInstanceOf[Link]
      _links += l
    }
    this
  }

  /**
   * check if contains datum fields definition
 *
   * @return
   */
  def hasFields = {
    !_fields.isEmpty
  }

  /**
   * check if contains links definition
 *
   * @return
   */
  def hasLinks = {
    !_links.isEmpty
  }
}

abstract class Selectable {
  var selector: DataSelector = null

  def select(selector: String) = {
    this.selector = Selector(selector)
    this
  }

  def on(selector: String) = select(selector)
}

class Field[C](_name: String) extends Selectable {

  def name = _name
}

class Link extends Selectable {}

case class ListField(_name: String) extends Field(_name)

case class ObjectField(_name: String) extends Field(_name)


object Selector {

  def build(syntax: String): DataSelector = {
    DataSelectorPipleBuilder.build(syntax);
  }

  def apply(syntax: String): DataSelector = build(syntax)

}
