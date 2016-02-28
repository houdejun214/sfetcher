package com.sfetcher.core

import scala.collection.Seq

/**
  * Created by dejun on 8/2/16.
 */
object CrawlDSL {

  type Description = () => Unit

  def field[C](name: String) = new Field[C](name)

  def link() = new Link

  def link(selector: String) = {
    val l = new Link
    l.select(selector)
    l
  }
}

abstract class EntryRef {
  private var _datumSchema: DatumSchema = null
  private var _point: EntryRef = null

  /**
    * generate empty DatumSchema with a name
 *
    * @return
    */
  def datum(name: String, fields: Seq[Selectable]): DatumSchema = {
    _datumSchema = DatumSchema(name, this)
    _datumSchema.fields(fields)
    _datumSchema
  }

  /**
    * generate links schema
 *
    * @param list
    * @return
    */
  def links(list: Seq[Selectable]) = {
    _datumSchema = DatumSchema("", this)
    _datumSchema.links(list)
  }

  def schema = _datumSchema

  def pointTo(target: EntryRef) = {
    _point = target
    this
  }

  def point = _point

  def ->(target: EntryRef) = pointTo(target)

  def hashPoint = _point != null

  /**
   * check if contains datum fields definition
    *
    * @return
   */
  def isDatumPage: Boolean = {
    _datumSchema != null && _datumSchema.hasFields
  }

  /**
   * check if contains links definition
    *
    * @return
   */
  def hasLinks = {
    _datumSchema != null && _datumSchema.hasLinks
  }
}

/**
  * define a const entry, it always be regard as the entry task of the pipeline.
  * @param path
  */
class ConstEntry(val path: Path) extends EntryRef with Parameterize {

  override def withParam(param: (String, String)): ConstEntry = {
    if (path.isInstanceOf[Parameterize]) {
      path.asInstanceOf[Parameterize].withParam(param)
    }
    this
  }

  override def withParams(params: scala.Seq[(String, String)]): ConstEntry = {
    if (path.isInstanceOf[Parameterize]) {
      path.asInstanceOf[Parameterize].withParams(params)
    }
    this
  }
}

class Pattern(_pattern: String) extends EntryRef {

  /**
   * check if match with the pattern
    *
    * @param input
   * @return
   */
  def isMatch(input: String) = {
    input.matches(_pattern)
  }
  def pattern = _pattern
}


object Entry {
  def apply(constInput: String) = {
    val path = Path(constInput)
    new ConstEntry(path)
  }
}

object Pattern {
  def apply(pattern: String) = {
    new Pattern(pattern)
  }
}