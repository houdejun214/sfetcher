package com.sfetcher.core

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

abstract class Entry {
  var _datumSchema: DatumSchema = null
  private var _point: Entry = null
  var _name: String = ""

  def name = _name

  def name(_name: String): Entry = {
    this._name = _name
    this
  }

  def datum(): DatumSchema = datum("")

  def datum(name: String): DatumSchema = {
    _datumSchema = DatumSchema(name, this)
    _datumSchema
  }

  def schema = _datumSchema

  def pointTo(target: Entry) = {
    _point = target
    this
  }

  def point = _point

  def ->(target: Entry) = pointTo(target)

  def hashPoint = (_point != null)

  /**
   * check if contains datum fields definition
   * @return
   */
  def isDatumPage: Boolean = {
    (_datumSchema != null && _datumSchema.hasFields)
  }

  /**
   * check if contains links definition
   * @return
   */
  def hasLinks = {
    (_datumSchema != null && _datumSchema.hasLinks)
  }
}

class ConstEntry(private var _entryUrl: String) extends Entry {
  def entryUrl = _entryUrl
}

class Pattern(_pattern: String) extends Entry {


  /**
   * check if match with the pattern
   * @param input
   * @return
   */
  def isMatch(input: String) = {
    input.matches(_pattern)
  }

  def pattern = _pattern

}


object Entry {
  def apply(entry: String) = {
    new ConstEntry(entry)
  }
}

object Pattern {
  def apply(pattern: String) = {
    new Pattern(pattern)
  }
}