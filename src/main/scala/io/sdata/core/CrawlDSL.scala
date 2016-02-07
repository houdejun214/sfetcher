package io.sdata.core

/**
 * Created by dejun on 8/2/16.
 */
object CrawlDSL {

  type Description = () => Unit
  def field[C](name:String)= new Field[C](name)
  def link()= new Link
//  type Selectable= Selectable
}

abstract class Entry {
  var _datumSchema:DatumSchema=null
  var _name:String=""
  def name = _name
  def name(_name:String): Entry ={
    this._name = _name
    this
  }

  def datum():DatumSchema = datum("")

  def datum(name:String):DatumSchema = {
    _datumSchema = DatumSchema(name, this)
    _datumSchema
  }
}

class ConstEntry(private var _entryUrl:String) extends Entry {
  private var target:Entry = null;
  def ->(target:Entry) = {
    this.target = target
  }

  def entryUrl = _entryUrl
}

class Pattern(_pattern:String) extends Entry{


  /**
   * check if match with the pattern
   * @param input
   * @return
   */
  def isMatch(input:String)={
    input.matches(_pattern)
  }

  def pattern = _pattern
}


object Entry {
  def apply(entry:String) = {
    new ConstEntry(entry)
  }
}

object Pattern {
  def apply(pattern:String) = {
    new Pattern(pattern)
  }
}