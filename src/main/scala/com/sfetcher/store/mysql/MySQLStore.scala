package com.sfetcher.store.mysql

import com.sfetcher.core.DatumSchema
import com.sfetcher.store.DBStore

import scala.collection.mutable

/**
 * Created by dejun on 9/2/16.
 */

object MySQLStore {

  def apply(jdbc: String) = {
    new MySQLStore(jdbc)
  }
}

class MySQLStore(jdbc: String) extends DBStore {

  private var _username = ""
  private var _password = ""

  def username(_username: String) = {
    this._username = _username
    this
  }

  def password(_password: String) = {
    this._password = _password
    this
  }

  override def store(schema: DatumSchema, datum: mutable.Map[String, _]): Unit = ???
}
