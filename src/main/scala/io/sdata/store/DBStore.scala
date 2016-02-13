package io.sdata.store

import io.sdata.core.DatumSchema

import scala.collection.mutable

/**
 * Created by dejun on 9/2/16.
 */
trait DBStore {
  def store(schema:DatumSchema, datum:mutable.Map[String, _])
}
