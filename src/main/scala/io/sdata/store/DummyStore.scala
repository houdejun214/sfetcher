package io.sdata.store

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import io.sdata.core.DatumSchema

import scala.collection.mutable
;

/**
 * Created by dejun on 9/2/16.
 */
object DummyStore extends DBStore{

  private val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  override def store(schema: DatumSchema, datum: mutable.Map[String, _]): Unit = {

    println(mapper.writeValueAsString(datum))
  }
}
