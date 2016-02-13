package io.sdata.store

import java.io.{FileWriter, BufferedWriter}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import io.sdata.core.DatumSchema

import scala.collection.mutable

/**
 * Created by dejun on 14/2/16.
 */


object FileJsonStore extends DBStore {

  private val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  private val writer = new BufferedWriter(new FileWriter("/Users/dejun/working/temp/taobao.json"))

  override def store(schema: DatumSchema, datum: mutable.Map[String, _]): Unit = {

    val row: String = mapper.writeValueAsString(datum)
    println(row)
    writer.write(s"$row\n");
    writer.flush()
  }
}