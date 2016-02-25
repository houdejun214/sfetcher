package com.sfetcher.store

import java.io.FileWriter

import com.sfetcher.core.DatumSchema
import org.supercsv.cellprocessor.Optional
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.io.CsvMapWriter
import org.supercsv.prefs.CsvPreference
import org.supercsv.quote.AlwaysQuoteMode

import scala.collection.{JavaConversions, mutable}

/**
 * Created by dejun on 14/2/16.
 */


object FileCsvStore extends DBStore {

  private var writer:FileCsvStore = null

  override def store(schema: DatumSchema, datum: mutable.Map[String, _]): Unit = {
    if(writer ==null){
      FileCsvStore.synchronized {
        if(writer ==null) {
          writer = new FileCsvStore("/Users/dejun/working/temp/taobao.csv", schema)
        }
      }
    }
    writer.store(schema, datum)
  }
}


class FileCsvStore(path:String, schema: DatumSchema) extends DBStore {

  private val writer = new FileWriter(path)

  val reference=new CsvPreference.Builder('\"', 44, "\r\n")
    .useQuoteMode(new AlwaysQuoteMode())
    .build()

  private val csv = new CsvMapWriter(writer, reference)
  val _names = mutable.ArrayBuffer[String]()
  val _cells = mutable.ArrayBuffer[CellProcessor]()
  schema.fields foreach {
    case (name,f)=>
      _names += name
      _cells += new Optional()
  }
  val namesArray = _names.toArray
  val cellsArray = _cells.toArray
  csv.writeHeader(namesArray:_*)

  override def store(schema: DatumSchema, datum: mutable.Map[String, _]): Unit = {
    csv.write(JavaConversions.mapAsJavaMap(datum), namesArray, cellsArray)
    csv.flush()
  }

  def close() = {
    csv.close()
  }
}

