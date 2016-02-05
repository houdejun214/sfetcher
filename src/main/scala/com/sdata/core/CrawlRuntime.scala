package com.sdata.core

import java.util.concurrent.atomic.AtomicLong

import org.mapdb._

/**
 * Created by dejun on 3/2/16.
 */
class CrawlRuntime {

  // runtime database
  val runDb = DBMaker.fileDB("./.crawl.run.mdb")
    .make()

  val memDb  = DBMaker
    .memoryDB()
    .make();

  val counters: HTreeMap[String, AtomicLong] = runDb.hashMap("counters")
    .keySerializer(Serializer.STRING)
    .valueSerializer(SerializerAtomicLong)
    .createOrOpen()

  def counterGet(key:String):Long ={
    counters.getOrDefault(key,new AtomicLong(1L)).get()
  }

  def counterInc(key:String, delta:Int) = {
    counters
      .getOrDefault(key,new AtomicLong(1L))
      .addAndGet(delta)
  }

  val SerializerAtomicLong = new Serializer[AtomicLong] {
    override def serialize(out: DataOutput2, value: AtomicLong): Unit = {
      out.writeLong(value.get())
    }
    override def deserialize(input: DataInput2, available: Int): AtomicLong = {
      new AtomicLong(input.readLong())
    }
  }
}



