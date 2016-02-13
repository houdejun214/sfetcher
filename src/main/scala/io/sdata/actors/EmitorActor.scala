package io.sdata.actors

import akka.actor.Actor
import com.google.inject.{Injector, Inject}
import io.sdata.actors.EmitorActor.EmitDatum
import io.sdata.core.{DatumSchema, CrawlContext}
import io.sdata.modules.ActorInject
import io.sdata.store.DBStore
import scala.collection.mutable

/**
 * Created by dejun on 10/2/16.
 */
object EmitorActor{

  case class EmitDatum(schema:DatumSchema, datum:mutable.Map[String, AnyRef])

}
class EmitorActor @Inject()(inject: Injector,
                                 crawlContext:CrawlContext
                                  ) extends Actor with ActorInject{
  def injector: Injector = inject

  override def receive: Receive = {
    case EmitDatum(schema, datum)=>

  }

  def emitDatum(schema:DatumSchema, datum:mutable.Map[String, AnyRef])(implicit dBStore: DBStore): Unit ={
    dBStore.store(schema,datum);
  }
}
