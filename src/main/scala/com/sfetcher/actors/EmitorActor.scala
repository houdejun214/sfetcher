package io.sdata.actors

import akka.actor.Actor
import com.google.inject.{Injector, Inject}
import io.sdata.actors.EmitorActor.EmitDatum
import com.sfetcher.core.{DatumSchema, CrawlContext}
import com.sfetcher.modules.ActorInject
import com.sfetcher.store.DBStore
import scala.collection.mutable

/**
 * Created by dejun on 10/2/16.
 */
object EmitorActor {

  case class EmitDatum(schema: DatumSchema, datum: mutable.Map[String, AnyRef])

}

class EmitorActor @Inject()(inject: Injector,
                            crawlContext: CrawlContext
                             ) extends Actor with ActorInject {
  import com.sfetcher.core.CrawlContext.Implicits.store

  def injector: Injector = inject

  override def receive: Receive = {
    case EmitDatum(schema, datum) =>
      emitDatum(schema, datum)
  }

  def emitDatum(schema: DatumSchema, datum: mutable.Map[String, AnyRef])(implicit dBStore: DBStore): Unit = {
    dBStore.store(schema, datum);
  }
}
