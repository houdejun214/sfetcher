package io.sdata.actors.queue

import akka.persistence.PersistentActor
import com.sfetcher.utils.NowProvider

class QueueActor extends PersistentActor with QueueActorStorage with QueueActorReceive with QueueActorRecover {
  override def persistenceId = "queue-actor"

  val nowProvider = new NowProvider()

  def receiveCommand = handleQueueMsg

  def receiveRecover = handleQueueEvent
}

