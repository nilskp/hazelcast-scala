package com.hazelcast.Scala.actor

import scala.beans.BeanProperty
import scala.concurrent.Future

import java.util.Map.Entry

import com.hazelcast.Scala._
import com.hazelcast.core.{ HazelcastInstance, HazelcastInstanceAware }
import com.hazelcast.map.IMap
import com.hazelcast.instance.impl.{ HazelcastInstanceImpl, HazelcastInstanceProxy }
import scala.concurrent.ExecutionContext

class Stage(private val actorMap: IMap[String, Array[Byte]]) {

  def this(name: String, hz: HazelcastInstance) = this(hz.getMap(name))

  def actorOf[A <: AnyRef](name: String, create: => A): HzActorRef[A] =
    new HzActorImpl(name, actorMap, create)

}

private class HzActorImpl[A <: AnyRef](
  val name: String,
  imap: IMap[String, Array[Byte]],
  create: => A)
extends HzActorRef[A] {

  private implicit def ec = ExecutionContext.parasitic

  def apply[R](thunk: (HazelcastInstance, A) => R): Future[R] = {
    val ep =
      new SingleEntryCallbackUpdater[String, Array[Byte], R]
      with HazelcastInstanceAware {
        @BeanProperty @transient
        var hazelcastInstance: HazelcastInstance = _
        var newState: Array[Byte] = _
        def onEntry(entry: Entry[String, Array[Byte]]): R = {
          val serializationService = hazelcastInstance match {
            case hz: HazelcastInstanceImpl => hz.getSerializationService
            case hz: HazelcastInstanceProxy => hz.getSerializationService
          }
          val actor = entry.value match {
            case null => create
            case bytes =>
              val inp = serializationService.createObjectDataInput(bytes)
              serializationService.readObject(inp)
          }
          val result = thunk(hazelcastInstance, actor)
          newState = {
            val out = serializationService.createObjectDataOutput()
            serializationService.writeObject(out, actor)
            out.toByteArray()
          }
          entry.value = newState
          result
        }
        override def processBackup(entry: Entry[String, Array[Byte]]): Unit = {
          entry.value = newState
        }
      }
    imap
      .submitToKey(name, ep)
      .asScala
  }
}
