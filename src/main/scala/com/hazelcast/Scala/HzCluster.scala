package com.hazelcast.Scala

import com.hazelcast.cluster._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise

class HzCluster(private val cluster: Cluster) extends AnyVal {

  def onMemberChange(
      runOn: ExecutionContext = null)(
      listener: PartialFunction[MemberEvent, Unit])
      : (ListenerRegistration, Future[InitialMembershipEvent]) = {
    val (future, mbrListener) = EventSubscription.asMembershipListener(listener, Option(runOn))
    val regId = cluster addMembershipListener mbrListener
    new ListenerRegistration {
      def cancel(): Boolean = cluster removeMembershipListener regId
    } -> future
  }

  /**
    * Execute code `thunk` if/when this member
    * is cluster leader.
    * @note This may never happen, thus the `Future` might never
    * yield a result (on a given member).
    *
    * @param thunk The code thunk to execute
    * @param ec The execution context to execute on
    * @return The future result, if executed
    */
  def executeWhenLeader[R](
      thunk: => Future[R])(
      implicit
      ec: ExecutionContext)
      : Future[R] = {

    val promise = Promise[R]()

      def executeThunkWhenLeader(cluster: Cluster) = {
        if (cluster.getLocalMember == cluster.getMembers.iterator.next) {
          promise completeWith Future(thunk).flatten
        }
      }

    this.cluster.onMemberChange(ec) {
      case mbrEvt: MemberRemoved =>
        executeThunkWhenLeader(mbrEvt.cluster)
    } match {
      case (registration, initialEvent) =>
        promise.future.onComplete { _ =>
          registration.cancel()
        }
        initialEvent
          .map(_.getCluster)
          .foreach(executeThunkWhenLeader)
    }

    promise.future

  }

}
