/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import scala.annotation.tailrec
import scala.util.control.NonFatal
import akka.actor.Actor
import akka.actor.Props
import akka.event.Logging
import akka.stream.MaterializerSettings

import org.reactivestreams.Subscriber

/**
 * INTERNAL API
 */
private[akka] object IteratorPublisher {
  def props(iterator: Iterator[Any], settings: MaterializerSettings): Props =
    Props(new IteratorPublisher(iterator, settings)).withDispatcher(settings.dispatcher)

  private case object PushMore

  private sealed trait State
  private sealed trait StopState extends State
  private case object Unitialized extends State
  private case object Initialized extends State
  private case object Cancelled extends StopState
  private case object Completed extends StopState
  private case class Errored(cause: Throwable) extends StopState
}

/**
 * INTERNAL API
 * Elements are produced from the iterator.
 */
private[akka] class IteratorPublisher(iterator: Iterator[Any], settings: MaterializerSettings) extends Actor {
  import IteratorPublisher._
  import ReactiveStreamsCompliance._

  private var exposedPublisher: ActorPublisher[Any] = _
  private var subscriber: Subscriber[Any] = _
  private var downstreamDemand: Long = 0L
  private var state: State = Unitialized
  private val maxPush = settings.maxInputBufferSize // FIXME why is this a good number?

  def receive = {
    case ExposedPublisher(publisher) ⇒
      exposedPublisher = publisher
      context.become(waitingForFirstSubscriber)
    case _ ⇒
      throw new IllegalStateException("The first message must be ExposedPublisher")
  }

  def waitingForFirstSubscriber: Receive = {
    case SubscribePending ⇒
      exposedPublisher.takePendingSubscribers() foreach registerSubscriber
      state = Initialized
      // hasNext might throw
      (try {
        if (iterator.hasNext) state // No change
        else Completed
      } catch {
        case NonFatal(e) ⇒ Errored(e)
      }) match {
        case stopState: StopState ⇒ stop(stopState)
        case _                    ⇒ context become active
      }
  }

  def active: Receive = {
    case RequestMore(_, elements) ⇒
      if (elements < 1) stop(Errored(numberOfElementsInRequestMustBePositiveException))
      else {
        downstreamDemand += elements
        // Long has overflown, reactive-streams specification rule 3.17
        if (downstreamDemand < 0) stop(Errored(totalPendingDemandMustNotExceedLongMaxValueException))
        else push()
      }
    case PushMore ⇒
      push()
    case _: Cancel ⇒
      stop(Cancelled)
    case SubscribePending ⇒
      exposedPublisher.takePendingSubscribers() foreach registerSubscriber
  }

  // note that iterator.hasNext is always true when calling push, completing as soon as hasNext is false
  private def push(): Unit = {
    // Returns whether it has more elements in the iterator or not
    @tailrec def doPush(n: Int, hasNext: Boolean): Boolean =
      if (hasNext && n > 0 && downstreamDemand > 0) {
        downstreamDemand -= 1
        tryOnNext(subscriber, iterator.next())
        doPush(n - 1, iterator.hasNext)
      } else hasNext

    val hasMoreElements =
      try doPush(n = maxPush, hasNext = true) catch {
        case sv: SpecViolation ⇒ throw sv.violation // FIXME is escalation the right course of action here?
        case NonFatal(e) ⇒
          stop(Errored(e))
          true // we have to assume that there were more elements, to avoid double-onError below
      }
    if (!hasMoreElements)
      stop(Completed)
    else if (downstreamDemand > 0)
      self ! PushMore
  }

  private def registerSubscriber(sub: Subscriber[Any]): Unit =
    if (subscriber ne null) rejectAdditionalSubscriber(sub)
    else {
      subscriber = sub
      tryOnSubscribe(sub, new ActorSubscription(self, sub))
    }

  private def stop(reason: StopState): Unit =
    state match {
      case _: StopState ⇒ throw new IllegalStateException(s"Already stopped. Transition attempted from $state to $reason")
      case _ ⇒
        state = reason
        context.stop(self)
    }

  override def postStop(): Unit = {
    state match {
      case Unitialized | Initialized | Cancelled ⇒
        if (exposedPublisher ne null) exposedPublisher.shutdown(ActorPublisher.NormalShutdownReason)
      case Completed ⇒
        try tryOnComplete(subscriber) finally exposedPublisher.shutdown(ActorPublisher.NormalShutdownReason)
      case Errored(e) ⇒
        try tryOnError(subscriber, e) finally exposedPublisher.shutdown(Some(e))
    }
    // if onComplete or onError throws we let normal supervision take care of it,
    // see reactive-streams specification rule 2:13
  }

}

