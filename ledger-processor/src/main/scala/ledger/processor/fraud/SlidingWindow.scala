package ledger.processor.fraud

import cats.kernel.CommutativeGroup
import cats.syntax.group._

import java.time.{Duration, Instant}
import scala.annotation.tailrec
import scala.collection.immutable.Queue

/** Running count and total of the payments in a window. */
final case class WindowStats(count: Long, totalCents: Long)

object WindowStats {
  def of(amountCents: Long): WindowStats = WindowStats(1L, amountCents)

  /**
   * WindowStats is a commutative group: combine adds, empty is zero, inverse makes the
   * negative. A monoid would only let me add. The inverse lets the window subtract an old
   * entry straight away instead of adding everything up again.
   */
  implicit val windowStatsGroup: CommutativeGroup[WindowStats] = new CommutativeGroup[WindowStats] {
    val empty: WindowStats                                  = WindowStats(0L, 0L)
    def combine(a: WindowStats, b: WindowStats): WindowStats = WindowStats(a.count + b.count, a.totalCents + b.totalCents)
    def inverse(a: WindowStats): WindowStats                = WindowStats(-a.count, -a.totalCents)
  }
}

/**
 * A sliding time window. Entries sit in a queue in time order. When I add one, I drop
 * anything older than `span` from the front. Each entry goes in once and comes out once,
 * so adding is O(1) on average and the total is always ready.
 *
 * The time comes from the event, not the clock, so a replay gives the same alerts.
 */
final case class SlidingWindow[A: CommutativeGroup](span: Duration, entries: Queue[(Instant, A)], aggregate: A) {

  def add(at: Instant, value: A): SlidingWindow[A] =
    copy(entries = entries.enqueue(at -> value), aggregate = aggregate |+| value).evictUpTo(at.minus(span))

  @tailrec
  private def evictUpTo(cutoff: Instant): SlidingWindow[A] =
    entries.dequeueOption match {
      case Some(((time, value), rest)) if !time.isAfter(cutoff) =>
        copy(entries = rest, aggregate = aggregate |-| value).evictUpTo(cutoff)
      case _ => this
    }

  def size: Int = entries.size
}

object SlidingWindow {
  def empty[A](span: Duration)(implicit G: CommutativeGroup[A]): SlidingWindow[A] =
    SlidingWindow(span, Queue.empty, G.empty)
}
