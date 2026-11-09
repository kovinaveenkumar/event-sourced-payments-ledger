package ledger.processor.fraud

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Duration, Instant}

class SlidingWindowSpec extends AnyWordSpec with Matchers {

  private val t0 = Instant.parse("2026-05-01T09:00:00Z")

  "SlidingWindow" should {
    "keep a running aggregate of entries inside the window" in {
      val w = SlidingWindow.empty[WindowStats](Duration.ofSeconds(60))
        .add(t0, WindowStats.of(100))
        .add(t0.plusSeconds(10), WindowStats.of(200))
        .add(t0.plusSeconds(20), WindowStats.of(300))
      w.aggregate shouldBe WindowStats(3, 600)
      w.size shouldBe 3
    }

    "evict entries that fall out of the window and subtract them from the aggregate" in {
      val w = SlidingWindow.empty[WindowStats](Duration.ofSeconds(60))
        .add(t0, WindowStats.of(100))
        .add(t0.plusSeconds(30), WindowStats.of(200))
        .add(t0.plusSeconds(61), WindowStats.of(50))
      w.aggregate shouldBe WindowStats(2, 250)
    }

    "treat the window as half-open: an entry exactly `span` old is evicted" in {
      val w = SlidingWindow.empty[WindowStats](Duration.ofSeconds(60))
        .add(t0, WindowStats.of(100))
        .add(t0.plusSeconds(60), WindowStats.of(1))
      w.aggregate shouldBe WindowStats(1, 1)
    }

    "match a brute-force recomputation over a long random stream" in {
      val rnd   = new scala.util.Random(42)
      val span  = Duration.ofSeconds(30)
      val times = Iterator.iterate(t0)(_.plusSeconds(rnd.nextInt(10).toLong)).take(2000).toVector
      val items = times.map(t => t -> (rnd.nextInt(1000) + 1).toLong)

      items.indices.foldLeft(SlidingWindow.empty[WindowStats](span)) { (w, i) =>
        val (t, v) = items(i)
        val next   = w.add(t, WindowStats.of(v))
        val inWindow = items.take(i + 1).filter { case (ti, _) => ti.isAfter(t.minus(span)) }
        next.aggregate shouldBe WindowStats(inWindow.size.toLong, inWindow.map(_._2).sum)
        next
      }
    }
  }

  "WindowStats group" should {
    import WindowStats.windowStatsGroup
    "satisfy combine/inverse identities" in {
      val a = WindowStats(3, 700)
      val b = WindowStats(1, 50)
      windowStatsGroup.combine(a, windowStatsGroup.inverse(a)) shouldBe windowStatsGroup.empty
      windowStatsGroup.combine(a, b) shouldBe windowStatsGroup.combine(b, a)
      windowStatsGroup.remove(windowStatsGroup.combine(a, b), b) shouldBe a
    }
  }
}
