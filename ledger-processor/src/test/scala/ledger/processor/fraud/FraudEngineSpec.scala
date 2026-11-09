package ledger.processor.fraud

import ledger.processor.Fixtures._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.util.UUID

class FraudEngineSpec extends AnyWordSpec with Matchers {

  private val config = FraudConfig(
    velocityMaxCount = 3,
    velocityWindow = Duration.ofSeconds(60),
    volumeMaxCents = 100000,          // 1,000.00
    volumeWindow = Duration.ofMinutes(10),
    largeTransactionCents = 500000    // 5,000.00
  )

  private def rules(alerts: List[FraudAlert]) = alerts.map(_.rule)

  "FraudEngine" should {
    "ignore incoming money" in {
      val acc = UUID.randomUUID()
      val (_, alerts) = FraudEngine(config).observe(deposited(acc, 2, 9999999))
      alerts shouldBe empty
    }

    "flag a single large outgoing payment" in {
      val acc = UUID.randomUUID()
      val (_, alerts) = FraudEngine(config).observe(sent(acc, 2, 500000))
      rules(alerts) should contain("LARGE_TRANSACTION")
    }

    "flag velocity once, on the payment that crosses the limit" in {
      val acc = UUID.randomUUID()
      val payments = (1 to 6).map(i => withdrawn(acc, i.toLong + 1, 100, t0.plusSeconds(i.toLong * 5)))
      val (_, alertsPerEvent) = payments.foldLeft((FraudEngine(config), Vector.empty[List[FraudAlert]])) {
        case ((engine, acc), e) =>
          val (next, alerts) = engine.observe(e)
          (next, acc :+ alerts)
      }
      alertsPerEvent.map(rules) shouldBe Vector(Nil, Nil, Nil, List("VELOCITY"), Nil, Nil)
    }

    "not flag velocity when payments are spread beyond the window" in {
      val acc = UUID.randomUUID()
      val payments = (1 to 6).map(i => withdrawn(acc, i.toLong + 1, 100, t0.plusSeconds(i.toLong * 30)))
      val all = payments.foldLeft((FraudEngine(config), List.empty[FraudAlert])) { case ((e, out), ev) =>
        val (n, a) = e.observe(ev); (n, out ++ a)
      }._2
      rules(all) should not contain "VELOCITY"
    }

    "flag volume when the windowed total crosses the limit" in {
      val acc = UUID.randomUUID()
      val e1 = sent(acc, 2, 60000, t0)
      val e2 = sent(acc, 3, 50000, t0.plusSeconds(120))
      val (s1, a1) = FraudEngine(config).observe(e1)
      val (_, a2)  = s1.observe(e2)
      a1 shouldBe empty
      rules(a2) shouldBe List("VOLUME")
      a2.head.eventId shouldBe e2.eventId
    }

    "keep windows separate per account" in {
      val a = UUID.randomUUID()
      val b = UUID.randomUUID()
      val (s1, _) = FraudEngine(config).observe(sent(a, 2, 90000))
      val (_, alerts) = s1.observe(sent(b, 2, 90000))
      alerts shouldBe empty
    }

    "use deterministic alert ids so reprocessing cannot duplicate an alert" in {
      val acc = UUID.randomUUID()
      val e   = sent(acc, 2, 600000)
      FraudEngine(config).observe(e)._2.map(_.alertId) shouldBe FraudEngine(config).observe(e)._2.map(_.alertId)
    }

    "warm up from history without emitting alerts, then continue the windows" in {
      val acc  = UUID.randomUUID()
      val warm = FraudEngine(config).warmUp((1 to 3).map(i => withdrawn(acc, i.toLong + 1, 100, t0.plusSeconds(i.toLong))))
      val (_, alerts) = warm.observe(withdrawn(acc, 5, 100, t0.plusSeconds(10)))
      rules(alerts) shouldBe List("VELOCITY")
    }
  }
}
