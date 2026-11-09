package ledger.processor.fraud

import ledger.processor.Fixtures._
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID
import scala.collection.mutable

class AlertRelaySpec extends AnyWordSpec with Matchers with EitherValues {

  private def alert(rule: String) = FraudAlert(rule, sent(UUID.randomUUID(), 2, 100), s"$rule detail")

  /** A fake outbox table in memory, so the relay logic can be tested without a database. */
  private class FakeOutbox(alerts: Seq[FraudAlert]) {
    val unsent: mutable.LinkedHashMap[UUID, FraudAlert] = mutable.LinkedHashMap(alerts.map(a => a.alertId -> a): _*)
    def fetch(limit: Int): Vector[FraudAlert] = unsent.values.take(limit).toVector
    def mark(ids: Seq[UUID]): Unit = ids.foreach(unsent.remove)
  }

  "AlertRelay" should {
    "send every unsent alert in order and mark them sent" in {
      val alerts = Seq(alert("VELOCITY"), alert("VOLUME"), alert("LARGE_TRANSACTION"))
      val outbox = new FakeOutbox(alerts)
      val kafka  = mutable.Buffer.empty[FraudAlert]

      new AlertRelay(outbox.fetch, outbox.mark, a => kafka += a).publishPending() shouldBe 3

      kafka.map(_.rule) shouldBe Seq("VELOCITY", "VOLUME", "LARGE_TRANSACTION")
      outbox.unsent shouldBe empty
    }

    "stop at the first failure, mark only what was sent, and send the rest next time" in {
      val alerts = Seq(alert("A"), alert("B"), alert("C"))
      val outbox = new FakeOutbox(alerts)
      val kafka  = mutable.Buffer.empty[FraudAlert]
      var kafkaDown = true
      val send: FraudAlert => Unit = a => {
        if (a.rule == "B" && kafkaDown) throw new RuntimeException("broker unavailable")
        kafka += a
      }
      val relay = new AlertRelay(outbox.fetch, outbox.mark, send)

      relay.publishPending() shouldBe 1
      outbox.unsent.values.map(_.rule).toSeq shouldBe Seq("B", "C")

      kafkaDown = false
      relay.publishPending() shouldBe 2
      kafka.map(_.rule) shouldBe Seq("A", "B", "C")
      outbox.unsent shouldBe empty
    }

    "do nothing when there is nothing to send" in {
      new AlertRelay(_ => Vector.empty, _ => fail("nothing to mark"), _ => fail("nothing to send")).publishPending() shouldBe 0
    }

    "respect the batch size" in {
      val outbox = new FakeOutbox((1 to 5).map(i => alert(s"R$i")))
      new AlertRelay(outbox.fetch, outbox.mark, _ => (), batchSize = 2).publishPending() shouldBe 2
      outbox.unsent.size shouldBe 3
    }
  }

  "AlertCodec" should {
    "round-trip an alert through JSON" in {
      val a = alert("VELOCITY")
      AlertCodec.decode(AlertCodec.encode(a)).value shouldBe a
    }

    "use the documented field names" in {
      val json = AlertCodec.encode(alert("VOLUME"))
      Seq("alertId", "accountId", "rule", "detail", "eventId", "occurredAt").foreach(f => json should include(s""""$f""""))
    }
  }
}
