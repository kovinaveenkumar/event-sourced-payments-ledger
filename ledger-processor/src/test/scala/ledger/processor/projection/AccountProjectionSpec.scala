package ledger.processor.projection

import ledger.processor.Fixtures._
import ledger.processor.projection.ProjectionResult._
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID

class AccountProjectionSpec extends AnyWordSpec with Matchers with OptionValues {

  private val acc = UUID.randomUUID()

  "AccountProjection" should {
    "build the balance by replaying the full history" in {
      val events = Seq(
        opened(acc),
        deposited(acc, 2, 10000),
        withdrawn(acc, 3, 2500),
        sent(acc, 4, 1000),
        received(acc, 5, 300, UUID.randomUUID())
      )
      val view = AccountProjection.replay(events).value
      view.balanceCents shouldBe 6800
      view.version shouldBe 5
      view.transactionCount shouldBe 4
    }

    "produce a transaction row with the running balance" in {
      val start = AccountProjection.replay(Seq(opened(acc), deposited(acc, 2, 1000)))
      val Applied(_, row) = AccountProjection.project(start, withdrawn(acc, 3, 400))
      row.value.direction shouldBe "OUT"
      row.value.balanceAfterCents shouldBe 600
    }

    "ignore an event it has already applied (at-least-once redelivery)" in {
      val first  = deposited(acc, 2, 1000)
      val state  = AccountProjection.replay(Seq(opened(acc), first))
      AccountProjection.project(state, first) shouldBe Duplicate(2)
      AccountProjection.project(state, opened(acc)) shouldBe Duplicate(2)
    }

    "detect a gap when an event arrives before the one it follows" in {
      val state = AccountProjection.replay(Seq(opened(acc)))
      AccountProjection.project(state, deposited(acc, 3, 1000)) shouldBe Gap(2, 3)
      AccountProjection.project(None, deposited(acc, 2, 1000)) shouldBe Gap(1, 2)
    }

    "be deterministic: replaying the same events twice gives the same view" in {
      val events = Seq(opened(acc), deposited(acc, 2, 500), sent(acc, 3, 200))
      AccountProjection.replay(events) shouldBe AccountProjection.replay(events)
      AccountProjection.replay(events ++ events) shouldBe AccountProjection.replay(events)
    }
  }
}
