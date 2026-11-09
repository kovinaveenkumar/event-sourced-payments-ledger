package ledger.processor.codec

import ledger.processor.domain.LedgerEvent._
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID

class EventCodecSpec extends AnyWordSpec with Matchers with EitherValues {

  private val acc = UUID.randomUUID()
  private val id  = UUID.randomUUID()

  // The same JSON the Java command service sends (see EventCodec.java and docs/EVENTS.md).
  private def json(eventType: String, version: Long, extra: String) =
    s"""{"eventId":"$id","eventType":"$eventType","accountId":"$acc","version":$version,
       |"occurredAt":"2026-05-01T09:00:00.123Z"$extra}""".stripMargin

  "EventCodec.decode" should {
    "decode every event type produced by the command service" in {
      val other = UUID.randomUUID()
      val tid   = UUID.randomUUID()
      EventCodec.decode(json("AccountOpened", 1, ""","owner":"Alice","currency":"USD"""")).value shouldBe an[AccountOpened]
      EventCodec.decode(json("FundsDeposited", 2, ""","amountCents":500,"reference":"pay"""")).value shouldBe
        FundsDeposited(id, acc, 2, java.time.Instant.parse("2026-05-01T09:00:00.123Z"), 500, Some("pay"))
      EventCodec.decode(json("FundsWithdrawn", 3, ""","amountCents":5,"reference":null""")).value
        .asInstanceOf[FundsWithdrawn].reference shouldBe None
      EventCodec.decode(json("TransferSent", 4, s""","transferId":"$tid","counterpartyAccountId":"$other","amountCents":7""")).value shouldBe an[TransferSent]
      EventCodec.decode(json("TransferReceived", 5, s""","transferId":"$tid","counterpartyAccountId":"$other","amountCents":7""")).value shouldBe an[TransferReceived]
    }

    "reject malformed JSON" in {
      EventCodec.decode("{not json").left.value.message should include("expected")
    }

    "reject unknown event types" in {
      EventCodec.decode(json("AccountClosed", 2, "")).left.value.message should include("Unknown eventType")
    }

    "accumulate every business validation error instead of stopping at the first" in {
      val bad = EventCodec.decode(json("FundsDeposited", 0, ""","amountCents":-5""")).left.value
      bad.errors.length shouldBe 2
      bad.message should (include("version") and include("amountCents"))
    }
  }
}
