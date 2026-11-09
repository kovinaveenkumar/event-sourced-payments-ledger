package ledger.processor.codec

import cats.data.{NonEmptyChain, ValidatedNec}
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure, HCursor}
import io.circe.parser.parse
import ledger.processor.domain.LedgerEvent
import ledger.processor.domain.LedgerEvent._

import java.time.Instant
import java.util.UUID

/**
 * Turns the JSON from Kafka into a checked LedgerEvent, in two steps:
 *  1. circe reads the JSON and stops at the first structure or type problem.
 *  2. cats Validated checks the values and collects every problem, not just the first,
 *     so a dead letter row shows everything that is wrong.
 */
object EventCodec {

  final case class InvalidEvent(raw: String, errors: NonEmptyChain[String]) {
    def message: String = errors.toChain.toList.mkString("; ")
  }

  private def common(c: HCursor): Decoder.Result[(UUID, UUID, Long, Instant)] =
    (c.get[UUID]("eventId"), c.get[UUID]("accountId"), c.get[Long]("version"), c.get[Instant]("occurredAt")).tupled

  implicit val ledgerEventDecoder: Decoder[LedgerEvent] = Decoder.instance { c =>
    c.get[String]("eventType").flatMap { eventType =>
      common(c).flatMap { case (eventId, accountId, version, at) =>
        eventType match {
          case "AccountOpened" =>
            (c.get[String]("owner"), c.get[String]("currency")).mapN(AccountOpened(eventId, accountId, version, at, _, _))
          case "FundsDeposited" =>
            (c.get[Long]("amountCents"), c.get[Option[String]]("reference")).mapN(FundsDeposited(eventId, accountId, version, at, _, _))
          case "FundsWithdrawn" =>
            (c.get[Long]("amountCents"), c.get[Option[String]]("reference")).mapN(FundsWithdrawn(eventId, accountId, version, at, _, _))
          case "TransferSent" =>
            (c.get[UUID]("transferId"), c.get[UUID]("counterpartyAccountId"), c.get[Long]("amountCents"))
              .mapN(TransferSent(eventId, accountId, version, at, _, _, _))
          case "TransferReceived" =>
            (c.get[UUID]("transferId"), c.get[UUID]("counterpartyAccountId"), c.get[Long]("amountCents"))
              .mapN(TransferReceived(eventId, accountId, version, at, _, _, _))
          case other =>
            Left(DecodingFailure(s"Unknown eventType '$other'", c.history))
        }
      }
    }
  }

  def validate(e: LedgerEvent): ValidatedNec[String, LedgerEvent] = {
    val version = if (e.version >= 1) e.validNec else s"version must be >= 1 but was ${e.version}".invalidNec
    val amount = LedgerEvent.movement(e) match {
      case Some(m) if m.amountCents <= 0 => s"amountCents must be positive but was ${m.amountCents}".invalidNec
      case _                             => e.validNec
    }
    val specific = e match {
      case o: AccountOpened if o.owner.trim.isEmpty => "owner must not be blank".invalidNec
      case o: AccountOpened if o.currency.length != 3 => s"currency '${o.currency}' is not ISO-4217".invalidNec
      case s: TransferSent if s.counterpartyAccountId == s.accountId => "transfer to self".invalidNec
      case _ => e.validNec
    }
    (version, amount, specific).mapN((_, _, _) => e)
  }

  def decode(raw: String): Either[InvalidEvent, LedgerEvent] =
    parse(raw)
      .flatMap(_.as[LedgerEvent])
      .leftMap(err => InvalidEvent(raw, NonEmptyChain.one(err.getMessage)))
      .flatMap(e => validate(e).toEither.leftMap(InvalidEvent(raw, _)))
}
