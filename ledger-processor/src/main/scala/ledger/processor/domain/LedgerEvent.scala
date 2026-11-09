package ledger.processor.domain

import java.time.Instant
import java.util.UUID

/** The events sent by the command service (format in docs/EVENTS.md). */
sealed trait LedgerEvent extends Product with Serializable {
  def eventId: UUID
  def accountId: UUID
  def version: Long
  def occurredAt: Instant
}

object LedgerEvent {

  final case class AccountOpened(eventId: UUID, accountId: UUID, version: Long, occurredAt: Instant,
                                 owner: String, currency: String) extends LedgerEvent

  final case class FundsDeposited(eventId: UUID, accountId: UUID, version: Long, occurredAt: Instant,
                                  amountCents: Long, reference: Option[String]) extends LedgerEvent

  final case class FundsWithdrawn(eventId: UUID, accountId: UUID, version: Long, occurredAt: Instant,
                                  amountCents: Long, reference: Option[String]) extends LedgerEvent

  final case class TransferSent(eventId: UUID, accountId: UUID, version: Long, occurredAt: Instant,
                                transferId: UUID, counterpartyAccountId: UUID, amountCents: Long) extends LedgerEvent

  final case class TransferReceived(eventId: UUID, accountId: UUID, version: Long, occurredAt: Instant,
                                    transferId: UUID, counterpartyAccountId: UUID, amountCents: Long) extends LedgerEvent

  sealed trait Direction
  object Direction {
    case object In  extends Direction
    case object Out extends Direction
  }

  /** Money going in or out of one account. AccountOpened moves no money, so it gives None. */
  final case class Movement(event: LedgerEvent, direction: Direction, amountCents: Long,
                            transferId: Option[UUID], counterparty: Option[UUID]) {
    def signedCents: Long = if (direction == Direction.In) amountCents else -amountCents
  }

  def movement(e: LedgerEvent): Option[Movement] = e match {
    case _: AccountOpened     => None
    case d: FundsDeposited    => Some(Movement(d, Direction.In, d.amountCents, None, None))
    case w: FundsWithdrawn    => Some(Movement(w, Direction.Out, w.amountCents, None, None))
    case s: TransferSent      => Some(Movement(s, Direction.Out, s.amountCents, Some(s.transferId), Some(s.counterpartyAccountId)))
    case r: TransferReceived  => Some(Movement(r, Direction.In, r.amountCents, Some(r.transferId), Some(r.counterpartyAccountId)))
  }
}
