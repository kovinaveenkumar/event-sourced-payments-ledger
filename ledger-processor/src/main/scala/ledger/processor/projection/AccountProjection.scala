package ledger.processor.projection

import ledger.processor.domain.LedgerEvent
import ledger.processor.domain.LedgerEvent._

import java.time.Instant
import java.util.UUID

/** One row of the account_view table, shaped for what the dashboard shows. */
final case class AccountView(
  accountId: UUID,
  owner: String,
  currency: String,
  balanceCents: Long,
  version: Long,
  transactionCount: Long,
  openedAt: Instant,
  updatedAt: Instant
)

final case class TransactionRow(
  eventId: UUID,
  accountId: UUID,
  eventType: String,
  direction: String,
  amountCents: Long,
  balanceAfterCents: Long,
  transferId: Option[UUID],
  counterpartyAccountId: Option[UUID],
  occurredAt: Instant
)

sealed trait ProjectionResult
object ProjectionResult {
  final case class Applied(view: AccountView, transaction: Option[TransactionRow]) extends ProjectionResult
  /** Kafka can deliver an event twice. Repeats are skipped. */
  final case class Duplicate(seenVersion: Long) extends ProjectionResult
  /** An event came before an earlier one. Shouldn't happen since each account uses one partition. */
  final case class Gap(expectedVersion: Long, actualVersion: Long) extends ProjectionResult
}

/**
 * Takes the current view and an event and returns the result. No database calls here,
 * so it's easy to test, and replaying all events always gives the same balances.
 *
 * An event is applied only if its version is exactly one more than the view's version.
 * That's how repeats get skipped.
 */
object AccountProjection {
  import ProjectionResult._

  def project(current: Option[AccountView], event: LedgerEvent): ProjectionResult = {
    val seen = current.map(_.version).getOrElse(0L)
    if (event.version <= seen) Duplicate(seen)
    else if (event.version != seen + 1) Gap(seen + 1, event.version)
    else (current, event) match {
      case (None, o: AccountOpened) =>
        Applied(AccountView(o.accountId, o.owner, o.currency, 0L, o.version, 0L, o.occurredAt, o.occurredAt), None)

      case (Some(view), e) =>
        LedgerEvent.movement(e) match {
          case Some(m) =>
            val updated = view.copy(
              balanceCents = view.balanceCents + m.signedCents,
              version = e.version,
              transactionCount = view.transactionCount + 1,
              updatedAt = e.occurredAt
            )
            val row = TransactionRow(
              eventId = e.eventId,
              accountId = e.accountId,
              eventType = e.productPrefix,
              direction = if (m.direction == Direction.In) "IN" else "OUT",
              amountCents = m.amountCents,
              balanceAfterCents = updated.balanceCents,
              transferId = m.transferId,
              counterpartyAccountId = m.counterparty,
              occurredAt = e.occurredAt
            )
            Applied(updated, Some(row))
          case None =>
            // A second AccountOpened would be a bug on the sending side, so treat it as a gap.
            Gap(seen + 1, e.version)
        }

      case (None, e) => Gap(1L, e.version)
    }
  }

  /** Builds a view from a full list of events. Handy in tests and for checking balances. */
  def replay(events: Seq[LedgerEvent]): Option[AccountView] =
    events.foldLeft(Option.empty[AccountView]) { (state, e) =>
      project(state, e) match {
        case Applied(view, _) => Some(view)
        case _                => state
      }
    }
}
