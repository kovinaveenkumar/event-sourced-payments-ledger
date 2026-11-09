package ledger.processor

import ledger.processor.domain.LedgerEvent._

import java.time.Instant
import java.util.UUID

object Fixtures {
  val t0: Instant = Instant.parse("2026-05-01T09:00:00Z")

  def opened(acc: UUID, v: Long = 1, owner: String = "Alice", at: Instant = t0): AccountOpened =
    AccountOpened(UUID.randomUUID(), acc, v, at, owner, "USD")

  def deposited(acc: UUID, v: Long, cents: Long, at: Instant = t0): FundsDeposited =
    FundsDeposited(UUID.randomUUID(), acc, v, at, cents, Some("test"))

  def withdrawn(acc: UUID, v: Long, cents: Long, at: Instant = t0): FundsWithdrawn =
    FundsWithdrawn(UUID.randomUUID(), acc, v, at, cents, None)

  def sent(acc: UUID, v: Long, cents: Long, at: Instant = t0, to: UUID = UUID.randomUUID()): TransferSent =
    TransferSent(UUID.randomUUID(), acc, v, at, UUID.randomUUID(), to, cents)

  def received(acc: UUID, v: Long, cents: Long, from: UUID, at: Instant = t0): TransferReceived =
    TransferReceived(UUID.randomUUID(), acc, v, at, UUID.randomUUID(), from, cents)
}
