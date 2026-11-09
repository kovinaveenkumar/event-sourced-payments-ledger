package ledger.processor.store

import ledger.processor.domain.LedgerEvent
import ledger.processor.fraud.FraudAlert
import ledger.processor.projection.{AccountView, TransactionRow}

import java.sql.{Connection, ResultSet, Timestamp}
import java.time.Instant
import java.util.UUID
import javax.sql.DataSource
import scala.io.Source
import scala.util.Using

/** Plain JDBC for the read tables. These calls block, so they run on their own thread pool. */
final class ReadModelRepository(ds: DataSource) {

  def migrate(): Unit = {
    val ddl = Using.resource(Source.fromResource("read-model.sql"))(_.mkString)
    Using.resource(ds.getConnection) { c =>
      Using.resource(c.createStatement())(_.execute(ddl))
    }
  }

  def inTransaction[A](f: Connection => A): A =
    Using.resource(ds.getConnection) { c =>
      c.setAutoCommit(false)
      try {
        val result = f(c)
        c.commit()
        result
      } catch {
        case t: Throwable =>
          c.rollback()
          throw t
      }
    }

  def findAccount(c: Connection, accountId: UUID): Option[AccountView] =
    Using.resource(c.prepareStatement(
      "SELECT * FROM account_view WHERE account_id = ? FOR UPDATE")) { ps =>
      ps.setObject(1, accountId)
      Using.resource(ps.executeQuery())(rs => if (rs.next()) Some(toView(rs)) else None)
    }

  def upsertAccount(c: Connection, v: AccountView): Unit =
    Using.resource(c.prepareStatement(
      """INSERT INTO account_view (account_id, owner, currency, balance_cents, version, transaction_count, opened_at, updated_at)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        |ON CONFLICT (account_id) DO UPDATE SET balance_cents = EXCLUDED.balance_cents, version = EXCLUDED.version,
        |  transaction_count = EXCLUDED.transaction_count, updated_at = EXCLUDED.updated_at""".stripMargin)) { ps =>
      ps.setObject(1, v.accountId)
      ps.setString(2, v.owner)
      ps.setString(3, v.currency)
      ps.setLong(4, v.balanceCents)
      ps.setLong(5, v.version)
      ps.setLong(6, v.transactionCount)
      ps.setTimestamp(7, Timestamp.from(v.openedAt))
      ps.setTimestamp(8, Timestamp.from(v.updatedAt))
      ps.executeUpdate()
    }

  def insertTransaction(c: Connection, t: TransactionRow): Unit =
    Using.resource(c.prepareStatement(
      """INSERT INTO account_transactions (event_id, account_id, event_type, direction, amount_cents,
        |  balance_after_cents, transfer_id, counterparty_account_id, occurred_at)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING""".stripMargin)) { ps =>
      ps.setObject(1, t.eventId)
      ps.setObject(2, t.accountId)
      ps.setString(3, t.eventType)
      ps.setString(4, t.direction)
      ps.setLong(5, t.amountCents)
      ps.setLong(6, t.balanceAfterCents)
      ps.setObject(7, t.transferId.orNull)
      ps.setObject(8, t.counterpartyAccountId.orNull)
      ps.setTimestamp(9, Timestamp.from(t.occurredAt))
      ps.executeUpdate()
    }

  def insertAlert(c: Connection, a: FraudAlert): Unit =
    Using.resource(c.prepareStatement(
      """INSERT INTO fraud_alerts (alert_id, account_id, rule, detail, event_id, occurred_at)
        |VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (alert_id) DO NOTHING""".stripMargin)) { ps =>
      ps.setObject(1, a.alertId)
      ps.setObject(2, a.accountId)
      ps.setString(3, a.rule)
      ps.setString(4, a.detail.take(300))
      ps.setObject(5, a.eventId)
      ps.setTimestamp(6, Timestamp.from(a.occurredAt))
      ps.executeUpdate()
    }

  def deadLetter(topic: String, partition: Int, offset: Long, reason: String, payload: String): Unit =
    Using.resource(ds.getConnection) { c =>
      Using.resource(c.prepareStatement(
        """INSERT INTO dead_letters (topic, partition_no, offset_no, reason, payload)
          |VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING""".stripMargin)) { ps =>
        ps.setString(1, topic)
        ps.setInt(2, partition)
        ps.setLong(3, offset)
        ps.setString(4, reason)
        ps.setString(5, payload)
        ps.executeUpdate()
      }
    }

  /** Outgoing payments since a given time, used to refill the fraud windows after a restart. */
  def outgoingSince(since: Instant): Vector[LedgerEvent] =
    Using.resource(ds.getConnection) { c =>
      Using.resource(c.prepareStatement(
        """SELECT event_id, account_id, amount_cents, occurred_at FROM account_transactions
          |WHERE direction = 'OUT' AND occurred_at > ? ORDER BY occurred_at""".stripMargin)) { ps =>
        ps.setTimestamp(1, Timestamp.from(since))
        Using.resource(ps.executeQuery()) { rs =>
          Iterator.continually(rs).takeWhile(_.next()).map { r =>
            LedgerEvent.FundsWithdrawn(
              r.getObject("event_id", classOf[UUID]), r.getObject("account_id", classOf[UUID]), 0L,
              r.getTimestamp("occurred_at").toInstant, r.getLong("amount_cents"), None): LedgerEvent
          }.toVector
        }
      }
    }

  /** Alerts not yet sent to Kafka, oldest first. */
  def unpublishedAlerts(limit: Int): Vector[FraudAlert] =
    Using.resource(ds.getConnection) { c =>
      Using.resource(c.prepareStatement(
        """SELECT alert_id, account_id, rule, detail, event_id, occurred_at FROM fraud_alerts
          |WHERE published_at IS NULL ORDER BY created_at, alert_id LIMIT ?""".stripMargin)) { ps =>
        ps.setInt(1, limit)
        Using.resource(ps.executeQuery()) { rs =>
          Iterator.continually(rs).takeWhile(_.next()).map { r =>
            FraudAlert(r.getObject("alert_id", classOf[UUID]), r.getObject("account_id", classOf[UUID]),
              r.getString("rule"), r.getString("detail"), r.getObject("event_id", classOf[UUID]),
              r.getTimestamp("occurred_at").toInstant)
          }.toVector
        }
      }
    }

  def markAlertsPublished(ids: Seq[UUID]): Unit =
    Using.resource(ds.getConnection) { c =>
      Using.resource(c.prepareStatement("UPDATE fraud_alerts SET published_at = now() WHERE alert_id = ?")) { ps =>
        ids.foreach { id => ps.setObject(1, id); ps.addBatch() }
        ps.executeBatch()
      }
    }

  def ping(): Boolean = Using.resource(ds.getConnection)(_.isValid(2))

  private def toView(rs: ResultSet): AccountView =
    AccountView(
      accountId = rs.getObject("account_id", classOf[UUID]),
      owner = rs.getString("owner"),
      currency = rs.getString("currency"),
      balanceCents = rs.getLong("balance_cents"),
      version = rs.getLong("version"),
      transactionCount = rs.getLong("transaction_count"),
      openedAt = rs.getTimestamp("opened_at").toInstant,
      updatedAt = rs.getTimestamp("updated_at").toInstant
    )
}
