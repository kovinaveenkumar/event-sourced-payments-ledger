package ledger.processor

import ledger.processor.domain.LedgerEvent
import ledger.processor.fraud.{FraudAlert, FraudEngine}
import ledger.processor.projection.{AccountProjection, ProjectionResult}
import ledger.processor.store.ReadModelRepository
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicLong

sealed trait Outcome
object Outcome {
  final case class Applied(alerts: List[FraudAlert]) extends Outcome
  case object Duplicate                              extends Outcome
  final case class Rejected(reason: String)          extends Outcome
}

/** Counters shown on /metrics. */
final class ProcessorMetrics {
  val applied     = new AtomicLong
  val duplicates  = new AtomicLong
  val deadLetters = new AtomicLong
  val alerts      = new AtomicLong
  @volatile var lastEventAt: Option[java.time.Instant] = None
}

/**
 * Applies one event to the read tables. The balance, the transaction row and any fraud
 * alerts are saved in one database transaction. I only update the fraud state after the
 * save works, so if it fails the event can be retried without counting it twice.
 *
 * The stream calls this one event at a time, which keeps each account in order.
 */
final class EventHandler(repo: ReadModelRepository, initialFraud: FraudEngine, metrics: ProcessorMetrics) {

  private val log = LoggerFactory.getLogger(getClass)
  @volatile private var fraud: FraudEngine = initialFraud

  def handle(event: LedgerEvent): Outcome = {
    val (outcome, nextFraud) = repo.inTransaction { c =>
      AccountProjection.project(repo.findAccount(c, event.accountId), event) match {
        case ProjectionResult.Duplicate(_) =>
          (Outcome.Duplicate, fraud)

        case ProjectionResult.Gap(expected, actual) =>
          (Outcome.Rejected(s"version gap for account ${event.accountId}: expected $expected, got $actual"), fraud)

        case ProjectionResult.Applied(view, txRow) =>
          repo.upsertAccount(c, view)
          txRow.foreach(repo.insertTransaction(c, _))
          val (engine, alerts) = fraud.observe(event)
          alerts.foreach(repo.insertAlert(c, _))
          (Outcome.Applied(alerts), engine)
      }
    }
    fraud = nextFraud
    record(event, outcome)
    outcome
  }

  private def record(event: LedgerEvent, outcome: Outcome): Unit = outcome match {
    case Outcome.Applied(alerts) =>
      metrics.applied.incrementAndGet()
      metrics.lastEventAt = Some(event.occurredAt)
      log.info(s"Applied ${event.productPrefix} accountId=${event.accountId} version=${event.version}")
      alerts.foreach { a =>
        metrics.alerts.incrementAndGet()
        log.warn(s"FRAUD_ALERT rule=${a.rule} accountId=${a.accountId} eventId=${a.eventId} detail=${a.detail}")
      }
    case Outcome.Duplicate =>
      metrics.duplicates.incrementAndGet()
      log.info(s"Skipped duplicate eventId=${event.eventId} accountId=${event.accountId} version=${event.version}")
    case Outcome.Rejected(reason) =>
      log.error(s"Rejected eventId=${event.eventId}: $reason")
  }
}
