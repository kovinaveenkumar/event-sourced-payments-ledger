package ledger.processor.fraud

import io.circe.{Decoder, Json}
import io.circe.parser.parse
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import java.time.Instant
import java.util.{Properties, UUID}
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

/** JSON format of a message on the fraud-alerts topic (see docs/EVENTS.md). */
object AlertCodec {

  def encode(a: FraudAlert): String =
    Json.obj(
      "alertId"    -> Json.fromString(a.alertId.toString),
      "accountId"  -> Json.fromString(a.accountId.toString),
      "rule"       -> Json.fromString(a.rule),
      "detail"     -> Json.fromString(a.detail),
      "eventId"    -> Json.fromString(a.eventId.toString),
      "occurredAt" -> Json.fromString(a.occurredAt.toString)
    ).noSpaces

  implicit val decoder: Decoder[FraudAlert] = Decoder.instance { c =>
    for {
      alertId    <- c.get[UUID]("alertId")
      accountId  <- c.get[UUID]("accountId")
      rule       <- c.get[String]("rule")
      detail     <- c.get[String]("detail")
      eventId    <- c.get[UUID]("eventId")
      occurredAt <- c.get[Instant]("occurredAt")
    } yield FraudAlert(alertId, accountId, rule, detail, eventId, occurredAt)
  }

  def decode(json: String): Either[io.circe.Error, FraudAlert] = parse(json).flatMap(_.as[FraudAlert])
}

/**
 * Same outbox idea as the Java command service, for alerts. The processor saves alerts in
 * the same database transaction as the balances. This relay then sends any unsent alerts to
 * Kafka and marks them as sent.
 *
 * If I sent them straight from the stream instead, a crash between saving and sending would
 * lose the alert: the event would come back from Kafka, be skipped as a duplicate, and the
 * alert would never go out.
 *
 * An alert can be sent twice (crash after sending, before marking). Alert ids never change
 * for the same rule and event, so consumers can skip repeats.
 */
final class AlertRelay(
  fetchUnsent: Int => Vector[FraudAlert],
  markSent: Seq[UUID] => Unit,
  send: FraudAlert => Unit,
  batchSize: Int = 100
) {
  private val log = LoggerFactory.getLogger(getClass)

  /** Sends one batch and returns how many were sent. Stops at the first failure to keep order. */
  def publishPending(): Int = {
    val pending = fetchUnsent(batchSize)
    val sent = pending.iterator.takeWhile { alert =>
      try { send(alert); true }
      catch {
        case NonFatal(e) =>
          log.warn(s"Could not send alert ${alert.alertId}; will retry", e)
          false
      }
    }.map(_.alertId).toVector
    if (sent.nonEmpty) {
      markSent(sent)
      log.info(s"Published ${sent.size} fraud alert(s)")
    }
    sent.size
  }
}

/** Kafka sender for alerts. Key = account id, so one account's alerts stay in order. */
final class KafkaAlertSender(bootstrapServers: String, topic: String) extends (FraudAlert => Unit) with AutoCloseable {
  private val producer = {
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    p.put(ProducerConfig.ACKS_CONFIG, "all")
    p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    new KafkaProducer[String, String](p, new StringSerializer, new StringSerializer)
  }

  def apply(alert: FraudAlert): Unit =
    producer.send(new ProducerRecord(topic, alert.accountId.toString, AlertCodec.encode(alert))).get(10, TimeUnit.SECONDS)

  def close(): Unit = producer.close()
}
