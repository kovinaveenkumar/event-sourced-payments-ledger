package ledger.processor

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.RestartSettings
import akka.stream.scaladsl.{RestartSource, Sink}
import ledger.processor.codec.EventCodec
import ledger.processor.store.ReadModelRepository
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Reads from Kafka, parses each message, updates the read tables, then commits the offset.
 *
 * - I commit the offset only after the database save, so nothing is lost. If a message
 *   comes again, the version check skips it.
 * - mapAsync(1) handles one message at a time, so order is kept.
 * - Bad messages go to the dead_letters table so they don't block the rest.
 * - If the database or Kafka goes down, the stream restarts with a growing wait and carries
 *   on from the last committed offset.
 */
final class LedgerStream(
  topic: String,
  consumerSettings: ConsumerSettings[String, String],
  handler: EventHandler,
  repo: ReadModelRepository,
  metrics: ProcessorMetrics,
  blockingEc: ExecutionContext
)(implicit system: ActorSystem) {

  private val log     = LoggerFactory.getLogger(getClass)
  private val control = new AtomicReference[Consumer.Control](Consumer.NoopControl)
  @volatile var running: Boolean = false

  def start(): Future[Done] = {
    val restart = RestartSettings(minBackoff = 1.second, maxBackoff = 30.seconds, randomFactor = 0.2)
    running = true
    RestartSource
      .onFailuresWithBackoff(restart) { () =>
        Consumer
          .committableSource(consumerSettings, Subscriptions.topics(topic))
          .mapMaterializedValue(c => control.set(c))
          .mapAsync(1)(msg => Future(process(msg.record))(blockingEc).map(_ => msg.committableOffset)(blockingEc))
          .via(Committer.flow(CommitterSettings(system)))
      }
      .runWith(Sink.ignore)
  }

  def stop(): Future[Done] = {
    running = false
    control.get().shutdown()
  }

  private[processor] def process(record: ConsumerRecord[String, String]): Unit =
    EventCodec.decode(record.value) match {
      case Left(invalid) =>
        deadLetter(record, s"invalid event: ${invalid.message}")
      case Right(event) =>
        handler.handle(event) match {
          case Outcome.Rejected(reason) => deadLetter(record, reason)
          case _                        => ()
        }
    }

  private def deadLetter(record: ConsumerRecord[String, String], reason: String): Unit = {
    log.error(s"DEAD_LETTER partition=${record.partition} offset=${record.offset} reason=$reason")
    repo.deadLetter(record.topic, record.partition, record.offset, reason, record.value)
    metrics.deadLetters.incrementAndGet()
  }
}

object LedgerStream {
  def consumerSettings(system: ActorSystem, bootstrap: String, groupId: String): ConsumerSettings[String, String] =
    ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(bootstrap)
      .withGroupId(groupId)
}
