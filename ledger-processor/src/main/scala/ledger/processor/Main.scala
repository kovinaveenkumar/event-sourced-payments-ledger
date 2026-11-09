package ledger.processor

import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.config.{Config, ConfigFactory}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.circe.Json
import ledger.processor.fraud.{FraudConfig, FraudEngine}
import ledger.processor.store.ReadModelRepository
import org.slf4j.LoggerFactory

import java.time.Instant
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object Main {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val config                      = ConfigFactory.load()
    val ledger                      = config.getConfig("ledger")
    implicit val system: ActorSystem = ActorSystem("ledger-processor", config)
    import system.dispatcher

    val dataSource = createDataSource(ledger.getConfig("db"))
    val repo       = new ReadModelRepository(dataSource)
    migrateWithRetry(repo, attempts = 30)

    val fraudConfig = loadFraudConfig(ledger.getConfig("fraud"))
    val lookback    = Seq(fraudConfig.velocityWindow, fraudConfig.volumeWindow).max
    val recent      = repo.outgoingSince(Instant.now().minus(lookback))
    val fraud       = FraudEngine(fraudConfig).warmUp(recent)
    log.info(s"Fraud engine warmed with ${recent.size} recent outgoing movements")

    val metrics = new ProcessorMetrics
    val handler = new EventHandler(repo, fraud, metrics)
    val kafka   = ledger.getConfig("kafka")
    val stream = new LedgerStream(
      topic = kafka.getString("topic"),
      consumerSettings = LedgerStream.consumerSettings(system, kafka.getString("bootstrap-servers"), kafka.getString("group-id")),
      handler = handler,
      repo = repo,
      metrics = metrics,
      blockingEc = system.dispatchers.lookup("ledger.blocking-dispatcher")
    )
    stream.start()
    log.info(s"Consuming topic ${kafka.getString("topic")} as group ${kafka.getString("group-id")}")

    val port = ledger.getInt("http.port")
    Http().newServerAt("0.0.0.0", port).bind(routes(repo, stream, metrics)).foreach { binding =>
      log.info(s"Health endpoint listening on ${binding.localAddress}")
    }

    CoordinatedShutdown(system).addJvmShutdownHook {
      log.info("Shutting down: stopping consumer and closing the connection pool")
      stream.stop()
      dataSource.close()
    }
  }

  def routes(repo: ReadModelRepository, stream: LedgerStream, metrics: ProcessorMetrics): Route =
    concat(
      path("health") {
        get {
          val dbUp   = Try(repo.ping()).getOrElse(false)
          val healthy = dbUp && stream.running
          val body = Json.obj(
            "status" -> Json.fromString(if (healthy) "UP" else "DOWN"),
            "database" -> Json.fromBoolean(dbUp),
            "consumer" -> Json.fromBoolean(stream.running)
          )
          complete(if (healthy) StatusCodes.OK else StatusCodes.ServiceUnavailable,
            HttpEntity(ContentTypes.`application/json`, body.noSpaces))
        }
      },
      path("metrics") {
        get {
          val body = Json.obj(
            "eventsApplied" -> Json.fromLong(metrics.applied.get),
            "duplicatesSkipped" -> Json.fromLong(metrics.duplicates.get),
            "deadLetters" -> Json.fromLong(metrics.deadLetters.get),
            "fraudAlerts" -> Json.fromLong(metrics.alerts.get),
            "lastEventAt" -> metrics.lastEventAt.map(t => Json.fromString(t.toString)).getOrElse(Json.Null)
          )
          complete(HttpEntity(ContentTypes.`application/json`, body.noSpaces))
        }
      }
    )

  private def createDataSource(db: Config): HikariDataSource = {
    val hc = new HikariConfig()
    hc.setJdbcUrl(db.getString("url"))
    hc.setUsername(db.getString("user"))
    hc.setPassword(db.getString("password"))
    hc.setMaximumPoolSize(db.getInt("pool-size"))
    hc.setInitializationFailTimeout(-1) // let the app start while Postgres is still booting
    new HikariDataSource(hc)
  }

  private def migrateWithRetry(repo: ReadModelRepository, attempts: Int): Unit =
    Try(repo.migrate()) match {
      case Success(_) => log.info("Read model schema is ready")
      case Failure(e) if attempts > 1 =>
        log.warn(s"Database not ready (${e.getMessage}); retrying in 2s")
        Thread.sleep(2.seconds.toMillis)
        migrateWithRetry(repo, attempts - 1)
      case Failure(e) => throw e
    }

  def loadFraudConfig(c: Config): FraudConfig =
    FraudConfig(
      velocityMaxCount = c.getInt("velocity.max-count"),
      velocityWindow = c.getDuration("velocity.window"),
      volumeMaxCents = c.getLong("volume.max-amount-cents"),
      volumeWindow = c.getDuration("volume.window"),
      largeTransactionCents = c.getLong("large-transaction-cents")
    )
}
