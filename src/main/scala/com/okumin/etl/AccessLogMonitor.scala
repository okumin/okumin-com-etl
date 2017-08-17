package com.okumin.etl

import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.options.{PipelineOptions, ValueProvider}
import org.joda.time.Duration
import play.api.libs.json.Json

object AccessLogMonitor {
  private[this] val UniqueIdKey = "unique_id"

  trait AccessLogMonitorOptions extends PipelineOptions {
    def getMonitorOutput: ValueProvider[String]
    def setMonitorOutput(value: ValueProvider[String]): Unit
  }

  private[this] def monitor(sc: ScioContext,
                            in: ValueProvider[String],
                            out: ValueProvider[String]) = {
    sc
      .customInput(
        "monitor-input",
        PubsubIO.readStrings().fromSubscription(in).withIdAttribute(UniqueIdKey)
      )
      .withFixedWindows(Duration.standardSeconds(60))
      .flatMap(AccessLogs.fromJson)
      .aggregate(Map.empty[Int, Long])(
        (acc, log) => acc.updated(log.status, acc.getOrElse(log.status, 0L) + 1),
        (acc1, acc2) => {
          val keys = acc1.keySet ++ acc2.keySet
          keys.foldLeft(Map.empty[Int, Long]) {
            case (acc, key) => acc.updated(key, acc1.getOrElse(key, 0L) + acc2.getOrElse(key, 0L))
          }
        }
      )
      .filter(_.exists {
        case (status, count) => status != 200  && status != 304 && count > 10
      })
      .map { stat =>
        val message = stat.map {
          case (key, value) => s"$key -> $value"
        }.mkString(", ")
        Json.stringify(Json.obj("message" -> message))
      }
      .saveAsCustomOutput("monitor-output", PubsubIO.writeStrings().to(out))
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    monitor(
      sc,
      // fromSubscription looks to evaluate eagerly
      StaticValueProvider.of(args("monitorInput")),
      sc.optionsAs[AccessLogMonitorOptions].getMonitorOutput
    )

    sc.close()
  }
}
