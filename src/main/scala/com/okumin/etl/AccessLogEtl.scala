package com.okumin.etl

import com.spotify.scio.io.Tap
import com.spotify.scio.{ContextAndArgs, ScioContext}
import java.nio.charset.StandardCharsets
import java.util.{Collections, UUID}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.pubsub.{PubsubIO, PubsubMessage}
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.options.{PipelineOptions, ValueProvider}
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsObject, Json}
import scala.concurrent.Future
import scala.util.control.NonFatal

object AccessLogEtl {
  private[this] val logger = LoggerFactory.getLogger(this.getClass)
  private[this] val UniqueIdKey = "unique_id"

  trait AccessLogEtlOptions extends PipelineOptions {
    def getTransformOutput: ValueProvider[String]
    def setTransformOutput(value: ValueProvider[String]): Unit

    def getLoadOutput: ValueProvider[String]
    def setLoadOutput(value: ValueProvider[String]): Unit
  }

  private[this] def transform(sc: ScioContext,
                              in: ValueProvider[String],
                              out: ValueProvider[String]): Future[Tap[PubsubMessage]] = {
    sc
      .customInput("transform-input", PubsubIO.readStrings().fromSubscription(in))
      .flatMap { entry =>
        try {
          val json = Json.parse(entry).as[JsObject]
          val line = json.value("textPayload").as[String]
          Some(line)
        } catch {
          case NonFatal(e) =>
            logger.error(s"Failed parsing LogEntry. $entry", e)
            None
        }
      }
      .flatMap(AccessLogs.fromNginxLog)
      .map { log =>
        val uniqueId = log.cloud_trace_context.getOrElse(UUID.randomUUID().toString)
        val attributes = Collections.singletonMap(UniqueIdKey, uniqueId)
        val json = AccessLogs.toJson(log).getBytes(StandardCharsets.UTF_8)
        new PubsubMessage(json, attributes)
      }
      .saveAsCustomOutput(
        "transform-output",
        PubsubIO.writeMessages().withIdAttribute(UniqueIdKey).to(out)
      )
  }

  private[this] def load(sc: ScioContext,
                         in: ValueProvider[String],
                         out: ValueProvider[String]): Future[Tap[AccessLog]] = {
    sc
      .customInput(
        "load-input",
        PubsubIO.readStrings().fromSubscription(in).withIdAttribute(UniqueIdKey)
      )
      .flatMap(AccessLogs.fromJson)
      .saveAsCustomOutput("load-output", BigQueryIO
        .write()
        .to(AccessLogs.dayPartitioner(out))
        .withFormatFunction(AccessLogs.formatter)
        .withSchema(AccessLog.schema)
        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
        .withWriteDisposition(WriteDisposition.WRITE_APPEND))
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    transform(
      sc,
      // fromSubscription looks to evaluate eagerly
      StaticValueProvider.of(args("transformInput")),
      sc.optionsAs[AccessLogEtlOptions].getTransformOutput
    )

    load(
      sc,
      // fromSubscription looks to evaluate eagerly
      StaticValueProvider.of(args("loadInput")),
      sc.optionsAs[AccessLogEtlOptions].getLoadOutput
    )

    sc.close()
  }
}
