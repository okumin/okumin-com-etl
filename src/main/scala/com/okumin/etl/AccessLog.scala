package com.okumin.etl

import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.bigquery.types.BigQueryType
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination
import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.joda.time.{DateTimeZone, Instant}
import org.slf4j.LoggerFactory
import play.api.libs.json.{Format, JsError, JsSuccess, Json}
import scala.util.control.NonFatal

@BigQueryType.toTable
final case class AccessLog(time: Instant,
                           host: String,
                           method: String,
                           scheme: String,
                           vhost: Option[String],
                           uri: String,
                           protocol: String,
                           reqsize: Long,
                           ua: Option[String],
                           referer: Option[String],
                           status: Int,
                           size: Long,
                           reqtime: Double,
                           runtime: Option[String],
                           apptime: Option[String],
                           via: Option[String],
                           cache: Option[String],
                           cloud_trace_context: Option[String])

object AccessLogs {
  private[this] val logger = LoggerFactory.getLogger(this.getClass)
  private[this] val PartitionFormat = DateTimeFormat
    .forPattern("yyyyMMdd")
    .withZone(DateTimeZone.UTC)

  private[this] final class DayPartitionFunction(table: ValueProvider[String])
    extends SerializableFunction[ValueInSingleWindow[AccessLog], TableDestination] {
    override def apply(input: ValueInSingleWindow[AccessLog]): TableDestination = {
      val partition = PartitionFormat.print(input.getValue.time)
      new TableDestination(s"${table.get()}$$$partition", "")
    }
  }
  private[this] object LogFormatter extends SerializableFunction[AccessLog, TableRow] {
    override def apply(input: AccessLog): TableRow = AccessLog.toTableRow(input)
  }

  implicit private[this] val InstantFormat: Format[Instant] =
    Format.invariantFunctorFormat.inmap[Long, Instant](Format.of[Long], new Instant(_), _.getMillis)
  implicit val AccessLogFormat: Format[AccessLog] = Json.format[AccessLog]

  def dayPartitioner(table: ValueProvider[String]): SerializableFunction[ValueInSingleWindow[AccessLog], TableDestination] = {
    new DayPartitionFunction(table)
  }

  def formatter: SerializableFunction[AccessLog, TableRow] = LogFormatter

  def fromNginxLog(line: String): Option[AccessLog] = {
    def parseLtsv(x: String): Option[Map[String, String]] = {
      try {
        val map = x
          .split("""\t""")
          .map(_.split(":", 2))
          .collect {
            case Array(k, v) if v != "-" => k -> v
          }
          .toMap
        Some(map)
      } catch {
        case NonFatal(e) =>
          logger.error(s"Failed parsing LTSV. $x", e)
          None
      }
    }
    for {
      map <- parseLtsv(line)
      // Health check requests don't have forwarded_proto
      if map.contains("forwarded_proto")
      log <- fromMap(map)
    } yield log
  }

  def fromMap(map: Map[String, String]): Option[AccessLog] = {
    try {
      val log = AccessLog(
        Instant.parse(map("time"), ISODateTimeFormat.dateTimeNoMillis()),
        map("forwardedfor").split(",").map(_.trim).head,
        map("method"),
        map("forwarded_proto"),
        map.get("vhost"),
        map("uri"),
        map("protocol"),
        map("reqsize").toLong,
        map.get("ua"),
        map.get("referer"),
        map("status").toInt,
        map("size").toLong,
        map("reqtime").toDouble,
        map.get("runtime"),
        map.get("apptime"),
        map.get("via"),
        map.get("cache"),
        map.get("cloudtracecontext")
      )
      Some(log)
    } catch {
      case NonFatal(e) =>
        logger.error(s"Failed parse HTTP log. $map", e)
        None
    }
  }

  def fromJson(json: String): Option[AccessLog] = {
    try {
      Json.fromJson[AccessLog](Json.parse(json)) match {
        case JsSuccess(log, _) => Some(log)
        case e: JsError =>
          logger.error(JsError.toJson(e).toString())
          None
      }
    } catch {
      case NonFatal(e) =>
        logger.error(s"Failed parsing json. $json", e)
        None
    }
  }

  def toJson(log: AccessLog): String = Json.stringify(Json.toJson(log))
}
