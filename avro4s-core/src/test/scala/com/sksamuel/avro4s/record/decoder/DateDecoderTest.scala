package com.sksamuel.avro4s.record.decoder

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultFieldMapper}
import org.apache.avro.generic.GenericData
import org.scalatest.{FunSuite, Matchers}
import cats.syntax.either._

//noinspection ScalaDeprecation
class DateDecoderTest extends FunSuite with Matchers {

  case class WithLocalTime(z: LocalTime)
  case class WithLocalDate(z: LocalDate)
  case class WithDate(z: Date)
  case class WithLocalDateTime(z: LocalDateTime)
  case class WithTimestamp(z: Timestamp)
  case class WithInstant(z: Instant)

  test("decode int to LocalTime") {
    val schema = AvroSchema[WithLocalTime]
    val record = new GenericData.Record(schema)
    record.put("z", 46245000)
    Decoder[WithLocalTime].decode(record, schema, DefaultFieldMapper) shouldBe WithLocalTime(LocalTime.of(12, 50, 45)).asRight
  }

  test("decode int to LocalDate") {
    val schema = AvroSchema[WithLocalDate]
    val record = new GenericData.Record(schema)
    record.put("z", 17784)
    Decoder[WithLocalDate].decode(record, schema, DefaultFieldMapper) shouldBe WithLocalDate(LocalDate.of(2018, 9, 10)).asRight
  }

  test("decode int to java.sql.Date") {
    val schema = AvroSchema[WithDate]
    val record = new GenericData.Record(schema)
    record.put("z", 17784)
    Decoder[WithDate].decode(record, schema, DefaultFieldMapper) shouldBe WithDate(Date.valueOf(LocalDate.of(2018, 9, 10))).asRight
  }

  test("decode long to LocalDateTime") {
    val schema = AvroSchema[WithLocalDateTime]
    val record = new GenericData.Record(schema)
    record.put("z", 1536580739000L)
    Decoder[WithLocalDateTime].decode(record, schema, DefaultFieldMapper) shouldBe WithLocalDateTime(LocalDateTime.of(2018, 9, 10, 11, 58, 59)).asRight
  }

  test("decode long to Timestamp") {
    val schema = AvroSchema[WithTimestamp]
    val record = new GenericData.Record(schema)
    record.put("z", 1538312231000L)
    Decoder[WithTimestamp].decode(record, schema, DefaultFieldMapper) shouldBe WithTimestamp(new Timestamp(1538312231000L)).asRight
  }

  test("decode long to Instant") {
    val schema = AvroSchema[WithInstant]
    val record = new GenericData.Record(schema)
    record.put("z", 1538312231000L)
    Decoder[WithInstant].decode(record, schema, DefaultFieldMapper) shouldBe WithInstant(Instant.ofEpochMilli(1538312231000L)).asRight
  }
}


