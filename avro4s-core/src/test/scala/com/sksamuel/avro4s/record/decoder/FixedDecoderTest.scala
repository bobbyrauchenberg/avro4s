package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.record.encoder.FixedValueType
import com.sksamuel.avro4s.{AvroFixed, AvroSchema, Decoder, DefaultFieldMapper}
import org.apache.avro.generic.GenericData
import org.scalatest.{FunSuite, Matchers}
import cats.syntax.either._

class FixedDecoderTest extends FunSuite with Matchers {

  case class FixedString(@AvroFixed(10) z: String)
  case class OptionalFixedValueType(z: Option[FixedValueType])

  test("decode bytes to String") {
    val schema = AvroSchema[FixedString]
    val record = new GenericData.Record(schema)
    record.put("z", Array[Byte](115, 97, 109))
    Decoder[FixedString].decode(record, schema, DefaultFieldMapper) shouldBe FixedString("sam").asRight
  }

  test("support options of fixed") {
    val schema = AvroSchema[OptionalFixedValueType]
    val record = new GenericData.Record(schema)
    record.put("z", new GenericData.Fixed(AvroSchema[FixedValueType], Array[Byte](115, 97, 109)))
    Decoder[OptionalFixedValueType].decode(record, schema, DefaultFieldMapper) shouldBe OptionalFixedValueType(Some(FixedValueType("sam"))).asRight
  }
}