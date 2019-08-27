package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroName, AvroSchema, Decoder, DefaultFieldMapper}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}
import cats.syntax.either._

class AvroNameDecoderTest extends FunSuite with Matchers {

  case class AvroNameDecoderTest(@AvroName("bar") foo: String)

  test("decoder should take into account @AvroName on fields") {
    val schema = AvroSchema[AvroNameDecoderTest]
    val record = new GenericData.Record(schema)
    record.put("bar", new Utf8("hello"))
    Decoder[AvroNameDecoderTest].decode(record, schema, DefaultFieldMapper) shouldBe AvroNameDecoderTest("hello").asRight
  }
}