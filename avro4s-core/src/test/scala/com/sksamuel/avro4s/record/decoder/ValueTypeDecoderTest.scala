package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultFieldMapper}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}
import cats.syntax.either._

class ValueTypeDecoderTest extends FunSuite with Matchers {

  case class Test(foo: FooValueType)
  case class OptionTest(foo: Option[FooValueType])

  test("top level value types") {
    val actual = Decoder[FooValueType].decode("hello", AvroSchema[FooValueType], DefaultFieldMapper)
    actual shouldBe FooValueType("hello").asRight
  }

  test("support fields that are value types") {
    val schema = AvroSchema[Test]

    val record1 = new GenericData.Record(schema)
    record1.put("foo", new Utf8("hello"))

    Decoder[Test].decode(record1, schema, DefaultFieldMapper) shouldBe Test(FooValueType("hello")).asRight
  }

  test("support value types inside Options") {
    val schema = AvroSchema[OptionTest]

    val record1 = new GenericData.Record(schema)
    record1.put("foo", new Utf8("hello"))

    Decoder[OptionTest].decode(record1, schema, DefaultFieldMapper) shouldBe OptionTest(Some(FooValueType("hello"))).asRight
  }
}

case class FooValueType(s: String) extends AnyVal
