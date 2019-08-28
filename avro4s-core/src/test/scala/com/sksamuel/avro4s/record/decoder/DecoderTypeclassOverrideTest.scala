package com.sksamuel.avro4s.record.decoder

import cats.syntax.either._
import com.sksamuel.avro4s.DecoderHelper.SafeDecode
import com.sksamuel.avro4s.{AvroSchema, Decoder, DecodingFailure, DefaultFieldMapper, FieldMapper, ImmutableRecord, SchemaFor}
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{FunSuite, Matchers}

class DecoderTypeclassOverrideTest extends FunSuite with Matchers {

  case class StringOverrideTest(s: String, i: Int)

  case class Foo(b: Boolean, i: Int)
  case class FooOverrideTest(s: String, f: Foo)

  case class ValueTypeOverrideTest(s: String, f: FooValueType)

  test("allow overriding built in Decoder implicit for a basic type") {

    implicit val StringAsBooleanSchemaFor = new SchemaFor[String] {
      override def schema(fieldMapper: FieldMapper): Schema = SchemaBuilder.builder().booleanType()
    }

    implicit val StringAsBooleanDecoder = new Decoder[String] {
      override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): SafeDecode[String] = value match {
        case true => "a".asRight
        case false => "b".asRight
        case _ => DecodingFailure("Only supporting booleans").asLeft
      }
    }

    val schema = AvroSchema[StringOverrideTest]

    val record1 = ImmutableRecord(schema, Vector(java.lang.Boolean.valueOf(true), java.lang.Integer.valueOf(123)))
    Decoder[StringOverrideTest].decode(record1, schema, DefaultFieldMapper) shouldBe StringOverrideTest("a", 123).asRight

    val record2 = ImmutableRecord(schema, Vector(java.lang.Boolean.valueOf(false), java.lang.Integer.valueOf(123)))
    Decoder[StringOverrideTest].decode(record2, schema, DefaultFieldMapper) shouldBe StringOverrideTest("b", 123).asRight
  }

  test("allow overriding built in Decoder implicit for a complex type") {

    implicit val FooOverrideSchemaFor = new SchemaFor[Foo] {
      override def schema(fieldMapper: FieldMapper): Schema = SchemaBuilder.builder().stringType()
    }

    implicit val FooOverrideEncoder = new Decoder[Foo] {
      override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): SafeDecode[Foo] = value match {
        case string: String =>
          val tokens = string.split(':')
          Foo(tokens(0).toBoolean, tokens(1).toInt).asRight
      }
    }

    val schema = AvroSchema[FooOverrideTest]

    val record1 = ImmutableRecord(schema, Vector("a", "true:123"))
    Decoder[FooOverrideTest].decode(record1, schema, DefaultFieldMapper) shouldBe FooOverrideTest("a", Foo(true, 123)).asRight

    val record2 = ImmutableRecord(schema, Vector("b", "false:555"))
    Decoder[FooOverrideTest].decode(record2, schema, DefaultFieldMapper) shouldBe FooOverrideTest("b", Foo(false, 555)).asRight
  }

  test("allow overriding built in Decoder implicit for a value type") {

    implicit val FooValueTypeOverrideSchemaFor = new SchemaFor[FooValueType] {
      override def schema(fieldMapper: FieldMapper): Schema = SchemaBuilder.builder().intType()
    }

    implicit val FooValueTypeOverrideDecoder = new Decoder[FooValueType] {
      override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): SafeDecode[FooValueType] = value match {
        case i: Int => FooValueType(i.toString).asRight
        case i: java.lang.Integer => FooValueType(i.toString).asRight
      }
    }

    val schema = AvroSchema[ValueTypeOverrideTest]

    val record1 = ImmutableRecord(schema, Vector("a", java.lang.Integer.valueOf(123)))
    Decoder[ValueTypeOverrideTest].decode(record1, schema, DefaultFieldMapper) shouldBe ValueTypeOverrideTest("a", FooValueType("123")).asRight

    val record2 = ImmutableRecord(schema, Vector("b", java.lang.Integer.valueOf(555)))
    Decoder[ValueTypeOverrideTest].decode(record2, schema, DefaultFieldMapper) shouldBe ValueTypeOverrideTest("b", FooValueType("555")).asRight
  }

  test("allow overriding built in Decoder implicit for a top level value type") {

    implicit val FooValueTypeOverrideSchemaFor = new SchemaFor[FooValueType] {
      override def schema(fieldMapper: FieldMapper): Schema = SchemaBuilder.builder().intType()
    }

    implicit val FooValueTypeOverrideDecoder = new Decoder[FooValueType] {
      override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): SafeDecode[FooValueType] = value match {
        case i: Int => FooValueType(i.toString).asRight
        case i: java.lang.Integer => FooValueType(i.toString).asRight
      }
    }

    val schema = AvroSchema[FooValueType]
    Decoder[FooValueType].decode(java.lang.Integer.valueOf(555), schema, DefaultFieldMapper) shouldBe FooValueType("555").asRight
    Decoder[FooValueType].decode(java.lang.Integer.valueOf(1234), schema, DefaultFieldMapper) shouldBe FooValueType("1234").asRight
  }
}
