package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.Decoder.BigDecimalDecoder
import com.sksamuel.avro4s.{AvroSchema, BigDecimals, Decoder, DefaultFieldMapper, FieldMapper, SchemaFor}
import org.apache.avro.generic.GenericData
import org.apache.avro.{Conversions, LogicalTypes, Schema}
import org.scalatest.{FlatSpec, Matchers}

import scala.language.higherKinds
import cats.syntax.either._



case class WithBigDecimal(decimal: BigDecimal)
case class OptionalBigDecimal(big: Option[BigDecimal])

class BigDecimalDecoderTest extends FlatSpec with Matchers {

  "Decoder" should "convert byte array to decimal" in {
    val schema = AvroSchema[WithBigDecimal]
    val record = new GenericData.Record(schema)
    val bytes = new Conversions.DecimalConversion().toBytes(BigDecimal(123.45).bigDecimal, null, LogicalTypes.decimal(8, 2))
    record.put("decimal", bytes)
    Decoder[WithBigDecimal].decode(record, schema, DefaultFieldMapper) shouldBe WithBigDecimal(BigDecimal(123.45)).asRight
  }

  it should "support optional big decimals" in {
    val schema = AvroSchema[OptionalBigDecimal]
    val bytes = new Conversions.DecimalConversion().toBytes(BigDecimal(123.45).bigDecimal, null, LogicalTypes.decimal(8, 2))
    val record = new GenericData.Record(schema)
    record.put("big", bytes)
    Decoder[OptionalBigDecimal].decode(record, schema, DefaultFieldMapper) shouldBe OptionalBigDecimal(Option(BigDecimal(123.45))).asRight

    val emptyRecord = new GenericData.Record(schema)
    emptyRecord.put("big", null)
    Decoder[OptionalBigDecimal].decode(emptyRecord, schema, DefaultFieldMapper) shouldBe OptionalBigDecimal(None).asRight
  }

  it should "be able to decode strings as bigdecimals" in {
    Decoder[BigDecimal].decode("123.45", BigDecimals.AsString.schema(DefaultFieldMapper), DefaultFieldMapper) shouldBe BigDecimal(123.45).asRight
  }

  it should "be able to decode generic fixed as bigdecimals" in {
    implicit object BigDecimalAsFixed extends SchemaFor[BigDecimal] {
      override def schema(fieldMapper: FieldMapper = DefaultFieldMapper) = LogicalTypes.decimal(10, 8).addToSchema(
        Schema.createFixed("BigDecimal", null, null, 8))
    }

    val fixed = GenericData.get().createFixed(null, Array[Byte](0, 4, 98, -43, 55, 43, -114, 0), BigDecimalAsFixed.schema())
    Decoder[BigDecimal].decode(fixed, BigDecimalAsFixed.schema(DefaultFieldMapper), DefaultFieldMapper) shouldBe BigDecimal(12345678).asRight
  }

//  it should "be able to decode longs as bigdecimals" in {
//    val schema = LogicalTypes.decimal(5, 2).addToSchema(SchemaBuilder.builder().longType())
//    BigDecimalDecoder.decode(12345, schema) shouldBe ""
//    BigDecimalDecoder.decode(9999, schema) shouldBe ""
//    BigDecimalDecoder.decode(java.lang.Long.valueOf(99887766), schema) shouldBe ""
//    BigDecimalDecoder.decode(java.lang.Integer.valueOf(654), schema) shouldBe ""
//  }
}