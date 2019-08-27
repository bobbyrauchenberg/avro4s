package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultFieldMapper}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}
import shapeless.{:+:, CNil, Coproduct}
import cats.syntax.either._

class CoproductDecoderTest extends FunSuite with Matchers {

  test("coproducts with primitives") {
    val schema = AvroSchema[CPWrapper]
    val record = new GenericData.Record(schema)
    record.put("u", new Utf8("wibble"))
    Decoder[CPWrapper].decode(record, schema, DefaultFieldMapper) shouldBe CPWrapper(Coproduct[CPWrapper.ISBG]("wibble")).asRight
  }

  test("coproducts with case classes") {
    val schema = AvroSchema[CPWrapper]
    val gimble = new GenericData.Record(AvroSchema[Gimble])
    gimble.put("x", new Utf8("foo"))
    val record = new GenericData.Record(schema)
    record.put("u", gimble)
    Decoder[CPWrapper].decode(record, schema, DefaultFieldMapper) shouldBe CPWrapper(Coproduct[CPWrapper.ISBG](Gimble("foo"))).asRight
  }

  test("coproducts with options") {
    val schema = AvroSchema[CPWithOption]
    val gimble = new GenericData.Record(AvroSchema[Gimble])
    gimble.put("x", new Utf8("foo"))
    val record = new GenericData.Record(schema)
    record.put("u", gimble)
    Decoder[CPWithOption].decode(record, schema, DefaultFieldMapper) shouldBe CPWithOption(Some(Coproduct[CPWrapper.ISBG](Gimble("foo")))).asRight
  }
}

case class CPWithArray(u: CPWrapper.SSI)

case class Gimble(x: String)
case class CPWrapper(u: CPWrapper.ISBG)
case class CPWithOption(u: Option[CPWrapper.ISBG])

object CPWrapper {
  type ISBG = Int :+: String :+: Boolean :+: Gimble :+: CNil
  type SSI = Seq[String] :+: Int :+: CNil
}

case class Coproducts(union: Int :+: String :+: Boolean :+: CNil)
case class CoproductsOfCoproducts(union: (Int :+: String :+: CNil) :+: Boolean :+: CNil)
