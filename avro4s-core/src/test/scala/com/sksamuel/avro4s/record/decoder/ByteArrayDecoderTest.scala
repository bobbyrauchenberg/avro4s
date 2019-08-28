package com.sksamuel.avro4s.record.decoder

import cats.syntax.either._
import java.nio.ByteBuffer

import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultFieldMapper}
import org.apache.avro.generic.GenericData
import org.scalatest.{FunSuite, Matchers}

import scala.language.higherKinds

class ByteArrayDecoderTest extends FunSuite with Matchers {

  case class ArrayTest(z: Array[Byte])
  case class ByteBufferTest(z: ByteBuffer)
  case class VectorTest(z: Vector[Byte])
  case class SeqTest(z: Array[Byte])
  case class ListTest(z: Array[Byte])

  test("decode byte arrays") {
    val schema = AvroSchema[ArrayTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[ArrayTest].decode(record, schema, DefaultFieldMapper).map(_.z.toList) shouldBe List[Byte](1, 4, 9).asRight
  }

  test("decode bytebuffers to array") {
    val schema = AvroSchema[ArrayTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[ArrayTest].decode(record, schema, DefaultFieldMapper)map(_.z.toList) shouldBe List[Byte](1, 4, 9).asRight
  }

  test("decode byte vectors") {
    val schema = AvroSchema[VectorTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[VectorTest].decode(record, schema, DefaultFieldMapper).map(_.z) shouldBe Vector[Byte](1, 4, 9).asRight
  }

  test("decode byte lists") {
    val schema = AvroSchema[ListTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[ListTest].decode(record, schema, DefaultFieldMapper).map(_.z.toList) shouldBe List[Byte](1, 4, 9).asRight
  }

  test("decode byte seqs") {
    val schema = AvroSchema[SeqTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[SeqTest].decode(record, schema, DefaultFieldMapper).map(_.z.toList) shouldBe Seq[Byte](1, 4, 9).asRight
  }

  test("decode top level byte arrays") {
    Decoder[Array[Byte]].decode(ByteBuffer.wrap(Array[Byte](1, 4, 9)), AvroSchema[Array[Byte]], DefaultFieldMapper).map(_.toList) shouldBe List[Byte](1, 4, 9).asRight
  }

  test("decode array to bytebuffers") {
    val schema = AvroSchema[ByteBufferTest]
    val record = new GenericData.Record(schema)
    record.put("z", Array[Byte](1, 4, 9))
    Decoder[ByteBufferTest].decode(record, schema, DefaultFieldMapper).map(_.z.array().toList) shouldBe List[Byte](1, 4, 9).asRight
  }

  test("decode bytebuffers") {
    val schema = AvroSchema[ByteBufferTest]
    val record = new GenericData.Record(schema)
    record.put("z", ByteBuffer.wrap(Array[Byte](1, 4, 9)))
    Decoder[ByteBufferTest].decode(record, schema, DefaultFieldMapper).map(_.z.array().toList) shouldBe List[Byte](1, 4, 9).asRight
  }

  test("decode top level ByteBuffers") {
    Decoder[ByteBuffer].decode(ByteBuffer.wrap(Array[Byte](1, 4, 9)), AvroSchema[ByteBuffer], DefaultFieldMapper).map(_.array.toList) shouldBe List[Byte](1, 4, 9).asRight
  }
}

