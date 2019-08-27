package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultFieldMapper}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}
import scala.collection.JavaConverters._
import cats.syntax.either._

class TupleDecoderTest extends FunSuite with Matchers {

  case class Test2(z: (String, Int))
  case class Test2Seq(z: (Seq[String], Int))
  case class Test3(z: (String, Int, Boolean))
  case class Test3Seq(z: (String, Seq[Int], Boolean))
  case class Test4(z: (String, Int, Boolean, Double))
  case class Test4Seq(z: (String, Int, Boolean, Seq[Double]))
  case class Test5(z: (String, Int, Boolean, Double, Long))
  case class Test5Seq(z: (String, Int, Boolean, Double, Seq[Long]))

  test("decode tuple2") {
    val schema = AvroSchema[Test2]
    val z = new GenericData.Record(AvroSchema[(String, Int)])
    z.put("_1", new Utf8("hello"))
    z.put("_2", java.lang.Integer.valueOf(214))
    val record = new GenericData.Record(schema)
    record.put("z", z)
    Decoder[Test2].decode(record, schema, DefaultFieldMapper) shouldBe Test2(("hello", 214)).asRight
  }

  test("decode tuple2 with seq") {
    val schema = AvroSchema[Test2Seq]
    val z = new GenericData.Record(AvroSchema[(Seq[String], Int)])
    z.put("_1", List(new Utf8("hello")).asJava)
    z.put("_2", java.lang.Integer.valueOf(214))
    val record = new GenericData.Record(schema)
    record.put("z", z)
    Decoder[Test2Seq].decode(record, schema, DefaultFieldMapper) shouldBe Test2Seq((Seq("hello"), 214)).asRight
  }

  test("decode tuple3") {
    val schema = AvroSchema[Test3]
    val z = new GenericData.Record(AvroSchema[(String, Int, Boolean)])
    z.put("_1", new Utf8("hello"))
    z.put("_2", java.lang.Integer.valueOf(214))
    z.put("_3", java.lang.Boolean.valueOf(true))
    val record = new GenericData.Record(schema)
    record.put("z", z)
    Decoder[Test3].decode(record, schema, DefaultFieldMapper) shouldBe Test3(("hello", 214, true)).asRight
  }

  test("decode tuple3 with seq") {
    val schema = AvroSchema[Test3Seq]
    val z = new GenericData.Record(AvroSchema[(String, Seq[Int], Boolean)])
    z.put("_1", new Utf8("hello"))
    z.put("_2", List(java.lang.Integer.valueOf(214)).asJava)
    z.put("_3", java.lang.Boolean.valueOf(true))
    val record = new GenericData.Record(schema)
    record.put("z", z)
    Decoder[Test3Seq].decode(record, schema, DefaultFieldMapper) shouldBe Test3Seq(("hello", Seq(214), true)).asRight
  }

  test("decode tuple4") {
    val schema = AvroSchema[Test4]
    val z = new GenericData.Record(AvroSchema[(String, Int, Boolean, Double)])
    z.put("_1", new Utf8("hello"))
    z.put("_2", java.lang.Integer.valueOf(214))
    z.put("_3", java.lang.Boolean.valueOf(true))
    z.put("_4", java.lang.Double.valueOf(56.45))
    val record = new GenericData.Record(schema)
    record.put("z", z)
    Decoder[Test4].decode(record, schema, DefaultFieldMapper) shouldBe Test4(("hello", 214, true, 56.45)).asRight
  }

  test("decode tuple4 with seq") {
    val schema = AvroSchema[Test4Seq]
    val z = new GenericData.Record(AvroSchema[(String, Int, Boolean, Seq[Double])])
    z.put("_1", new Utf8("hello"))
    z.put("_2", java.lang.Integer.valueOf(214))
    z.put("_3", java.lang.Boolean.valueOf(true))
    z.put("_4", List(java.lang.Double.valueOf(56.45)).asJava)
    val record = new GenericData.Record(schema)
    record.put("z", z)
    Decoder[Test4Seq].decode(record, schema, DefaultFieldMapper) shouldBe Test4Seq(("hello", 214, true, Seq(56.45))).asRight
  }

  test("decode tuple5") {
    val schema = AvroSchema[Test5]
    val z = new GenericData.Record(AvroSchema[(String, Int, Boolean, Double, Long)])
    z.put("_1", new Utf8("hello"))
    z.put("_2", java.lang.Integer.valueOf(214))
    z.put("_3", java.lang.Boolean.valueOf(true))
    z.put("_4", java.lang.Double.valueOf(56.45))
    z.put("_5", java.lang.Long.valueOf(9999999999L))
    val record = new GenericData.Record(schema)
    record.put("z", z)
    Decoder[Test5].decode(record, schema, DefaultFieldMapper) shouldBe Test5(("hello", 214, true, 56.45, 9999999999L)).asRight
  }

  test("decode tuple5 with seq") {
    val schema = AvroSchema[Test5Seq]
    val z = new GenericData.Record(AvroSchema[(String, Int, Boolean, Double, Seq[Long])])
    z.put("_1", new Utf8("hello"))
    z.put("_2", java.lang.Integer.valueOf(214))
    z.put("_3", java.lang.Boolean.valueOf(true))
    z.put("_4", java.lang.Double.valueOf(56.45))
    z.put("_5", List(java.lang.Long.valueOf(9999999999L)).asJava)
    val record = new GenericData.Record(schema)
    record.put("z", z)
    Decoder[Test5Seq].decode(record, schema, DefaultFieldMapper) shouldBe Test5Seq(("hello", 214, true, 56.45, Seq(9999999999L))).asRight
  }
}


