package com.sksamuel.avro4s

import cats.syntax.either._
import com.sksamuel.avro4s.DecoderHelper.SafeDecode
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, GenericData}
import org.apache.avro.util.Utf8

protected abstract class SafeFrom[T: Decoder] {
  val decoder: Decoder[T] = implicitly[Decoder[T]]
  def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): SafeDecode[T]
}

object SafeFrom {

  import scala.reflect.runtime.universe._

  def makeSafeFrom[T: Decoder : WeakTypeTag : Manifest]: SafeFrom[T] = {
    import scala.reflect.runtime.universe.typeOf

    val tpe = implicitly[WeakTypeTag[T]].tpe

    if (tpe <:< typeOf[java.lang.String]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): SafeDecode[T] = {
          value match {
            case _: Utf8 => decoder.decode(value, schema, fieldMapper)
            case _: String => decoder.decode(value, schema, fieldMapper)
            case v => DecodingFailure(v.toString).asLeft
          }
        }
      }
    } else if (tpe <:< typeOf[Boolean]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): SafeDecode[T] = {
          value match {
            case true | false => decoder.decode(value, schema, fieldMapper)
            case v => DecodingFailure(v.toString).asLeft
          }
        }
      }
    } else if (tpe <:< typeOf[Int]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): SafeDecode[T] = {
          value match {
            case _: Int => decoder.decode(value, schema, fieldMapper)
            case v => DecodingFailure(v.toString).asLeft
          }
        }
      }
    } else if (tpe <:< typeOf[Long]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): SafeDecode[T] = {
          value match {
            case _: Long => decoder.decode(value, schema, fieldMapper)
            case v => DecodingFailure(v.toString).asLeft
          }
        }
      }
    } else if (tpe <:< typeOf[Double]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): SafeDecode[T] = {
          value match {
            case _: Double => decoder.decode(value, schema, fieldMapper)
            case v => DecodingFailure(v.toString).asLeft
          }
        }
      }
    } else if (tpe <:< typeOf[Float]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): SafeDecode[T] = {
          value match {
            case _: Float => decoder.decode(value, schema, fieldMapper)
            case v => DecodingFailure(v.toString).asLeft
          }
        }
      }
    } else if (tpe <:< typeOf[Array[_]] ||
      tpe <:< typeOf[java.util.Collection[_]] ||
      tpe <:< typeOf[Iterable[_]]) {

      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): SafeDecode[T] = {
          value match {
            case _: GenericData.Array[_] => decoder.decode(value, schema, fieldMapper)
            case v => DecodingFailure(v.toString).asLeft
          }
        }
      }
    } else if (tpe <:< typeOf[java.util.Map[_, _]] ||
      tpe <:< typeOf[Map[_, _]]) {

      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): SafeDecode[T] = {
          value match {
            case _: java.util.Map[_, _] => decoder.decode(value, schema, fieldMapper)
            case v => DecodingFailure(v.toString).asLeft
          }
        }
      }
    } else {
      new SafeFrom[T] {

        private val nameExtractor = NameExtractor(manifest.runtimeClass)
        private val typeName = nameExtractor.fullName

        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): SafeDecode[T] = {
          value match {
            case container: GenericContainer if typeName == container.getSchema.getFullName =>
              val s = schema.getType match {
                case Schema.Type.UNION => SchemaHelper.extractTraitSubschema(nameExtractor.fullName, schema)
                case _ => schema
              }
              decoder.decode(value, s, fieldMapper)
            case v => DecodingFailure(v.toString).asLeft
          }
        }
      }
    }
  }
}
