package com.sksamuel.avro4s

import cats.CommutativeApplicative
import com.sksamuel.avro4s.Decoder.Typeclass
import magnolia.Param
import org.apache.avro.Schema
import cats.syntax.either._

case class DecodingFailure(message: String)

object DecoderHelper {

  type SafeDecode[T] = Either[DecodingFailure, T]

  def tryDecode[T](fieldMapper: FieldMapper, schema: Schema, param: Param[Typeclass, T], value: Any): SafeDecode[param.PType] = {
    val decodeResult: Either[DecodingFailure, param.PType] = util.Try {
      param.typeclass.decode(value, schema.getFields.get(param.index).schema, fieldMapper)
    }.toEither.leftMap(e => DecodingFailure(e.getMessage)).flatMap(identity)
    (decodeResult, param.default) match {
      case (Right(v), _) => v.asRight
      case (Left(DecodingFailure(_)), Some(default)) => default.asRight
      case (Left(DecodingFailure(msg)), _) => DecodingFailure(msg).asLeft
    }
  }

  def safeDecode[T](value: Any, f: Any => T, errorMapper: Throwable => String = _.getMessage): Either[DecodingFailure, T] =
    Either.catchNonFatal {
      f(value)
    }.leftMap(e => DecodingFailure(errorMapper(e)))
}
