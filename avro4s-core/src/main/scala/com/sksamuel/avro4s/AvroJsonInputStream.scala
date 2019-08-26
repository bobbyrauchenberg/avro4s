package com.sksamuel.avro4s

import java.io.InputStream

import cats.syntax.either._
import com.sksamuel.avro4s.DecoderHelper.SafeDecode
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory

import scala.util.Try

final case class AvroJsonInputStream[T](in: InputStream,
                                        writerSchema: Schema,
                                        readerSchema: Schema,
                                        fieldMapper: FieldMapper = DefaultFieldMapper)
                                       (implicit decoder: Decoder[T]) extends AvroInputStream[T] {

  private val datumReader = new DefaultAwareDatumReader[GenericRecord](writerSchema, readerSchema)
  private val jsonDecoder = DecoderFactory.get.jsonDecoder(writerSchema, in)

  private def next = Try {
    datumReader.read(null, jsonDecoder)
  }

  def iterator: Iterator[SafeDecode[T]] = Iterator.continually(next)
    .takeWhile(_.isSuccess)
    .map(_.get)
    .map(decoder.decode(_, readerSchema, fieldMapper))

  def tryIterator: Iterator[Try[T]] = Iterator.continually(next)
    .takeWhile(_.isSuccess)
    .map(_.get)
    .map(record => decoder.decode(record, readerSchema, fieldMapper).leftMap(e => new RuntimeException(e.message)).toTry)

  def singleEntity: Try[T] = next.flatMap(decoder.decode(_, readerSchema, fieldMapper).leftMap(e => new RuntimeException(e.message)).toTry)

  override def close(): Unit = in.close()
}
