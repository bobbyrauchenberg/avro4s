package com.sksamuel.avro4s

import java.io.InputStream

import cats.syntax.either._
import com.sksamuel.avro4s.DecoderHelper.SafeDecode
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.DatumReader

import scala.util.Try

class AvroDataInputStream[T](in: InputStream,
                             writerSchema: Option[Schema],
                             readerSchema: Option[Schema],
                             fieldMapper: FieldMapper = DefaultFieldMapper)
                            (implicit decoder: Decoder[T]) extends AvroInputStream[T] {

  // if no reader or writer schema is specified, then we create a reader that uses what's present in the files
  private val datumReader = (writerSchema, readerSchema) match {
    case (None, None) => GenericData.get.createDatumReader(null)
    case (Some(writer), None) => GenericData.get.createDatumReader(writer)
    case (None, Some(reader)) => GenericData.get.createDatumReader(reader)
    case (Some(writer), Some(reader)) => GenericData.get.createDatumReader(writer, reader)
  }

  private val dataFileReader = new DataFileStream[GenericRecord](in, datumReader.asInstanceOf[DatumReader[GenericRecord]])

  override def iterator: Iterator[SafeDecode[T]] = new Iterator[SafeDecode[T]] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): SafeDecode[T] = {
      val record = dataFileReader.next
      decoder.decode(record, readerSchema.getOrElse(record.getSchema), fieldMapper)
    }
  }

  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): Try[T] = {
      val record = dataFileReader.next
      decoder.decode(record, readerSchema.getOrElse(record.getSchema), fieldMapper).leftMap(e => new RuntimeException(e.message)).toTry
    }

  }

  override def close(): Unit = in.close()
}
