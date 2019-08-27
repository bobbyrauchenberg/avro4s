package com.sksamuel.avro4s

import com.sksamuel.avro4s.DecoderHelper.SafeDecode
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

/**
  * Brings together [[ToRecord]] and [[FromRecord]] in a single interface.
  */
trait RecordFormat[T] extends ToRecord[T] with FromRecord[T] with Serializable

/**
  * Returns a [[RecordFormat]] that will convert to/from
  * instances of T and avro Record's.
  */
object RecordFormat {

  def apply[T: Encoder : Decoder : SchemaFor]: RecordFormat[T] = apply(AvroSchema[T])

  def apply[T: Encoder : Decoder](schema: Schema)(implicit fieldMapper: FieldMapper = DefaultFieldMapper): RecordFormat[T] = new RecordFormat[T] {
    private val fromRecord = FromRecord[T](schema)
    private val toRecord = ToRecord[T](schema)
    override def from(record: IndexedRecord): SafeDecode[T] = fromRecord.from(record)
    override def to(t: T): Record = toRecord.to(t)
  }
}