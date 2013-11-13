package org.apache.spark.graph.impl

import java.io.{EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.spark.serializer._


/** A special shuffle serializer for VertexBroadcastMessage[Int]. */
class IntVertexBroadcastMsgSerializer extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[VertexBroadcastMsg[Int]]
        writeLong(msg.vid)
        writeInt(msg.data)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      override def readObject[T](): T = {
        new VertexBroadcastMsg[Int](0, readLong(), readInt()).asInstanceOf[T]
      }
    }
  }
}

/** A special shuffle serializer for VertexBroadcastMessage[Long]. */
class LongVertexBroadcastMsgSerializer extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[VertexBroadcastMsg[Long]]
        writeLong(msg.vid)
        writeLong(msg.data)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      override def readObject[T](): T = {
        val a = readLong()
        val b = readLong()
        new VertexBroadcastMsg[Long](0, a, b).asInstanceOf[T]
      }
    }
  }
}

/** A special shuffle serializer for VertexBroadcastMessage[Double]. */
class DoubleVertexBroadcastMsgSerializer extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[VertexBroadcastMsg[Double]]
        writeLong(msg.vid)
        writeDouble(msg.data)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      def readObject[T](): T = {
        val a = readLong()
        val b = readDouble()
        new VertexBroadcastMsg[Double](0, a, b).asInstanceOf[T]
      }
    }
  }
}


/** A special shuffle serializer for AggregationMessage[Int]. */
class IntAggMsgSerializer extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[AggregationMsg[Int]]
        writeLong(msg.vid)
        writeUnsignedVarInt(msg.data)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      override def readObject[T](): T = {
        val a = readLong()
        val b = readUnsignedVarInt()
        new AggregationMsg[Int](a, b).asInstanceOf[T]
      }
    }
  }
}

/** A special shuffle serializer for AggregationMessage[Long]. */
class LongAggMsgSerializer extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[AggregationMsg[Long]]
        writeVarLong(msg.vid, optimizePositive = false)
        writeVarLong(msg.data, optimizePositive = true)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      override def readObject[T](): T = {
        val a = readVarLong(optimizePositive = false)
        val b = readVarLong(optimizePositive = true)
        new AggregationMsg[Long](a, b).asInstanceOf[T]
      }
    }
  }
}


/** A special shuffle serializer for AggregationMessage[Double]. */
class DoubleAggMsgSerializer extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance {

    override def serializeStream(s: OutputStream) = new ShuffleSerializationStream(s) {
      def writeObject[T](t: T) = {
        val msg = t.asInstanceOf[AggregationMsg[Double]]
        writeVarLong(msg.vid, optimizePositive = false)
        writeDouble(msg.data)
        this
      }
    }

    override def deserializeStream(s: InputStream) = new ShuffleDeserializationStream(s) {
      def readObject[T](): T = {
        val a = readVarLong(optimizePositive = false)
        val b = readDouble()
        new AggregationMsg[Double](a, b).asInstanceOf[T]
      }
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
// Helper classes to shorten the implementation of those special serializers.
////////////////////////////////////////////////////////////////////////////////

sealed abstract class ShuffleSerializationStream(s: OutputStream) extends SerializationStream {
  // The implementation should override this one.
  def writeObject[T](t: T): SerializationStream

  def writeInt(v: Int) {
    s.write(v >> 24)
    s.write(v >> 16)
    s.write(v >> 8)
    s.write(v)
  }

  def writeUnsignedVarInt(value: Int) {
    if ((value >>> 7) == 0) {
      s.write(value.toInt)
    } else if ((value >>> 14) == 0) {
      s.write((value & 0x7F) | 0x80)
      s.write(value >>> 7)
    } else if ((value >>> 21) == 0) {
      s.write((value & 0x7F) | 0x80)
      s.write(value >>> 7 | 0x80)
      s.write(value >>> 14)
    } else if ((value >>> 28) == 0) {
      s.write((value & 0x7F) | 0x80)
      s.write(value >>> 7 | 0x80)
      s.write(value >>> 14 | 0x80)
      s.write(value >>> 21)
    } else {
      s.write((value & 0x7F) | 0x80)
      s.write(value >>> 7 | 0x80)
      s.write(value >>> 14 | 0x80)
      s.write(value >>> 21 | 0x80)
      s.write(value >>> 28)
    }
  }

  def writeVarLong(value: Long, optimizePositive: Boolean) {
    val v = if (!optimizePositive) (value << 1) ^ (value >> 63) else value
    if ((v >>> 7) == 0) {
      s.write(v.toInt)
    } else if ((v >>> 14) == 0) {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7).toInt)
    } else if ((v >>> 21) == 0) {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7 | 0x80).toInt)
      s.write((v >>> 14).toInt)
    } else if ((v >>> 28) == 0) {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7 | 0x80).toInt)
      s.write((v >>> 14 | 0x80).toInt)
      s.write((v >>> 21).toInt)
    } else if ((v >>> 35) == 0) {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7 | 0x80).toInt)
      s.write((v >>> 14 | 0x80).toInt)
      s.write((v >>> 21 | 0x80).toInt)
      s.write((v >>> 28).toInt)
    } else if ((v >>> 42) == 0) {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7 | 0x80).toInt)
      s.write((v >>> 14 | 0x80).toInt)
      s.write((v >>> 21 | 0x80).toInt)
      s.write((v >>> 28 | 0x80).toInt)
      s.write((v >>> 35).toInt)
    } else if ((v >>> 49) == 0) {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7 | 0x80).toInt)
      s.write((v >>> 14 | 0x80).toInt)
      s.write((v >>> 21 | 0x80).toInt)
      s.write((v >>> 28 | 0x80).toInt)
      s.write((v >>> 35 | 0x80).toInt)
      s.write((v >>> 42).toInt)
    } else if ((v >>> 56) == 0) {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7 | 0x80).toInt)
      s.write((v >>> 14 | 0x80).toInt)
      s.write((v >>> 21 | 0x80).toInt)
      s.write((v >>> 28 | 0x80).toInt)
      s.write((v >>> 35 | 0x80).toInt)
      s.write((v >>> 42 | 0x80).toInt)
      s.write((v >>> 49).toInt)
    } else {
      s.write(((v & 0x7F) | 0x80).toInt)
      s.write((v >>> 7 | 0x80).toInt)
      s.write((v >>> 14 | 0x80).toInt)
      s.write((v >>> 21 | 0x80).toInt)
      s.write((v >>> 28 | 0x80).toInt)
      s.write((v >>> 35 | 0x80).toInt)
      s.write((v >>> 42 | 0x80).toInt)
      s.write((v >>> 49 | 0x80).toInt)
      s.write((v >>> 56).toInt)
    }
  }

  def writeLong(v: Long) {
    s.write((v >>> 56).toInt)
    s.write((v >>> 48).toInt)
    s.write((v >>> 40).toInt)
    s.write((v >>> 32).toInt)
    s.write((v >>> 24).toInt)
    s.write((v >>> 16).toInt)
    s.write((v >>> 8).toInt)
    s.write(v.toInt)
  }

  //def writeDouble(v: Double): Unit = writeUnsignedVarLong(java.lang.Double.doubleToLongBits(v))
  def writeDouble(v: Double): Unit = writeLong(java.lang.Double.doubleToLongBits(v))

  override def flush(): Unit = s.flush()

  override def close(): Unit = s.close()
}


sealed abstract class ShuffleDeserializationStream(s: InputStream) extends DeserializationStream {
  // The implementation should override this one.
  def readObject[T](): T

  def readInt(): Int = {
    val first = s.read()
    if (first < 0) throw new EOFException
    (first & 0xFF) << 24 | (s.read() & 0xFF) << 16 | (s.read() & 0xFF) << 8 | (s.read() & 0xFF)
  }

  def readUnsignedVarInt(): Int = {
    var value: Int = 0
    var i: Int = 0
    def readOrThrow(): Int = {
      val in = s.read()
      if (in < 0) throw new java.io.EOFException
      in & 0xFF
    }
    var b: Int = readOrThrow()
    while ((b & 0x80) != 0) {
      value |= (b & 0x7F) << i
      i += 7
      if (i > 35) throw new IllegalArgumentException("Variable length quantity is too long")
      b = readOrThrow()
    }
    value | (b << i)
  }

  def readVarLong(optimizePositive: Boolean): Long = {
    // TODO: unroll the while loop.
    var value: Long = 0L
    var i: Int = 0
    def readOrThrow(): Int = {
      val in = s.read()
      if (in < 0) throw new java.io.EOFException
      in & 0xFF
    }
    var b: Int = readOrThrow()
    while ((b & 0x80) != 0) {
      value |= (b & 0x7F).toLong << i
      i += 7
      if (i > 63) throw new IllegalArgumentException("Variable length quantity is too long")
      b = readOrThrow()
    }
    val ret = value | (b.toLong << i)
    if (!optimizePositive) (ret >>> 1) ^ -(ret & 1) else ret
  }

  def readLong(): Long = {
    val first = s.read()
    if (first < 0) throw new EOFException()
    (first.toLong << 56) |
      (s.read() & 0xFF).toLong << 48 |
      (s.read() & 0xFF).toLong << 40 |
      (s.read() & 0xFF).toLong << 32 |
      (s.read() & 0xFF).toLong << 24 |
      (s.read() & 0xFF) << 16 |
      (s.read() & 0xFF) << 8 |
      (s.read() & 0xFF)
  }

  //def readDouble(): Double = java.lang.Double.longBitsToDouble(readUnsignedVarLong())
  def readDouble(): Double = java.lang.Double.longBitsToDouble(readLong())

  override def close(): Unit = s.close()
}


sealed trait ShuffleSerializerInstance extends SerializerInstance {

  override def serialize[T](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def deserialize[T](bytes: ByteBuffer): T = throw new UnsupportedOperationException

  override def deserialize[T](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException

  // The implementation should override the following two.
  override def serializeStream(s: OutputStream): SerializationStream
  override def deserializeStream(s: InputStream): DeserializationStream
}
