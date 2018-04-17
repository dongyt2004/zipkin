/**
 * Copyright 2015-2018 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.internal;

import java.util.Map;

/**
 * Everything here assumes the field numbers are less than 16, implying a 1 byte tag.
 */
//@Immutable
final class Proto3Fields {
  /**
   * Define the wire types, except the deprecated ones (groups)
   *
   * <p>See https://developers.google.com/protocol-buffers/docs/encoding#structure
   */
  static final int
    WIRETYPE_VARINT = 0,
    WIRETYPE_FIXED64 = 1,
    WIRETYPE_LENGTH_DELIMITED = 2,
    WIRETYPE_FIXED32 = 5;

  static class Field {
    final int fieldNumber;
    final int wireType;
    /**
     * "Each key in the streamed message is a varint with the value {@code (field_number << 3) | wire_type}"
     *
     * <p>See https://developers.google.com/protocol-buffers/docs/encoding#structure
     */
    final int key;

    Field(int fieldNumber, int wireType) {
      this(fieldNumber, wireType, (fieldNumber << 3) | wireType);
    }

    Field(int fieldNumber, int wireType, int key) {
      this.fieldNumber = fieldNumber;
      this.wireType = wireType;
      this.key = key;
    }
  }

  static abstract class LengthDelimitedField<T> extends Field {
    LengthDelimitedField(int fieldNumber) {
      super(fieldNumber, WIRETYPE_LENGTH_DELIMITED);
    }

    final int sizeInBytes(T value) {
      if (value == null) return 0;
      int sizeOfValue = sizeOfValue(value);
      return sizeOfLengthDelimitedField(sizeOfValue);
    }

    final void write(Buffer b, T value) {
      if (value == null) return;
      int sizeOfValue = sizeOfValue(value);
      if (sizeOfValue == 0) return;
      b.writeByte(key);
      b.writeVarint(sizeOfValue); // length prefix
      writeValue(b, value);
    }

    abstract int sizeOfValue(T value);

    abstract void writeValue(Buffer b, T value);
  }

  static class BytesField extends LengthDelimitedField<byte[]> {
    BytesField(int fieldNumber) {
      super(fieldNumber);
    }

    @Override int sizeOfValue(byte[] bytes) {
      return bytes.length;
    }

    @Override void writeValue(Buffer b, byte[] bytes) {
      b.write(bytes);
    }
  }

  static class HexField extends LengthDelimitedField<String> {
    HexField(int fieldNumber) {
      super(fieldNumber);
    }

    @Override int sizeOfValue(String hex) {
      if (hex == null) return 0;
      return hex.length() / 2;
    }

    @Override void writeValue(Buffer b, String hex) {
      // similar logic to okio.ByteString.decodeHex
      for (int i = 0, length = hex.length(); i < length; i++) {
        int d1 = decodeLowerHex(hex.charAt(i++)) << 4;
        int d2 = decodeLowerHex(hex.charAt(i));
        b.writeByte((byte) (d1 + d2));
      }
    }

    static int decodeLowerHex(char c) {
      if (c >= '0' && c <= '9') return c - '0';
      if (c >= 'a' && c <= 'f') return c - 'a' + 10;
      throw new AssertionError("not lowerHex " + c); // bug
    }
  }

  static class Utf8Field extends LengthDelimitedField<String> {
    Utf8Field(int fieldNumber) {
      super(fieldNumber);
    }

    @Override int sizeOfValue(String utf8) {
      return utf8 != null ? Buffer.utf8SizeInBytes(utf8) : 0;
    }

    @Override void writeValue(Buffer b, String utf8) {
      b.writeUtf8(utf8);
    }
  }

  static final class Fixed64Field extends Field {
    Fixed64Field(int fieldNumber) {
      super(fieldNumber, WIRETYPE_FIXED64);
    }

    void write(Buffer b, long number) {
      if (number == 0) return;
      b.writeByte(key);
      b.writeLongLe(number);
    }

    int sizeInBytes(long number) {
      if (number == 0) return 0;
      return 1 + 8; // tag + 8 byte number
    }
  }

  static final class VarintField extends Field {
    VarintField(int fieldNumber) {
      super(fieldNumber, WIRETYPE_VARINT);
    }

    int sizeInBytes(byte number) {
      return number != 0 ? 2 : 0; // tag + varint
    }

    void write(Buffer b, byte number) {
      if (number == 0) return;
      b.writeByte(key);
      b.writeByte(number);
    }

    int sizeInBytes(int number) {
      return number != 0 ? 1 + Buffer.varintSizeInBytes(number) : 0; // tag + varint
    }

    void write(Buffer b, int number) {
      if (number == 0) return;
      b.writeByte(key);
      b.writeVarint(number);
    }

    int sizeInBytes(long number) {
      return number != 0 ? 1 + Buffer.varintSizeInBytes(number) : 0; // tag + varint
    }

    void write(Buffer b, long number) {
      if (number == 0) return;
      b.writeByte(key);
      b.writeVarint(number);
    }
  }

  static class MapEntryField extends LengthDelimitedField<Map.Entry<String, String>> {
    static final Utf8Field KEY = new Utf8Field(1);
    static final Utf8Field VALUE = new Utf8Field(2);

    MapEntryField(int fieldNumber) {
      super(fieldNumber);
    }

    @Override int sizeOfValue(Map.Entry<String, String> value) {
      return KEY.sizeInBytes(value.getKey()) + VALUE.sizeInBytes(value.getValue());
    }

    @Override void writeValue(Buffer b, Map.Entry<String, String> value) {
      KEY.write(b, value.getKey());
      VALUE.write(b, value.getValue());
    }
  }

  // added for completion as later we will skip fields we don't use
  static final class Fixed32Field extends Field {
    Fixed32Field(int fieldNumber) {
      super(fieldNumber, WIRETYPE_FIXED32);
    }

    int sizeInBytes(int number) {
      if (number == 0) return 0;
      return 1 + 4; // tag + 4 byte number
    }
  }

  static int sizeOfLengthDelimitedField(int sizeInBytes) {
    return 1 + Buffer.varintSizeInBytes(sizeInBytes) + sizeInBytes; // tag + len + bytes
  }
}
