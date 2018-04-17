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

import org.junit.Test;
import zipkin2.internal.Proto3Fields.Fixed64Field;
import zipkin2.internal.Proto3Fields.MapEntryField;
import zipkin2.internal.Proto3Fields.Utf8Field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;
import static zipkin2.internal.Proto3Fields.Field;
import static zipkin2.internal.Proto3Fields.Fixed32Field;
import static zipkin2.internal.Proto3Fields.WIRETYPE_LENGTH_DELIMITED;

public class Proto3FieldsTest {

  /** Shows we can reliably look at a byte zero to tell if we are decoding proto3 repeated fields. */
  @Test public void field_key_fieldOneLengthDelimited() {
    Field field = new Field(1, WIRETYPE_LENGTH_DELIMITED);
    assertThat(field.key)
      .isEqualTo(0b00001010) // (field_number << 3) | wire_type = 1 << 3 | 2
      .isEqualTo(10); // for sanity of those looking at debugger, 4th bit + 2nd bit = 10
  }

  @Test public void utf8_sizeInBytes() {
    Utf8Field field = new Utf8Field(1);
    assertThat(field.sizeInBytes("12345678"))
      .isEqualTo(0
        + 1 /* tag of string field */ + 1 /* len */ + 8 // 12345678
      );
  }

  /** A map entry is an embedded messages: one for field the key and one for the value */
  @Test public void mapEntry_sizeInBytes() {
    MapEntryField field = new MapEntryField(1);
    assertThat(field.sizeInBytes(entry("123", "56789")))
      .isEqualTo(0
        + 1 /* tag of embedded key field */ + 1 /* len */ + 3
        + 1 /* tag of embedded value field  */ + 1 /* len */ + 5
        + 1 /* tag of map entry field */ + 1 /* len */
      );
  }

  @Test public void fixed64_sizeInBytes() {
    Fixed64Field field = new Fixed64Field(1);
    assertThat(field.sizeInBytes(Long.MIN_VALUE))
      .isEqualTo(9);
  }

  @Test public void fixed32_sizeInBytes() {
    Fixed32Field field = new Fixed32Field(1);
    assertThat(field.sizeInBytes(Integer.MIN_VALUE))
      .isEqualTo(5);
  }
}
