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

import java.util.List;
import zipkin2.Span;

import static zipkin2.internal.Proto3Fields.sizeOfLengthDelimitedField;
import static zipkin2.internal.Proto3ZipkinFields.SPAN;

//@Immutable
final class Proto3SpanWriter implements Buffer.Writer<Span> {

  static final byte[] EMPTY_ARRAY = new byte[0];

  @Override public int sizeInBytes(Span span) {
    return SPAN.sizeInBytes(span);
  }

  @Override public void write(Span value, Buffer b) {
    SPAN.write(b, value);
  }

  @Override public String toString() {
    return "Span";
  }

  /** Encodes per ListOfSpans data wireType, where field one is repeated spans */
  public byte[] writeList(List<Span> spans) {
    int lengthOfSpans = spans.size();
    if (lengthOfSpans == 0) return EMPTY_ARRAY;
    if (lengthOfSpans == 1) return write(spans.get(0));

    int sizeInBytes = 0;
    int[] sizeOfValues = new int[lengthOfSpans];
    for (int i = 0; i < lengthOfSpans; i++) {
      int sizeOfValue = sizeOfValues[i] = SPAN.sizeOfValue(spans.get(i));
      sizeInBytes += sizeOfLengthDelimitedField(sizeOfValue);
    }
    Buffer result = new Buffer(sizeInBytes);
    for (int i = 0; i < lengthOfSpans; i++) {
      Span next = spans.get(i);
      result.writeByte(SPAN.key);
      result.writeVarint(sizeOfValues[i]); // length prefix
      SPAN.writeValue(result, next);
    }
    return result.toByteArray();
  }

  byte[] write(Span onlySpan) {
    int sizeOfValue = SPAN.sizeOfValue(onlySpan);
    Buffer result = new Buffer(sizeOfLengthDelimitedField(sizeOfValue));
    write(onlySpan, result);
    return result.toByteArray();
  }

  int writeList(List<Span> spans, byte[] out, int pos) {
    int lengthOfSpans = spans.size();
    if (lengthOfSpans == 0) return 0;

    Buffer result = new Buffer(out, pos);
    for (int i = 0; i < lengthOfSpans; i++) {
      SPAN.write(result, spans.get(i));
    }
    return result.pos - pos;
  }
}
