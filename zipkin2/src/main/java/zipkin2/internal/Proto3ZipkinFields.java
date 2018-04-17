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
import java.util.Map;
import zipkin2.Annotation;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.internal.Proto3Fields.MapEntryField;
import zipkin2.internal.Proto3Fields.Utf8Field;

import static zipkin2.internal.Proto3Fields.BytesField;
import static zipkin2.internal.Proto3Fields.Fixed64Field;
import static zipkin2.internal.Proto3Fields.HexField;
import static zipkin2.internal.Proto3Fields.LengthDelimitedField;
import static zipkin2.internal.Proto3Fields.VarintField;

//@Immutable
final class Proto3ZipkinFields {
  /** This is the only field in the ListOfSpans type */
  static final SpanField SPAN = new SpanField();
  static final Endpoint EMPTY_ENDPOINT = Endpoint.newBuilder().build();

  static class EndpointField extends LengthDelimitedField<Endpoint> {
    static final Utf8Field SERVICE_NAME = new Utf8Field(1);
    static final BytesField IPV4 = new BytesField(2);
    static final BytesField IPV6 = new BytesField(3);
    static final VarintField PORT = new VarintField(4);

    EndpointField(int fieldNumber) {
      super(fieldNumber);
    }

    @Override int sizeOfValue(Endpoint value) {
      if (EMPTY_ENDPOINT.equals(value)) return 0;
      int result = 0;
      result += SERVICE_NAME.sizeInBytes(value.serviceName());
      result += IPV4.sizeInBytes(value.ipv4Bytes());
      result += IPV6.sizeInBytes(value.ipv6Bytes());
      result += PORT.sizeInBytes(value.portAsInt());
      return result;
    }

    @Override void writeValue(Buffer b, Endpoint value) {
      SERVICE_NAME.write(b, value.serviceName());
      IPV4.write(b, value.ipv4Bytes());
      IPV6.write(b, value.ipv6Bytes());
      PORT.write(b, value.portAsInt());
    }
  }

  static class AnnotationField extends LengthDelimitedField<Annotation> {
    static final Fixed64Field TIMESTAMP = new Fixed64Field(1);
    static final Utf8Field VALUE = new Utf8Field(2);

    AnnotationField(int fieldNumber) {
      super(fieldNumber);
    }

    @Override int sizeOfValue(Annotation value) {
      return TIMESTAMP.sizeInBytes(value.timestamp()) + VALUE.sizeInBytes(value.value());
    }

    @Override void writeValue(Buffer b, Annotation value) {
      TIMESTAMP.write(b, value.timestamp());
      VALUE.write(b, value.value());
    }
  }

  static class SpanField extends LengthDelimitedField<Span> {
    static final HexField TRACE_ID = new HexField(1);
    static final HexField PARENT_ID = new HexField(2);
    static final HexField ID = new HexField(3);
    static final VarintField KIND = new VarintField(4);
    static final Utf8Field NAME = new Utf8Field(5);
    static final Fixed64Field TIMESTAMP = new Fixed64Field(6);
    static final VarintField DURATION = new VarintField(7);
    static final EndpointField LOCAL_ENDPOINT = new EndpointField(8);
    static final EndpointField REMOTE_ENDPOINT = new EndpointField(9);
    static final AnnotationField ANNOTATION = new AnnotationField(10);
    static final MapEntryField TAG = new MapEntryField(11);
    static final VarintField DEBUG = new VarintField(12);
    static final VarintField SHARED = new VarintField(13);

    SpanField() {
      super(1);
    }

    @Override int sizeOfValue(Span span) {
      int sizeOfSpan = TRACE_ID.sizeInBytes(span.traceId());
      sizeOfSpan += PARENT_ID.sizeInBytes(span.parentId());
      sizeOfSpan += ID.sizeInBytes(span.id());
      sizeOfSpan += KIND.sizeInBytes(span.kind() != null ? 1 : 0);
      sizeOfSpan += NAME.sizeInBytes(span.name());
      sizeOfSpan += TIMESTAMP.sizeInBytes(span.timestampAsLong());
      sizeOfSpan += DURATION.sizeInBytes(span.durationAsLong());
      sizeOfSpan += LOCAL_ENDPOINT.sizeInBytes(span.localEndpoint());
      sizeOfSpan += REMOTE_ENDPOINT.sizeInBytes(span.remoteEndpoint());

      List<Annotation> annotations = span.annotations();
      int annotationCount = annotations.size();
      for (int i = 0; i < annotationCount; i++) {
        sizeOfSpan += ANNOTATION.sizeInBytes(annotations.get(i));
      }

      Map<String, String> tags = span.tags();
      int tagCount = tags.size();
      if (tagCount > 0) { // avoid allocating an iterator when empty
        for (Map.Entry<String, String> entry : tags.entrySet()) {
          sizeOfSpan += TAG.sizeInBytes(entry);
        }
      }

      sizeOfSpan += DEBUG.sizeInBytes(Boolean.TRUE.equals(span.debug()) ? 1 : 0);
      sizeOfSpan += SHARED.sizeInBytes(Boolean.TRUE.equals(span.shared()) ? 1 : 0);
      return sizeOfSpan;
    }

    @Override void writeValue(Buffer b, Span value) {
      TRACE_ID.write(b, value.traceId());
      PARENT_ID.write(b, value.parentId());
      ID.write(b, value.id());
      KIND.write(b, toByte(value.kind()));
      NAME.write(b, value.name());
      TIMESTAMP.write(b, value.timestampAsLong());
      DURATION.write(b, value.durationAsLong());
      LOCAL_ENDPOINT.write(b, value.localEndpoint());
      REMOTE_ENDPOINT.write(b, value.remoteEndpoint());

      List<Annotation> annotations = value.annotations();
      int annotationLength = annotations.size();
      for (int i = 0; i < annotationLength; i++) {
        ANNOTATION.write(b, annotations.get(i));
      }

      Map<String, String> tags = value.tags();
      if (!tags.isEmpty()) { // avoid allocating an iterator when empty
        for (Map.Entry<String, String> entry : tags.entrySet()) {
          TAG.write(b, entry);
        }
      }

      SpanField.DEBUG.write(b, Boolean.TRUE.equals(value.debug()) ? 1 : 0);
      SpanField.SHARED.write(b, Boolean.TRUE.equals(value.shared()) ? 1 : 0);
    }

    // in java, there's no zero index for unknown
    int toByte(Span.Kind kind) {
      return kind != null ? kind.ordinal() + 1 : 0;
    }
  }
}
