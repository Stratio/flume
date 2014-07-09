/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.serialization;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.codec.binary.Hex;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * A deserializer that parses Avro container files, generating one Flume event
 * per record in the Avro file, and storing binary avro-encoded records in
 * the Flume event body.
 */
public class AvroEventDeserializer implements EventDeserializer {

  private static final Logger logger = LoggerFactory.getLogger
      (AvroEventDeserializer.class);

  private final AvroSchemaType schemaType;
  private final InputStream in;

  private Schema schema;
  private String schemaHashString;
  private DataFileReader<GenericRecord> fileReader;
  private GenericDatumWriter<GenericRecord> datumWriter;
  private GenericRecord record;
  private ByteArrayOutputStream out;
  private BinaryEncoder encoder;

  @VisibleForTesting
  public static enum AvroSchemaType {
    HASH,
    LITERAL;
  }

  public static final String CONFIG_SCHEMA_TYPE_KEY = "schemaType";
  public static final String AVRO_SCHEMA_HEADER_HASH
      = "flume.avro.schema.hash";
  public static final String AVRO_SCHEMA_HEADER_LITERAL
      = "flume.avro.schema.literal";

  private AvroEventDeserializer(Context context, InputStream in) {
    this.in = in;
    schemaType = AvroSchemaType.valueOf(
        context.getString(CONFIG_SCHEMA_TYPE_KEY,
            AvroSchemaType.HASH.toString()).toUpperCase());
    if (schemaType == AvroSchemaType.LITERAL) {
      logger.warn(CONFIG_SCHEMA_TYPE_KEY + " set to " +
          AvroSchemaType.LITERAL.toString() + ", so storing full Avro " +
          "schema in the header of each event, which may be inefficient. " +
          "Consider using the hash of the schema " +
          "instead of the literal schema.");
    }
  }

  private void initialize() throws IOException, NoSuchAlgorithmException {
    SeekableInputBridge in = new SeekableInputBridge(this.in);
    long pos = in.tell();
    in.seek(0L);
    fileReader = new DataFileReader<GenericRecord>(in,
        new GenericDatumReader<GenericRecord>());
    fileReader.sync(pos);

    schema = fileReader.getSchema();
    datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    out = new ByteArrayOutputStream();
    encoder = EncoderFactory.get().binaryEncoder(out, encoder);

    byte[] schemaHash = SchemaNormalization.parsingFingerprint("CRC-64-AVRO", schema);
    schemaHashString = Hex.encodeHexString(schemaHash);
  }

  @Override
  public Event readEvent() throws IOException {
    if (fileReader.hasNext()) {
      record = fileReader.next(record);
      out.reset();
      datumWriter.write(record, encoder);
      encoder.flush();
      // annotate header with 64-bit schema CRC hash in hex
      Event event = EventBuilder.withBody(out.toByteArray());
      if (schemaType == AvroSchemaType.HASH) {
        event.getHeaders().put(AVRO_SCHEMA_HEADER_HASH, schemaHashString);
      } else {
        event.getHeaders().put(AVRO_SCHEMA_HEADER_LITERAL, schema.toString());
      }
      return event;
    }
    return null;
  }

  @Override
  public List<Event> readEvents(int numEvents) throws IOException {
    List<Event> events = Lists.newArrayList();
    for (int i = 0; i < numEvents && fileReader.hasNext(); i++) {
      Event event = readEvent();
      if (event != null) {
        events.add(event);
      }
    }
    return events;
  }

  @Override
  public void mark() throws IOException {
    long pos = fileReader.previousSync() - DataFileConstants.SYNC_SIZE;
    if (pos < 0) pos = 0;
    ((Seekable) in).mark(pos);
  }

  @Override
  public void reset() throws IOException {
    long pos = ((Seekable) in).point();
    fileReader.sync(pos);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  public static class Builder implements EventDeserializer.Builder {

    @Override
    public EventDeserializer build(Context context, InputStream in) {
      if (!(in instanceof Seekable)) {
        throw new IllegalArgumentException("Cannot use this deserializer " +
            "without a Seekable input stream");
      }
      AvroEventDeserializer deserializer
          = new AvroEventDeserializer(context, in);
      try {
        deserializer.initialize();
      } catch (Exception e) {
        throw new FlumeException("Cannot instantiate deserializer", e);
      }
      return deserializer;
    }
  }

  private static class SeekableInputBridge implements SeekableInput {

    InputStream in;

    public SeekableInputBridge(InputStream in) {
      this.in = in;
    }

    @Override
    public void seek(long p) throws IOException {
      ((Seekable) in).seek(p);
    }

    @Override
    public long tell() throws IOException {
      return ((Seekable) in).tell();
    }

    @Override
    public long length() throws IOException {
      return ((Seekable) in).length();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return in.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }

}
