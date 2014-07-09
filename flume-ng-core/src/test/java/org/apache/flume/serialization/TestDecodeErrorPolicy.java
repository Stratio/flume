/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.serialization;

import com.google.common.base.Charsets;
import org.junit.Test;

import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;

import static org.junit.Assert.assertEquals;

public class TestDecodeErrorPolicy {

  @Test
  public void testNewDecoderFail() {
    CharsetDecoder decoder = DecodeErrorPolicy.FAIL.newDecoder(Charsets.UTF_8);
    assertEquals(Charsets.UTF_8, decoder.charset());
    assertEquals(CodingErrorAction.REPORT, decoder.malformedInputAction());
    assertEquals(CodingErrorAction.REPORT, decoder.unmappableCharacterAction());
  }

  @Test
  public void testNewDecoderIgnore() {
    CharsetDecoder decoder = DecodeErrorPolicy.IGNORE.newDecoder(Charsets.UTF_8);
    assertEquals(Charsets.UTF_8, decoder.charset());
    assertEquals(CodingErrorAction.IGNORE, decoder.malformedInputAction());
    assertEquals(CodingErrorAction.IGNORE, decoder.unmappableCharacterAction());
  }

  @Test
  public void testNewDecoderReplace() {
    CharsetDecoder decoder = DecodeErrorPolicy.REPLACE.newDecoder(Charsets.UTF_8);
    assertEquals(Charsets.UTF_8, decoder.charset());
    assertEquals(CodingErrorAction.REPLACE, decoder.malformedInputAction());
    assertEquals(CodingErrorAction.REPLACE, decoder.unmappableCharacterAction());
  }

}
