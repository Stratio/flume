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

import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public enum DecodeErrorPolicy {
  FAIL {
    @Override
    protected CodingErrorAction toCodingErrorAction() {
      return CodingErrorAction.REPORT;
    }
  },
  REPLACE {
    @Override
    protected CodingErrorAction toCodingErrorAction() {
      return CodingErrorAction.REPLACE;
    }
  },
  IGNORE {
    @Override
    protected CodingErrorAction toCodingErrorAction() {
      return CodingErrorAction.IGNORE;
    }
  };

  /**
   * Creates a new instance of {@link CharsetDecoder}
   * for the given {@link Charset}.
   * The new decoder's behavior concerning malformed input
   * and unmappable characters depends on
   * the enum instance on which this method is called.
   * @param charset The character set to create a decoder for
   * @return a new instance of {@link CharsetDecoder}
   * for the given {@link Charset}.
   */
  public CharsetDecoder newDecoder(Charset charset) {
    CharsetDecoder decoder = charset.newDecoder();
    CodingErrorAction errorAction = toCodingErrorAction();
    decoder.onMalformedInput(errorAction);
    decoder.onUnmappableCharacter(errorAction);
    return decoder;
  }

  protected abstract CodingErrorAction toCodingErrorAction();

}
