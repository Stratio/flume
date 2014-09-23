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
package org.apache.flume.sink.jdbc;

import com.google.common.collect.ImmutableMap;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Test;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public abstract class AbstractTemplateJDBCEventSerializerTest {

    private Map<String,String> baseContext() {
        final Map<String,String> ctx = new HashMap<String, String>();
        ctx.put("sql", "INSERT INTO test VALUES (1)");
        return ctx;
    }

    @Test
    public void regularConf() {
        final Configurable configurable = new TemplateJDBCEventSerializer();
        configurable.configure(new Context(baseContext()));
    }

    @Test(expected = ConfigurationException.class)
    public void confMissingSql() {
        Map<String, String> ctxMap = baseContext();
        ctxMap.remove("sql");
        final Configurable configurable = new TemplateJDBCEventSerializer();
        configurable.configure(new Context(ctxMap));
    }

    @Test(expected = ConfigurationException.class)
    public void confBadSQLDialect() {
        Map<String, String> ctxMap = baseContext();
        ctxMap.put("sqlDialect", "_INVALID_");
        final Configurable configurable = new TemplateJDBCEventSerializer();
        configurable.configure(new Context(ctxMap));
    }

    @Test
    public void simpleInsert() {
        Map<String, String> ctxMap = baseContext();
        ctxMap.put("sql", "INSERT INTO \"PUBLIC\".\"test\" (MY_INT_FIELD) VALUES (${header:myIntField:integer})");
        final TemplateJDBCEventSerializer ser = new TemplateJDBCEventSerializer();
        ser.configure(new Context(ctxMap));

        Event e = EventBuilder.withBody(new byte[0], ImmutableMap.of("myIntField", "42"));
        ser.insertEvents(connection(), Arrays.asList(e));

        DSLContext create = DSL.using(connection(), SQLDialect.SQL99);
        List<Result<Record>> results = create.selectFrom(DSL.tableByName("PUBLIC", "test")).fetchMany();
        assertEquals(1, results.size());
        assertEquals(Arrays.asList(42), results.get(0).getValues("my_int_field"));
    }

    protected abstract Connection connection();

}
