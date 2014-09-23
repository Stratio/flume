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

import org.apache.flume.*;
import org.apache.flume.conf.ConfigurationException;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TemplateJDBCEventSerializer implements JDBCEventSerializer {

    private static final Logger log = LoggerFactory.getLogger(TemplateJDBCEventSerializer.class);

    private static final Pattern PARAMETER_PATTERN = Pattern.compile("\\$\\{(?<PART>[^\\s.{}]+)(?:\\.(?<HEADER>[^\\s.{}]+))?:(?<TYPE>[^\\s.{}]+)\\}");

    private static final String DEFAULT_SQL_DIALECT = "SQL99";
    private static final String CONF_SQL_DIALECT = "sqlDialect";
    private static final String CONF_SQL = "sql";

    private static final String BODY = "BODY";
    private static final String HEADER = "HEADER";
    private static final String PART = "PART";
    private static final String TYPE = "TYPE";

    private SQLDialect sqlDialect;
    private List<Parameter> parameters;
    private String sql;

    public TemplateJDBCEventSerializer() {

    }

    @Override
    public void configure(final Context ctx) {
        sql = ctx.getString(CONF_SQL);
        if (sql == null) {
            throw new ConfigurationException(CONF_SQL + " setting is mandatory");
        }

        try {
            sqlDialect = SQLDialect.valueOf(
                    ctx.getString(CONF_SQL_DIALECT, DEFAULT_SQL_DIALECT)
                            .toUpperCase(Locale.ENGLISH)
            );
        } catch (IllegalArgumentException ex) {
            throw new ConfigurationException("Invalid sqlDialect", ex);
        }

        final Matcher m = PARAMETER_PATTERN.matcher(sql);
        parameters = new ArrayList<>();

        while (m.find()) {
            final String part = m.group(PART).toUpperCase(Locale.ENGLISH);
            final String header = m.group(HEADER);
            final String type = m.group(TYPE).toUpperCase(Locale.ENGLISH);

            DataType<?> dataType = DefaultDataType.getDataType(sqlDialect, type);
            if (BODY.equalsIgnoreCase(part)) {
                if (header != null) {
                    throw new IllegalArgumentException("BODY parameter must have no header name specifier (${body:" + type + "}, not  (${body." + header + ":" + type + "}");
                }
            }
            final Parameter parameter = new Parameter(header, dataType);
            parameters.add(parameter);
            log.trace("Parameter: {}", parameter);
        }

        this.sql = m.replaceAll("?");
        log.debug("Generated SQL: {}", this.sql);
    }

    @Override
    public void insertEvents(final Connection connection, final List<Event> events) {
        final DSLContext dslContext = DSL.using(connection, sqlDialect);
        List<Query> queries = new ArrayList<>();
        for (int i = 0; i < events.size(); i++) {
            final Object[] bindings = new Object[this.parameters.size()];
            for (int j = 0; j < this.parameters.size(); j++) {
                bindings[j] = this.parameters.get(j).binding(events.get(i));
            }
            queries.add(dslContext.query(this.sql, bindings));
        }
        dslContext.batch(queries).execute();
    }

    @Override
    public void close() {

    }

    private static class Parameter {

        private final String header;
        private final DataType<?> dataType;

        public Parameter(final String header, final DataType dataType) {
            this.header = header;
            this.dataType = dataType;
        }

        public Object binding(final Event event) {
            if (header == null) {
                final byte body[] = event.getBody();
                return dataType.convert(body);
            } else {
                final Map<String, String> headers = event.getHeaders();
                for (final Map.Entry<String, String> entry : headers.entrySet()) {
                    if (entry.getKey().equals(header)) {
                        return dataType.convert(entry.getValue());
                    }
                }
            }
            log.trace("No bindable field found for {}", this);
            return null;
        }

        @Override
        public String toString() {
            return com.google.common.base.Objects.toStringHelper(Parameter.class)
                    .add("header", header).add("dataType", dataType).toString();
        }
    }

}
