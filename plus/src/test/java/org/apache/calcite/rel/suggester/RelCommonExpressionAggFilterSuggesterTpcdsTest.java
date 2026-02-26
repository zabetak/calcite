/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.suggester;

import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.test.RelSuggesterFixture;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Tests for {@link RelCommonExpressionAggFilterSuggester} on TPC-DS queries.
 */
class RelCommonExpressionAggFilterSuggesterTpcdsTest {
  private static RelSuggesterFixture fixture() {
    return RelSuggesterFixture.of(RelCommonExpressionAggFilterSuggesterTpcdsTest.class,
        new RelCommonExpressionAggFilterSuggester()).withSchema(new TpcdsSchema(1));
  }

  private static RelSuggesterFixture query(int num) {
    String sql = getResourceAsString(inputSqlFile(num));
    return fixture().withSubQueryRules().withRules(CoreRules.PROJECT_FILTER_TRANSPOSE).withSql(sql);
  }

  @Test void testQ9() {
    query(9).withSubQueryRules().checkSuggestions();
  }

  @Test void testQ28() {
    query(28).withSubQueryRules().checkSuggestions();
  }

  private static String inputSqlFile(int query) {
    return String.format(Locale.getDefault(), "sql/tpcds/%02d.sql", query);
  }

  private static String getResourceAsString(String name) {
    ClassLoader cl = RelCommonExpressionAggFilterSuggesterTpcdsTest.class.getClassLoader();
    try (InputStream in = cl.getResourceAsStream(name);
        Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(reader)) {
      return bufferedReader.lines().collect(Collectors.joining("\n"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
