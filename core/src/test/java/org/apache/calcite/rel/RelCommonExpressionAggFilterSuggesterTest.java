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
package org.apache.calcite.rel;

import org.apache.calcite.rel.suggester.RelCommonExpressionAggFilterSuggester;
import org.apache.calcite.test.RelSuggesterFixture;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RelCommonExpressionAggFilterSuggester}.
 */
public class RelCommonExpressionAggFilterSuggesterTest {

  private static RelSuggesterFixture fixture() {
    return RelSuggesterFixture.of(RelCommonExpressionAggFilterSuggesterTest.class,
        new RelCommonExpressionAggFilterSuggester());
  }

  private static RelSuggesterFixture sql(String sql) {
    return fixture().withSql(sql);
  }

  @Test void testPattern1() {
    String sql = "select case when (select count(*) \n"
        + "                  from emp \n"
        + "                  where deptno between 1 and 20) > 100000\n"
        + "            then (select avg(sal) \n"
        + "                  from emp \n"
        + "                  where deptno between 1 and 20) \n"
        + "            else (select min(sal)\n"
        + "                  from emp\n"
        + "                  where deptno between 1 and 20) end bucket1,\n"
        + "       case when (select count(*)\n"
        + "                  from emp\n"
        + "                  where deptno between 21 and 40) > 200000\n"
        + "            then (select avg(sal)\n"
        + "                  from emp\n"
        + "                  where deptno between 21 and 40) \n"
        + "            else (select max(sal)\n"
        + "                  from emp\n"
        + "                  where deptno between 21 and 40) end bucket2";
    sql(sql).withSubQueryRules().checkSuggestions();
  }

  @AfterAll static void checkActualAndReferenceFiles() {
    fixture().checkActualAndReferenceFiles();
  }

}
