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
package org.apache.calcite.test;

import org.apache.calcite.rel.rules.AggregateFilterToConditionalAggregateRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.util.Optionality;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link org.apache.calcite.rel.rules.AggregateFilterToConditionalAggregateRule}.
 */
class AggregateFilterToConditionalAggregateRuleTest {

  private static RelOptFixture fixture() {
    return RelOptFixture.DEFAULT.withDiffRepos(
        DiffRepository.lookup(AggregateFilterToConditionalAggregateRuleTest.class));
  }

  private static RelOptFixture sql(String sql) {
    return fixture().sql(sql);
  }

  @Test void testSumAggregateWithFilter() {
    String sql = "select sum(sal) from emp where deptno = 10";
    sql(sql).withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(AggregateFilterToConditionalAggregateRule.Config.DEFAULT.toRule()).check();
  }

  @Test void testCountStarAggregateWithFilter() {
    String sql = "select count(*) from emp where deptno = 10";
    sql(sql).withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(AggregateFilterToConditionalAggregateRule.Config.DEFAULT.toRule()).check();
  }

  @Test void testMultiAggregatesWithFilter() {
    String sql = "select sum(sal), min(sal), max(sal), count(*) from emp where deptno = 10";
    sql(sql).withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(AggregateFilterToConditionalAggregateRule.Config.DEFAULT.toRule()).check();
  }

  @Test void testNullAwareAggregateWithFilter() {
    String sql = "select null_aware_sum(sal) from emp where deptno = 10";
    sql(sql).withFactory(
        t -> t.withOperatorTable(
            tbl -> SqlOperatorTables.of(SqlStdOperatorTable.EQUALS, nullAwareSum())))
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(AggregateFilterToConditionalAggregateRule.Config.DEFAULT.toRule())
        .checkUnchanged();
  }

  @Test void testSumAggregateWithGroupByOnColumnAndFilter() {
    String sql = "select sum(sal) from emp where deptno = 10 group by job";
    sql(sql).withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(AggregateFilterToConditionalAggregateRule.Config.DEFAULT.toRule())
        .checkUnchanged();
  }

  @Test void testSumAggregateWithGroupingSetsAndFilter() {
    String sql =
        "select sum(sal) from emp where deptno = 10 group by grouping sets ((job), (ename))";
    sql(sql).withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(AggregateFilterToConditionalAggregateRule.Config.DEFAULT.toRule())
        .checkUnchanged();
  }

  @Test void testSumAggregateWithEmptyGroupByAndFilter() {
    String sql = "select sum(sal) from emp where deptno = 10 group by ()";
    sql(sql).withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(AggregateFilterToConditionalAggregateRule.Config.DEFAULT.toRule()).check();
  }

  @Test void testSumAggregateWithEmptyGroupingSetsAndFilter() {
    String sql = "select sum(sal) from emp where deptno = 10 group by grouping sets (())";
    sql(sql).withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(AggregateFilterToConditionalAggregateRule.Config.DEFAULT.toRule()).check();
  }

  private static SqlOperator nullAwareSum() {
    return new SqlAggFunction("NULL_AWARE_SUM", null, SqlKind.SUM, ReturnTypes.ARG0_NULLABLE, null,
        OperandTypes.NUMERIC, SqlFunctionCategory.NUMERIC, false, false,
        Optionality.FORBIDDEN) {
      @Override public boolean skipsNullInputs() {
        return false; // NULL values are semantically relevant
      }
    };
  }

  @AfterAll static void checkActualAndReferenceFiles() {
    fixture().diffRepos.checkActualAndReferenceFiles();
  }
}
