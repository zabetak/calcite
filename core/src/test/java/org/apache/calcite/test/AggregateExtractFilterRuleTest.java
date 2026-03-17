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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.apache.calcite.rel.rules.CoreRules.AGGREGATE_EXTRACT_FILTER;
import static org.apache.calcite.rel.rules.CoreRules.AGGREGATE_PROJECT_MERGE;
import static org.apache.calcite.rel.rules.CoreRules.FILTER_PROJECT_TRANSPOSE;

/**
 * Unit tests for {@link AggregateExtractFilterRule}.
 *
 * <p>Relevant tickets:
 * <ul>
 * <li><a href="https://issues.apache.org/jira/browse/CALCITE-XXXX">
 * [CALCITE-XXXX] TODO
 * </a></li>
 * </ul>
 */
class AggregateExtractFilterRuleTest {

  private static RelOptFixture fixture() {
    return RelOptFixture.DEFAULT.withDiffRepos(
        DiffRepository.lookup(AggregateExtractFilterRuleTest.class));
  }

  private static RelOptFixture sql(String sql) {
    return fixture().sql(sql);
  }

  @Test void testSingleFilteredAggregate() {
    String sql = "select sum(sal) filter (where deptno = 10) from emp";
    sql(sql).withPreRule(AGGREGATE_PROJECT_MERGE)
        .withRule(AGGREGATE_EXTRACT_FILTER, FILTER_PROJECT_TRANSPOSE).check();
  }

  @Test void testMultipleFilteredAggregates() {
    String sql = "select "
        + "sum(sal) filter (where deptno = 10), "
        + "avg(sal) filter (where deptno = 20) "
        + "from emp";
    sql(sql).withPreRule(AGGREGATE_PROJECT_MERGE)
        .withRule(AGGREGATE_EXTRACT_FILTER, FILTER_PROJECT_TRANSPOSE).check();
  }

  @Test void testMixNormalAndFilteredAggregates() {
    String sql = "select "
        + "sum(sal) filter (where deptno = 10), "
        + "avg(sal) filter (where deptno = 20), "
        + "count(*) "
        + "from emp";
    sql(sql).withPreRule(AGGREGATE_PROJECT_MERGE)
        .withRule(AGGREGATE_EXTRACT_FILTER, FILTER_PROJECT_TRANSPOSE).checkUnchanged();
  }

  @AfterAll static void checkActualAndReferenceFiles() {
    fixture().diffRepos.checkActualAndReferenceFiles();
  }
}
