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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule to extract a filter from an aggregate and push it down towards the input.
 *
 * <p>Before
 * <pre><code>
 *   SELECT SUM(salary) FILTER (WHERE deptno = 10)
 *   FROM Emp
 *  </code></pre>
 *
 * <p>After
 * <pre><code>
 *   SELECT SUM(salary) FILTER (WHERE deptno = 10)
 *   FROM Emp
 *   WHERE deptno = 10
 *  </code></pre>
 *
 * <p>The transformation is particularly useful since it facilitates
 * the application of other optimizations such as index pushdown,
 * partition pruning, etc.
 */
@Value.Enclosing public class AggregateExtractFilterRule
    extends RelRule<AggregateExtractFilterRule.Config> {

  private AggregateExtractFilterRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Aggregate aggregate = call.rel(0);
    if (!aggregate.getGroupSet().isEmpty()) {
      // At the moment we only support the transformation for grand totals, i.e.,
      // aggregates with no grouping keys.
      return;
    }
    RelBuilder builder = call.builder();
    builder.push(aggregate.getInput());
    List<RexNode> conditions = new ArrayList<>();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      // If there is an aggregate without a filter then we essentially
      // need all rows so we can't push down any filter conditions.
      if (!aggCall.hasFilter()) {
        return;
      }
      conditions.add(builder.field(aggCall.filterArg));
    }
    RexNode newCondition = builder.or(conditions);
    if (newCondition.isAlwaysTrue()) {
      return;
    }
    RelMetadataQuery mq = call.getMetadataQuery();
    RelOptPredicateList predicateList = mq.getPulledUpPredicates(aggregate.getInput());
    if (predicateList.pulledUpPredicates.contains(newCondition)) {
      // If the condition already exists, then we don't need to push it down again.
      return;
    }
    builder.filter(newCondition);
    builder.aggregate(builder.groupKey(), aggregate.getAggCallList());
    call.transformTo(builder.build());
  }

  /** Rule configuration. */
  @Value.Immutable public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateExtractFilterRule.Config.of()
        .withOperandSupplier(
            a -> a.operand(Aggregate.class).anyInputs());

    @Override default AggregateExtractFilterRule toRule() {
      return new AggregateExtractFilterRule(this);
    }
  }
}
