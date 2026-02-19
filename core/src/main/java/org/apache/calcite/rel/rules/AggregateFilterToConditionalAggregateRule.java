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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;

/**
 * Rule that converts an aggregate on top of a filter into an aggregate with CASE expressions.
 */
@Value.Enclosing public class AggregateFilterToConditionalAggregateRule
    extends RelRule<AggregateFilterToConditionalAggregateRule.Config> {

  private AggregateFilterToConditionalAggregateRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Aggregate aggregate = call.rel(0);
    Filter filter = call.rel(1);
    if (!aggregate.getGroupSet().isEmpty()) {
      // At the moment we only support the transformation for grand totals;
      // aggregates with no grouping keys.
      return;
    }
    RelBuilder relBuilder = call.builder();
    relBuilder.push(filter.getInput());
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    List<AggregateCall> newAggCalls = new ArrayList<>();
    List<RexNode> projects = new ArrayList<>();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (!aggCall.getAggregation().skipsNullInputs()) {
        return;
      }
      RexNode thenExpr = null;
      if (aggCall.getArgList().size() == 1) {
        thenExpr = rexBuilder.makeInputRef(filter, aggCall.getArgList().getFirst());
      } else if (aggCall.getArgList().isEmpty()) {
        // TODO: Code assumes that we are dealing with COUNT(*) but not checking it explicitly
        thenExpr = rexBuilder.makeExactLiteral(BigDecimal.ONE);
      }
      if (thenExpr != null) {
        RexNode elseExpr = rexBuilder.makeNullLiteral(thenExpr.getType());
        RexNode exp = rexBuilder.makeCall(CASE, filter.getCondition(), thenExpr, elseExpr);
        int i = addExpression(exp, projects);
        newAggCalls.add(aggCall.withArgList(Collections.singletonList(i)));
      }
    }
    if (newAggCalls.size() != aggregate.getAggCallList().size()) {
      return;
    }
    relBuilder.project(projects);
    relBuilder.aggregate(relBuilder.groupKey(), newAggCalls);
    call.transformTo(relBuilder.build());
  }

  private static int addExpression(RexNode x, List<RexNode> list) {
    int i = list.indexOf(x);
    if (i == -1) {
      list.add(x);
      i = list.size() - 1;
    }
    return i;
  }

  /** Rule configuration. */
  @Value.Immutable public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateFilterToConditionalAggregateRule.Config.of()
        .withOperandSupplier(
            a -> a.operand(Aggregate.class).oneInput(f -> f.operand(Filter.class).anyInputs()));

    @Override default RelOptRule toRule() {
      return new AggregateFilterToConditionalAggregateRule(this);
    }
  }
}
