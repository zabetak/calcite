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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;

import org.immutables.value.Value;

/**
 * Rule for registering aggregate-filter-scan expressions in the registry.
 */
@Value.Enclosing public class AggregateFilterScanRegisterRule
    extends RelRule<AggregateFilterScanRegisterRule.Config> {

  public AggregateFilterScanRegisterRule(Config c) {
    super(c);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Aggregate a = call.rel(0);
    Filter f = call.rel(1);
    TableScan ts = call.rel(2);
    ScanRegistry r = call.getPlanner().getContext().unwrap(ScanRegistry.class);
    r.add(
        ts,
        new ScanRegistry.NodeInfo(
            call.builder().push(f).fields(),
            f.getCondition(),
            a.getGroupSet(),
            a.getAggCallList()));
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        ImmutableAggregateFilterScanRegisterRule.Config.of().withOperandSupplier(
            a -> a.operand(Aggregate.class).oneInput(
                f -> f.operand(Filter.class).oneInput(
                    s -> s.operand(TableScan.class).noInputs())));

    @Override default RelOptRule toRule() {
      return new AggregateFilterScanRegisterRule(this);
    }
  }
}
