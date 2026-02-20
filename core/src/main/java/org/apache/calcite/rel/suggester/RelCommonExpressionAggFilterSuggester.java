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

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCommonExpressionSuggester;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.AggregateFilterToConditionalAggregateRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Suggester for combining aggregate filter expressions.
 */
public class RelCommonExpressionAggFilterSuggester implements RelCommonExpressionSuggester {

  private final List<RelNode> suggestions = new ArrayList<>();

  @Override public List<RelNode> suggest(RelNode input, @Nullable Context context) {
    HepProgramBuilder b = new HepProgramBuilder();
    b.addRuleCollection(
        Arrays.asList(CoreRules.PROJECT_MERGE, CoreRules.FILTER_PROJECT_TRANSPOSE,
        CoreRules.AGGREGATE_PROJECT_MERGE));
    b.addRuleCollection(
        Arrays.asList(AggregateFilterScanRegisterRule.Config.DEFAULT.toRule(),
        AggregateFilterToConditionalAggregateRule.Config.DEFAULT.toRule(),
        AggregateProjectScanRegisterRule.Config.DEFAULT.toRule()));
    ScanRegistry sr = new ScanRegistry();
    HepPlanner planner = new HepPlanner(b.build(), Contexts.of(sr));
    planner.setRoot(input);
    planner.findBestExp();
    RelBuilder builder = RelFactories.LOGICAL_BUILDER.create(input.getCluster(), null);
    sr.names().forEach(name -> {
      List<ScanRegistry.NodeInfo> info = sr.getInfo(name);
      TableScan scan = sr.getScan(name);
      Map<RexNode, List<ScanRegistry.NodeInfo>> filterToAggregate = new HashMap<>();

      for (ScanRegistry.NodeInfo nodeInfo : info) {
        List<ScanRegistry.NodeInfo> filterAggs =
            filterToAggregate.getOrDefault(nodeInfo.filterCondition, new ArrayList<>());
        filterAggs.add(nodeInfo);
        filterToAggregate.put(nodeInfo.filterCondition, filterAggs);
      }
      filterToAggregate.forEach((key, value) -> {
        builder.clear();
        builder.push(scan);
        builder.filter(key);
        ImmutableBitSet groupSet = ImmutableBitSet.of();
        Set<AggregateCall> aggCalls = new HashSet<>();
        List<RexNode> projects = new ArrayList<>();
        for (ScanRegistry.NodeInfo nodeInfo : value) {
          // TODO Decide what to do about the groupSets
          ImmutableBitSet.Builder newGroupSet = ImmutableBitSet.builder();
          for (int groupKey : nodeInfo.groupSet) {
            RexNode p = nodeInfo.project.get(groupKey);
            int ix = projects.indexOf(p);
            if (ix < 0) {
              ix = projects.size();
              projects.add(p);
            }
            newGroupSet.set(ix);
          }
          // TODO Do we want the union or something smarter?
          groupSet = groupSet.union(newGroupSet.build());
          for (AggregateCall c : nodeInfo.aggCalls) {
            List<Integer> remappedArgs = new ArrayList<>();
            for (int i : c.getArgList()) {
              RexNode p = nodeInfo.project.get(i);
              int ix = projects.indexOf(p);
              if (ix < 0) {
                ix = projects.size();
                projects.add(p);
              }
              remappedArgs.add(ix);
            }
            aggCalls.add(c.withArgList(remappedArgs));
          }
        }
        if (!projects.isEmpty()) {
          builder.project(projects);
        }
        // Sort the aggregate calls to avoid generating different suggestions based on the order
        // in which we see the aggregates in the original plan
        List<AggregateCall> sortedAggCalls =
            aggCalls.stream().sorted(Comparator.comparing(AggregateCall::toString))
                .collect(Collectors.toList());
        builder.aggregate(builder.groupKey(groupSet), sortedAggCalls);
        addSuggestion(builder.build());
      });
      // Create a big disjunctive filter over the table scan
      builder.clear();
      builder.push(scan);
      List<RexNode> interestingFilters = new ArrayList<>(filterToAggregate.keySet());
      interestingFilters.removeIf(RexNode::isAlwaysTrue);
      builder.filter(builder.or(interestingFilters));
      addSuggestion(builder.build());
      // TODO: Check if we want to have the conditional aggregate with big disjunctive filter
    });
    return suggestions;
  }

  private void addSuggestion(RelNode rel) {
    suggestions.add(rel);
  }
}
