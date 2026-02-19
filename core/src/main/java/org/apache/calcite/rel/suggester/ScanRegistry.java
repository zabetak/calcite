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

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Registry for collecting information around table scans.
 */
public class ScanRegistry {
  private final Map<String, List<NodeInfo>> tableToInfo = new HashMap<>();
  private final Map<String, TableScan> nameToScan = new HashMap<>();

  public void add(TableScan ts, NodeInfo nodeInfo) {
    String name = String.join(".", ts.getTable().getQualifiedName());
    List<NodeInfo> infos = tableToInfo.getOrDefault(name, new ArrayList<>());
    infos.add(nodeInfo);
    tableToInfo.put(name, infos);
    nameToScan.put(name, ts);
  }

  /**
   * Information about a particular node.
   */
  public static class NodeInfo {
    public final List<RexNode> project;
    public final RexNode filterCondition;
    public final ImmutableBitSet groupSet;
    public final List<AggregateCall> aggCalls;

    public NodeInfo(List<RexNode> project, RexNode filterCondition, ImmutableBitSet groupSet,
        List<AggregateCall> aggCalls) {
      this.project = project;
      this.filterCondition = filterCondition;
      this.groupSet = groupSet;
      this.aggCalls = aggCalls;
    }
  }

  public Stream<String> names() {
    return nameToScan.keySet().stream();
  }

  public List<NodeInfo> getInfo(String name) {
    return tableToInfo.getOrDefault(name, Collections.emptyList());
  }

  public TableScan getScan(String name) {
    return nameToScan.get(name);
  }
}
