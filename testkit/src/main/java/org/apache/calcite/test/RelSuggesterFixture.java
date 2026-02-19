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

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCommonExpressionSuggester;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * A fixture for testing implementations of the {@link RelCommonExpressionSuggester} API.
 */
public class RelSuggesterFixture {

  /** Creates the default fixture for a given test class and suggester implementation. */
  public static RelSuggesterFixture of(Class<?> clazz, RelCommonExpressionSuggester suggester) {
    return new RelSuggesterFixture("?", suggester, DiffRepository.lookup(clazz), defaultSchema(),
        null);
  }

  private final String sql;
  private final RelCommonExpressionSuggester suggester;
  private final DiffRepository diffRepo;
  private final SchemaPlus schema;
  private final HepProgram program;

  private RelSuggesterFixture(String sql, RelCommonExpressionSuggester suggester,
      DiffRepository diffRepo, SchemaPlus schema, HepProgram program) {
    this.sql = requireNonNull(sql, "sql");
    this.suggester = requireNonNull(suggester, "suggester");
    this.diffRepo = requireNonNull(diffRepo, "diffRepo");
    this.schema = requireNonNull(schema, "schema");
    this.program = program;
  }

  /** Creates a copy of this fixture that uses a given SQL query. */
  public RelSuggesterFixture withSql(String sql) {
    return new RelSuggesterFixture(sql, suggester, diffRepo, schema, program);
  }

    /** Creates a copy of this fixture that uses a given schema. */
  public RelSuggesterFixture withSchema(Schema schema) {
    SchemaPlus root = CalciteSchema.createRootSchema(false, false).plus();
    Lookup<Table> tables = schema.tables();
    tables.getNames(LikePattern.any()).forEach(t -> root.add(t, tables.get(t)));
    return new RelSuggesterFixture(sql, suggester, diffRepo, root, program);
  }

  /** Applies the sub-query to correlate rules before running the suggester. */
  public RelSuggesterFixture withSubQueryRules() {
    HepProgramBuilder newProgram = HepProgram.builder();
    if (program != null) {
      newProgram.addSubprogram(program);
    }
    List<RelOptRule> rules = new ArrayList<>();
    rules.add(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE);
    rules.add(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE);
    rules.add(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
    newProgram.addRuleCollection(rules);
    return new RelSuggesterFixture(sql, suggester, diffRepo, schema, newProgram.build());
  }

  public RelSuggesterFixture withRules(RelOptRule... rules) {
    HepProgramBuilder newProgram = HepProgram.builder();
    if (program != null) {
      newProgram.addSubprogram(program);
    }
    newProgram.addRuleCollection(Arrays.stream(rules).collect(Collectors.toList()));
    return new RelSuggesterFixture(sql, suggester, diffRepo, schema, newProgram.build());
  }

  /**
   * Checks that the suggester returns the expected plans for the specified SQL statement.
   *
   * <p>The expected suggestions are defined in a reference file handled by the provided
   * DiffRepository.
   */
  public void checkSuggestions() {
    RelNode rel = toRel(sql);
    AtomicInteger i = new AtomicInteger();
    suggester.suggest(rel, null).stream().map(RelOptUtil::toString).sorted().forEach(plan -> {
      String tag = "suggestion_" + i.getAndIncrement();
      diffRepo.assertEquals(tag, "${" + tag + "}", plan);
    });
  }

  /** Checks that the actual and reference file are consistent. */
  public void checkActualAndReferenceFiles() {
    diffRepo.checkActualAndReferenceFiles();
  }

  private RelNode toRel(String sql) {
    Planner planner =
        Frameworks.getPlanner(Frameworks.newConfigBuilder().defaultSchema(schema).build());
    try {
      RelNode rel = planner.rel(planner.validate(planner.parse(sql))).rel;
      if (program != null) {
        HepPlanner hep = new HepPlanner(program);
        hep.setRoot(rel);
        rel = hep.findBestExp();
      }
      return rel;
    } catch (SqlParseException | RelConversionException | ValidationException e) {
      throw new RuntimeException(e);
    }
  }

  private static SchemaPlus defaultSchema() {
    SchemaPlus schema = CalciteSchema.createRootSchema(false, false).plus();
    schema.add("EMP", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("EMPNO", SqlTypeName.INTEGER)
            .add("ENAME", SqlTypeName.VARCHAR)
            .add("DEPTNO", SqlTypeName.INTEGER)
            .add("SAL", SqlTypeName.INTEGER)
            .build();
      }
    });
    schema.add("DEPT", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("DEPTNO", SqlTypeName.INTEGER)
            .add("DNAME", SqlTypeName.VARCHAR)
            .add("EMPNO", SqlTypeName.INTEGER)
            .build();
      }
    });
    return schema;
  }
}
