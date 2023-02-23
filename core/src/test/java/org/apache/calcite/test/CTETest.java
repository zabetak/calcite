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

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;

/**
 * Sample plans and queries with common table expressions.
 *
 * Proof of concept representation of common table expressions using spools.
 */
public class CTETest {

  private static final SchemaPlus SCHEMA =
      CalciteAssert.addSchema(Frameworks.createRootSchema(true), CalciteAssert.SchemaSpec.HR);
  private static final FrameworkConfig FRAMEWORK_CONFIG = Frameworks.newConfigBuilder()
      .defaultSchema(SCHEMA).build();

  @Test void testPlanFromSQLWithImplicitCTE() throws URISyntaxException, IOException {
    sql(readResource("CTETest/q1.sql")).checkUnchanged();
  }

  @Test void testPlanFromSQLWithExplicitCTE() throws URISyntaxException, IOException {
    sql(readResource("CTETest/q2.sql")).checkUnchanged();
  }

  @Test void testSpoolPlanWithBuilder() {
    relFn(b -> {
      RelNode joinRel = b
          .scan("emps").as("e")
          .scan("depts").as("d")
          .join(JoinRelType.INNER,
              b.equals(b.field(2, 0, "deptno"),
              b.field(2, 1, "deptno")))
          .project(b.field("e", "name"),
              b.field("e", "salary"),
              b.field("d", "name"))
          .build();
      return b
          .push(joinRel)
          .tableSpool("CTE")
          .transientScan("CTE")
          .join(JoinRelType.INNER, b.literal(true))
          .filter(
              b.and(
              b.greaterThan(b.field(4), b.field(1)),
              b.equals(b.field(2), b.literal("Engineering")),
              b.equals(b.field(5), b.literal("Support"))))
          .project(b.field(3), b.field(0)).build();
    }).checkUnchanged();
  }

  /** SchemaSupplier. */
  private static class SchemaSupplier implements RelSupplier {
    private final Function<RelBuilder, RelNode> fn;

    SchemaSupplier(Function<RelBuilder, RelNode> fn) {
      this.fn = fn;
    }

    @Override public RelNode apply(RelOptFixture fixture) {
      return fn.apply(RelBuilder.create(CTETest.FRAMEWORK_CONFIG));
    }

    @Override public RelNode apply2(RelMetadataFixture metadataFixture) {
      return fn.apply(RelBuilder.create(CTETest.FRAMEWORK_CONFIG));
    }
  }

  RelOptFixture sql(String query) {
    return fixture().sql(query);
  }

  RelOptFixture relFn(Function<RelBuilder, RelNode> fn) {
    return fixture().withRelSupplier(new SchemaSupplier(fn));
  }

  RelOptFixture fixture() {
    final CalciteSchema schema = Objects.requireNonNull(SCHEMA.unwrap(CalciteSchema.class));
    CalciteConnectionConfig conf =
        CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
    return Fixtures.forRules()
        .withCatalogReaderFactory(
            (typeFactory, caseSensitive) ->
                new CalciteCatalogReader(schema, Collections.emptyList(), typeFactory, conf))
        .withDiffRepos(DiffRepository.lookup(CTETest.class))
        .withPlanner(new HepPlanner(HepProgram.builder().build()));
  }

  private static String readResource(String s) throws URISyntaxException, IOException {
    Path path = Paths.get(CTETest.class.getResource(s).toURI());
    return String.join("\n", Files.readAllLines(path));
  }
}
