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
package org.apache.calcite.examples.foodmart.java;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/**
 * A template for filling in the end to end example.
 */
public class EndToEndExampleBindableTemplate {

  private static final List<Object[]> AUTHOR_DATA = Arrays.asList(
      new Object[]{0, "Victor", "Hugo"},
      new Object[]{1, "Alexandre", "Dumas"}
  );

  private static final List<Object[]> BOOK_DATA = Arrays.asList(
      new Object[]{1, "Les Miserables", 1862, 0},
      new Object[]{2, "The Hunchback of Notre-Dame", 1829, 0},
      new Object[]{3, "The Last Day of a Condemned Man", 1829, 0},
      new Object[]{4, "The three Musketeers", 1844, 1},
      new Object[]{5, "The Count of Monte Cristo", 1884, 1}
  );

  @Test
  public void example() throws Exception {
    // Instantiate a type factory for creating types (e.g., VARCHAR, NUMERIC, etc.)
    // Create the root schema describing the data model
    // Define type for authors table
    // Initialize authors table with data
    // Add authors table to the schema
    // Define type for books table
    // Initialize books table with data
    // Add books table to the schema
    // Create an SQL parser
    // Parse the query into an AST
    // Configure and instantiate validator
    // Validate the initial AST
    // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster
    // Convert the valid AST into a logical plan
    // Display the logical plan
    // Initialize optimizer/planner with the necessary rules
    // Define the type of the output plan (in this case we want a physical plan in
    // BindableConvention)
    // Start the optimization process to obtain the most efficient physical plan based on the
    // provided rule set.
    // Display the physical plan
    // Run the executable plan using a context simply providing access to the schema
  }

  /**
   * A simple table based on a list.
   */
  private static class ListTable extends AbstractTable implements ScannableTable {
    private final RelDataType rowType;
    private final List<Object[]> data;

    ListTable(RelDataType rowType, List<Object[]> data) {
      this.rowType = rowType;
      this.data = data;
    }

    @Override public Enumerable<Object[]> scan(final DataContext root) {
      return Linq4j.asEnumerable(data);
    }

    @Override public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
      return rowType;
    }
  }

  private static RelOptCluster newCluster(RelDataTypeFactory factory) {
    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(factory));
  }

  private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath
      , viewPath) -> null;

  /**
   * A simple data context only with schema information.
   */
  private static final class SchemaOnlyDataContext implements DataContext {
    private final SchemaPlus schema;

    SchemaOnlyDataContext(CalciteSchema calciteSchema) {
      this.schema = calciteSchema.plus();
    }

    @Override public SchemaPlus getRootSchema() {
      return schema;
    }

    @Override public JavaTypeFactory getTypeFactory() {
      return new JavaTypeFactoryImpl();
    }

    @Override public QueryProvider getQueryProvider() {
      return null;
    }

    @Override public Object get(final String name) {
      return null;
    }
  }
}

// End EndToEndExampleBindableTemplate.java
