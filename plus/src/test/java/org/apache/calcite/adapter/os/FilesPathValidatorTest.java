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
package org.apache.calcite.adapter.os;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link FilesPathValidator}.
 */
class FilesPathValidatorTest {

  @Test void testDefaultPathUnderDefaultRoot() {
    FilesPathValidator validator = FilesPathValidator.parse(".");
    assertEquals(".", validator.check("."));
  }

  @Test void testSomePathUnderDefaultRoot() throws IOException {
    FilesPathValidator validator = FilesPathValidator.parse(".");
    try (Stream<Path> files = Files.list(Paths.get("."))) {
      Path p = files.findAny().orElseThrow(AssertionError::new);
      assertEquals(p.toString(), validator.check(p.toString()));
    }
  }

  @Test void testPathNotUnderDefaultRootThrows(@TempDir Path tmpDir) {
    FilesPathValidator validator = FilesPathValidator.parse(".");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> validator.check(tmpDir.toString()));
    assertEquals("Path is not under any of the allowed roots", e.getMessage());
  }

  @Test void testPathUnderRoot(@TempDir Path tmpDir) throws IOException {
    FilesPathValidator validator = FilesPathValidator.parse(tmpDir.toString());
    Path f1 = Files.createFile(tmpDir.resolve("f1.txt"));
    assertEquals(f1.toString(), validator.check(f1.toString()));
  }

  @Test void testPathNotUnderRootThrows(@TempDir Path tmpDir) throws IOException {
    Path r1 = Files.createDirectory(tmpDir.resolve("r1"));
    Path r2 = Files.createDirectory(tmpDir.resolve("r2"));
    FilesPathValidator validator = FilesPathValidator.parse(r1.toString());
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> validator.check(r2.toString()));
    assertEquals("Path is not under any of the allowed roots", e.getMessage());
  }

  @Test void testPathWithSingleQuoteThrows(@TempDir Path tmpDir) {
    FilesPathValidator validator = FilesPathValidator.parse(tmpDir.toString());
    String path = tmpDir.resolve("fi'le.txt").toString();
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> validator.check(path));
    assertEquals("Path with single quote characters is not supported", e.getMessage());
  }

  @Test void testPathWithLeadingDashThrows() {
    FilesPathValidator validator = FilesPathValidator.parse(".");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> validator.check("-file.txt"));
    assertEquals("Path with leading dash character is not supported", e.getMessage());
  }

  @Test void testMissingPathThrows(@TempDir Path tmpDir) {
    FilesPathValidator validator = FilesPathValidator.parse(tmpDir.toString());
    String path = tmpDir.resolve("missing.txt").toString();
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> validator.check(path));
    assertEquals("Invalid path", e.getMessage());
  }

  @Test void testPathWithRelativeNotationUnderRoot(@TempDir Path tmpDir) throws IOException {
    FilesPathValidator validator = FilesPathValidator.parse(tmpDir.toString());
    Path subdir = Files.createDirectory(tmpDir.resolve("subdir"));
    Files.createFile(subdir.resolve("file.txt"));
    Path p = tmpDir.resolve("subdir")
        .resolve("..")
        .resolve("subdir")
        .resolve("file.txt");
    assertEquals(p.toString(), validator.check(p.toString()));
  }

  @Test void testPathsUnderDifferentRoots(@TempDir Path tmpDir) throws IOException {
    Path r1 = Files.createDirectory(tmpDir.resolve("r1"));
    Path r2 = Files.createDirectory(tmpDir.resolve("r2"));
    Path r1f1 = Files.createFile(r1.resolve("r1f1.txt"));
    Path r2f1 = Files.createFile(r2.resolve("r2f1.txt"));
    FilesPathValidator validator = FilesPathValidator.parse(r1 + "," + r2);
    assertEquals(r1f1.toString(), validator.check(r1f1.toString()));
    assertEquals(r2f1.toString(), validator.check(r2f1.toString()));
  }

  @Test void testParseWithMissingRoot() {
    Path r = Paths.get("bogus", "path", "not", "present");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> FilesPathValidator.parse(r.toString()));
    assertEquals("Specified root path cannot be resolved to a real path.", e.getMessage());
  }
}
