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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Path validator to check the paths used in the {@link FilesTableFunction}.
 */
class FilesPathValidator {

  private final List<Path> allowedRoots;

  static FilesPathValidator parse(String commaSeparatedRootPaths) {
    List<Path> paths = Arrays.stream(commaSeparatedRootPaths.split(","))
            .map(Paths::get)
            .map(p -> {
              try {
                return p.toRealPath();
              } catch (IOException e) {
                throw new IllegalArgumentException(
                    "Specified root path cannot be resolved to a real path.", e);
              }
            })
            .collect(Collectors.toList());
    return new FilesPathValidator(paths);
  }

  private FilesPathValidator(List<Path> allowed) {
    this.allowedRoots = allowed;
  }

  /**
   * Checks that {@code path} is valid and will not cause mischief.
   */
  String check(String path) {
    if (path.contains("'")) {
      throw new IllegalArgumentException("Path with single quote characters is not supported");
    }
    if (path.startsWith("-")) {
      throw new IllegalArgumentException("Path with leading dash character is not supported");
    }
    Path realPath;
    try {
      realPath = Paths.get(path).toRealPath();
    } catch (IOException e) {
      // The actual cause is intentionally "hidden" to prevent sensitive
      // information leaking to the callers
      throw new IllegalArgumentException("Invalid path");
    }
    if (allowedRoots.stream().noneMatch(realPath::startsWith)) {
      throw new IllegalArgumentException("Path is not under any of the allowed roots");
    }
    return path;
  }
}
