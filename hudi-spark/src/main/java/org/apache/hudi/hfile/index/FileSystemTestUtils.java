/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.hfile.index;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public class FileSystemTestUtils {

  public static final String TEMP = "tmp";
  public static final String FORWARD_SLASH = "/";
  public static final String FILE_SCHEME = "file";
  public static final String COLON = ":";
  static final Random RANDOM = new Random();

  /*static Path getRandomOuterInMemPath() {
    String randomFileName = UUID.randomUUID().toString();
    String fileSuffix = COLON + FORWARD_SLASH + TEMP + FORWARD_SLASH + randomFileName;
    return new Path(InMemoryFileSystem.SCHEME + fileSuffix);
  }*/

  public static Path getRandomOuterFSPath() {
    String randomFileName = UUID.randomUUID().toString();
    String fileSuffix = COLON + FORWARD_SLASH + TEMP + FORWARD_SLASH + randomFileName;
    return new Path(FILE_SCHEME + fileSuffix);
  }

  public static Path getRandomPath() {
    String randomFileName = UUID.randomUUID().toString();
    String fileSuffix = FORWARD_SLASH + TEMP + FORWARD_SLASH + randomFileName;
    return new Path(fileSuffix);
  }


  /*
  static Path getPhantomFile(Path outerPath, long startOffset, long inlineLength) {
    // Generate phathom inline file
    return InLineFSUtils.getEmbeddedInLineFilePath(outerPath, FILE_SCHEME, startOffset, inlineLength);
  }
  */

  /**
   * Cleans a directory without deleting it.
   *
   * @param directory directory to clean
   * @throws IOException in case cleaning is unsuccessful
   */
  public static void cleanDirectory(File directory) throws IOException {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (files == null) {  // null if security restricted
        throw new IOException("Failed to list contents of " + directory);
      }

      IOException exception = null;
      for (File file : files) {
        try {
          forceDelete(file);
        } catch (IOException ioe) {
          exception = ioe;
        }
      }

      if (null != exception) {
        throw exception;
      }
    }
  }

  public static void deleteDir(File dir) {
    if (dir.exists()) {
      if (dir.isDirectory()) {
        File[] files = dir.listFiles();
        for (File file : files) {
          deleteDir(file);
        }
      } else {
        dir.delete();
      }
    }
  }

  public static void forceDelete(File file) throws IOException {
    if (file.isDirectory()) {
      deleteDirectory(file);
    } else {
      boolean filePresent = file.exists();
      if (!file.delete()) {
        if (!filePresent) {
          throw new FileNotFoundException("File does not exist: " + file);
        }
        String message =
            "Unable to delete file: " + file;
        throw new IOException(message);
      }
    }
  }

  public static void deleteDirectory(File directory) throws IOException {
    if (!directory.exists()) {
      return;
    }
    if (!directory.delete()) {
      String message =
          "Unable to delete directory " + directory + ".";
      throw new IOException(message);
    }
  }
}
