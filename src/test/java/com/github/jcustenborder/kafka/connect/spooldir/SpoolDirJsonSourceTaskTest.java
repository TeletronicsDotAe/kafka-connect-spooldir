/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.spooldir;

import com.google.common.io.Files;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static com.github.jcustenborder.kafka.connect.spooldir.TestEnvironments.json;
import static com.github.jcustenborder.kafka.connect.spooldir.TestEnvironments.schemaGenerationOn;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpoolDirJsonSourceTaskTest extends SpoolDirSourceTaskTest<SpoolDirJsonSourceTask> {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirJsonSourceTaskTest.class);

  @Override
  protected SpoolDirJsonSourceTask createTask() {
    return new SpoolDirJsonSourceTask();
  }

  @Override
  protected void settings(Map<String, String> settings) {
    settings.put(SpoolDirCsvSourceConnectorConfig.CSV_FIRST_ROW_AS_HEADER_CONF, "true");
    settings.put(SpoolDirCsvSourceConnectorConfig.PARSER_TIMESTAMP_DATE_FORMATS_CONF, "yyyy-MM-dd'T'HH:mm:ss'Z'");
    settings.put(SpoolDirCsvSourceConnectorConfig.CSV_NULL_FIELD_INDICATOR_CONF, "BOTH");
  }

  @TestFactory
  public Stream<DynamicTest> poll() throws IOException {
    final String packageName = "json";
    List<TestCase> testCases = loadTestCases(packageName);

    return testCases.stream().map(testCase -> {
      String name = Files.getNameWithoutExtension(testCase.path.toString());
      return dynamicTest(name, () -> {
        poll(packageName, testCase);
      });
    });
  }


  @Test
  public void testThatAutomaticSchemaGenerationWorks() throws InterruptedException {
    Map<String, String> settings = schemaGenerationOn(json());
    Map<String, Object> offset = new HashMap<>();
    SpoolDirJsonSourceTask task = new SpoolDirJsonSourceTask();
    task.initialize(mockedContext(offset));
    task.start(settings);

    setupResourceForConsumption(settings, "json/DataHasMoreFields.data");

    List<SourceRecord> result = task.poll();
    assertEquals(20, result.size());


    result = task.poll();
    assertTrue(result.isEmpty());

    task.stop();
  }

  private SourceTaskContext mockedContext(Map<String, Object> offset) {
    SourceTaskContext result = mock(SourceTaskContext.class);
    OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
    when(offsetStorageReader.offset(anyMap())).thenReturn(offset);
    when(result.offsetStorageReader()).thenReturn(offsetStorageReader);
    return result;
  }

  private void setupResourceForConsumption(Map<String, String> settings, String resourceName) throws InterruptedException {
    TestResourceLoader.loadAndPlace(resourceName,
        Paths.get(settings.get(SpoolDirSourceConnectorConfig.INPUT_PATH_CONFIG), UUID.randomUUID().toString() + ".json"));
    Thread.sleep(Integer.parseInt(settings.get(SpoolDirSourceConnectorConfig.FILE_MINIMUM_AGE_MS_CONF)) * 10);
  }
}
