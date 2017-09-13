package com.github.jcustenborder.kafka.connect.spooldir;

import com.google.common.io.Files;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class TestEnvironments {

  public static Map<String, String> csv() {

    File tempDir = Files.createTempDir();
    File input = new File(tempDir, "input");
    File finished = new File(tempDir, "finished");
    File error = new File(tempDir, "error");
    input.mkdir();
    finished.mkdir();
    error.mkdir();

    Map<String, String> result = new HashMap<>();
    result.put(SpoolDirSourceConnectorConfig.INPUT_PATH_CONFIG, input.getAbsolutePath());
    result.put(SpoolDirSourceConnectorConfig.FINISHED_PATH_CONFIG, finished.getAbsolutePath());
    result.put(SpoolDirSourceConnectorConfig.ERROR_PATH_CONFIG, error.getAbsolutePath());
    result.put(SpoolDirSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, ".+\\.csv");
    result.put(SpoolDirSourceConnectorConfig.EMPTY_POLL_WAIT_MS_CONF, "10");
    result.put(SpoolDirSourceConnectorConfig.FILE_MINIMUM_AGE_MS_CONF, "10");

    result.put(SpoolDirSourceConnectorConfig.TOPIC_CONF, "testing");

    result.put(SpoolDirCsvSourceConnectorConfig.CSV_FIRST_ROW_AS_HEADER_CONF, "true");
    result.put(SpoolDirCsvSourceConnectorConfig.PARSER_TIMESTAMP_DATE_FORMATS_CONF, "yyyy-MM-dd'T'HH:mm:ss'Z'");
    result.put(SpoolDirCsvSourceConnectorConfig.CSV_NULL_FIELD_INDICATOR_CONF, "BOTH");

    return result;
  }

  public static Map<String, String> schemaGenerationOn(Map<String, String> settings) {
    Map<String, String> result = new HashMap<>(settings);
    result.put(SpoolDirSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF, "true");
    result.put(SpoolDirSourceConnectorConfig.SCHEMA_GENERATION_KEY_NAME_CONF, "some.key");
    result.put(SpoolDirSourceConnectorConfig.SCHEMA_GENERATION_VALUE_NAME_CONF, "some.value");
    return result;
  }

}
