package com.github.jcustenborder.kafka.connect.spooldir;

import com.google.common.io.Files;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class SpoolDirSourceConnectorConfigTest {
  protected Map<String, String> settings;

  @BeforeEach
  public void createTempDir() {
    File tempRoot = Files.createTempDir();
    File inputPath = new File(tempRoot, "input");
    inputPath.mkdirs();
    File finishedPath = new File(tempRoot, "finished");
    finishedPath.mkdirs();
    File errorPath = new File(tempRoot, "error");
    errorPath.mkdirs();

    settings = new LinkedHashMap<>();
    settings.put(SpoolDirSourceConnectorConfig.INPUT_PATH_CONFIG, inputPath.getAbsolutePath());
    settings.put(SpoolDirSourceConnectorConfig.FINISHED_PATH_CONFIG, finishedPath.getAbsolutePath());
    settings.put(SpoolDirSourceConnectorConfig.ERROR_PATH_CONFIG, errorPath.getAbsolutePath());
    settings.put(SpoolDirSourceConnectorConfig.TOPIC_CONF, "dummy-topic");
    settings.put(SpoolDirSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, "dummy-pattern");
    settings.put(SpoolDirSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF, "true");
  }

  @Test
  public void undefinedPathsInConfig() {
    settings.remove(SpoolDirSourceConnectorConfig.INPUT_PATH_CONFIG);
    SpoolDirSourceConnector connector = createConnector();

    Config config = connector.validate(settings);
    ConfigValue inputPathConfig = config.configValues().stream().
        filter(configValue -> configValue.name().equals(SpoolDirSourceConnectorConfig.INPUT_PATH_CONFIG)).
        findFirst().get();
    assertEquals(2, inputPathConfig.errorMessages().size(),
        "Error message for both 'undefined input path' and 'non-existing directory' should be generated");
  }

  @Test
  public void nonExistingPathsInConfig() {
    settings.put(SpoolDirSourceConnectorConfig.INPUT_PATH_CONFIG, "nonExisting");
    SpoolDirSourceConnector connector = createConnector();

    Config config = connector.validate(settings);
    ConfigValue inputPathConfig = config.configValues().stream().
        filter(configValue -> configValue.name().equals(SpoolDirSourceConnectorConfig.INPUT_PATH_CONFIG)).
        findFirst().get();
    assertEquals(1, inputPathConfig.errorMessages().size(),
        "Error message for 'non-existing directory' should be generated");
  }

  @Test
  public void emptyConfig() {
    settings = new LinkedHashMap<>();
    SpoolDirSourceConnector connector = createConnector();
    Config config = connector.validate(settings);
    Set<String> configValuesWithErrors = config.configValues().stream().
        filter(configValue -> !configValue.errorMessages().isEmpty()).
        map(ConfigValue::name).
        collect(Collectors.toSet());
    Set<String> expectedConfigValuesWithErrors = new HashSet<>(Arrays.asList(
        "input.file.pattern", "finished.path", "topic", "error.path", "input.path", "key.schema", "value.schema"));
    assertEquals(expectedConfigValuesWithErrors, configValuesWithErrors);
  }

  @Test
  public void minimalValidConfig() {
    SpoolDirSourceConnector connector = createConnector();
    Config config = connector.validate(settings);
    Set<String> configValuesWithErrors = config.configValues().stream().
        filter(configValue -> !configValue.errorMessages().isEmpty()).
        map(ConfigValue::name).
        collect(Collectors.toSet());
    Set<String> expectedConfigValuesWithErrors = Collections.emptySet();
    assertEquals(expectedConfigValuesWithErrors, configValuesWithErrors);
    //Settings was validated -> possible to create connector based on settings.
    connector.config(settings);
  }

  protected abstract SpoolDirSourceConnector createConnector();
}
