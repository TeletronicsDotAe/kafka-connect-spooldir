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

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class SpoolDirSourceConnector<CONF extends SpoolDirSourceConnectorConfig> extends SourceConnector {
  private static Logger log = LoggerFactory.getLogger(SpoolDirSourceConnector.class);
  protected Map<String, String> settings;
  private CONF config;

  protected abstract CONF config(Map<String, String> settings);

  protected abstract SchemaGenerator<CONF> generator(Map<String, String> settings);

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(final Map<String, String> input) {
    this.config = config(input);
    final Map<String, String> settings = new LinkedHashMap<>(input);

    if (null == this.config.valueSchema || null == this.config.keySchema) {
      log.info("Key or Value schema was not defined. Running schema generator.");
      SchemaGenerator<CONF> generator = generator(settings);

      try {
        List<File> inputFiles = Arrays.stream(this.config.inputPath.listFiles(this.config.inputFilenameFilter))
            .limit(5)
            .collect(Collectors.toList());
        Preconditions.checkState(
            !inputFiles.isEmpty(),
            "Could not find any input file(s) to infer schema from."
        );

        Map<String, Map.Entry<Schema, Schema>> schemas = new HashMap<>();
        Multimap<String, File> schemaToFiles = HashMultimap.create();

        for (File inputFile : inputFiles) {
          Map.Entry<Schema, Schema> schemaEntry = generator.generate(inputFile, this.config.keyFields);
          String schema = ObjectMapperFactory.INSTANCE.writeValueAsString(schemaEntry.getValue());
          schemaToFiles.put(schema, inputFile);
          schemas.put(schema, schemaEntry);
        }

        Map<String, Collection<File>> schemaToFilesMap = schemaToFiles.asMap();
        if (1 != schemaToFilesMap.keySet().size()) {
          StringBuilder builder = new StringBuilder();
          builder.append("More than one schema was found for the input pattern.\n");
          for (String schema : schemaToFilesMap.keySet()) {
            builder.append("Schema: ");
            builder.append(schema);
            builder.append("\n");

            for (File f : schemaToFilesMap.get(schema)) {
              builder.append("  ");
              builder.append(f);
              builder.append("\n");
            }
          }

          throw new DataException(builder.toString());
        }

        Map.Entry<Schema, Schema> schemaPair = null;
        for (Map.Entry<Schema, Schema> s : schemas.values()) {
          schemaPair = s;
          break;
        }

        if (null == schemaPair) {
          throw new DataException("Schema could not be generated.");
        }

        final String keySchema = ObjectMapperFactory.INSTANCE.writeValueAsString(schemaPair.getKey());
        log.info("Setting {} to {}", SpoolDirSourceConnectorConfig.KEY_SCHEMA_CONF, keySchema);
        final String valueSchema = ObjectMapperFactory.INSTANCE.writeValueAsString(schemaPair.getValue());
        log.info("Setting {} to {}", SpoolDirSourceConnectorConfig.VALUE_SCHEMA_CONF, valueSchema);
        settings.put(SpoolDirSourceConnectorConfig.KEY_SCHEMA_CONF, keySchema);
        settings.put(SpoolDirSourceConnectorConfig.VALUE_SCHEMA_CONF, valueSchema);
      } catch (IOException e) {
        throw new ConnectException("Exception thrown while generating schema", e);
      }
      this.settings = settings;
    }

    this.settings = settings;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    return Arrays.asList(this.settings);
  }

  @Override
  public void stop() {

  }

  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    ConfigValidationRules.Rule<String> nonEmptyString =
        new ConfigValidationRules.Rule<>(String.class, s -> !s.isEmpty(), "must be non-empty");
    ConfigValidationRules.Rule<String> validSchema = new ConfigValidationRules.Rule<>(String.class, s -> {
      try {
        ObjectMapperFactory.INSTANCE.readValue(s, Schema.class);
        return true;
      } catch (Exception e) {
        // Do nothing
      }
      return false;
    }, "must be valid Schema");

    ConfigValidationRules rules = new ConfigValidationRules(super.validate(connectorConfigs));
    rules.when(SpoolDirCsvSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF, false).
        validate(SpoolDirCsvSourceConnectorConfig.KEY_SCHEMA_CONF, validSchema).
        validate(SpoolDirCsvSourceConnectorConfig.VALUE_SCHEMA_CONF, validSchema);
    rules.when(SpoolDirCsvSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF, true).
        validate(SpoolDirCsvSourceConnectorConfig.SCHEMA_GENERATION_KEY_NAME_CONF, nonEmptyString).
        validate(SpoolDirCsvSourceConnectorConfig.SCHEMA_GENERATION_VALUE_NAME_CONF, nonEmptyString);
    rules.when(SpoolDirCsvSourceConnectorConfig.TIMESTAMP_MODE_CONF, SpoolDirSourceConnectorConfig.TimestampMode.FIELD.toString()).
        validate(SpoolDirCsvSourceConnectorConfig.TIMESTAMP_FIELD_CONF, nonEmptyString);

    return rules.getConfig();
  }
}
