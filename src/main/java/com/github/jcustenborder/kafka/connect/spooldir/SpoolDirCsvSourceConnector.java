/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.spooldir;

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import java.util.Map;

@Description("The `SpoolDirCsvSourceConnector` will monitor the directory specified in `input.path` for files and read them as a CSV " +
    "converting each of the records to the strongly typed equivalent specified in `key.schema` and `value.schema`.")
public class SpoolDirCsvSourceConnector extends SpoolDirSourceConnector<SpoolDirCsvSourceConnectorConfig> {
  @Override
  protected SpoolDirCsvSourceConnectorConfig config(Map<String, String> settings) {
    return new SpoolDirCsvSourceConnectorConfig(false, settings);
  }

  @Override
  protected SchemaGenerator<SpoolDirCsvSourceConnectorConfig> generator(Map<String, String> settings) {
    return new CsvSchemaGenerator(settings);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SpoolDirCsvSourceTask.class;
  }

  @Override
  public ConfigDef config() {
    return SpoolDirCsvSourceConnectorConfig.conf();
  }

  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    ConfigValidationRules rules = new ConfigValidationRules(super.validate(connectorConfigs));
    ConfigValidationRules.Rule<String> nonEmptyString =
            new ConfigValidationRules.Rule<>(String.class, s -> !s.isEmpty(), "must be non-empty");

    rules.when(SpoolDirCsvSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF, false).
            validate(SpoolDirCsvSourceConnectorConfig.KEY_SCHEMA_CONF, nonEmptyString).
            validate(SpoolDirCsvSourceConnectorConfig.VALUE_SCHEMA_CONF, nonEmptyString);
    rules.when(SpoolDirCsvSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF, true).
            validate(SpoolDirCsvSourceConnectorConfig.SCHEMA_GENERATION_KEY_NAME_CONF, nonEmptyString).
            validate(SpoolDirCsvSourceConnectorConfig.SCHEMA_GENERATION_VALUE_NAME_CONF, nonEmptyString);
    rules.when(SpoolDirCsvSourceConnectorConfig.TIMESTAMP_MODE_CONF, SpoolDirSourceConnectorConfig.TimestampMode.FIELD.toString()).
            validate(SpoolDirCsvSourceConnectorConfig.TIMESTAMP_FIELD_CONF, nonEmptyString);

    return rules.getConfig();
  }
}
