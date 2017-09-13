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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.github.jcustenborder.kafka.connect.spooldir.SpoolDirSourceConnectorConfig.KEY_SCHEMA_CONF;
import static com.github.jcustenborder.kafka.connect.spooldir.SpoolDirSourceConnectorConfig.PARSER_TIMESTAMP_DATE_FORMATS_CONF;
import static com.github.jcustenborder.kafka.connect.spooldir.SpoolDirSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF;
import static com.github.jcustenborder.kafka.connect.spooldir.SpoolDirSourceConnectorConfig.SCHEMA_GENERATION_KEY_NAME_CONF;
import static com.github.jcustenborder.kafka.connect.spooldir.SpoolDirSourceConnectorConfig.SCHEMA_GENERATION_VALUE_NAME_CONF;
import static com.github.jcustenborder.kafka.connect.spooldir.SpoolDirSourceConnectorConfig.TIMESTAMP_FIELD_CONF;
import static com.github.jcustenborder.kafka.connect.spooldir.SpoolDirSourceConnectorConfig.TIMESTAMP_MODE_CONF;
import static com.github.jcustenborder.kafka.connect.spooldir.SpoolDirSourceConnectorConfig.VALUE_SCHEMA_CONF;

public abstract class SpoolDirSourceConnector<CONF extends SpoolDirSourceConnectorConfig> extends SourceConnector {
  private static Logger log = LoggerFactory.getLogger(SpoolDirSourceConnector.class);
  protected Map<String, String> settings;
  private CONF config;

  protected abstract CONF config(Map<String, String> settings);

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(final Map<String, String> input) {
    this.config = config(input);
    this.settings = new LinkedHashMap<>(input);
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    return Collections.singletonList(this.settings);
  }

  @Override
  public void stop() {

  }

  /**
   * Throw a ConnectException on the first validation error encountered
   * @param connectorConfigs settings
   */
  public void failOnValidationErrors(Map<String, String> connectorConfigs) {
    validate(connectorConfigs).configValues().stream().filter(cv -> !cv.errorMessages().isEmpty()).findFirst().map(
        cv -> {
          throw new ConnectException(cv.errorMessages().get(0));
        });
  }

  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    ConfigValidationRules rules = new ConfigValidationRules(super.validate(connectorConfigs));

    ConfigValidationRules.Rule<String> nonEmptyString = new ConfigValidationRules.Rule<>(String.class,
        "must be non-empty", s -> !s.isEmpty());

    ConfigValidationRules.Rule<String> validSchema = new ConfigValidationRules.Rule<>(String.class,
        "must be valid schema", s -> toSchema(s).isPresent());

    rules.validate(PARSER_TIMESTAMP_DATE_FORMATS_CONF, new ConfigValidationRules.Rule<>(List.class,
        "must be valid list of Date formats", dateFormats -> {
      try {
        //noinspection unchecked
        ((List<String>) dateFormats).forEach(SimpleDateFormat::new);
        return true;
      } catch (Exception e) {
        return false;
      }
    }));

    rules.when(SCHEMA_GENERATION_ENABLED_CONF, false)
        .validate(KEY_SCHEMA_CONF, validSchema)
        .validate(VALUE_SCHEMA_CONF, validSchema);

    rules.when(SCHEMA_GENERATION_ENABLED_CONF, true)
        .validate(SCHEMA_GENERATION_KEY_NAME_CONF, nonEmptyString)
        .validate(SCHEMA_GENERATION_VALUE_NAME_CONF, nonEmptyString);

    rules.when(TIMESTAMP_MODE_CONF, SpoolDirSourceConnectorConfig.TimestampMode.FIELD.toString())
        .validate(TIMESTAMP_FIELD_CONF, new ConfigValidationRules.Rule<>(String.class,
            String.format("must be present in value.schema, cannot be optional, and must be in the correct format (i.e. %s)", timestampExample()),
            s -> rules.findValue(String.class, VALUE_SCHEMA_CONF)
                .flatMap(this::toSchema).flatMap(schema -> Optional.ofNullable(schema.field(s))).map(Field::schema)
                .map(schema -> !schema.isOptional() && Timestamp.LOGICAL_NAME.equals(schema.name())).orElse(false)
        ));

    return rules.getConfig();
  }

  private Optional<Schema> toSchema(String s) {
    try {
      return Optional.of(ObjectMapperFactory.INSTANCE.readValue(s, Schema.class));
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private String timestampExample() {
    try {
      return ObjectMapperFactory.INSTANCE.writeValueAsString(Timestamp.SCHEMA);
    } catch (JsonProcessingException e) {
      throw new ConnectException("Exception thrown while generating timestamp schema example", e);
    }
  }
}
