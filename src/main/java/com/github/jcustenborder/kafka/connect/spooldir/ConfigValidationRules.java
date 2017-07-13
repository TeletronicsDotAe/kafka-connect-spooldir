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

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;

import java.util.Optional;
import java.util.function.Predicate;

/**
 * Used to specify validation dependencies between configuration parameters, i. e.
 * "When app.id is 123, validate that app.foo is a non-empty string and that app.bar is between 1 and 100"
 */
class ConfigValidationRules {
  private Config config;

  ConfigValidationRules(Config config) {
    this.config = config;
  }

  <T> When<T> when(String name, T value) {
    @SuppressWarnings("unchecked") Class<T> type = (Class<T>) value.getClass();
    return when(name, new Rule<>(type, v -> v.equals(value), "is " + value));
  }

  <T> When<T> when(String name, Rule<T> rule) {
    return new When<>(name, rule);
  }

  <T> ConfigValidationRules validate(String name, Rule<T> rule) {
    if (!match(name, rule)) {
      addError(name, rule.description);
    }
    return this;
  }

  private <T> boolean match(String name, Rule<T> rule) {
    return findValue(name).map(ConfigValue::value).map(v -> rule.inputType.cast(v)).filter(rule.predicate).isPresent();
  }

  private Optional<ConfigValue> findValue(String name) {
    return config.configValues().stream().filter(cv -> cv.name().equals(name)).findFirst();
  }

  private void addError(String name, String message) {
    ConfigValue configValue = findValue(name).orElseGet(() -> {
      ConfigValue cv = new ConfigValue(name);
      config.configValues().add(cv);
      return cv;
    });
    configValue.addErrorMessage(message);
  }

  Config getConfig() {
    return config;
  }

  static class Rule<T> {
    private Class<T> inputType;
    private Predicate<T> predicate;
    private String description;

    Rule(Class<T> inputType, Predicate<T> predicate, String description) {
      this.inputType = inputType;
      this.predicate = predicate;
      this.description = description;
    }
  }

  class When<S> {
    private String description;

    When(String name, Rule<S> rule) {
      this.description = match(name, rule) ? String.format(" when %s %s", name, rule.description) : "";
    }

    <T> When<S> validate(String name, Rule<T> rule) {
      if (!description.isEmpty()) {
        ConfigValidationRules.this.validate(name, new Rule<>(
            rule.inputType,
            rule.predicate,
            String.format("%s%s", rule.description, description))
        );
      }
      return this;
    }
  }
}
