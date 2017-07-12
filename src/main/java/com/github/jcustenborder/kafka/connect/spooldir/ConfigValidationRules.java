package com.github.jcustenborder.kafka.connect.spooldir;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;

import java.util.Optional;
import java.util.function.Predicate;

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
    return findValue(name).map(ConfigValue::value).map(v -> rule.type.cast(v)).filter(rule.predicate).isPresent();
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
    private Class<T> type;
    private Predicate<T> predicate;
    private String description;

    Rule(Class<T> type, Predicate<T> predicate, String description) {
      this.type = type;
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
            rule.type,
            rule.predicate,
            String.format("%s%s", rule.description, description))
        );
      }
      return this;
    }
  }
}