package com.github.jcustenborder.kafka.connect.spooldir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class TestResourceLoader {
  public static InputStream load(String resourceName) {
    return TestResourceLoader.class.getResourceAsStream(resourceName);
  }

  public static void loadAndPlace(String resourceName, Path destination) {
    try (InputStream inputStream = load(resourceName)) {
      Files.copy(inputStream, destination, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
