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

import com.google.common.base.Joiner;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SpoolDirCsvSourceTask extends SpoolDirSourceTask<SpoolDirCsvSourceConnectorConfig> {
  String[] fieldNames;
  private CSVParser csvParser;
  private CSVReader csvReader;
  private InputStreamReader streamReader;
  private Map<String, String> fileMetadata;


  @Override
  protected SpoolDirCsvSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirCsvSourceConnectorConfig(true, settings);
  }

  @Override
  protected SchemaGenerator<SpoolDirCsvSourceConnectorConfig> generator(Map<String, Object> settings) {
    return new CsvSchemaGenerator(settings);
  }

  @Override
  protected void configure(File inputFile, Map<String, String> metadata, final Long lastOffset) throws IOException {
    super.configure(inputFile, metadata, lastOffset);
    log.trace("configure() - creating csvParser");
    this.csvParser = this.config.createCSVParserBuilder().build();
    this.streamReader = new InputStreamReader(new FileInputStream(inputFile), this.config.charset);
    CSVReaderBuilder csvReaderBuilder = this.config.createCSVReaderBuilder(this.streamReader, csvParser);
    this.csvReader = csvReaderBuilder.build();

    String[] fieldNames;

    if (this.config.firstRowAsHeader) {
      log.trace("configure() - Reading the header row.");
      fieldNames = Arrays.stream(this.csvReader.readNext())
          .map(HeaderTranslator::toValidSchemaKey).toArray(String[]::new);
      log.info("configure() - field names from header row. fields = {}", Joiner.on(", ").join(fieldNames));
    } else {
      log.trace("configure() - Using fields from schema {}", this.config.valueSchema.name());
      fieldNames = new String[this.config.valueSchema.fields().size()];
      int index = 0;
      for (Field field : this.config.valueSchema.fields()) {
        fieldNames[index++] = field.name();
      }
      log.info("configure() - field names from schema order. fields = {}", Joiner.on(", ").join(fieldNames));
    }

    if (null != lastOffset) {
      log.info("Found previous offset. Skipping {} line(s).", lastOffset.intValue());
      String[] row = null;
      while (null != (row = this.csvReader.readNext()) && this.csvReader.getLinesRead() < lastOffset) {
        log.trace("skipped row");
      }
    }

    this.fieldNames = fieldNames;
    this.fileMetadata = metadata;
  }

  @Override
  public void start(Map<String, String> settings) {
    super.start(settings);
  }

  @Override
  public long recordOffset() {
    return this.csvReader.getLinesRead();
  }

  @Override
  public List<SourceRecord> process() throws IOException {
    List<SourceRecord> records = new ArrayList<>(this.config.batchSize);

    while (records.size() < this.config.batchSize) {
      String[] row = this.csvReader.readNext();

      if (row == null || row.length == 0) {
        csvReader.close();
        break;
      }
      log.trace("process() - Row on line {} has {} field(s)", recordOffset(), row.length);

      Struct keyStruct = this.config.keySchema == null ? null : new Struct(this.config.keySchema);
      Struct valueStruct = new Struct(this.config.valueSchema);

      for (int i = 0; i < this.fieldNames.length; i++) {
        String fieldName = fieldNames[i];
        String column = row[i];
        boolean foundKey = buildStruct(keyStruct, this.config.keySchema, fieldName, column);
        boolean foundValue = buildStruct(valueStruct, this.config.valueSchema, fieldName, column);
        if (!(foundKey || foundValue)) {
          String message = String.format("Could not find field '%s' in linenumber=%s", fieldName, this.recordOffset());
          throw new DataException(message);
        }
      }

      if (log.isInfoEnabled() && this.csvReader.getLinesRead() % ((long) this.config.batchSize * 20) == 0) {
        log.info("Processed {} lines of {}", this.csvReader.getLinesRead(), this.fileMetadata);
      }

      addRecord(records, keyStruct, valueStruct);
    }
    return records;
  }

  private boolean buildStruct(Struct struct, Schema schema, String fieldName, String column) {
    try {
      if (struct == null) {
        return false;
      }
      Field field = schema.field(fieldName);
      if (field == null) {
        return false;
      }
      struct.put(fieldName, this.parser.parseString(field.schema(), column));
      return true;
    } catch (Exception ex) {
      String message = String.format("Exception thrown while parsing data for '%s'. linenumber=%s", fieldName, this.recordOffset());
      throw new DataException(message, ex);
    }
  }
}
