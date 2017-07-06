package com.github.jcustenborder.kafka.connect.spooldir;

public class SpoolDirCvsSourceConnectorConfigTest extends SpoolDirSourceConnectorConfigTest {
    @Override
    protected SpoolDirSourceConnector createConnector() {
        return new SpoolDirCsvSourceConnector();
    }
}
