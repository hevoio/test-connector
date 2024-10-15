package io.hevo.connector.test_noncdata_connector;

import io.hevo.connector.GenericConnector;
import io.hevo.connector.exceptions.ConnectorException;
import io.hevo.connector.model.ConnectorContext;
import io.hevo.connector.model.ExecutionResult;
import io.hevo.connector.model.ObjectDetails;
import io.hevo.connector.model.ObjectSchema;
import io.hevo.connector.model.enums.SourceObjectStatus;
import io.hevo.connector.processor.ConnectorProcessor;
import java.util.List;

public class TestNonCdataConnector implements GenericConnector {
  @Override
  public void initializeConnection() throws ConnectorException {}

  @Override
  public List<ObjectDetails> getObjects() {
    ObjectDetails ob1 =
        ObjectDetails.builder()
            .table("tb1")
            .type("TABLE")
            .sourceObjectStatus(SourceObjectStatus.ACTIVE)
            .delimiter(".")
            .build();
    ObjectDetails ob2 =
        ObjectDetails.builder()
            .table("tb2")
            .type("TABLE")
            .sourceObjectStatus(SourceObjectStatus.ACTIVE)
            .delimiter(".")
            .build();
    return List.of(ob1, ob2);
  }

  @Override
  public List<ObjectSchema> fetchSchemaFromSource(List<ObjectDetails> list)
      throws ConnectorException {
    return null;
  }

  @Override
  public ExecutionResult fetchDataFromSource(
      ConnectorContext connectorContext, ConnectorProcessor connectorProcessor)
      throws ConnectorException {
    return null;
  }

  @Override
  public void close() throws ConnectorException {}
}
