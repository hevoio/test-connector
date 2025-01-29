package io.hevo.connector.test_connector;

import io.hevo.connector.GenericConnector;
import io.hevo.connector.exceptions.ConnectorException;
import io.hevo.connector.model.ConnectorContext;
import io.hevo.connector.model.ConnectorMeta;
import io.hevo.connector.model.ExecutionResult;
import io.hevo.connector.model.ObjectDetails;
import io.hevo.connector.model.ObjectSchema;
import io.hevo.connector.model.enums.OpType;
import io.hevo.connector.model.enums.SourceObjectStatus;
import io.hevo.connector.model.field.data.datum.hudt.HDateTime;
import io.hevo.connector.model.field.data.datum.hudt.HDatum;
import io.hevo.connector.model.field.data.datum.hudt.HInteger;
import io.hevo.connector.model.field.data.datum.hudt.HStruct;
import io.hevo.connector.model.field.schema.base.Field;
import io.hevo.connector.model.field.schema.enumeration.FieldState;
import io.hevo.connector.model.field.schema.hudt.HDataType;
import io.hevo.connector.model.field.schema.hudt.HDateTimeField;
import io.hevo.connector.model.field.schema.hudt.HIntegerField;
import io.hevo.connector.offset.Offset;
import io.hevo.connector.processor.ConnectorProcessor;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestConnector implements GenericConnector {

  private static final Logger log = LoggerFactory.getLogger(TestConnector.class);

  @Override
  public void initializeConnection() {
    log.info("Initialized connection");
  }

  /**
   * Retrieves a list of object details matching the specified criteria.
   *
   * @return A linked list of {@code ObjectDetails} representing the objects matching the criteria,
   *     excluding any unsupported tables defined in {@code BULK_MODE_UNSUPPORTED_TABLES}.
   * @throws ConnectorException If an error occurs while fetching object details.
   */
  @Override
  public List<ObjectDetails> getObjects() {
    ObjectDetails o1 =
        ObjectDetails.builder()
            .table("o1")
            .type("TABLE")
            .delimiter(".")
            .sourceObjectStatus(SourceObjectStatus.INACCESSIBLE)
            .build();
    ObjectDetails o2 =
        ObjectDetails.builder()
            .table("o2")
            .type("TABLE")
            .delimiter(".")
            .sourceObjectStatus(SourceObjectStatus.ACTIVE)
            .build();

    return Arrays.asList(o1, o2);
  }

  /**
   * Fetches the schema details from the data source based on the specified criteria.
   *
   * @return A linked hash map containing object details mapped to their corresponding object
   *     configurations
   */
  @Override
  public List<ObjectSchema> fetchSchemaFromSource(List<ObjectDetails> objectDetails) {
    Set<Field> fields1 = new HashSet<>();
    HIntegerField.Builder id1 =
        new HIntegerField.Builder("id", "INTEGER", 1, FieldState.ACTIVE).isNullable(false);
    id1.pkPos(1);
    HDateTimeField.Builder ts1 =
        new HDateTimeField.Builder("updated_ts", "TIMESTAMP", 2, FieldState.ACTIVE, 9)
            .isNullable(false);
    ts1.ckOrdinal(1);
    fields1.add(id1.build());
    fields1.add(ts1.build());

    Set<Field> fields2 = new HashSet<>();
    HIntegerField.Builder id2 =
        new HIntegerField.Builder("id", "INTEGER", 1, FieldState.ACTIVE).isNullable(false);
    //    id2.pkPos(2);
    HDateTimeField.Builder ts2 =
        new HDateTimeField.Builder("updated_ts", "TIMESTAMP", 2, FieldState.ACTIVE, 9)
            .isNullable(false);
    ts2.ckOrdinal(2);
    fields2.add(id2.build());
    fields2.add(ts2.build());

    ObjectSchema os1 = new ObjectSchema(objectDetails.get(0), fields1);
    ObjectSchema os2 = new ObjectSchema(objectDetails.get(1), fields2);
    return Arrays.asList(os1, os2);
  }

  @Override
  public ExecutionResult fetchDataFromSource(
      ConnectorContext connectorContext, ConnectorProcessor connectorProcessor)
      throws ConnectorException {
    List<HDatum> row = new ArrayList<>(connectorContext.schema().fields().size());

    Offset.Builder currentRecordOffset = Offset.builder();

    for (Field col : connectorContext.schema().fields()) {
      HDataType hDataType = HDataType.fromLogicalType(col.logicalType());
      if (hDataType.equals(HDataType.INTEGER)) {
        row.add(new HInteger(1));
      } else if (hDataType.equals(HDataType.DATE_TIME)) {
        row.add(new HDateTime(LocalDateTime.now()));
      }
    }
    connectorProcessor.publish(
        new HStruct(row), ConnectorMeta.builder().opType(OpType.READ).build());
    return new ExecutionResult(1, currentRecordOffset.build());
  }

  @Override
  public void close() {
    // Clean resources
  }
}
