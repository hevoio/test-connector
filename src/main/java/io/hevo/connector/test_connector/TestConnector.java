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
import io.hevo.connector.model.field.data.datum.hudt.HBoolean;
import io.hevo.connector.model.field.data.datum.hudt.HByte;
import io.hevo.connector.model.field.data.datum.hudt.HDate;
import io.hevo.connector.model.field.data.datum.hudt.HDateTime;
import io.hevo.connector.model.field.data.datum.hudt.HDateTimeTZ;
import io.hevo.connector.model.field.data.datum.hudt.HDatum;
import io.hevo.connector.model.field.data.datum.hudt.HDecimal;
import io.hevo.connector.model.field.data.datum.hudt.HDouble;
import io.hevo.connector.model.field.data.datum.hudt.HFloat;
import io.hevo.connector.model.field.data.datum.hudt.HInteger;
import io.hevo.connector.model.field.data.datum.hudt.HJson;
import io.hevo.connector.model.field.data.datum.hudt.HLong;
import io.hevo.connector.model.field.data.datum.hudt.HShort;
import io.hevo.connector.model.field.data.datum.hudt.HStruct;
import io.hevo.connector.model.field.data.datum.hudt.HTime;
import io.hevo.connector.model.field.data.datum.hudt.HTimeTZ;
import io.hevo.connector.model.field.data.datum.hudt.HUnsupported;
import io.hevo.connector.model.field.data.datum.hudt.HVarchar;
import io.hevo.connector.model.field.schema.base.Field;
import io.hevo.connector.model.field.schema.enumeration.FieldState;
import io.hevo.connector.model.field.schema.hudt.HDataType;
import io.hevo.connector.model.field.schema.hudt.HDateTimeField;
import io.hevo.connector.model.field.schema.hudt.HIntegerField;
import io.hevo.connector.offset.Offset;
import io.hevo.connector.processor.ConnectorProcessor;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Overall Salesforce TODO List
 *
 * <ol>
 *   <li>SourceObjectStatus - Handle unsupported statuses appropriately.
 *   <li>Check unsupported fields in Bulk v2 - To be added post testing
 * </ol>
 */
public class TestConnector implements GenericConnector {

  private static final Logger log = LoggerFactory.getLogger(TestConnector.class);

  private static final String CURSOR_FIELD = "SystemModstamp";

  @Override
  public void initializeConnection() throws ConnectorException {
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
  public List<ObjectDetails> getObjects() throws ConnectorException {
    ObjectDetails o1 =
        ObjectDetails.builder()
            .table("o1")
            .type("TABLE")
            .delimiter(".")
            .sourceObjectStatus(SourceObjectStatus.ACTIVE)
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
   *     configurations, excluding any objects whose table names are defined in {@code
   *     BULK_MODE_UNSUPPORTED_TABLES}.
   * @throws ConnectorException If an error occurs while fetching schema details.
   */
  @Override
  public List<ObjectSchema> fetchSchemaFromSource(List<ObjectDetails> objectDetails)
      throws ConnectorException {
    Set<Field> fields1 = new HashSet<>();
    HIntegerField.Builder id1 =
        new HIntegerField.Builder("id", "INTEGER", 1, FieldState.ACTIVE).isNullable(false);
    id1.pkPos(1);
    HDateTimeField.Builder ts1 =
        new HDateTimeField.Builder("updated_ts", "TIMESTAMP", 2, FieldState.ACTIVE, 9)
            .isNullable(false);
    ts1.ckOrdinal(2);
    fields1.add(id1.build());
    fields1.add(ts1.build());

    Set<Field> fields2 = new HashSet<>();
    HIntegerField.Builder id2 =
        new HIntegerField.Builder("id", "INTEGER", 1, FieldState.ACTIVE).isNullable(false);
    id2.pkPos(1);
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

  @SuppressWarnings("java:S3776")
  private HDatum createHDatum(HDataType hDataType, Object colVal) {
    switch (hDataType) {
      case BOOLEAN -> {
        return new HBoolean(Objects.isNull(colVal) ? null : (Boolean) colVal);
      }
      case BYTE -> {
        return new HByte(Objects.isNull(colVal) ? null : (Byte) colVal);
      }
      case DATE -> {
        return new HDate(Objects.isNull(colVal) ? null : ((Date) colVal).toLocalDate());
      }
      case DECIMAL -> {
        return new HDecimal(Objects.isNull(colVal) ? null : (BigDecimal) colVal);
      }
      case DOUBLE -> {
        return new HDouble(Objects.isNull(colVal) ? null : (Double) colVal);
      }
      case FLOAT -> {
        return new HFloat(Objects.isNull(colVal) ? null : (Float) colVal);
      }
      case INTEGER -> {
        return new HInteger(Objects.isNull(colVal) ? null : (Integer) colVal);
      }
      case JSON -> {
        return new HJson(Objects.isNull(colVal) ? null : (String) colVal);
      }
      case LONG -> {
        return new HLong(Objects.isNull(colVal) ? null : (Long) colVal);
      }
      case SHORT -> {
        return new HShort(Objects.isNull(colVal) ? null : (Short) colVal);
      }
      case TIME -> {
        return new HTime(Objects.isNull(colVal) ? null : ((Time) colVal).toLocalTime());
      }
        // -ve value check
      case TIME_TZ -> {
        return new HTimeTZ(
            Objects.isNull(colVal)
                ? null
                : OffsetTime.of(((Time) colVal).toLocalTime(), ZoneOffset.UTC));
      }
      case DATE_TIME -> {
        return new HDateTime(Objects.isNull(colVal) ? null : LocalDateTime.now());
      }
      case DATE_TIME_TZ -> {
        return new HDateTimeTZ(
            Objects.isNull(colVal) ? null : OffsetDateTime.parse((String) colVal));
      }
      case VARCHAR -> {
        return new HVarchar(Objects.isNull(colVal) ? null : (String) colVal);
      }
      default -> {
        log.debug("Unsupported field type {}", hDataType);
        return new HUnsupported();
      }
    }
  }

  @Override
  public void close() throws ConnectorException {
    // Clean resources
  }
}
