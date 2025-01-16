package io.hevo.connector.test_connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.hevo.connector.exceptions.ConnectorException;
import io.hevo.connector.model.ConnectorContext;
import io.hevo.connector.model.ConnectorMeta;
import io.hevo.connector.model.ExecutionResult;
import io.hevo.connector.model.ObjectDetails;
import io.hevo.connector.model.ObjectSchema;
import io.hevo.connector.model.enums.SourceObjectStatus;
import io.hevo.connector.model.field.data.datum.hudt.HStruct;
import io.hevo.connector.model.field.schema.base.Field;
import io.hevo.connector.model.field.schema.enumeration.FieldState;
import io.hevo.connector.model.field.schema.hudt.HDateTimeField;
import io.hevo.connector.model.field.schema.hudt.HIntegerField;
import io.hevo.connector.processor.ConnectorProcessor;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
class TestTestConnector {
  private TestConnector testConnector;

  @BeforeEach
  public void setUp() {
    testConnector = new TestConnector();
  }

  @Test
  void testGetObjects() {
    List<ObjectDetails> objectDetails = testConnector.getObjects();
    Assertions.assertEquals("o1", objectDetails.get(0).tableName());
    Assertions.assertEquals("o2", objectDetails.get(1).tableName());
  }

  @Test
  void testFetchSchemaFromSource() {
    List<ObjectSchema> objects =
        testConnector.fetchSchemaFromSource(
            List.of(
                ObjectDetails.builder()
                    .table("o1")
                    .sourceObjectStatus(SourceObjectStatus.INACCESSIBLE)
                    .build(),
                ObjectDetails.builder()
                    .table("o2")
                    .sourceObjectStatus(SourceObjectStatus.ACTIVE)
                    .build()));

    assertEquals(2, objects.size());

    Iterator<ObjectSchema> iterator = objects.iterator();

    ObjectDetails objectDetails1 = iterator.next().objectDetail();
    ObjectDetails objectDetails2 = iterator.next().objectDetail();

    assertEquals(2, objects.size());
    assertEquals("o1", objectDetails1.tableName());
    assertNull(objectDetails1.catalogName());
    assertNull(objectDetails1.schemaName());
    assertEquals(objectDetails1.tableName(), objectDetails1.getTableFullyQualifiedName());
    assertEquals(SourceObjectStatus.INACCESSIBLE, objectDetails1.sourceObjectStatus());

    assertEquals("o2", objectDetails2.tableName());
    assertNull(objectDetails2.catalogName());
    assertNull(objectDetails2.schemaName());
    assertEquals(objectDetails2.tableName(), objectDetails2.getTableFullyQualifiedName());
    assertEquals(SourceObjectStatus.ACTIVE, objectDetails2.sourceObjectStatus());
  }

  @Test
  void testFetchData() throws ConnectorException {
    // Mock ConnectorContext
    ConnectorContext mockContext = Mockito.mock(ConnectorContext.class);
    ConnectorProcessor mockProcessor = Mockito.mock(ConnectorProcessor.class);

    // Mock schema fields
    Set<Field> fields = new HashSet<>();
    fields.add(
        new HIntegerField.Builder("id", "INTEGER", 1, FieldState.ACTIVE).isNullable(false).build());
    fields.add(
        new HDateTimeField.Builder("updated_ts", "TIMESTAMP", 2, FieldState.ACTIVE, 9)
            .isNullable(false)
            .build());

    // Mock schema
    ObjectSchema mockSchema = Mockito.mock(ObjectSchema.class);
    Mockito.when(mockSchema.fields()).thenReturn(fields);
    Mockito.when(mockContext.schema()).thenReturn(mockSchema);

    // Call the method
    ExecutionResult result = testConnector.fetchDataFromSource(mockContext, mockProcessor);

    // Verify the results
    Mockito.verify(mockProcessor)
        .publish(Mockito.any(HStruct.class), Mockito.any(ConnectorMeta.class));
    assertEquals(1, result.fetchedRecords());
    assertNotNull(result.lastReadOffset());
  }
}
