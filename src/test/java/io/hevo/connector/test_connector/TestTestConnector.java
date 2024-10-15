package io.hevo.connector.test_connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.hevo.connector.exceptions.ConnectorException;
import io.hevo.connector.exceptions.RateLimitException;
import io.hevo.connector.jdbc.JdbcConnector;
import io.hevo.connector.model.AccountType;
import io.hevo.connector.model.AuthCredentials;
import io.hevo.connector.model.AuthType;
import io.hevo.connector.model.ObjectDetails;
import io.hevo.connector.model.ObjectSchema;
import io.hevo.connector.model.enums.SourceObjectStatus;
import io.hevo.connector.model.field.data.datum.hudt.HDateTime;
import io.hevo.connector.model.field.schema.enumeration.FieldState;
import io.hevo.connector.model.field.schema.hudt.HDateTimeField;
import io.hevo.connector.offset.Offset;
import io.hevo.connector.utils.ConnectorFrameworkUtils;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import schemacrawler.schema.Catalog;
import schemacrawler.schema.Column;
import schemacrawler.schema.ColumnDataType;
import schemacrawler.schema.JavaSqlType;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schema.TableType;
import schemacrawler.tools.utility.SchemaCrawlerUtility;

@SuppressWarnings("unchecked")
class TestTestConnector {

  private TestConnector testConnector;
  private Connection connection;

  private DatabaseMetaData databaseMetaData;

  @BeforeEach
  public void setUp() throws Exception {
    testConnector = new TestConnector();
    databaseMetaData = mock(DatabaseMetaData.class);
    when(databaseMetaData.getCatalogSeparator()).thenReturn(".");
    when(databaseMetaData.getIdentifierQuoteString()).thenReturn("\"");
    connection = mock(Connection.class);
    when(connection.getMetaData()).thenReturn(databaseMetaData);
    Field connectionField = JdbcConnector.class.getDeclaredField("connection");
    connectionField.setAccessible(true);
    connectionField.set(testConnector, connection);
    connectionField.setAccessible(false);
  }

  @Test
  void testInitializeConnectionFailure() {
    assertThrows(ConnectorException.class, testConnector::initializeConnection);
  }

  @Test
  void testGetObjects() throws Exception {
    Table table1 = mockTable("AcceptedEventRelation", null, null, "TABLE");
    Table table2 = mockTable("Account", null, null, "TABLE");
    LinkedHashSet<Table> linkedHashSet = new LinkedHashSet<>();
    linkedHashSet.add(table1);
    linkedHashSet.add(table2);
    Catalog catalog = mockCatalog(linkedHashSet);

    try (MockedStatic<SchemaCrawlerUtility> schemaCrawlerUtility =
        mockStatic(SchemaCrawlerUtility.class)) {
      schemaCrawlerUtility
          .when(() -> SchemaCrawlerUtility.getCatalog(any(), any()))
          .thenReturn(catalog);
      List<ObjectDetails> objects = testConnector.getObjects();
      assertEquals(2, objects.size());
      assertEquals("AcceptedEventRelation", objects.get(0).getTableName());
      assertNull(objects.get(0).getCatalogName());
      assertNull(objects.get(0).getSchemaName());
      assertEquals(".", objects.get(0).getDelimiter());
      assertEquals(objects.get(0).getTableName(), objects.get(0).getTableFullyQualifiedName());
      assertEquals("TABLE", objects.get(0).getType());
      assertEquals(SourceObjectStatus.INACCESSIBLE, objects.get(0).getSourceObjectStatus());

      assertEquals("Account", objects.get(1).getTableName());
      assertNull(objects.get(1).getCatalogName());
      assertNull(objects.get(1).getSchemaName());
      assertEquals(".", objects.get(1).getDelimiter());
      assertEquals(objects.get(1).getTableName(), objects.get(1).getTableFullyQualifiedName());
      assertEquals("TABLE", objects.get(1).getType());
      assertEquals(SourceObjectStatus.ACTIVE, objects.get(1).getSourceObjectStatus());
    }
  }

  @Test
  void testGetObjectsWithRateLimitException() throws SQLException {
    Table table1 = mockTable("AcceptedEventRelation", null, null, "TABLE");
    Table table2 = mockTable("Account", null, null, "TABLE");
    LinkedHashSet<Table> linkedHashSet = new LinkedHashSet<>();
    linkedHashSet.add(table1);
    linkedHashSet.add(table2);
    Catalog catalog = mockCatalog(linkedHashSet);

    try (MockedStatic<SchemaCrawlerUtility> schemaCrawlerUtility =
        mockStatic(SchemaCrawlerUtility.class)) {
      schemaCrawlerUtility
          .when(() -> SchemaCrawlerUtility.getCatalog(any(), any()))
          .thenReturn(catalog);
      when(connection.getMetaData()).thenThrow(new SQLException("REQUEST_LIMIT_EXCEEDED"));
      Assertions.assertThrows(RateLimitException.class, () -> testConnector.getObjects());
    }
  }

  @Test
  void testFetchSchemaFromSource() throws ConnectorException {
    Column tb1col1 = mockColumn("Id", "INTEGER", true);
    Column tb1col2 = mockColumn("SystemModstamp", "TIMESTAMP", false);
    Column tb2col1 = mockColumn("Id", "INTEGER", false);
    Column tb2col2 = mockColumn("SystemModstamp", "TIMESTAMP", false);
    Table table1 = mockTable("AcceptedEventRelation", null, List.of(tb1col1, tb1col2), "TABLE");
    Table table2 = mockTable("Account", null, List.of(tb2col1, tb2col2), "TABLE");
    LinkedHashSet<Table> linkedHashSet = new LinkedHashSet<>();
    linkedHashSet.add(table1);
    linkedHashSet.add(table2);
    Catalog catalog = mockCatalog(linkedHashSet);

    try (MockedStatic<SchemaCrawlerUtility> schemaCrawlerUtility =
        mockStatic(SchemaCrawlerUtility.class)) {
      schemaCrawlerUtility
          .when(() -> SchemaCrawlerUtility.getCatalog(any(), any()))
          .thenReturn(catalog);

      List<ObjectSchema> objects =
          testConnector.fetchSchemaFromSource(
              List.of(
                  ObjectDetails.builder().table("AcceptedEventRelation").build(),
                  ObjectDetails.builder().table("Account").build()));

      assertEquals(2, objects.size());

      Iterator<ObjectSchema> iterator = objects.iterator();

      ObjectDetails objectDetails1 = iterator.next().objectDetail();
      ObjectDetails objectDetails2 = iterator.next().objectDetail();

      assertEquals(2, objects.size());
      assertEquals("AcceptedEventRelation", objectDetails1.getTableName());
      assertNull(objectDetails1.getCatalogName());
      assertNull(objectDetails1.getSchemaName());
      assertEquals(".", objectDetails1.getDelimiter());
      assertEquals(objectDetails1.getTableName(), objectDetails1.getTableFullyQualifiedName());
      assertEquals("TABLE", objectDetails1.getType());
      assertEquals(SourceObjectStatus.INACCESSIBLE, objectDetails1.getSourceObjectStatus());

      assertEquals("Account", objectDetails2.getTableName());
      assertNull(objectDetails2.getCatalogName());
      assertNull(objectDetails2.getSchemaName());
      assertEquals(".", objectDetails2.getDelimiter());
      assertEquals(objectDetails2.getTableName(), objectDetails2.getTableFullyQualifiedName());
      assertEquals("TABLE", objectDetails2.getType());
      assertEquals(SourceObjectStatus.ACTIVE, objectDetails2.getSourceObjectStatus());
    }
  }

  @Test
  void testCursorFields() throws Exception {
    ObjectDetails objectDetails = ObjectDetails.builder().build();
    Method method =
        TestConnector.class.getDeclaredMethod("getCursorFieldNames", ObjectDetails.class);
    method.setAccessible(true);
    Set<String> cursorFields = (Set<String>) method.invoke(testConnector, objectDetails);
    method.setAccessible(false);
    assertEquals(Set.of("SystemModstamp"), cursorFields);
  }

  @Test
  void testCompareOffset() throws Exception {

    /*
    Cases:
    cursor empty => current offset
    cursor not empty, previous offset empty map, current offset empty map => current offset
    cursor not empty, previous offset empty map, current offset with no ck field => current offset
    cursor not empty, previous offset empty map, current offset with ck field => current offset
    cursor not empty, previous offset with no ck field, current offset empty map => current offset
    cursor not empty, previous offset with no ck field, current offset with no ck field => current offset
    cursor not empty, previous offset with no ck field, current offset with ck field => current offset
    cursor not empty, previous offset with ck field, current offset empty map => previous offset
    cursor not empty, previous offset with ck field, current offset with no ck field => previous offset
    cursor not empty, previous offset with ck field, current offset with ck field => compare and return
     */

    Method method =
        TestConnector.class.getDeclaredMethod(
            "compareAndGetLatestOffset",
            ObjectDetails.class,
            Offset.class,
            Offset.class,
            List.class);
    ObjectDetails objectIdentity = ObjectDetails.builder().build();
    io.hevo.connector.model.field.schema.base.Field ckField =
        new HDateTimeField.Builder("ckfield", "TIMESTAMP", 0, FieldState.ACTIVE, 9)
            .ckOrdinal(0)
            .build();
    io.hevo.connector.model.field.schema.base.Field nonCkField =
        new HDateTimeField.Builder("nonckfield", "TIMESTAMP", 0, FieldState.ACTIVE, 9).build();
    {
      //            cursor empty => current offset
      Offset oldOffset = Offset.empty();
      Offset currentOffset = Offset.empty();
      method.setAccessible(true);
      Offset offset =
          (Offset)
              method.invoke(
                  testConnector, objectIdentity, oldOffset, currentOffset, List.of(nonCkField));
      assertEquals(currentOffset, offset);
    }

    {
      //            cursor not empty, previous offset empty map, current offset empty map => current
      // offset
      Offset oldOffset = Offset.empty();
      Offset currentOffset = Offset.empty();
      method.setAccessible(true);
      Offset offset =
          (Offset)
              method.invoke(
                  testConnector,
                  objectIdentity,
                  oldOffset,
                  currentOffset,
                  List.of(ckField, nonCkField));
      assertEquals(currentOffset, offset);
    }

    {
      //            cursor not empty, previous offset empty map, current offset with no ck field =>
      // current offset
      Offset oldOffset = Offset.empty();
      Offset currentOffset = Offset.get(nonCkField, new HDateTime(LocalDateTime.now()));
      method.setAccessible(true);
      Offset offset =
          (Offset)
              method.invoke(
                  testConnector,
                  objectIdentity,
                  oldOffset,
                  currentOffset,
                  List.of(ckField, nonCkField));
      assertEquals(currentOffset, offset);
    }

    {
      //            cursor not empty, previous offset empty map, current offset with ck field =>
      // current offset
      Offset oldOffset = Offset.empty();
      Offset currentOffset = Offset.get(ckField, new HDateTime(LocalDateTime.now()));
      method.setAccessible(true);
      Offset offset =
          (Offset)
              method.invoke(
                  testConnector,
                  objectIdentity,
                  oldOffset,
                  currentOffset,
                  List.of(ckField, nonCkField));
      assertEquals(currentOffset, offset);
    }

    {
      //            cursor not empty, previous offset with no ck field, current offset empty map =>
      // current offset
      Offset oldOffset = Offset.get(nonCkField, new HDateTime(LocalDateTime.now()));
      Offset currentOffset = Offset.empty();
      method.setAccessible(true);
      Offset offset =
          (Offset)
              method.invoke(
                  testConnector,
                  objectIdentity,
                  oldOffset,
                  currentOffset,
                  List.of(ckField, nonCkField));
      assertEquals(currentOffset, offset);
    }

    {
      //            cursor not empty, previous offset with no ck field, current offset with no ck
      // field => current offset
      Offset oldOffset = Offset.get(nonCkField, new HDateTime(LocalDateTime.now()));
      Offset currentOffset = Offset.get(nonCkField, new HDateTime(LocalDateTime.now()));
      method.setAccessible(true);
      Offset offset =
          (Offset)
              method.invoke(
                  testConnector,
                  objectIdentity,
                  oldOffset,
                  currentOffset,
                  List.of(ckField, nonCkField));
      assertEquals(currentOffset, offset);
    }

    {
      //            cursor not empty, previous offset with no ck field, current offset with ck field
      // => current offset
      Offset oldOffset = Offset.get(nonCkField, new HDateTime(LocalDateTime.now()));
      Offset currentOffset = Offset.get(ckField, new HDateTime(LocalDateTime.now()));
      method.setAccessible(true);
      Offset offset =
          (Offset)
              method.invoke(
                  testConnector,
                  objectIdentity,
                  oldOffset,
                  currentOffset,
                  List.of(ckField, nonCkField));
      assertEquals(currentOffset, offset);
    }

    {
      //            cursor not empty, previous offset with ck field, current offset empty map =>
      // current offset
      Offset oldOffset = Offset.get(ckField, new HDateTime(LocalDateTime.now()));
      Offset currentOffset = Offset.empty();
      method.setAccessible(true);
      Offset offset =
          (Offset)
              method.invoke(
                  testConnector,
                  objectIdentity,
                  oldOffset,
                  currentOffset,
                  List.of(ckField, nonCkField));
      assertEquals(oldOffset, offset);
    }

    {
      //            cursor not empty, previous offset with ck field, current offset with no ck field
      // => current offset
      Offset oldOffset = Offset.get(ckField, new HDateTime(LocalDateTime.now()));
      Offset currentOffset = Offset.get(nonCkField, new HDateTime(LocalDateTime.now()));
      method.setAccessible(true);
      Offset offset =
          (Offset)
              method.invoke(
                  testConnector,
                  objectIdentity,
                  oldOffset,
                  currentOffset,
                  List.of(ckField, nonCkField));
      assertEquals(oldOffset, offset);
    }

    {
      //            cursor not empty, previous offset with ck field, current offset with ck field =>
      // compare and return
      Offset oldOffset = Offset.get(ckField, new HDateTime(LocalDateTime.now().minusHours(1)));
      Offset currentOffset = Offset.get(ckField, new HDateTime(LocalDateTime.now()));
      method.setAccessible(true);
      Offset offset =
          (Offset)
              method.invoke(
                  testConnector,
                  objectIdentity,
                  oldOffset,
                  currentOffset,
                  List.of(ckField, nonCkField));
      assertEquals(currentOffset, offset);
    }

    {
      //            cursor not empty, previous offset with ck field, current offset with ck field =>
      // compare and return
      Offset oldOffset = Offset.get(ckField, new HDateTime(LocalDateTime.now()));
      Offset currentOffset = Offset.get(ckField, new HDateTime(LocalDateTime.now().minusHours(1)));
      method.setAccessible(true);
      Offset offset =
          (Offset)
              method.invoke(
                  testConnector,
                  objectIdentity,
                  oldOffset,
                  currentOffset,
                  List.of(ckField, nonCkField));
      assertEquals(oldOffset, offset);
    }
  }

  @Test
  void testAccountTypePopulation() throws Exception {
    TestConnector testConnector = new TestConnector();
    Map<String, Object> properties =
        Map.of(
            "account_type", "SANDBOX",
            "user", "root",
            "password", "password");

    ConnectorFrameworkUtils.getInstance()
        .mapPropertiesToConnector(properties, TestConnector.class, testConnector, null);

    Field accountTypeField = TestConnector.class.getDeclaredField("accountType");
    accountTypeField.setAccessible(true);
    assertEquals(AccountType.SANDBOX, accountTypeField.get(testConnector));
    accountTypeField.setAccessible(false);
  }

  @Test
  void testAuthCredentialsPopulation() throws Exception {
    TestConnector testConnector = new TestConnector();
    Map<String, Object> properties =
        Map.of(
            "oauth_token", 123,
            "user", "root",
            "password", "password");

    AuthCredentials.AuthGenerator authGenerator =
        new AuthCredentials.AuthGenerator() {
          @Override
          public AuthCredentials generateAuthCredentials() throws ConnectorException {
            return new AuthCredentials("client_id1", "client_secret1", "refresh_token1");
          }
        };
    ConnectorFrameworkUtils.getInstance()
        .mapPropertiesToConnector(properties, TestConnector.class, testConnector, authGenerator);

    Field authCredentialsField = TestConnector.class.getDeclaredField("authCredentials");
    authCredentialsField.setAccessible(true);
    AuthCredentials authCredentials = (AuthCredentials) authCredentialsField.get(testConnector);
    assertEquals(AuthType.OAUTH, authCredentials.getAuthType());
    assertEquals("client_id1", authCredentials.getClientId());
    assertEquals("client_secret1", authCredentials.getClientSecret());
    assertEquals("refresh_token1", authCredentials.getRefreshToken());
    authCredentialsField.setAccessible(false);
    assertThrows(ConnectorException.class, this.testConnector::initializeConnection);
  }

  private Table mockTable(String name, Schema schema, List<Column> columns, String type) {
    Table table = mock(Table.class);
    when(table.getName()).thenReturn(name);
    when(table.getTableType()).thenReturn(new TableType(type));
    when(table.getColumns()).thenReturn(columns);
    when(table.getSchema()).thenReturn(schema);
    return table;
  }

  private Catalog mockCatalog(LinkedHashSet<Table> tables) {
    Catalog catalog = mock(Catalog.class);
    when(catalog.getTables()).thenReturn(tables);
    return catalog;
  }

  private Column mockColumn(String name, String type, boolean isPk) {
    return mockColumn(name, type, isPk, type, null, 0);
  }

  private Column mockColumn(
      String name,
      String type,
      boolean isPk,
      String databaseSpecificTypeName,
      String defaultValue,
      int precision) {
    Column column = mock(Column.class);
    when(column.getName()).thenReturn(name);

    JavaSqlType javaSqlType = mock(JavaSqlType.class);
    when(javaSqlType.getName()).thenReturn(type);
    ColumnDataType columnDataType = mock(ColumnDataType.class);
    when(columnDataType.getDatabaseSpecificTypeName()).thenReturn(databaseSpecificTypeName);
    when(columnDataType.getJavaSqlType()).thenReturn(javaSqlType);

    when(column.getType()).thenReturn(columnDataType);
    when(column.getColumnDataType()).thenReturn(columnDataType);
    when(column.isPartOfPrimaryKey()).thenReturn(isPk);
    when(column.getDefaultValue()).thenReturn(defaultValue);
    when(column.getSize()).thenReturn(precision);
    return column;
  }

  @Test
  void testHandleExceptions() throws ConnectorException {
    try (TestConnector connector = new TestConnector()) {
      Assertions.assertThrows(
          RateLimitException.class,
          () -> connector.handleExceptions("error", new SQLException("REQUEST_LIMIT_EXCEEDED")));
      Assertions.assertThrows(
          ConnectorException.class,
          () -> connector.handleExceptions("error", new SQLException("Other exceptions")));
    }
  }
}
