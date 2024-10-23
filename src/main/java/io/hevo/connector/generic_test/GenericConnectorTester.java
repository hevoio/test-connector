package io.hevo.connector.generic_test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hevo.connector.GenericConnector;
import io.hevo.connector.exceptions.ConnectorException;
import io.hevo.connector.exceptions.ConnectorRuntimeException;
import io.hevo.connector.model.AuthCredentials;
import io.hevo.connector.model.AuthType;
import io.hevo.connector.model.ConnectorContext;
import io.hevo.connector.model.ExecutionResult;
import io.hevo.connector.model.ObjectDetails;
import io.hevo.connector.model.ObjectSchema;
import io.hevo.connector.model.field.schema.base.FieldProperties;
import io.hevo.connector.model.field.schema.hudt.HField;
import io.hevo.connector.offset.Offset;
import io.hevo.connector.test_connector.TestConnector;
import io.hevo.connector.ui.Auth;
import io.hevo.connector.ui.Property;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A generic tester for any connector that can initialize a connection, retrieve objects, fetch
 * schema, fetch data, and close the connection.
 *
 * @param <T> The type of the connector.
 */
public class GenericConnectorTester<T extends GenericConnector> {

  private final Class<T> connectorClass;
  private final T connectorInstance;
  private final ObjectMapper objectMapper;

  /**
   * Constructs a GenericConnectorTester for the specified connector class.
   *
   * @param connectorClass The Class object of the connector.
   * @throws ConnectorException If instantiation fails.
   */
  public GenericConnectorTester(Class<T> connectorClass) throws ConnectorException {
    this.connectorClass = connectorClass;
    this.objectMapper = new ObjectMapper();
    try {
      this.connectorInstance =
          (T)
              ServiceLoader.load(GenericConnector.class)
                  .findFirst()
                  .orElseThrow(
                      () -> new ConnectorRuntimeException("Connector has not been configured"));
      ;
    } catch (Exception e) {
      throw new ConnectorException("Failed to instantiate connector class.", e);
    }
  }

  /**
   * Loads connector configuration from a JSON file.
   *
   * @param filePath The path to the JSON configuration file.
   * @throws ConnectorException If loading or setting fields fails.
   */
  public void loadConfigurationFromFile(String filePath) throws ConnectorException {
    try {
      Map<String, Object> config = objectMapper.readValue(new File(filePath), Map.class);
      setAnnotatedFields(config);
    } catch (IOException e) {
      throw new ConnectorException("Failed to read configuration file.", e);
    }
  }

  private List<Field> getAllAnnotatedFields(Class<?> clazz) {
    List<Field> annotatedFields = new ArrayList<>();
    Class<?> currentClass = clazz;
    while (currentClass != null) {
      for (Field field : currentClass.getDeclaredFields()) {
        if (field.isAnnotationPresent(Property.class) || field.isAnnotationPresent(Auth.class)) {
          // Skip static or final fields
          if (Modifier.isStatic(field.getModifiers()) || Modifier.isFinal(field.getModifiers())) {
            continue;
          }
          annotatedFields.add(field);
        }
      }
      currentClass = currentClass.getSuperclass();
    }
    return annotatedFields;
  }

  /**
   * Sets the annotated fields of the connector instance based on the provided configuration map.
   *
   * @param config A map containing field names and their corresponding values.
   * @throws ConnectorException If setting fields fails.
   */
  private void setAnnotatedFields(Map<String, Object> config) throws ConnectorException {
    List<Field> fields = getAllAnnotatedFields(connectorClass);
    for (Field field : fields) {
      String fieldName = field.getName();
      if (!config.containsKey(fieldName)) {
        throw new ConnectorException("Missing configuration for field: " + fieldName);
      }
      Object value = config.get(fieldName);
      try {
        field.setAccessible(true);
        Object parsedValue = parseValue(value, field.getType(), true);
        field.set(connectorInstance, parsedValue);
      } catch (IllegalAccessException e) {
        throw new ConnectorException("Failed to set field '" + fieldName + "'.", e);
      }
    }
  }

  /**
   * Prompts the user to input connector configuration via the console.
   *
   * @throws ConnectorException If setting fields fails.
   */
  public void promptForConfiguration() throws ConnectorException {
    Scanner scanner = new Scanner(System.in);
    List<Field> fields = getAllAnnotatedFields(connectorClass);
    Map<String, Object> configMap = new HashMap<>();

    for (Field field : fields) {
      // Skip static or final fields
      if (Modifier.isStatic(field.getModifiers()) || Modifier.isFinal(field.getModifiers())) {
        continue;
      }

      field.setAccessible(true);
      String fieldName = field.getName();
      Class<?> fieldType = field.getType();

      System.out.println(
          "Enter value for field '" + fieldName + "' (Type: " + fieldType.getSimpleName() + "): ");
      String input = scanner.nextLine();

      Object value = parseValue(input, fieldType, false);
      configMap.put(fieldName, value);
      setAnnotatedFields(configMap);
    }
  }

  /**
   * Executes the connector methods in the specified order.
   *
   * @throws ConnectorException If any connector method fails.
   */
  public void executeConnector() throws ConnectorException {
    try {
      // Initialize connection
      connectorInstance.initializeConnection();
      System.out.println("Connection initialized successfully.");

      // Fetch all objects
      List<ObjectDetails> allObjectDetails = connectorInstance.getObjects();
      System.out.println("Fetched {} objects from the source." + allObjectDetails.size());

      // Prompt user to select polling mode
      Scanner scanner = new Scanner(System.in);
      System.out.println("Select 1 for custom objects polling and 2 for all objects.");
      String choice = scanner.nextLine().trim();

      // Determine which objects to poll
      List<ObjectDetails> objectsToPoll = new ArrayList<>();
      if ("1".equals(choice)) {
        System.out.println(
            "Enter comma-separated list of objects to poll (fully qualified names):");
        String objectsInput = scanner.nextLine().trim();

        // Split input and trim whitespace
        List<String> objectsInputList =
            Arrays.stream(objectsInput.split(",")).map(String::trim).toList();

        // Match input objects with fetched ObjectDetails
        objectsToPoll =
            allObjectDetails.stream()
                .filter(od -> objectsInputList.contains(od.getTableFullyQualifiedName()))
                .collect(Collectors.toList());

        // Log unmatched objects
        List<String> unmatchedObjects =
            objectsInputList.stream()
                .filter(
                    name ->
                        allObjectDetails.stream()
                            .noneMatch(od -> od.getTableFullyQualifiedName().equals(name)))
                .toList();
        if (!unmatchedObjects.isEmpty()) {
          System.out.println(
              "The following objects were not found and will be skipped: " + unmatchedObjects);
        }

        System.out.println("Selected  " + objectsToPoll.size() + " objects to poll");

        if (objectsToPoll.isEmpty()) {
          System.out.println("No valid objects selected for polling. Exiting execution.");
          return;
        }
      } else if ("2".equals(choice)) {
        objectsToPoll = allObjectDetails;
        System.out.println("Selected all objects to poll : " + objectsToPoll.size());
      } else {
        System.err.println("Invalid choice " + choice + " .Please select either 1 or 2.");
        return;
      }

      System.out.println("Fetching object schemas...");
      List<ObjectSchema> objectSchemas = connectorInstance.fetchSchemaFromSource(objectsToPoll);

      String schemaCSV = "src/main/java/io/hevo/connector/generic_test/output/object_schemas.csv";
      exportObjectSchemasToCsv(objectSchemas, schemaCSV);

      Map<ObjectSchema, ExecutionResult> objectFetchResult = new HashMap<>();
      // Historical Data Fetching
      System.out.println("Starting historical data fetching...");
      for (ObjectSchema objectSchema : objectSchemas) {
        String outputFileName =
            "src/main/java/io/hevo/connector/generic_test/output/historical_"
                + objectSchema.objectDetail().getTableFullyQualifiedName()
                + ".csv";
        try (FileConnectorProcessor fileProcessor = new FileConnectorProcessor(outputFileName)) {
          ExecutionResult executionResult =
              connectorInstance.fetchDataFromSource(
                  new ConnectorContext(objectSchema, Offset.empty()), fileProcessor);
          objectFetchResult.put(objectSchema, executionResult);
          System.out.println(
              "Historical data fetched for object "
                  + objectSchema.objectDetail().getTableFullyQualifiedName());
        } catch (ConnectorException e) {
          System.err.println(
              "Failed to fetch historical data for object "
                  + objectSchema.objectDetail().getTableFullyQualifiedName()
                  + " error : "
                  + e.getMessage());
        }
      }

      // Incremental Data Fetching
      System.out.println("Starting incremental data fetching...");
      for (ObjectSchema objectSchema : objectSchemas) {
        ExecutionResult lastExecutionResult = objectFetchResult.get(objectSchema);
        if (lastExecutionResult == null) {
          System.out.println(
              "Skipping incremental fetch for object '{}' due to previous failures."
                  + objectSchema.objectDetail().getTableFullyQualifiedName());
          continue;
        }

        String outputFileName =
            "src/main/java/io/hevo/connector/generic_test/output/incremental_"
                + objectSchema.objectDetail().getTableFullyQualifiedName()
                + ".csv";
        try (FileConnectorProcessor fileProcessor = new FileConnectorProcessor(outputFileName)) {
          ExecutionResult executionResult =
              connectorInstance.fetchDataFromSource(
                  new ConnectorContext(objectSchema, lastExecutionResult.lastReadOffset()),
                  fileProcessor);
          objectFetchResult.put(objectSchema, executionResult);
          System.out.println(
              "Incremental data fetched for object "
                  + objectSchema.objectDetail().getTableFullyQualifiedName());
        } catch (ConnectorException e) {
          System.out.println(
              "Failed to fetch incremental data for object "
                  + objectSchema.objectDetail().getTableFullyQualifiedName()
                  + " error : "
                  + e.getMessage());
        }
      }
      System.out.println("Data fetching process completed successfully.");
    } finally {
      try {
        connectorInstance.close();
      } catch (ConnectorException e) {
        System.err.println("Failed to close the connection" + e.getMessage());
      }
    }
  }

  private void exportObjectSchemasToCsv(List<ObjectSchema> objectSchemas, String filePath) {
    // Define the CSV header
    String header =
        "fullyQualifiedName,sourceFieldName,sourceDataType,logicalType,"
            + "position,defaultValue,pkPos,customPkPos,ckOrdinal,"
            + "isNullable,isToasted,isInternal,fieldProvider,"
            + "shouldReplicateToDestination,length,precision,scale";

    // Ensure the output directory exists
    File file = new File(filePath);
    File parentDir = file.getParentFile();
    if (parentDir != null && !parentDir.exists()) {
      if (parentDir.mkdirs()) {
        System.out.println("Created output directory: " + parentDir.getAbsolutePath());
      } else {
        System.err.println("Failed to create output directory: " + parentDir.getAbsolutePath());
        return;
      }
    }

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      // Write header
      writer.write(header);
      writer.newLine();

      // Iterate through each ObjectSchema
      for (ObjectSchema objectSchema : objectSchemas) {
        String fullyQualifiedName = objectSchema.objectDetail().getTableFullyQualifiedName();
        Set<io.hevo.connector.model.field.schema.base.Field> fields = objectSchema.fields();

        for (io.hevo.connector.model.field.schema.base.Field field : fields) {
          String sourceFieldName = "";
          String sourceDataType = "";

          // Check if the field is an instance of HField to get source-specific data
          if (field instanceof HField) {
            HField hField = (HField) field;
            sourceFieldName = hField.sourceFieldName();
            sourceDataType = hField.sourceDataType();
          }

          String logicalType = field.logicalType();
          FieldProperties properties = field.properties();

          // Prepare the row data
          StringBuilder row = new StringBuilder();
          row.append(fullyQualifiedName).append(",");
          row.append(sourceFieldName).append(",");
          row.append(sourceDataType).append(",");
          row.append(logicalType).append(",");
          row.append(properties.position()).append(",");
          row.append(properties.defaultValue().orElse("")).append(",");
          row.append(properties.pkPos().isPresent() ? properties.pkPos().get() : "").append(",");
          row.append(properties.customPkPos().isPresent() ? properties.customPkPos().get() : "")
              .append(",");
          row.append(properties.ckOrdinal().isPresent() ? properties.ckOrdinal().get() : "")
              .append(",");
          row.append(properties.isNullable()).append(",");
          row.append(properties.isToasted()).append(",");
          row.append(properties.isInternal()).append(",");
          row.append(
                  properties.fieldProvider() != null ? properties.fieldProvider().toString() : "")
              .append(",");
          row.append(properties.shouldReplicateToDestination()).append(",");
          row.append(properties.length().isPresent() ? properties.length().get() : "").append(",");
          row.append(properties.precision().isPresent() ? properties.precision().get() : "")
              .append(",");
          row.append(properties.scale().isPresent() ? properties.scale().get() : "");

          // Write the row to CSV
          writer.write(row.toString());
          writer.newLine();
        }
      }
      System.out.println("CSV file created successfully at: " + filePath);
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Error while writing CSV file.");
    }
  }

  /**
   * Parses a string input into the specified type.
   *
   * @param input The input string.
   * @param type The Class object of the desired type.
   * @return The parsed object.
   * @throws ConnectorException If parsing fails.
   */
  private Object parseValue(Object input, Class<?> type, boolean fileMode)
      throws ConnectorException {
    if (input == null) {
      return null;
    }

    try {
      if (type.equals(String.class)) {
        return input.toString();
      } else if (type.equals(Integer.class) || type.equals(int.class)) {
        return Integer.parseInt(input.toString());
      } else if (type.equals(Long.class) || type.equals(long.class)) {
        return Long.parseLong(input.toString());
      } else if (type.equals(Boolean.class) || type.equals(boolean.class)) {
        return Boolean.parseBoolean(input.toString());
      } else if (type.equals(Double.class) || type.equals(double.class)) {
        return Double.parseDouble(input.toString());
      } else if (type.isEnum()) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Object enumValue = Enum.valueOf((Class<Enum>) type, input.toString().toUpperCase());
        return enumValue;
      } else if (type.equals(AuthCredentials.class)) {
        // Handle nested AuthCredentials manually
        // For simplicity, prompt for each nested field
        System.out.println("Configuring nested object 'AuthCredentials':");
        return configureAuthCredentials(input, fileMode);
      } else {
        // For complex objects, attempt to convert using Jackson
        return objectMapper.convertValue(input, type);
      }
    } catch (Exception e) {
      throw new ConnectorException(
          "Failed to parse input '" + input.toString() + "' to type " + type.getSimpleName(), e);
    }
  }

  /**
   * Manually configures the AuthCredentials object via prompts.
   *
   * @return Configured AuthCredentials instance.
   * @throws ConnectorException If configuration fails.
   */
  private AuthCredentials configureAuthCredentials(Object input, boolean fileMode)
      throws ConnectorException {
    if (fileMode) {
      Map<String, String> authMap =
          objectMapper.convertValue(input, new TypeReference<Map<String, String>>() {});
      if (authMap.get("authType").equals("OAUTH")) {
        return new AuthCredentials(
            authMap.get("clientId"), authMap.get("clientSecret"), authMap.get("refreshToken"));
      } else if (authMap.get("authType").equals("PRIVATE_KEY")) {
        return new AuthCredentials(authMap.get("apiKey"));
      } else {
        throw new ConnectorException("Incorrect auth type");
      }
    } else {
      Scanner scanner = new Scanner(System.in);

      System.out.println("Select Auth Type:");
      for (AuthType type : AuthType.values()) {
        System.out.println("- " + type);
      }
      System.out.println("Enter Auth Type: ");
      String authTypeInput = scanner.nextLine().trim().toUpperCase();

      AuthType authType;
      try {
        authType = AuthType.valueOf(authTypeInput);
      } catch (IllegalArgumentException e) {
        throw new ConnectorException("Invalid AuthType: " + authTypeInput, e);
      }

      if (authType == AuthType.OAUTH) {
        System.out.println("Enter Client ID: ");
        String clientId = scanner.nextLine().trim();

        System.out.println("Enter Client Secret: ");
        String clientSecret = scanner.nextLine().trim();

        System.out.println("Enter Refresh Token: ");
        String refreshToken = scanner.nextLine().trim();

        return new AuthCredentials(clientId, clientSecret, refreshToken);
      } else if (authType == AuthType.PRIVATE_APP) {
        System.out.println("Enter API Key: ");
        String apiKey = scanner.nextLine().trim();
        return new AuthCredentials(apiKey);
      } else {
        throw new ConnectorException("Unsupported AuthType: " + authType);
      }
    }
  }

  /**
   * Returns the connector instance for further manipulation if needed.
   *
   * @return The connector instance.
   */
  public T getConnectorInstance() {
    return connectorInstance;
  }

  // Example usage
  public static void main(String[] args) {
    try {
      // Replace SalesforceConnector.class with your actual connector class
      GenericConnectorTester<?> tester = new GenericConnectorTester<>(TestConnector.class);

      Scanner scanner = new Scanner(System.in);
      System.out.println("Choose configuration method:");
      System.out.println("1. Load from properties file");
      System.out.println("2. Enter manually via prompts");
      System.out.println("Enter choice (1 or 2): ");
      String choice = scanner.nextLine();

      if ("1".equals(choice)) {
        String filePath = "src/main/java/io/hevo/connector/generic_test/config.json";
        tester.loadConfigurationFromFile(filePath);
      } else if ("2".equals(choice)) {
        tester.promptForConfiguration();
      } else {
        System.err.println("Invalid choice. Exiting.");
        return;
      }
      tester.executeConnector();

    } catch (ConnectorException e) {
      System.err.println("Connector testing failed: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
