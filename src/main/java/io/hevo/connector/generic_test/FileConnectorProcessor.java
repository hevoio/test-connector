package io.hevo.connector.generic_test;

import io.hevo.connector.exceptions.ConnectorException;
import io.hevo.connector.model.ConnectorMeta;
import io.hevo.connector.model.field.data.datum.hudt.HStruct;
import io.hevo.connector.processor.ConnectorProcessor;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileConnectorProcessor implements ConnectorProcessor, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(FileConnectorProcessor.class);

  private final Path filePath;

  private final BufferedWriter writer;

  /**
   * Constructs a FileConnectorProcessor.
   *
   * @param outputFilePath The path to the output file.
   * @throws ConnectorException If the file cannot be opened.
   */
  public FileConnectorProcessor(String outputFilePath) throws ConnectorException {
    this.filePath = Paths.get(outputFilePath).toAbsolutePath();
    try {
      // Extract directory path
      Path directoryPath = this.filePath.getParent();

      // Check if directory exists; if not, create it
      if (directoryPath != null && !Files.exists(directoryPath)) {
        Files.createDirectories(directoryPath);
        log.info("Created directories: {}", directoryPath.toString());
      }

      // Initialize BufferedWriter with append option
      this.writer =
          Files.newBufferedWriter(
              this.filePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND);

      log.info("FileConnectorProcessor initialized. Writing to: {}", this.filePath.toString());
    } catch (IOException e) {
      throw new ConnectorException("Failed to open file for writing: " + filePath, e);
    }
  }

  @Override
  public synchronized void publish(HStruct hStruct, ConnectorMeta connectorMeta)
      throws ConnectorException {
    if (hStruct == null) {
      throw new ConnectorException("HStruct is null. Cannot write to file.");
    }

    // Convert HStruct and ConnectorMeta to a desired string format.
    // For simplicity, we'll use JSON representation. You can customize this as needed.

    String record = convertToString(hStruct, connectorMeta);
    try {
      writer.write(record);
      writer.newLine();
      writer.flush();
    } catch (Exception e) {
      throw new ConnectorException("Failed to write record to file: " + filePath, e);
    }
  }

  /**
   * Converts HStruct and ConnectorMeta to a JSON string. Customize this method based on how you
   * want to format the output.
   *
   * @param hStruct The data structure.
   * @param connectorMeta The metadata associated with the data.
   * @return A JSON-formatted string representing the record.
   */
  private String convertToString(HStruct hStruct, ConnectorMeta connectorMeta) {
    Optional<String[]> stringArray = hStruct.asArrayOfString();
    if (stringArray.isPresent()) {
      StringBuilder sb = new StringBuilder();
      // Assuming HStruct has a method to retrieve field values as a list
      for (String value : stringArray.get()) { // Replace with actual method
        sb.append(value).append(",");
      }
      return sb.toString();
    } else {
      log.info("Nothing to write.");
      return "";
    }
  }

  /**
   * Closes the BufferedWriter. Should be called when processing is complete.
   *
   * @throws ConnectorException If an error occurs while closing the writer.
   */
  @Override
  public void close() throws ConnectorException {
    try {
      if (writer != null) {
        writer.close();
      }
    } catch (IOException e) {
      throw new ConnectorException("Failed to close file writer: " + filePath, e);
    }
  }
}
