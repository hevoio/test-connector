package io.hevo.connector.test_connector;

import io.hevo.connector.exceptions.ConnectorException;
import io.hevo.connector.exceptions.RateLimitException;
import io.hevo.connector.jdbc.DbConnectionSource;
import io.hevo.connector.jdbc.JdbcConnector;
import io.hevo.connector.model.AccountType;
import io.hevo.connector.model.AuthCredentials;
import io.hevo.connector.model.AuthType;
import io.hevo.connector.model.ObjectDetails;
import io.hevo.connector.model.ObjectSchema;
import io.hevo.connector.model.enums.SourceObjectStatus;
import io.hevo.connector.model.field.data.datum.hudt.HDatum;
import io.hevo.connector.model.field.schema.base.Field;
import io.hevo.connector.offset.Offset;
import io.hevo.connector.ui.Auth;
import io.hevo.connector.ui.OptionsRef;
import io.hevo.connector.ui.OptionsRefType;
import io.hevo.connector.ui.Property;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
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
public class TestConnector extends JdbcConnector {

  private static final Logger log = LoggerFactory.getLogger(TestConnector.class);

  private static final String CURSOR_FIELD = "SystemModstamp";

  @Property(
      name = "account_type",
      displayName = "Account Type",
      defaultValue = "PRODUCTION",
      fieldOrder = 2,
      optionsRef =
          @OptionsRef(
              type = OptionsRefType.STATIC,
              allowedValues = {"PRODUCTION", "SANDBOX"}))
  private AccountType accountType;

  @Auth(name = "auth_type", displayName = "Auth Type", type = AuthType.OAUTH, fieldOrder = 1)
  private AuthCredentials authCredentials;

  @Override
  public void initializeConnection() throws ConnectorException {
    String jdbcUrlProtocol = "jdbc:salesforce:";

    Properties properties = new Properties();

    if (null != authCredentials && AuthType.OAUTH.equals(authCredentials.getAuthType())) {
      properties.setProperty("AuthScheme", "OAuth");
      properties.setProperty("OAuthClientId", authCredentials.getClientId());
      properties.setProperty("OAuthClientSecret", authCredentials.getClientSecret());
      properties.setProperty("OAuthRefreshToken", authCredentials.getRefreshToken());
      properties.setProperty("InitiateOAuth", "REFRESH");
    }

    properties.setProperty(
        "UseSandbox", AccountType.SANDBOX.equals(accountType) ? "true" : "false");
    properties.setProperty("UseBulkAPI", "true");
    properties.setProperty("BulkAPIVersion", "v2");
    properties.setProperty("ConnectOnOpen", "true");
    properties.setProperty("AutoCache", "false");
    StringBuilder otherProperties = new StringBuilder();
    otherProperties.append("OEMKey=").append(getDriverKey()).append(";");
    properties.setProperty("Other", otherProperties.toString());
    // Mandatory Base Properties
    properties.setProperty("LogFile", getLogFilePath());
    properties.setProperty("Verbosity", getLogVerbosity());

    try {
      this.connection = DriverManager.getConnection(jdbcUrlProtocol, properties);
      databaseConnectionSource = new DbConnectionSource(connection);
    } catch (SQLException e) {
      log.error("Initialize connection failed");
      throw new ConnectorException(e);
    }
  }

  private static final Set<String> BULK_MODE_UNSUPPORTED_TABLES =
      Set.of(
          "AcceptedEventRelation",
          "AppTabMember",
          "ColorDefinition",
          "EntityDefinition",
          "SiteDetail",
          "KnowledgeArticleVersionHistory",
          "RelationshipInfo",
          "DeclinedEventRelation",
          "KnowledgeArticleVersion",
          "ContentFolderItem",
          "OutgoingEmailRelation",
          "TopicAssignment",
          "TaskStatus",
          "DataType",
          "Announcement",
          "DatacloudAddress",
          "IconDefinition",
          "FieldSecurityClassification",
          "ListViewChartInstance",
          "ContentDocumentLink",
          "BackgroundOperationResult",
          "PicklistValueInfo",
          "EntitySubscription",
          "PartnerRole",
          "BotEventLog",
          "DataAssetSemanticGraphEdge",
          "NetworkUserHistoryRecent",
          "DatacloudDandBCompany",
          "ListViewChartInstances",
          "OwnerChangeOptionInfo",
          "RecentlyViewed",
          "FlexQueueItem",
          "KnowledgeArticle",
          "FlowVersionView",
          "ApexPageInfo",
          "RelationshipDomain",
          "TaskPriority",
          "FlowVariableView",
          "SolutionStatus",
          "FieldChangeSnapshot",
          "FieldDefinition",
          "ContractStatus",
          "TaskWhoRelation",
          "PlatformAction",
          "EventWhoRelation",
          "SearchLayout",
          "FeedRevision",
          "EntityParticle",
          "FeedComment",
          "RecentFieldChange",
          "AccountUserTerritory2View",
          "ContentFolderMember",
          "DataStatistics",
          "FeedItem",
          "KnowledgeArticleVoteStat",
          "CaseStatus",
          "AuraDefinitionInfo",
          "OutgoingEmail",
          "DataAssetUsageTrackingInfo",
          "FeedAttachment",
          "OrderStatus",
          "IdeaComment",
          "KnowledgeArticleViewStat");

  /**
   * Retrieves a list of object details matching the specified criteria.
   *
   * @return A linked list of {@code ObjectDetails} representing the objects matching the criteria,
   *     excluding any unsupported tables defined in {@code BULK_MODE_UNSUPPORTED_TABLES}.
   * @throws ConnectorException If an error occurs while fetching object details.
   */
  @Override
  public List<ObjectDetails> getObjects() throws ConnectorException {
    List<ObjectDetails> objectDetails = super.getObjects();

    List<ObjectDetails> result = new ArrayList<>();
    for (ObjectDetails obj : objectDetails) {
      if (BULK_MODE_UNSUPPORTED_TABLES.contains(obj.getTableName())) {
        ObjectDetails details =
            ObjectDetails.builder()
                .catalog(obj.getCatalogName())
                .schema(obj.getSchemaName())
                .table(obj.getTableName())
                .type(obj.getType())
                .delimiter(obj.getDelimiter())
                .sourceObjectStatus(SourceObjectStatus.INACCESSIBLE)
                .blockReason("Unsupported in bulk mode.")
                .build();
        result.add(details);
        continue;
      }
      result.add(obj);
    }
    return result;
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
    List<ObjectSchema> objectData = super.fetchSchemaFromSource(objectDetails);
    List<ObjectSchema> result = new ArrayList<>(objectData.size());
    for (ObjectSchema objectSchema : objectData) {
      ObjectDetails objectDetail = objectSchema.objectDetail();
      if (BULK_MODE_UNSUPPORTED_TABLES.contains(objectDetail.getTableName())) {
        ObjectDetails details =
            ObjectDetails.builder()
                .catalog(objectDetail.getCatalogName())
                .schema(objectDetail.getSchemaName())
                .table(objectDetail.getTableName())
                .type(objectDetail.getType())
                .delimiter(objectDetail.getDelimiter())
                .sourceObjectStatus(SourceObjectStatus.INACCESSIBLE)
                .blockReason("Unsupported in bulk mode.")
                .build();
        result.add(new ObjectSchema(details, objectSchema.fields()));
        continue;
      }
      result.add(objectSchema);
    }
    return result;
  }

  /**
   * Retrieves the names of cursor fields for the specified object details. For salesforce it is
   * same for all if applicable.
   *
   * @param objectDetails The details of the object for which cursor field names are retrieved.
   * @return A set containing the names of the cursor fields.
   */
  @Override
  protected Set<String> getCursorFieldNames(ObjectDetails objectDetails) {
    return Set.of(CURSOR_FIELD);
  }

  /**
   * Compares the previous and current offsets based on the provided cursor fields and returns the
   * latest offset.
   *
   * @param previousOffset The previous offset to compare.
   * @param currentOffset The current offset to compare.
   * @param selectedFields The list of selected fields.
   * @return The latest offset between the previous and current offsets.
   */
  @Override
  protected Offset compareAndGetLatestOffset(
      ObjectDetails objectDetail,
      Offset previousOffset,
      Offset currentOffset,
      List<Field> selectedFields) {
    Optional<Field> cursorFieldOpt =
        selectedFields.stream().filter(f -> f.properties().isCK()).findAny();
    // Salesforce contains only 1 cursor field
    if (cursorFieldOpt.isEmpty()) {
      return currentOffset;
    }

    LocalDateTime previousOffsetDateValue =
        Optional.ofNullable(previousOffset)
            .map(Offset::getOffset)
            .map(m -> m.get(cursorFieldOpt.get()))
            .flatMap(HDatum::asDateTime)
            .orElse(null);
    LocalDateTime currentOffsetDateValue =
        Optional.ofNullable(currentOffset)
            .map(Offset::getOffset)
            .map(m -> m.get(cursorFieldOpt.get()))
            .flatMap(HDatum::asDateTime)
            .orElse(null);

    if (previousOffsetDateValue == null) {
      return currentOffset;
    }

    if (currentOffsetDateValue == null) {
      return previousOffset;
    }

    return (currentOffsetDateValue.isAfter(previousOffsetDateValue))
        ? currentOffset
        : previousOffset;
  }

  @Override
  public void handleExceptions(String errorMessage, Exception e) throws ConnectorException {
    log.error(errorMessage);
    if (null != e.getMessage() && e.getMessage().contains("REQUEST_LIMIT_EXCEEDED")) {
      throw new RateLimitException(
          "Encountered rate limit exception. It will be retried in the next schedule. If it still fails, please increase the rate limit at the source.");
    }
    throw new ConnectorException(errorMessage, e);
  }
}
