// package io.hevo.connector.test_connector;
//
// import io.hevo.connector.model.field.schema.base.Field;
// import io.hevo.connector.GenericConnector;
// import io.hevo.connector.client.fortress.FortressClient;
// import io.hevo.connector.model.Context;
// import io.hevo.connector.model.ExecutionResult;
// import io.hevo.connector.model.ObjectConfig;
// import io.hevo.connector.model.ObjectDetails;
// import io.hevo.connector.model.ObjectIdentity;
// import io.hevo.connector.model.PollingConfig;
// import io.hevo.connector.offset.Offset;
// import io.hevo.connector.processor.DummyHeartbeater;
// import io.hevo.connector.utils.ConnectorFactory;
// import io.hevo.connector.utils.ConnectorFrameworkUtils;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.concurrent.ExecutorService;
// import java.util.concurrent.Executors;
// import java.util.concurrent.atomic.AtomicInteger;
//
// public class SfTest {
//
//  private static final Logger log = LoggerFactory.getLogger(SfTest.class);
//  private static final AtomicInteger index = new AtomicInteger(0); // Thread-safe counter
//
//  private static void testConnector() throws Exception {
//    GenericConnector connector = ConnectorFactory.getInstance().getConnector();
//    log.info(connector.getClass().getCanonicalName());
//
//    Map<String, Object> config = new HashMap<>();
//    /*
//            config.put("clientId",
//     "3MVG9NnK0U_HimV58kS1maPwjG5yPqiZ3kD50U9du3o05W3HnfvQAWoGDFCTv.62i8GC5DenUDMJzFolh6kQ6");
//            config.put("clientSecret",
//     "150B6D4F7FA29269CDF51AA006EBC5841CCCD2ECAD868F9EEBC89FE83A7DE706");
//            config.put("refreshToken",
//     "5Aep861ZToW_TPW2I9IVwhtv4Df08gZRDO75gt_Y.YNBbf.ve8ZrayV9Z7HhJ.cOCjKhffFT75kSaDCOVAEgENn");
//    */
//
//    //    config.put("account_type", "PRODUCTION");
//    //    config.put("authorized_account", 106330L);
//
//        ConnectorFrameworkUtils.getInstance()
//            .mapPropertiesToConnector(
//                config,
//                connector.getClass(),
//                connector,
//                SourceType.SALESFORCE,
//                new FortressClient(
//                    "integrations", "integrations@hevo",
//     "https://gamma-fortress-service.hevo.me"));
//    connector.initializeConnection();
//
//    String objToTest = "Account";
//    List<ObjectDetails> objectDetails = connector.getObjects(null, null, null, null);
//    ObjectDetails engagementTasksObj = null;
//    log.info("FETCHING OBJECTS:");
//    for (ObjectDetails object : objectDetails) {
//      log.info("Object = " + object.getCatalogName() + " " + object.getTableName());
//      if (object.getTableName().equals(objToTest)) {
//        engagementTasksObj = object;
//      }
//    }
//
//    log.info("FETCHING FIELDS");
//    ObjectConfig engagementObjectConfig = connector.getFields(engagementTasksObj);
//    for (Field f : engagementObjectConfig.fields()) {
//      log.info(f.name() + " " + f.logicalType() + " " + f.properties().isCK());
//    }
//
//    PollingConfig pollingConfig =
//        PollingConfig.builder()
//            .sourceObjectId("1")
//            .filterConditions(List.of())
//            .objectConfig(engagementObjectConfig)
//            .offset(Offset.builder().build())
//            .build();
//    Context context =
//        new JdbcConnectorContext(ObjectIdentity.builder().table(objToTest).build(),
// pollingConfig);
//
//    // Create a thread pool with a fixed number of threads
//    ExecutorService executor = Executors.newFixedThreadPool(1000);
//
//    try {
//      //           Submit 1000 tasks for execution
//      for (int i = 0; i < 1; i++) {
//        executor.submit(
//            () -> {
//              try {
//                // Log the count for each thread
//                log.info("Count = " + index.incrementAndGet());
//
//                // Fetch data from the source using the connector
//                ExecutionResult executionResult =
//                    (ExecutionResult)
//                        connector
//                            .fetchDataFromSource(context, null, new DummyHeartbeater())
//                            .values()
//                            .stream()
//                            .findFirst()
//                            .get();
//
//                // Log the fetched records
//                log.info("Records Fetched: " + executionResult.fetchedRecords());
//
//                // Log the last offset
//                log.info(
//                    "Last Offset: "
//                        + executionResult.lastReadOffset().getOffset().values().stream()
//                            .findFirst()
//                            .orElse(null));
//              } catch (Exception e) {
//                log.error("Error executing thread", e);
//              }
//            });
//      }
//    } finally {
//      //           Gracefully shut down the executor
//      executor.shutdown();
//    }
//
//    // Test Incremental
//    /*
//     * HField cursor = (HField) engagementObjectConfig.getFields().stream().filter(h ->
//     * h.isCK()).findFirst().get(); HDatum hTimeStamp =
//     * executionResult.lastReadOffset().getOffset().values().stream().findFirst().orElse(null);
//     * PollingConfig.FilterCondition filterCondition = new PollingConfig.FilterCondition(cursor,
//     * hTimeStamp, PollingConfig.Op.gte); fetchParameters = new HashMap<>(); pollingConfig =
//     * PollingConfig.builder() .sourceObjectId("1") .filterConditions(List.of(filterCondition))
//     * .objectConfig(engagementObjectConfig) .offset(new Offset()) .build();
//     * fetchParameters.put(ObjectIdentity.builder().table(objToTest).build(), pollingConfig); if
//     * (hTimeStamp != null) { System.out.println("Adding context."); context = new
//     * Context(fetchParameters); } executionResult = (ExecutionResult)
//     * connector.fetchDataFromSource(context, null).values().stream().findFirst().get();
//     * System.out.println("Records Fetched: " + executionResult.fetchedRecords());
//     * System.out.println("Last Offset: " +
//     * executionResult.lastReadOffset().getOffset().values().stream().findFirst().orElse(null));
//     */
//    //    connector.close();
//  }
//
//  public static void main(String[] args) throws Exception {
//    testConnector();
//  }
// }
