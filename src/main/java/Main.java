import com.microsoft.azure.documentdb.*;

public class Main {

    private static String databaseAccountUri = "<ACCOUNT>";
    private static String accountKey = "<KEY>";

    public static void main(final String[] args) throws DocumentClientException, InterruptedException {

        ConnectionPolicy policy = new ConnectionPolicy();
        policy.setConnectionMode(ConnectionMode.DirectHttps);

        DocumentClient client = new DocumentClient(databaseAccountUri, accountKey, policy, ConsistencyLevel.Session);
        client.getConnectionPolicy().setRetryOptions(new RetryOptions());

        RetryOptions retryOptions = new RetryOptions();
        retryOptions.setMaxRetryAttemptsOnThrottledRequests(1000);
        retryOptions.setMaxRetryWaitTimeInSeconds(1000);

        client.getConnectionPolicy().setRetryOptions(retryOptions);

        RequestOptions options = new RequestOptions();
        options.setPopulateQuotaInfo(true);
        options.setPopulatePartitionKeyRangeStatistics(true);
        long sleepTime = 30000;
        long sourceCollectionCount;
        long prevCount = 0;

        while (true) {
            ResourceResponse<DocumentCollection> coll1 = client.readCollection("dbs/<DB NAME>/colls/<TEST COLLECTION>", options);
            sourceCollectionCount = coll1.getDocumentCountUsage();
            if (prevCount == 0) prevCount = sourceCollectionCount;
            double currentRate = Math.floor((sourceCollectionCount - prevCount) * 1000.0 / sleepTime);
            System.out.println("current count = " + sourceCollectionCount + " docs");
            System.out.println("current ingestion rate = " + currentRate + " docs/sec");
            System.out.println("Size = " + coll1.getCollectionSizeUsage() / 1024 + " MB");
            prevCount = sourceCollectionCount;
            Thread.sleep(sleepTime);
        }
    }
}
