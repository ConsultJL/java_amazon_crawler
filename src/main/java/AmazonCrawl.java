import com.jauntium.Browser;
import com.jauntium.Element;
import com.jauntium.Elements;
import com.jauntium.NotFound;
import org.json.JSONException;
import org.json.JSONObject;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

import java.nio.charset.StandardCharsets;
import java.util.Random;

public class AmazonCrawl {
    public static void main(String[] args) throws NotFound {
        // Setup chrome headless browser
        // @TODO: Change this path to yours for the chromedriver
        System.setProperty("webdriver.chrome.driver", "/Users/jlancaster/chromedriver");
        ChromeOptions options = new ChromeOptions();
        options.addArguments("--headless");

        // Setup browser and go to amazon
        Browser browser = new Browser(new ChromeDriver(options));
        browser.visit("https://www.amazon.com/gp/offer-listing/B086542G1M");

        // Find all offers for this ASIN
        Elements offers = browser.doc.findEvery("<div class=olpOffer>");

        // Loop through each offer
        for (Element offer : offers) {
            // Only get information on new offers
            if (offer.findFirst("<span class=olpCondition>").getText().equals("New")) {

                // Collect price, condition and seller
                String price = offer.findFirst("<span class=olpOfferPrice>").getText();
                String condition = offer.findFirst("<span class=olpCondition>").getText();
                String seller = offer.findFirst("<h3 class=olpSellerName>").getText();

                // If seller is empty it's Amazon
                if (seller.equals("")) {
                    seller = "Amazon";
                }

                // Output what we found
                System.out.println("-------- Amazon Offer ----------");
                System.out.format("Price: %s\n", price);
                System.out.format("Condition: %s\n", condition);
                System.out.format("Seller: %s\n", seller);
                System.out.println("--------------------------------");

                // Setup kinesis stream name and create the kinesis client
                String streamName = "jeremy_testing";
                Region region = Region.US_EAST_1;
                KinesisClient kinesisClient = KinesisClient.builder()
                        .region(region)
                        .build();

                // Validate that it's an active and enabled kinesis stream
                validateStream(kinesisClient, streamName);

                // Create the object to be sent to kinesis
                JSONObject crawlrecord = new JSONObject();
                try {
                    crawlrecord.put("price", price);
                    crawlrecord.put("condition", condition);
                    crawlrecord.put("seller", seller);
                } catch (JSONException e) {
                    e.printStackTrace();
                }

                // Send it to kinesis and close the client
                sendCrawlRecords(crawlrecord, kinesisClient, streamName);
                kinesisClient.close();
            }
        }
    }

    /**
     * Validate that the provider kinesis stream is active and enabled.
     *
     * @param kinesisClient
     * @param streamName
     */
    private static void validateStream(KinesisClient kinesisClient, String streamName) {
        try {
            DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                    .streamName(streamName)
                    .build();

            DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(describeStreamRequest);

            if (!describeStreamResponse.streamDescription().streamStatus().toString().equals("ACTIVE")) {
                System.err.println("Stream " + streamName + " is not active. Please wait a few moments and try again.");
                System.exit(1);
            }
        } catch (KinesisException e) {
            System.err.println("Error found while describing the stream " + streamName);
            System.err.println(e);
            System.exit(1);
        }
    }

    /**
     * Send a crawl record to Kinesis using AWS SDK
     *
     * @param crawlRecord
     * @param kinesisClient
     * @param streamName
     */
    private static void sendCrawlRecords(JSONObject crawlRecord, KinesisClient kinesisClient,
                                         String streamName) {
        byte[] bytes;
        String partitionKey;
        bytes = crawlRecord.toString().getBytes(StandardCharsets.UTF_8);
        partitionKey = genRandomString();

        System.out.println("Putting record: (" + partitionKey + ") " + crawlRecord.toString());
        PutRecordRequest request = PutRecordRequest.builder()
                .partitionKey(partitionKey)
                .streamName(streamName)
                .data(SdkBytes.fromByteArray(bytes))
                .build();
        try {
            kinesisClient.putRecord(request);
        } catch (KinesisException e) {
            e.getMessage();
        }
    }

    /**
     * Generate a random string of chars to use as the partition key for kinesis
     *
     * @return
     */
    public static String genRandomString() {
        // Numeral 0
        int leftLimit = 48;

        // Letter z
        int rightLimit = 122;

        // String length to hit
        int targetStringLength = 10;

        Random random = new Random();
        String generatedString = random.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();

        return generatedString;
    }
}
