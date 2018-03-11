package boontorrent;


import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.*;
import com.amazonaws.services.kinesis.model.*;

import java.nio.ByteBuffer;
import java.util.List;


public class KinesisTest {

    public static final String STREAM_NAME = "boontorrent-test";

    private static AmazonKinesis kinesis;

    public static void init() {
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        kinesis = AmazonKinesisClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
    }

    public static void main(String[] args) {
        init();

        // Describe the stream and check if it exists.
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(STREAM_NAME);
        try {
            kinesis.describeStream(describeStreamRequest);
            StreamDescription streamDescription = kinesis.describeStream(describeStreamRequest).getStreamDescription();

            System.out.printf("Stream %s has a status of %s.\n", STREAM_NAME, streamDescription.getStreamStatus());

        } catch (ResourceNotFoundException ex) {
            System.out.printf("Stream %s does not exist.\n", STREAM_NAME);
            System.exit(1);
        }

        // List all of my streams.
        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        listStreamsRequest.setLimit(10);
        ListStreamsResult listStreamsResult = kinesis.listStreams(listStreamsRequest);
        List<String> streamNames = listStreamsResult.getStreamNames();
        while (listStreamsResult.isHasMoreStreams()) {
            if (streamNames.size() > 0) {
                listStreamsRequest.setExclusiveStartStreamName(streamNames.get(streamNames.size() - 1));
            }

            listStreamsResult = kinesis.listStreams(listStreamsRequest);
            streamNames.addAll(listStreamsResult.getStreamNames());
        }
        // Print all of my streams.
        System.out.println("List of my streams: ");
        for (int i = 0; i < streamNames.size(); i++) {
            System.out.println("\t- " + streamNames.get(i));
        }

        for (int i = 0; i < 5; i++) {
            long createTime = System.currentTimeMillis();
            PutRecordRequest putRecordRequest = new PutRecordRequest()
                    .withStreamName(STREAM_NAME)
                    .withPartitionKey("partition")
                    .withData(ByteBuffer.wrap(Integer.toString(i).getBytes()));
            PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
            System.out.printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
                    putRecordRequest.getPartitionKey(),
                    putRecordResult.getShardId(),
                    putRecordResult.getSequenceNumber());
        }
    }
}

