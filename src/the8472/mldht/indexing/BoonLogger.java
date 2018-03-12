package the8472.mldht.indexing;

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.*;
import lbms.plugins.mldht.kad.messages.GetPeersRequest;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class BoonLogger {

    private static final String AWS_REGION = "us-east-1";
    private static final String STREAM_NAME = "boontorrent-test";
    private static final String BUCKET_NAME = "boontorrent";

    private static BoonLogger logger = new BoonLogger();

    private TransferManager transferManager;
    private AmazonKinesis kinesis;

    private BoonLogger() {
        AWSCredentialsProviderChain credentials = DefaultAWSCredentialsProviderChain.getInstance();
        AmazonS3 client = AmazonS3ClientBuilder.standard()
                .withCredentials(credentials)
                .withRegion(AWS_REGION)
                .build();
        transferManager = TransferManagerBuilder.standard().withS3Client(client).build();
        kinesis = AmazonKinesisClientBuilder.standard()
                .withCredentials(credentials)
                .withRegion(AWS_REGION)
                .build();
    }


    public static BoonLogger getLogger() {
        return logger;
    }


    public void logGetPeers(GetPeersRequest gpr) {

    }

    public void logAnnounce() {

    }

    public void log(CharSequence str) {
        StringBuilder s = new StringBuilder();
        s.append("[BOON]");
        s.append(str);
        System.out.println(s);

        PutRecordRequest putRecordRequest = new PutRecordRequest()
                .withStreamName(STREAM_NAME)
                .withPartitionKey("partition")
                .withData(ByteBuffer.wrap(str.toString().getBytes()));
        kinesis.putRecord(putRecordRequest);
    }


    public void batchTorrentUpload(Path torrentDir) {
        try {
            System.out.println("[BOON] batchTorrentUpload called");
            List<File> files = Files.list(torrentDir).map(Path::toFile).collect(Collectors.toList());
            if (files.isEmpty()) {
                return;
            }
            MultipleFileUpload upload = transferManager.uploadFileList(
                    BUCKET_NAME,
                    null,
                    torrentDir.toFile(),
                    files);
            upload.waitForCompletion();
            System.out.println("[BOON] batchTorrentUpload finished");
            files.forEach(File::delete);
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }

    }
}