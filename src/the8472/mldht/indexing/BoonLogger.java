package the8472.mldht.indexing;

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.*;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;
import lbms.plugins.mldht.kad.KBucketEntry;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.RPCServer;
import lbms.plugins.mldht.kad.messages.AbstractLookupRequest;
import lbms.plugins.mldht.kad.messages.AnnounceRequest;
import lbms.plugins.mldht.kad.messages.GetPeersRequest;
import the8472.mldht.TorrentFetcher;
import the8472.mldht.cli.TorrentInfo;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class BoonLogger {

    private static final String AWS_REGION = "us-east-1";
    private static final String STREAM_NAME = "boonlog";
    private static final String BUCKET_NAME = "boontorrent";
    private static final int LOG_VERSION = 1;
    private static final File database = new File(System.getProperty("user.home"), "GeoLite2-City.mmdb");

    private static final BoonLogger logger = new BoonLogger();
    private Key ourID;


    private TransferManager transferManager;
    private AmazonKinesis kinesis;
    private DatabaseReader geoReader;

    private JsonFactory jsonFactory = new JsonFactory();

    private BoonLogger() {
        try {
            geoReader = new DatabaseReader.Builder(database).withCache(new CHMCache()).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

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

    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();
    private static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    private void addDHTVersionInfo(JsonGenerator generator, AbstractLookupRequest req) throws IOException {
        if (req.getVersion().isPresent()) {
            byte[] versionBytes = req.getVersion().get();

            char[] versionID = new char[2];
            versionID[0] = (char) versionBytes[0];
            versionID[1] = (char) versionBytes[1];

            generator.writeStringField("version", new String(versionID));
            generator.writeStringField("version_hex", bytesToHex(versionBytes));
        }
    }

    private void addGeoInfo(JsonGenerator generator, InetAddress ip) throws IOException {
        if (ip instanceof Inet6Address) {
            return;
        }
        CityResponse response;
        try {
            response = geoReader.city(ip);
        } catch (GeoIp2Exception | IOException e) {
            return;
        }

        generator.writeObjectFieldStart("location");

        Location location = response.getLocation();
        Country country = response.getCountry();
        Subdivision subdivision = response.getMostSpecificSubdivision();
        City city = response.getCity();

        if(country != null) {
            generator.writeStringField("country_iso", country.getIsoCode());
            generator.writeStringField("country", country.getName());
        }
        if(subdivision != null) {
            generator.writeStringField("sub_iso", subdivision.getIsoCode());
            generator.writeStringField("subdivision", subdivision.getName());
        }
        if(city != null) {
            generator.writeStringField("city", city.getName());
        }
        if(location != null) {
            generator.writeNumberField("latitude", location.getLatitude());
            generator.writeNumberField("longitude", location.getLongitude());
            generator.writeNumberField("accuracy", location.getAccuracyRadius());
        }
        generator.writeEndObject();
    }

    public void logGetPeers(GetPeersRequest gpr) {
        RPCServer srv = gpr.getServer();

        ourID = srv.getDerivedID();
        Key theirID = gpr.getID();
        Key infohash = gpr.getInfoHash();
        InetAddress theirIP = gpr.getOrigin().getAddress();

        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        try {
            JsonGenerator generator = jsonFactory.createGenerator(stream, JsonEncoding.UTF8);
            generator.writeStartObject();
            generator.writeStringField("type", "get_peers");
            generator.writeNumberField("v", LOG_VERSION);
            generator.writeStringField("infohash", infohash.toString(false));
            generator.writeStringField("our_id", ourID.toString(false));
            generator.writeStringField("their_id", theirID.toString(false));
            generator.writeStringField("their_ip", theirIP.getHostAddress());

            addDHTVersionInfo(generator, gpr);
            addGeoInfo(generator, theirIP);

            generator.writeEndObject();
            generator.close();

            sendToKinesis(stream, infohash.toString(false));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void logAnnounce(AnnounceRequest anr) {
        RPCServer srv = anr.getServer();

        ourID        = srv.getDerivedID();
        Key theirID  = anr.getID();
        Key infohash = anr.getInfoHash();

        InetAddress theirIP = anr.getOrigin().getAddress();

        boolean isSeed = anr.isSeed();
        Optional<String> name = anr.getNameUTF8();

        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        try {
            JsonGenerator generator = jsonFactory.createGenerator(stream, JsonEncoding.UTF8);
            generator.writeStartObject();
            generator.writeStringField("type", "announce");
            generator.writeNumberField("v", LOG_VERSION);
            generator.writeStringField("infohash", infohash.toString(false));
            if(name.isPresent()) {
                generator.writeStringField("name", name.get());
            }

            generator.writeStringField("our_id", ourID.toString(false));
            generator.writeStringField("their_id", theirID.toString(false));
            generator.writeStringField("their_ip", theirIP.getHostAddress());

            addDHTVersionInfo(generator, anr);

            generator.writeBooleanField("is_seed", isSeed);

            addGeoInfo(generator, theirIP);

            generator.writeEndObject();
            generator.close();

            sendToKinesis(stream, infohash.toString(false));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void logResolve(ByteBuffer torrent, TorrentDumper.FetchStats stats, TorrentFetcher.FetchTask task) {

        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        try {
            JsonGenerator generator = jsonFactory.createGenerator(stream, JsonEncoding.UTF8);
            generator.writeStartObject();
            generator.writeStringField("type", "resolve");
            generator.writeStringField("our_id", ourID.toString(false));
            generator.writeNumberField("v", LOG_VERSION);
            TorrentInfo.decodeTorrent(torrent, generator);

            List<KBucketEntry> sources = stats.recentSources;
            generator.writeArrayFieldStart("peers");
            for(KBucketEntry kbe : sources) {
                generator.writeStartObject();
                generator.writeStringField("ip", kbe.getAddress().getAddress().getHostAddress().toString());
                generator.writeStringField("node_id", kbe.getID().toString(false));
                addGeoInfo(generator, kbe.getAddress().getAddress());
                generator.writeEndObject();
            }
            generator.writeEndArray();
            generator.writeEndObject();
            generator.close();

            sendToKinesis(stream, task.infohash().toString(false));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendToKinesis(ByteArrayOutputStream stream, String infohash) {
        System.out.println(stream);

        PutRecordRequest putRecordRequest = new PutRecordRequest()
                .withStreamName(STREAM_NAME)
                .withPartitionKey(infohash)
                .withData(ByteBuffer.wrap(stream.toByteArray()));
        kinesis.putRecord(putRecordRequest);
    }


    public void batchTorrentUpload(Path torrentDir) {
        try {
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
            files.forEach(File::delete);
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }

    }
}