package boontorrent;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

public class MaxmindTest {
    public static void main(String[] args) throws IOException {
        // A File object pointing to your GeoIP2 or GeoLite2 database
        File database = new File(System.getProperty("user.home"), "GeoLite2-City.mmdb");

        // This creates the DatabaseReader object. To improve performance, reuse
        // the object across lookups. The object is thread-safe.
        DatabaseReader reader = new DatabaseReader.Builder(database).build();

        InetAddress ipAddress = InetAddress.getByName("67.205.246.131");

        // Replace "city" with the appropriate method for your database, e.g.,
        // "country".
        CityResponse response = null;
        try {
            response = reader.city(ipAddress);
        } catch (GeoIp2Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        Country country = response.getCountry();
        System.out.println(country.getIsoCode());            // 'US'
        System.out.println(country.getName());               // 'United States'

        Subdivision subdivision = response.getMostSpecificSubdivision();
        System.out.println(subdivision.getName());    // 'Minnesota'
        System.out.println(subdivision.getIsoCode()); // 'MN'

        City city = response.getCity();
        System.out.println(city.getName()); // 'Minneapolis'

        Postal postal = response.getPostal();
        System.out.println(postal.getCode()); // '55455'

        Location location = response.getLocation();
        System.out.println(location.getLatitude());  // 44.9733
        System.out.println(location.getLongitude()); // -93.2323


        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonFactory jsonFactory = new JsonFactory();
        JsonGenerator generator = jsonFactory.createGenerator(stream, JsonEncoding.UTF8);
        generator.writeStartObject();
        generator.writeStringField("country", country.getName());
        generator.writeNumberField("Lat", location.getLatitude());
        generator.writeEndObject();
        generator.close();
        System.out.println(stream.toString());
    }
}
