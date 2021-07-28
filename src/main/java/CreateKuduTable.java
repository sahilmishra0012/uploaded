import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import java.util.ArrayList;
import java.util.List;


public class CreateKuduTable {

    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "127.0.0.1:7051");

    public static void main(String[] args) {

        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();

        try {
            createExampleTable(client);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Table already exist or has creation problem.");
        }
        try {
            createExampleTable1(client);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Table already exist or has creation problem.");
        }
        try {
            createExampleTable2(client);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Table already exist or has creation problem.");
        }
        try {
            createExampleTable3(client);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Table already exist or has creation problem.");
        }
        try {
            createExampleTable4(client);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Table already exist or has creation problem.");
        }
        try {
            createExampleTable5(client);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Table already exist or has creation problem.");
        }
        try {
            createExampleTable6(client);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Table already exist or has creation problem.");
        }
    }

    static void createExampleTable(KuduClient client) throws KuduException {

        List<ColumnSchema> columns = new ArrayList<>(5);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_id", Type.INT64)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("starting_stop", Type.INT64).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("last_stop", Type.INT64).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("start_time", Type.UNIXTIME_MICROS).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("end_time", Type.UNIXTIME_MICROS).nullable(true)
                .build());
        Schema schema = new Schema(columns);

        CreateTableOptions cto = new CreateTableOptions();
        cto.setNumReplicas(1);
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("trip_id");
        int numBuckets = 8;
        cto.addHashPartitions(hashKeys, numBuckets);

        // Create the table.
        client.createTable("stops_schedule", schema, cto);
        System.out.println("Created table stops_schedule");
    }
    static void createExampleTable1(KuduClient client) throws KuduException {

        List<ColumnSchema> columns = new ArrayList<>(16);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("primary_key", Type.STRING)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_id", Type.INT64)
                .nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("route_id", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_direction", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("actual_trip_start_time", Type.UNIXTIME_MICROS).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("last_stop_arrival_time", Type.UNIXTIME_MICROS).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("vehicle_label", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("license_plate", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("last_stop_id", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("speed", Type.DOUBLE).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("observationDateTime", Type.UNIXTIME_MICROS).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_delay", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("location_type", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("latitude", Type.DOUBLE).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("longitude", Type.DOUBLE).nullable(true)
                .build());
        Schema schema = new Schema(columns);

        CreateTableOptions cto = new CreateTableOptions();
        cto.setNumReplicas(1);
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("primary_key");
        int numBuckets = 8;
        cto.addHashPartitions(hashKeys, numBuckets);

        // Create the table.
        client.createTable("completed_trips", schema, cto);
        System.out.println("Created table completed_trips");
    }
    static void createExampleTable2(KuduClient client) throws KuduException {

        List<ColumnSchema> columns = new ArrayList<>(16);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("primary_key", Type.STRING)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_id", Type.INT64)
                .nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("route_id", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_direction", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("actual_trip_start_time", Type.UNIXTIME_MICROS).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("last_stop_arrival_time", Type.UNIXTIME_MICROS).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("vehicle_label", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("license_plate", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("last_stop_id", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("speed", Type.DOUBLE).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("observationDateTime", Type.UNIXTIME_MICROS).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_delay", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("location_type", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("latitude", Type.DOUBLE).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("longitude", Type.DOUBLE).nullable(true)
                .build());
        Schema schema = new Schema(columns);

        CreateTableOptions cto = new CreateTableOptions();
        cto.setNumReplicas(1);
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("primary_key");
        int numBuckets = 8;
        cto.addHashPartitions(hashKeys, numBuckets);

        // Create the table.
        client.createTable("eta_common", schema, cto);
        System.out.println("Created table eta_common");
    }
    static void createExampleTable3(KuduClient client) throws KuduException {

        List<ColumnSchema> columns = new ArrayList<>(5);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_id", Type.INT64)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("status", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_time", Type.FLOAT).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("last_stop_arrival_time", Type.UNIXTIME_MICROS).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("latitude", Type.DOUBLE).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("longitude", Type.DOUBLE).nullable(true)
                .build());
        Schema schema = new Schema(columns);

        CreateTableOptions cto = new CreateTableOptions();
        cto.setNumReplicas(1);
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("trip_id");
        int numBuckets = 8;
        cto.addHashPartitions(hashKeys, numBuckets);

        // Create the table.
        client.createTable("trip_status", schema, cto);
        System.out.println("Created table trip_status");
    }
    static void createExampleTable4(KuduClient client) throws KuduException {

        List<ColumnSchema> columns = new ArrayList<>(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_id", Type.INT64)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_delay", Type.FLOAT).nullable(true)
                .build());
        Schema schema = new Schema(columns);

        CreateTableOptions cto = new CreateTableOptions();
        cto.setNumReplicas(1);
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("trip_id");
        int numBuckets = 8;
        cto.addHashPartitions(hashKeys, numBuckets);

        // Create the table.
        client.createTable("completed_trips_with_delay", schema, cto);
        System.out.println("Created table completed_trips_with_delay");
    }
    static void createExampleTable5(KuduClient client) throws KuduException {

        List<ColumnSchema> columns = new ArrayList<>(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_id", Type.INT64)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_delay", Type.FLOAT).nullable(true)
                .build());
        Schema schema = new Schema(columns);

        CreateTableOptions cto = new CreateTableOptions();
        cto.setNumReplicas(1);
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("trip_id");
        int numBuckets = 8;
        cto.addHashPartitions(hashKeys, numBuckets);

        // Create the table.
        client.createTable("live_with_delay", schema, cto);
        System.out.println("Created table live_with_delay");
    }
    static void createExampleTable6(KuduClient client) throws KuduException {

        List<ColumnSchema> columns = new ArrayList<>(6);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("primary_key", Type.STRING)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("arrival_time", Type.UNIXTIME_MICROS).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_id", Type.INT64)
                .nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("stop_sequence", Type.INT32)
                .nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("stop_id", Type.INT64)
                .nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("departure_time", Type.UNIXTIME_MICROS).nullable(true)
                .build());
        Schema schema = new Schema(columns);

        CreateTableOptions cto = new CreateTableOptions();
        cto.setNumReplicas(1);
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("primary_key");
        int numBuckets = 8;
        cto.addHashPartitions(hashKeys, numBuckets);

        // Create the table.
        client.createTable("not_started_trips", schema, cto);
        System.out.println("Created table not_started_trips");
    }
}

