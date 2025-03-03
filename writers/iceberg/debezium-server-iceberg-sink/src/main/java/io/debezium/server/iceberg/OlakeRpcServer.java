package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.iceberg.batchsizewait.InterfaceBatchSizeWait;
import io.debezium.server.iceberg.batchsizewait.NoBatchSizeWait;
import io.debezium.server.iceberg.rpc.OlakeRowsIngester;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import jakarta.enterprise.context.Dependent;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Dependent
public class OlakeRpcServer {

    protected static final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
    protected static final Serde<JsonNode> keySerde = DebeziumSerdes.payloadJson(JsonNode.class);
    final static Configuration hadoopConf = new Configuration();
    final static Map<String, String> icebergProperties = new ConcurrentHashMap<>();
    static Catalog icebergCatalog;
    static Deserializer<JsonNode> valDeserializer;
    static Deserializer<JsonNode> keyDeserializer;
    static boolean upsert_records = true;
    static boolean createIdFields = true;


    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Please provide a JSON config as an argument.");
            System.exit(1);
        }

        String jsonConfig = args[0];
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> configMap = objectMapper.readValue(jsonConfig, new TypeReference<Map<String, String>>() {
        });

        configMap.forEach(hadoopConf::set);
        icebergProperties.putAll(configMap);
        String catalogName = "iceberg";
        if (configMap.get("catalog-name") != null) {
            catalogName = configMap.get("catalog-name");
        }

        if (configMap.get("table-namespace") == null) {
            throw new Exception("Iceberg table namespace not found");
        }

        if (configMap.get("upsert") != null) {
            upsert_records = Boolean.parseBoolean(configMap.get("upsert"));
        }

        icebergCatalog = CatalogUtil.buildIcebergCatalog(catalogName, icebergProperties, hadoopConf);

        // TODO : change this to MaxBatchSizeWait based on config later

        InterfaceBatchSizeWait batchSizeWait = new NoBatchSizeWait();
        batchSizeWait.initizalize();

        // configure and set
        valSerde.configure(Collections.emptyMap(), false);
        valDeserializer = valSerde.deserializer();
        // configure and set
        keySerde.configure(Collections.emptyMap(), true);
        keyDeserializer = keySerde.deserializer();

        OlakeRowsIngester ori;


        // Retrieve a CDI-managed bean from the container
        ori = new OlakeRowsIngester(upsert_records);
        ori.setIcebergNamespace(configMap.get("table-namespace"));
        ori.setIcebergCatalog(icebergCatalog);


        // Build the server to listen on port 50051
        int port = 50051; // Default port
        if (configMap.get("port") != null) {
            port = Integer.parseInt(configMap.get("port"));
        }
        
        // Get max message size from config or use a reasonable default (500MB)
        int maxMessageSize = 500 * 1024 * 1024; // 500MB default
        if (configMap.get("max-message-size") != null) {
            maxMessageSize = Integer.parseInt(configMap.get("max-message-size"));
        }
        
        Server server = ServerBuilder.forPort(port)
                .addService(ori)
                .maxInboundMessageSize(maxMessageSize)
                .build()
                .start();

        System.out.println("Server started on port " + port + " with max message size: " + 
                           (maxMessageSize / (1024 * 1024)) + "MB and configuration: " + configMap);
        server.awaitTermination();
    }


}
