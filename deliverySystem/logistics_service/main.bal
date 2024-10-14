import ballerinax/kafka;
import ballerinax/mongodb;
import ballerina/log;
import ballerina/time;

configurable string kafkaBootstrapServers = "localhost:9092";
configurable string mongodbUrl = "mongodb://localhost:27017";

service / on new kafka:Listener(kafkaBootstrapServers, {
    groupId: "standard-delivery-group",
    topics: ["standard-delivery"]
}) {
    private final kafka:Producer kafkaProducer;
    private final mongodb:Client mongoClient;

    function init() returns error? {
        self.kafkaProducer = check new (kafkaBootstrapServers);
        self.mongoClient = check new (mongodbUrl);
    }

    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string shipmentId = check string:fromBytes(value);

            // Retrieve shipment details from MongoDB
            map<json> shipment = check self.mongoClient->findOne("logistics_db", "shipments", {_id: shipmentId});

            // Process the standard delivery request
            // (In a real system, you'd implement more complex scheduling logic here)
            time:Utc scheduleTime = time:utcAddSeconds(time:utcNow(), 3600); // Schedule 1 hour from now
            shipment["status"] = "scheduled";
            shipment["scheduledPickupTime"] = scheduleTime.toString();

            // Update shipment in MongoDB
            _ = check self.mongoClient->update(shipment, "logistics_db", "shipments", {_id: shipmentId});

            // Send confirmation back to the main service
            check self.kafkaProducer->send({
                topic: "delivery-confirmations",
                value: ("Standard delivery scheduled for shipment: " + shipmentId).toBytes()
            });

            log:printInfo("Standard delivery scheduled", shipmentId = shipmentId, scheduledTime = scheduleTime.toString());
        }
    }
}