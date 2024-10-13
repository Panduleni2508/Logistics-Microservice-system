import ballerina/io;
import ballerina/lang.value;
import ballerinax/kafka;
import ballerinax/mongodb;
import ballerina/log;

// Initialize MongoDB client
mongodb:Client mongoClient = check new ({
    connection: {
        serverAddress: {
            host: "localhost",
            port: 27017
        }
    }
});

// Kafka consumer configuration for logistics service
kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "logistics-group",
    topics: ["delivery-requests", "tracking-requests", "delivery-confirmations"],
    pollingInterval: 1,
    autoCommit: false
};

final string DEFAULT_URL = "localhost:9092";

// Listener for Kafka to handle incoming messages
listener kafka:Listener kafkaListener = new (DEFAULT_URL, consumerConfigs);

// Kafka service that processes incoming records
service kafka:Service on kafkaListener {

    // Triggered when new consumer records are received
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            // Process each Kafka record
            check processKafkaRecord(kafkaRecord);
        }

        // Commit offsets after processing records
        kafka:Error? commitResult = caller->commit();
        if commitResult is error {
            log:printError("Error occurred while committing the offsets for the consumer", 'error = commitResult);
        }
    }
}

// Process individual Kafka record based on the topic
function processKafkaRecord(kafka:BytesConsumerRecord kafkaRecord) returns error? {
    byte[] value = kafkaRecord.value;
    string topic = kafkaRecord.offset.partition.topic;
    string valueString = check string:fromBytes(value);

    // Handle different topics with match statement
    match topic {
        "delivery-requests" => {
            check processDeliveryRequest(valueString);
        }
        "tracking-requests" => {
            check processTrackingRequest(valueString);
        }
        "delivery-confirmations" => {
            check processDeliveryConfirmation(valueString);
        }
        _ => {
            io:println("Unknown topic: ", topic);
        }
    }
}

// Process delivery request records and insert them into MongoDB
function processDeliveryRequest(string requestStr) returns error? {
    json request = check value:fromJsonString(requestStr);
    io:println("Processing Delivery Request: ", request.requestId);

    mongodb:Database logistics = check mongoClient->getDatabase("logistics");
    mongodb:Collection requests = check logistics->getCollection("requests");
    _ = check requests->insertOne(<map<json>>request);

    // Forward the request to the appropriate Kafka topic
    check forwardToService(check request.shipmentType, request);
}

// Process tracking request records and search for tracking information in MongoDB
function processTrackingRequest(string requestStr) returns error? {
    json request = check value:fromJsonString(requestStr);
    string requestId = check request.requestId;
    io:println("Processing Tracking Request: ", requestId);

    mongodb:Database logistics = check mongoClient->getDatabase("logistics");
    mongodb:Collection requests = check logistics->getCollection("requests");
    record {|anydata...;|}? result = check requests->findOne({"requestId": requestId});

    // Print tracking information if found, otherwise notify that it's missing
    if result is record {|anydata...;|} {
        io:println("Tracking information found for request ", requestId);
    } else {
        io:println("No tracking information found for request ", requestId);
    }
}

// Process delivery confirmation records and update the status in MongoDB
function processDeliveryConfirmation(string confirmationStr) returns error? {
    json confirmation = check value:fromJsonString(confirmationStr);
    string requestId = check confirmation.requestId;
    io:println("Processing Delivery Confirmation: ", requestId);

    mongodb:Database logistics = check mongoClient->getDatabase("logistics");
    mongodb:Collection requests = check logistics->getCollection("requests");

    // Update the delivery request with confirmation details
    mongodb:Update update = {
        "$set": {
            "status": check confirmation.status,
            "pickupTime": check confirmation.pickupTime,
            "estimatedDeliveryTime": check confirmation.estimatedDeliveryTime
        }
    };
    _ = check requests->updateOne({"requestId": requestId}, update);
}

// Forward processed request to the relevant Kafka topic
function forwardToService(string topic, json request) returns error? {
    kafka:Producer kafkaProducer = check setupKafkaProducer();
    byte[] serializedMsg = request.toJsonString().toBytes();
    check kafkaProducer->send({topic: topic, value: serializedMsg});
    check kafkaProducer->'flush();
    check kafkaProducer->'close();
}

// Setup Kafka producer with necessary configurations
function setupKafkaProducer() returns kafka:Producer|error {
    kafka:ProducerConfiguration producerConfigs = {
        clientId: "logistics-service",
        acks: "all",
        retryCount: 3
    };
    return new (DEFAULT_URL, producerConfigs);
}

// Placeholder function to show records for a specific value
function showOutline(string val) returns error? {
    io:println("Showing outline for value: ", val);
}

// Placeholder function to show all records
function showAllOutline() returns error? {
    io:println("Showing all outlines.");
}

// Custom error types for better error handling
type DeliveryRequestError distinct error<record {|string message;|}>;
type TrackingRequestError distinct error<record {|string message;|}>;
type DeliveryConfirmationError distinct error<record {|string message;|}>;
