import ballerina/io;
import ballerina/lang.value;
import ballerinax/kafka;
import ballerinax/mongodb;
import ballerina/log;
import ballerina/http;

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

    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            check processKafkaRecord(kafkaRecord);
        }

        kafka:Error? commitResult = caller->commit();
        if commitResult is error {
            log:printError("Error occurred while committing the offsets for the consumer", 'error = commitResult);
        }
    }
}

function processKafkaRecord(kafka:BytesConsumerRecord kafkaRecord) returns error? {
    byte[] value = kafkaRecord.value;
    string topic = kafkaRecord.offset.partition.topic;
    string valueString = check string:fromBytes(value);

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

function processDeliveryRequest(string requestStr) returns error? {
    json request = check value:fromJsonString(requestStr);
    io:println("Processing Delivery Request: ", request.requestId);

    mongodb:Database logistics = check mongoClient->getDatabase("logistics");
    mongodb:Collection requests = check logistics->getCollection("requests");
    _ = check requests->insertOne(<map<json>>request);

    check forwardToService(check request.shipmentType, request);
}

function processTrackingRequest(string requestStr) returns error? {
    json request = check value:fromJsonString(requestStr);
    string requestId = check request.requestId;
    io:println("Processing Tracking Request: ", requestId);

    mongodb:Database logistics = check mongoClient->getDatabase("logistics");
    mongodb:Collection requests = check logistics->getCollection("requests");

    // Perform the MongoDB query and handle the result properly
    record {| anydata...; |}? result = check requests->findOne({"requestId": requestId});

    if result is record {| anydata...; |} {
        // Convert the record to JSON
        json resultJson = result.toJson();  // Corrected conversion
        io:println("Tracking information found for request ", requestId);
        kafka:Producer kafkaProducer = check setupKafkaProducer();
        byte[] serializedMsg = resultJson.toJsonString().toBytes();
        check kafkaProducer->send({topic: "tracking-responses", value: serializedMsg});
        check kafkaProducer->'flush();
        check kafkaProducer->'close();
    } else {
        io:println("No tracking information found for request ", requestId);
        kafka:Producer kafkaProducer = check setupKafkaProducer();
        string notFoundMessage = "No tracking info found for requestId: " + requestId;
        byte[] serializedMsg = notFoundMessage.toBytes();
        check kafkaProducer->send({topic: "tracking-responses", value: serializedMsg});
        check kafkaProducer->'flush();
        check kafkaProducer->'close();
    }
}

function processDeliveryConfirmation(string confirmationStr) returns error? {
    json confirmation = check value:fromJsonString(confirmationStr);
    string requestId = check confirmation.requestId;
    io:println("Processing Delivery Confirmation: ", requestId);

    mongodb:Database logistics = check mongoClient->getDatabase("logistics");
    mongodb:Collection requests = check logistics->getCollection("requests");

    mongodb:Update update = {
        "$set": {
            "status": check confirmation.status,
            "pickupTime": check confirmation.pickupTime,
            "estimatedDeliveryTime": check confirmation.estimatedDeliveryTime
        }
    };
    _ = check requests->updateOne({"requestId": requestId}, update);
}

function forwardToService(string topic, json request) returns error? {
    kafka:Producer kafkaProducer = check setupKafkaProducer();
    byte[] serializedMsg = request.toJsonString().toBytes();
    check kafkaProducer->send({topic: topic, value: serializedMsg});
    check kafkaProducer->'flush();
    check kafkaProducer->'close();
}

function setupKafkaProducer() returns kafka:Producer|error {
    kafka:ProducerConfiguration producerConfigs = {
        clientId: "logistics-service",
        acks: "all",
        retryCount: 3
    };
    return new (DEFAULT_URL, producerConfigs);
}

// HTTP API to track packages based on requestId
service /track on new http:Listener(8080) {

    resource function get trackInfo(string requestId) returns json|error {
        mongodb:Database logistics = check mongoClient->getDatabase("logistics");
        mongodb:Collection requests = check logistics->getCollection("requests");

        // Perform the MongoDB query and handle the result properly
        record {| anydata...; |}? result = check requests->findOne({"requestId": requestId});
        
        if result is record {| anydata...; |} {
            json resultJson = result.toJson();  // Corrected conversion
            return resultJson;
        } else {
            return { "message": "No tracking information found for requestId: " + requestId };
        }
    }
}
