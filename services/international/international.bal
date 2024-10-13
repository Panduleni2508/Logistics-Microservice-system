import ballerinax/kafka;
import ballerina/log;
import ballerina/lang.value;

// Kafka consumer configuration for international delivery service
kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "international-delivery-group",
    topics: ["international-delivery"],
    pollingInterval: 1,
    autoCommit: false
};

listener kafka:Listener kafkaListener = new ("localhost:9092", consumerConfigs);

// Kafka service that processes incoming records
service kafka:Service on kafkaListener {

    // Triggered when new consumer records are received
    remote function onConsumerRecord(kafka:Caller caller, kafka:AnydataConsumerRecord[] records) returns error? {
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

// Process individual Kafka record
function processKafkaRecord(kafka:AnydataConsumerRecord kafkaRecord) returns error? {
    byte[] value = <byte[]>kafkaRecord.value;
    string messageContent = check string:fromBytes(value);
    log:printInfo("Processing Kafka Record: " + messageContent);

    // Process the international delivery request and send a confirmation
    check processInternationalDelivery(messageContent);
    log:printInfo("Processed International Delivery Request.");
}

// Process international delivery request
function processInternationalDelivery(string requestStr) returns error? {
    json request = check value:fromJsonString(requestStr);
    log:printInfo("Processing international delivery request: " + request.toJsonString());

    // Send confirmation for the delivery request
    check sendConfirmation(request);
}

// Send confirmation for the processed delivery request
function sendConfirmation(json request) returns error? {
    kafka:Producer kafkaProducer = check createKafkaProducer();
    json confirmation = check createConfirmationJson(request);
    check sendKafkaMessage(kafkaProducer, confirmation);
    check kafkaProducer->'close();
}

// Create the Kafka producer for sending messages
function createKafkaProducer() returns kafka:Producer|error {
    kafka:ProducerConfiguration producerConfigs = {
        clientId: "international-delivery-service",
        acks: "all",
        retryCount: 3
    };
    return new ("localhost:9092", producerConfigs);
}

// Create the confirmation JSON response
function createConfirmationJson(json request) returns json|error {
    return {
        "requestId": check request.requestId,
        "status": "confirmed",
        "pickupTime": "2023-05-10T10:00:00Z",
        "estimatedDeliveryTime": "2023-05-15T14:00:00Z"  // Longer delivery time for international
    };
}

// Send the Kafka message with the confirmation details
function sendKafkaMessage(kafka:Producer producer, json message) returns error? {
    byte[] serializedMsg = message.toJsonString().toBytes();
    check producer->send({
        topic: "delivery-confirmations",
        value: serializedMsg
    });
    check producer->'flush();
}
