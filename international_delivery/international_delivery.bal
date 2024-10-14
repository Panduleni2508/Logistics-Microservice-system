import ballerinax/kafka;
import ballerina/log;
import ballerina/lang.value;

// Kafka consumer configuration for the international delivery service
kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "international-delivery-group",  // Consumer group ID for the international delivery service
    topics: ["international-delivery"],       // Topic that this service listens to, "international-delivery"
    pollingInterval: 1,                       // Polling interval in seconds
    autoCommit: false                         // Disable auto commit for manual offset management
};

// Create a Kafka listener to consume messages from the Kafka broker at localhost:9092
listener kafka:Listener kafkaListener = new ("localhost:9092", consumerConfigs);

// Kafka service to process incoming records from the "international-delivery" topic
service kafka:Service on kafkaListener {

    // This remote function is triggered whenever new Kafka records are received
    remote function onConsumerRecord(kafka:Caller caller, kafka:AnydataConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            // Process each Kafka record received from the "international-delivery" topic
            check processKafkaRecord(kafkaRecord);
        }

        // Commit offsets after processing the records to avoid re-processing them
        kafka:Error? commitResult = caller->commit();
        if commitResult is error {
            // Log an error if there was a problem committing the offsets
            log:printError("Error occurred while committing the offsets for the consumer", 'error = commitResult);
        }
    }
}

// Process an individual Kafka record
function processKafkaRecord(kafka:AnydataConsumerRecord kafkaRecord) returns error? {
    // Extract the byte array from the Kafka record
    byte[] value = <byte[]>kafkaRecord.value;

    // Convert the byte array into a string (the message content is expected to be JSON formatted)
    string messageContent = check string:fromBytes(value);
    log:printInfo("Processing Kafka Record: " + messageContent);

    // Process the international delivery request contained in the message
    check processInternationalDelivery(messageContent);
    log:printInfo("Processed International Delivery Request.");
}

// Function to handle the international delivery request
function processInternationalDelivery(string requestStr) returns error? {
    // Convert the request string (JSON) to a JSON object
    json request = check value:fromJsonString(requestStr);
    log:printInfo("Processing international delivery request: " + request.toJsonString());

    // Send a confirmation message for the processed delivery request
    check sendConfirmation(request);
}

// Send a confirmation message for the processed delivery request
function sendConfirmation(json request) returns error? {
    // Create a Kafka producer to send the confirmation message
    kafka:Producer kafkaProducer = check createKafkaProducer();

    // Create the confirmation message in JSON format
    json confirmation = check createConfirmationJson(request);

    // Send the confirmation message to Kafka
    check sendKafkaMessage(kafkaProducer, confirmation);

    // Close the Kafka producer after sending the message
    check kafkaProducer->'close();
}

// Create a Kafka producer with appropriate configurations to send messages
function createKafkaProducer() returns kafka:Producer|error {
    kafka:ProducerConfiguration producerConfigs = {
        clientId: "international-delivery-service",  // Unique client ID for this producer
        acks: "all",                                // Require acknowledgment from all replicas for message reliability
        retryCount: 3                               // Retry 3 times if message sending fails
    };

    // Return a new Kafka producer with the specified configurations
    return new ("localhost:9092", producerConfigs);
}

// Create a JSON object for the confirmation message
function createConfirmationJson(json request) returns json|error {
    // Construct the confirmation JSON object with the requestId and delivery details
    return {
        "requestId": check request.requestId,
        "status": "confirmed",
        "pickupTime": "2023-05-10T10:00:00Z",            // Static pickup time for the request
        "estimatedDeliveryTime": "2023-05-15T14:00:00Z"  // Longer estimated delivery time for international deliveries
    };
}

// Send the confirmation message to the Kafka topic "delivery-confirmations"
function sendKafkaMessage(kafka:Producer producer, json message) returns error? {
    // Serialize the JSON confirmation message into a byte array
    byte[] serializedMsg = message.toJsonString().toBytes();

    // Send the serialized message to the "delivery-confirmations" Kafka topic
    check producer->send({
        topic: "delivery-confirmations",  // Topic where the confirmation will be sent
        value: serializedMsg              // Serialized confirmation message as a byte array
    });

    // Ensure all messages are flushed to Kafka (forces a push to Kafka)
    check producer->'flush();
}
