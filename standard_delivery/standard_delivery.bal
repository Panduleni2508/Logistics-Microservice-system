import ballerinax/kafka;
import ballerina/log;
import ballerina/lang.value;

// Kafka consumer configuration for the standard delivery service
kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "standard-delivery-group",      // Group ID to identify the consumer group
    topics: ["standard-delivery"],           // Topic to subscribe to, in this case, "standard-delivery"
    pollingInterval: 1,                      // Polling interval in seconds
    autoCommit: false                        // Disable auto commit, as we will manually commit offsets after processing
};

// Create a Kafka listener to consume messages from Kafka broker at localhost:9092
listener kafka:Listener kafkaListener = new ("localhost:9092", consumerConfigs);

// Kafka service to process incoming records from the "standard-delivery" topic
service kafka:Service on kafkaListener {

    // This remote function is triggered whenever new Kafka records are received
    remote function onConsumerRecord(kafka:Caller caller, kafka:AnydataConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            // Process each Kafka record
            check processKafkaRecord(kafkaRecord);
        }

        // Commit the offsets after all records have been processed to ensure they aren't processed again
        kafka:Error? commitResult = caller->commit();
        if commitResult is error {
            log:printError("Error occurred while committing the offsets for the consumer", 'error = commitResult);
        }
    }
}

// Process an individual Kafka record
function processKafkaRecord(kafka:AnydataConsumerRecord kafkaRecord) returns error? {
    // Extract the byte array from the Kafka record
    byte[] value = <byte[]>kafkaRecord.value;

    // Convert the byte array to a string (JSON format expected)
    string messageContent = check string:fromBytes(value);
    log:printInfo("Processing Kafka Record: " + messageContent);

    // Process the standard delivery request contained in the message
    check processStandardDelivery(messageContent);
    log:printInfo("Processed Standard Delivery Request.");
}

// Function to handle the standard delivery request
function processStandardDelivery(string requestStr) returns error? {
    // Convert the request string (JSON) to a JSON object
    json request = check value:fromJsonString(requestStr);
    log:printInfo("Processing standard delivery request: " + request.toJsonString());

    // Send a confirmation for the processed delivery request
    check sendConfirmation(request);
}

// Send a confirmation for the processed delivery request
function sendConfirmation(json request) returns error? {
    // Create a Kafka producer to send the confirmation message
    kafka:Producer kafkaProducer = check createKafkaProducer();

    // Create the confirmation message in JSON format
    json confirmation = check createConfirmationJson(request);

    // Send the confirmation message to the Kafka topic
    check sendKafkaMessage(kafkaProducer, confirmation);

    // Close the Kafka producer after sending the message
    check kafkaProducer->'close();
}

// Create a Kafka producer with appropriate configurations to send messages
function createKafkaProducer() returns kafka:Producer|error {
    kafka:ProducerConfiguration producerConfigs = {
        clientId: "standard-delivery-service",  // Client ID for producer
        acks: "all",                           // Wait for all replicas to acknowledge the message
        retryCount: 3                          // Number of retries if message sending fails
    };

    // Return a new Kafka producer with the given configurations
    return new ("localhost:9092", producerConfigs);
}

// Create a JSON object for the confirmation message
function createConfirmationJson(json request) returns json|error {
    // Construct the confirmation JSON with requestId and delivery details
    return {
        "requestId": check request.requestId,
        "status": "confirmed",
        "pickupTime": "2023-05-10T10:00:00Z",            // Static pickup time for demonstration
        "estimatedDeliveryTime": "2023-05-12T14:00:00Z"  // Static delivery time for demonstration
    };
}

// Send the confirmation message to the Kafka topic "delivery-confirmations"
function sendKafkaMessage(kafka:Producer producer, json message) returns error? {
    // Serialize the JSON message into a byte array
    byte[] serializedMsg = message.toJsonString().toBytes();

    // Send the message to the Kafka topic
    check producer->send({
        topic: "delivery-confirmations",  // Topic to send the confirmation message
        value: serializedMsg              // The serialized message as a byte array
    });

    // Ensure that all messages are flushed to Kafka (forces a push)
    check producer->'flush();
}
