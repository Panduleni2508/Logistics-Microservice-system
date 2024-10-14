import ballerinax/kafka;
import ballerina/lang.value;
import ballerina/log;
import ballerina/io;

// Kafka consumer configuration for the express delivery service
kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "express-delivery-group",  // Consumer group ID for the express delivery service
    topics: ["express-delivery"],       // Topic the service listens to, "express-delivery"
    pollingInterval: 1,                 // Polling interval in seconds to fetch new records
    autoCommit: false                   // Disable auto commit to manage offset commits manually
};

// Create a Kafka listener that listens on the specified topic on localhost:9092
listener kafka:Listener kafkaListener = new ("localhost:9092", consumerConfigs);

// Kafka service to process incoming records from the "express-delivery" topic
service kafka:Service on kafkaListener {

    // This function is triggered whenever new consumer records are received from the Kafka topic
    remote function onConsumerRecord(kafka:Caller caller, kafka:AnydataConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            // Process each Kafka record individually
            check processKafkaRecord(kafkaRecord);
        }

        // Commit offsets after processing all records to ensure they aren't processed again
        kafka:Error? commitResult = caller->commit();
        if commitResult is error {
            // Log an error if offset commit fails
            log:printError("Error occurred while committing the offsets for the consumer", 'error = commitResult);
        }
    }
}

// Function to process each individual Kafka record
function processKafkaRecord(kafka:AnydataConsumerRecord kafkaRecord) returns error? {
    // Extract the byte array from the Kafka record
    byte[] value = <byte[]>kafkaRecord.value;

    // Convert the byte array to a string (message content is expected to be in JSON format)
    string messageContent = check string:fromBytes(value);
    io:println("Processing Kafka Record: ", messageContent);  // Print the message content for visibility

    // Process the express delivery request contained in the message
    check processExpressDelivery(messageContent);
    io:println("Processed Express Delivery Request.");
}

// Function to handle the express delivery request
function processExpressDelivery(string requestStr) returns error? {
    // Convert the request string (JSON) to a JSON object
    json request = check value:fromJsonString(requestStr);
    log:printInfo("Processing express delivery request: " + request.toJsonString());

    // Send a confirmation for the processed delivery request
    check sendConfirmation(request);
}

// Send a confirmation for the processed delivery request
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

// Create a Kafka producer with the necessary configurations for sending messages
function createKafkaProducer() returns kafka:Producer|error {
    kafka:ProducerConfiguration producerConfigs = {
        clientId: "express-delivery-service",  // Client ID for this Kafka producer
        acks: "all",                          // Wait for acknowledgment from all replicas
        retryCount: 3                         // Number of retries if message sending fails
    };

    // Return a new Kafka producer instance with the specified configurations
    return new ("localhost:9092", producerConfigs);
}

// Create a JSON object for the confirmation message
function createConfirmationJson(json request) returns json|error {
    // Construct the confirmation message with requestId and delivery details
    return {
        "requestId": check request.requestId,
        "status": "confirmed",
        "pickupTime": "2023-05-10T10:00:00Z",            // Static pickup time for the demo
        "estimatedDeliveryTime": "2023-05-11T14:00:00Z"  // Static delivery time for express deliveries
    };
}

// Send the confirmation message to the Kafka topic "delivery-confirmations"
function sendKafkaMessage(kafka:Producer producer, json message) returns error? {
    // Serialize the JSON message into a byte array
    byte[] serializedMsg = message.toJsonString().toBytes();

    // Send the serialized message to the "delivery-confirmations" Kafka topic
    check producer->send({
        topic: "delivery-confirmations",  // The Kafka topic to send confirmation to
        value: serializedMsg              // Serialized message to be sent as a byte array
    });

    // Ensure that all messages are flushed to Kafka (pushes all messages to Kafka)
    check producer->'flush();
}
