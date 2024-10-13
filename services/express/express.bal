import ballerinax/kafka;
import ballerina/lang.value;
import ballerina/log;
import ballerina/io;

// // Configurable variables are now implicitly readonly as of Swan Lake Beta1
// configurable string groupId = "express-delivery-group";
// configurable string consumeTopic = "express-delivery";
// configurable string produceTopic = "delivery-confirmations";

// // The main function is now marked as isolated for improved concurrency safety
// public isolated function main() returns error? {
//     kafka:Consumer kafkaConsumer = check createKafkaConsumer();
//     log:printInfo("Express delivery service started. Waiting for requests...");

//     check startConsumerLoop(kafkaConsumer);
// }

// // Separate function for creating the Kafka consumer
// isolated function createKafkaConsumer() returns kafka:Consumer|error {
//     kafka:ConsumerConfiguration consumerConfigs = {
//         groupId,
//         topics: [consumeTopic],
//         offsetReset: "earliest"
//     };
//     return new (kafka:DEFAULT_URL, consumerConfigs);
// }

// // Main consumer loop is now in its own function
// isolated function startConsumerLoop(kafka:Consumer kafkaConsumer) returns error? {
//     while true {
//         kafka:AnydataConsumerRecord[] records = check kafkaConsumer->poll(1);
//         check processRecords(records);
//     }
// }

// // Separate function for processing records
// isolated function processRecords(kafka:AnydataConsumerRecord[] records) returns error? {
//     foreach kafka:AnydataConsumerRecord rec in records {
//         string stringValue = check string:fromBytes(check rec.value.ensureType());
//         check processExpressDelivery(stringValue);
//     }
// }

// // Process express delivery requests
// isolated function processExpressDelivery(string requestStr) returns error? {
//     json request = check value:fromJsonString(requestStr);
//     log:printInfo("Processing express delivery request: " + request.toJsonString());
//     check sendConfirmation(request);
// }

// // Send confirmation for the delivery request
// isolated function sendConfirmation(json request) returns error? {
//     kafka:Producer kafkaProducer = check createKafkaProducer();
//     json confirmation = check createConfirmationJson(request);
//     check sendKafkaMessage(kafkaProducer, confirmation);
//     check kafkaProducer->'close();
// }

// // Separate function for creating the Kafka producer
// isolated function createKafkaProducer() returns kafka:Producer|error {
//     kafka:ProducerConfiguration producerConfigs = {
//         clientId: "express-delivery-service",
//         acks: "all",
//         retryCount: 3
//     };
//     return new (kafka:DEFAULT_URL, producerConfigs);
// }

// // Create the confirmation JSON
// isolated function createConfirmationJson(json request) returns json|error {
//     return {
//         "requestId": check request.requestId,
//         "status": "confirmed",
//         "pickupTime": "2023-05-10T10:00:00Z",
//         "estimatedDeliveryTime": "2023-05-11T14:00:00Z"
//     };
// }

// // Send the Kafka message
// isolated function sendKafkaMessage(kafka:Producer producer, json message) returns error? {
//     byte[] serializedMsg = message.toJsonString().toBytes();
//     check producer->send({
//         topic: produceTopic,
//         value: serializedMsg
//     });
//     check producer->'flush();
// }





// Kafka consumer configuration for express delivery service
kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "express-delivery-group",
    topics: ["express-delivery"],
    pollingInterval: 1,
    autoCommit: false
};

listener kafka:Listener kafkaListener = new ("localhost:9092", consumerConfigs);

// Kafka service that processes incoming records
service kafka:Service on kafkaListener {

    // Triggered when new consumer records are received
    remote function onConsumerRecord(kafka:Caller caller, kafka:AnydataConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
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
    io:println("Processing Kafka Record: ", messageContent);

    // Process the express delivery request and send a confirmation
    check processExpressDelivery(messageContent);
    io:println("Processed Express Delivery Request.");
}

// Process express delivery request
function processExpressDelivery(string requestStr) returns error? {
    json request = check value:fromJsonString(requestStr);
    log:printInfo("Processing express delivery request: " + request.toJsonString());

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
        clientId: "express-delivery-service",
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
        "estimatedDeliveryTime": "2023-05-11T14:00:00Z"
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

