// import ballerina/log;
// import ballerinax/kafka;
import ballerinax/kafka;

// public function main() returns error? {
//     // Pass the Kafka bootstrap servers directly
//     kafka:Consumer kafkaConsumer = check new (["localhost:9092"], {
//         groupId: "express-service",
//         topics: ["logistics_requests"]
//     });

//     while true {
//         // Poll messages from Kafka, expecting an array of AnydataConsumerRecord
//         kafka:AnydataConsumerRecord[]|error result = kafkaConsumer->poll(1000);

//         if (result is kafka:AnydataConsumerRecord[]) {
//             foreach kafka:AnydataConsumerRecord consumerRecord in result {
//                 // Convert the value from byte[] to string
//                 byte[] recordValue = <byte[]>consumerRecord.value;
//                 string receivedMessage = check string:fromBytes(recordValue);
//                 log:printInfo("Express delivery request received: " + receivedMessage);
//                 // Process the express delivery request
//             }
//         } else {
//             log:printError("Error while polling messages from Kafka", result);
//         }
//     }
// }

kafka:Consumer kafkaConsumer = check new (["localhost:9092"], {
        groupId: "express-service",
        topics: ["logistics_requests"]
    });


