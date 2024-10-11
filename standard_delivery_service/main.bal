import ballerina/log;
import ballerinax/kafka;

public function main() returns error? {
    // Kafka Consumer configuration
    kafka:Consumer kafkaConsumer = check new (["localhost:9092"], {
        groupId: "standard-service",
        topics: ["logistics_requests"]
    });

    while true {
        // Poll messages from Kafka
        kafka:AnydataConsumerRecord[]|error result = kafkaConsumer->poll(1000);

        if (result is kafka:AnydataConsumerRecord[]) {
            // Loop through each record and process
            foreach kafka:AnydataConsumerRecord consumerRecord in result {
                // Cast the value to byte[] and convert it to a string
                byte[] recordValue = <byte[]>consumerRecord.value;
                string receivedMessage = check string:fromBytes(recordValue);
                log:printInfo("Standard delivery request received: " + receivedMessage);
            }
        } else {
            log:printError("Error while polling messages from Kafka", result);
        }
    }
}
