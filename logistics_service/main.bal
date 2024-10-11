import ballerina/io;
import ballerinax/kafka;

kafka:Producer kafkaProducer = check new (["localhost:9092"], {
    clientId: "logistics-service"
});

public function main() returns error? {

    // Collecting Customer Information
    io:println("Customer Information");
    string firstName = io:readln("Please enter your first name: ");
    string lastName = io:readln("Please enter your last name: ");
    io:println("");

    // Collecting Shipment Type
    io:println("Shipment Type");
    string shipmentType = io:readln("Please enter the shipment type (e.g., standard, express, international): ");
    io:println("");

    // Collecting Delivery Schedule
    io:println("Delivery Schedule");
    string pickupLocation = io:readln("Please enter the package pick-up location: ");
    string pickupTimeSlot = io:readln("Please enter the package pick-up time: ");
    string dropoffLocation = io:readln("Please enter the delivery location: ");
    string dropoffTimeSlot = io:readln("Please enter the package delivery time: ");
    io:println("");

    // Creating the logistics request message
    string message = "{'firstName':'" + firstName + "', 'lastName':'" + lastName + 
                     "', 'shipmentType':'" + shipmentType + "', 'pickupLocation':'" + pickupLocation + 
                     "', 'pickupTimeSlot':'" + pickupTimeSlot + "', 'dropoffLocation':'" + dropoffLocation +
                     "', 'dropoffTimeSlot':'" + dropoffTimeSlot + "'}";

    // Serializing the message to bytes
    byte[] serializedMsg = message.toBytes();

    // Sending the message to the Kafka topic 'logistics_requests'
    check kafkaProducer->send({
        topic: "logistics_requests",
        value: serializedMsg
    });

    io:println("Logistics request sent: " + message);

    // Flushing and closing the Kafka producer
    check kafkaProducer->'flush();
    // check kafkaProducer->close();

    // return;
}
