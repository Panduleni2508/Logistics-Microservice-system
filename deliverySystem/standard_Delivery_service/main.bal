import ballerina/http;
import ballerinax/kafka;
import ballerinax/mongodb;
import ballerina/uuid;
import ballerina/log;

configurable string kafkaBootstrapServers = "localhost:9092";
configurable string mongodbUrl = "mongodb://localhost:27017";

type ShipmentRequest record {
    string type;
    string pickupLocation;
    string deliveryLocation;
    string[] preferredTimeSlots;
    CustomerInfo customerInfo;
};

type CustomerInfo record {
    string firstName;
    string lastName;
    string contactNumber;
};

service / on new http:Listener(8080) {
    private final kafka:Producer kafkaProducer;
    private final mongodb:Client mongoClient;

    function init() returns error? {
        self.kafkaProducer = check new (kafkaBootstrapServers);
        self.mongoClient = check new (mongodbUrl);
    }

    resource function post request(@http:Payload ShipmentRequest shipmentRequest) returns http:Ok|error {
        string customerId = uuid:createType1AsString();
        
        // Store customer info in MongoDB
        map<json> customerDoc = {
            _id: customerId,
            firstName: shipmentRequest.customerInfo.firstName,
            lastName: shipmentRequest.customerInfo.lastName,
            contactNumber: shipmentRequest.customerInfo.contactNumber
        };
        _ = check self.mongoClient->insert(customerDoc, "logistics_db", "customers");

        // Create shipment document
        string shipmentId = uuid:createType1AsString();
        map<json> shipmentDoc = {
            _id: shipmentId,
            customerId: customerId,
            type: shipmentRequest.type,
            pickupLocation: shipmentRequest.pickupLocation,
            deliveryLocation: shipmentRequest.deliveryLocation,
            status: "requested"
        };
        _ = check self.mongoClient->insert(shipmentDoc, "logistics_db", "shipments");

        // Send request to appropriate Kafka topic based on shipment type
        string topic = shipmentRequest.type + "-delivery";
        check self.kafkaProducer->send({
            topic: topic,
            value: shipmentId.toBytes()
        });

        log:printInfo("Shipment request processed", shipmentId = shipmentId, type = shipmentRequest.type);

        return http:OK;
    }
}