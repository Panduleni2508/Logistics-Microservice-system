import ballerina/io;
import ballerinax/kafka;
import ballerina/uuid;

public function main() returns error? {
    // Welcome message for the Logistics System
    io:println("Welcome to the Nust Logistics System");
    
    // Start the main menu loop
    check runMainMenu();
}

// Main menu loop that allows the user to select different options
function runMainMenu() returns error? {
    while true {
        // Display the menu options to the user
        displayMainMenu();
        
        // Get the user's selected menu option
        int option = check getMenuOption();
        
        // Exit the system if option 3 is selected
        if option == 3 {
            io:println("Goodbye!");
            break;
        }
        
        // Handle the chosen menu option
        check handleMenuOption(option);
    }
}

// Function to display the main menu options
function displayMainMenu() {
    io:println("\nPlease select an option:");
    io:println("1. Submit a new delivery request");
    io:println("2. Track a shipment");
    io:println("3. Exit");
}

// Get the user's selected option from the main menu
function getMenuOption() returns int|error {
    return int:fromString(io:readln("Enter your choice (1-3): "));
}

// Handle the chosen option from the menu
function handleMenuOption(int option) returns error? {
    match option {
        // Submit a new delivery request
        1 => {
            check submitDeliveryRequest();
        }
        // Track an existing shipment
        2 => {
            check trackShipment();
        }
        // Handle invalid menu options
        _ => {
            io:println("Invalid option. Please try again.");
        }
    }
}

// Function to handle submission of a new delivery request
function submitDeliveryRequest() returns error? {
    io:println("\nSubmitting a new delivery request");

    // Get the shipment type as a string input
    string shipmentType = check getShipmentType();
     
    // Collect other delivery details from the user
    json payload = collectDeliveryDetails(shipmentType);
    
    // Send the collected details to the Kafka topic "delivery-requests"
    check sendToKafka(payload, "delivery-requests");
     
    // Confirm submission by displaying the tracking ID
    displaySubmissionConfirmation((check payload.requestId).toString());
}

// Function to get shipment type from the user by typing it out
function getShipmentType() returns string|error {
    io:println("\nEnter shipment type (standard/express/international):");

    // Read the user's input for shipment type
    string shipmentType = io:readln();
    
    // Validate the input to make sure it's a valid shipment type
    if shipmentType == "standard" || shipmentType == "express" || shipmentType == "international" {
        return shipmentType;
    } else {
        io:println("Invalid choice. Defaulting to standard shipment.");
        return "standard";
    }
}

// Collect delivery details from the user (e.g., locations, times, and contact info)
function collectDeliveryDetails(string shipmentType) returns json {
    // Generate a unique request ID using UUID
    string requestId = uuid:createType1AsString();

    // Return the collected details as a JSON object
    return {
        "requestId": requestId,
        "shipmentType": shipmentType,
        "pickupLocation": io:readln("Enter pickup location: "),
        "deliveryLocation": io:readln("Enter delivery location: "),
        "preferredPickupTime": io:readln("Enter preferred pickup time (YYYY-MM-DD HH:MM): "),
        "preferredDeliveryTime": io:readln("Enter preferred delivery time (YYYY-MM-DD HH:MM): "),
        "firstName": io:readln("Enter first name: "),
        "lastName": io:readln("Enter last name: "),
        "contactNumber": io:readln("Enter contact number: ")
    };
}

// Display a confirmation message once the delivery request is submitted
function displaySubmissionConfirmation(string requestId) {
    io:println("Delivery request submitted successfully!");
    io:println("Your tracking number is: " + requestId);
    io:println("You can use this tracking number to check the status of your shipment.");
}

// Function to handle shipment tracking
function trackShipment() returns error? {
    // Get the tracking number from the user
    string trackingNumber = io:readln("Enter tracking number: ");
    
    // Create a JSON payload for the tracking request
    json trackingRequest = { "requestId": trackingNumber };
    
    // Send the tracking request to the Kafka topic "tracking-requests"
    check sendToKafka(trackingRequest, "tracking-requests");
    
    // Display confirmation after sending the tracking request
    displayTrackingConfirmation(trackingNumber);
}

// Display a message confirming that the tracking information was requested
function displayTrackingConfirmation(string trackingNumber) {
    io:println("Tracking information for " + trackingNumber + " has been requested.");
    io:println("Please check back later for updates on your shipment.");
}

// Function to send a message to a specified Kafka topic (used for both delivery and tracking)
function sendToKafka(json payload, string topic) returns error? {
    // Create a Kafka producer
    kafka:Producer kafkaProducer = check createKafkaProducer();
    
    // Serialize the payload to a byte array to send over Kafka
    byte[] serializedMsg = payload.toJsonString().toBytes();
    
    // Create a Kafka producer record with the topic and message
    kafka:BytesProducerRecord producerRecord = {
        topic: topic,
        value: serializedMsg
    };
    
    // Send the message to Kafka and ensure it's sent
    check kafkaProducer->send(producerRecord);
    check kafkaProducer->'flush();
    
    // Close the Kafka producer
    check kafkaProducer->'close();
}

// Function to create and configure a Kafka producer (reused for all Kafka interactions)
function createKafkaProducer() returns kafka:Producer|error {
    // Kafka producer configurations (client ID, acknowledgment mode, retries)
    kafka:ProducerConfiguration producerConfigs = {
        clientId: "logistics-client",
        acks: "all", // Wait for acknowledgment from all Kafka brokers
        retryCount: 3 // Retry up to 3 times if there is a failure
    };

    // Create and return a new Kafka producer instance
    return new (kafka:DEFAULT_URL, producerConfigs);
}
