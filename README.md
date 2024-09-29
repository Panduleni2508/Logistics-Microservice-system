
# Apache Kafka Installation on Windows 11

Follow these steps to install Apache Kafka 3.8.0 with ZooKeeper on your Windows 11 device.

## 1. Download and Extract Kafka

1. Download Kafka from [this link](https://dlcdn.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz).
2. Extract the `.tgz` file using tools like **7-Zip** or **WinRAR**.
   - First, extract the `.tar` file from the `.tgz`.
   - Next, extract the `.tar` file into a folder (e.g., `C:\kafka\`).

## 2. Install Java (JDK)

1. Download and install **Java JDK** (JDK 11 or higher) from [Oracle](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html).
2. Set the `JAVA_HOME` environment variable:
   - Open **Start Menu** → Search for **Environment Variables** → Select **Edit the system environment variables**.
   - In **System Properties**, click **Environment Variables**.
   - Click **New** under **System Variables** and add:
     - **Variable name**: `JAVA_HOME`
     - **Variable value**: `C:\Program Files\Java\jdk-<version>`
   - Click **OK** to save.

3. Verify the installation by running the following command in **Command Prompt**:
   ```bash
   java -version
   ```

## 3. Configure ZooKeeper

1. In the extracted Kafka folder, navigate to `config/zookeeper.properties`.
2. Optionally, edit the `dataDir` to change the ZooKeeper data directory (default is `tmp/zookeeper`):
   ```properties
   dataDir=/tmp/zookeeper
   clientPort=2181
   maxClientCnxns=0
   ```

## 4. Start ZooKeeper

1. Open **Command Prompt** and navigate to the Kafka folder.
2. Run the following command to start ZooKeeper:
   ```bash
   .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
   ```

## 5. Configure Kafka

1. Open `config/server.properties` to configure Kafka:
   ```properties
   broker.id=0
   listeners=PLAINTEXT://localhost:9092
   log.dirs=/tmp/kafka-logs
   zookeeper.connect=localhost:2181
   ```
   Ensure `zookeeper.connect` points to your ZooKeeper instance (localhost:2181).

## 6. Start Kafka

1. Open another **Command Prompt** and navigate to the Kafka folder.
2. Run the following command to start Kafka:
   ```bash
   .\bin\windows\kafka-server-start.bat .\config\server.properties
   ```

## 7. Verify Installation

### Create a Topic
```bash
.\bin\windows\kafka-topics.bat --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### List Topics
```bash
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

### Start Producer
```bash
.\bin\windows\kafka-console-producer.bat --topic test-topic --bootstrap-server localhost:9092
```

### Start Consumer
```bash
.\bin\windows\kafka-console-consumer.bat --topic test-topic --from-beginning --bootstrap-server localhost:9092
```

## 8. Stopping Kafka and ZooKeeper

- **Stop Kafka**: Press **Ctrl + C** in the terminal running Kafka.
- **Stop ZooKeeper**: Press **Ctrl + C** in the terminal running ZooKeeper.

---

You have successfully installed and configured Kafka with ZooKeeper on your Windows 11 device.
