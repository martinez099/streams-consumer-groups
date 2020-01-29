# streams-consumer-groups
a sample showing Redis Streams with consumer groups in action

## Prerequisites

- Java 8
- Maven 3
- Redis 5

## Compile

- `mvn clean compile`

## Run

### Producer

- `mvn exec:java -Dexec.mainClass="com.redislabs.demo.streams.consumergroups.Producer" -Dexec.classpathScope=runtime`

### Consumer groups

- ```mvn exec:java -Dexec.mainClass="com.redislabs.demo.streams.consumergroups.Consumer" -Dexec.classpathScope=runtime -Dexec.args="group1 3"```
- ```mvn exec:java -Dexec.mainClass="com.redislabs.demo.streams.consumergroups.Consumer" -Dexec.classpathScope=runtime -Dexec.args="group2 3"```
- ```mvn exec:java -Dexec.mainClass="com.redislabs.demo.streams.consumergroups.Consumer" -Dexec.classpathScope=runtime -Dexec.args="group3 3"```
