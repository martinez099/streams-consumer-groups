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

- `mvn exec:java -Dexec.mainClass="com.redislabs.demo.streams.consumergroups.Agent" -Dexec.classpathScope=runtime -Dexec.args=produce`

### Consumer groups

- ```mvn exec:java -Dexec.mainClass="com.redislabs.demo.streams.consumergroups.Agent" -Dexec.classpathScope=runtime -Dexec.args="consume group1 3"```
- ```mvn exec:java -Dexec.mainClass="com.redislabs.demo.streams.consumergroups.Agent" -Dexec.classpathScope=runtime -Dexec.args="consume group2 3"```
- ```mvn exec:java -Dexec.mainClass="com.redislabs.demo.streams.consumergroups.Agent" -Dexec.classpathScope=runtime -Dexec.args="consume group3 3"```

