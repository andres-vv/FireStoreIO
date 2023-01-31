#!/bin/bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_311.jdk/Contents/Home
export PATH=/Users/andresvervaecke/tools/apache-maven-3.8.6/bin:$PATH

cd firestore/
mvn clean
mvn package -DskipTests
cd target
java -jar firestore-transforms-bundled-0.1.jar 12345

