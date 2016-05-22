# Datafoundry service broker for OCDP Hadoop services
A service broker for OCDP Hadoop services by using the [Spring Cloud - Cloud Foundry Service Broker](https://github.com/spring-cloud/spring-cloud-cloudfoundry-service-broker).

# Overview

This project uses the Spring Cloud - Cloud Foundry Service Broker to implement OCDP Hadoop services.

## Getting Started

You need to install and run OCDP cluster somewhere and configure connectivity(LDAP, kerberos, Hadoop) in [application.yml](src/main/resources/application.yml).

Build it:

    ./gradlew build

After building, you can navigate to build directory:

    cd build/libs

Then you can run service broker by run java -jar command like below:

    java -jar datafoundry-ocdp-service-broker.jar

