# Datafoundry service broker for OCDP Hadoop services
A service broker for OCDP Hadoop services by using the [Spring Cloud - Cloud Foundry Service Broker](https://github.com/spring-cloud/spring-cloud-cloudfoundry-service-broker).

# Overview

This project uses the Spring Cloud - Cloud Foundry Service Broker to implement OCDP Hadoop services.

## Getting Started

You need to install and run MongoDB somewhere and configure connectivity in [application.yml](src/main/resources/application.yml).

Build it:

    ./gradlew build

After building, you can push the broker app to Cloud Foundry or deploy it some other way and then [register it to Cloud Foundry](http://docs.cloudfoundry.org/services/managing-service-brokers.html#register-broker).



