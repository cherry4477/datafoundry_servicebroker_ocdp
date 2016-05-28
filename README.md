# Datafoundry service broker for OCDP Hadoop services
A service broker for OCDP Hadoop services by using the [Spring Cloud - Cloud Foundry Service Broker](https://github.com/spring-cloud/spring-cloud-cloudfoundry-service-broker).

# Overview

This project uses the Spring Cloud - Cloud Foundry Service Broker to implement OCDP Hadoop services.

## Getting Started

### 1 Configure connection properties
Configure connectivity properties (e.g. LDAP, kerberos, Hadoop ...) in system environment variables:

     export BROKER_USERNAME=<broker username>
     export BROKER_PASSWORD=<broker password>

     export ETCD_ENDPOINT=<etcd endpoint>

     export LDAP_URL=<LDAP server URL>
     export LDAP_USER_DN=<root userdn>
     export LDAP_PASSWORD=<password>
     export LDAP_BASE=<base dn>

     export KRB_KDC_HOST=<KDC hostname>
     export KRB_USER_PRINCIPAL=<admin user principal>
     export KRB_KEYTAB_LOCATION=<admin user keytab file path>
     export KRB_ADMIN_PASSWORD=<admin user password>>
     export KRB_REALM=<kerberos realm>

     export RANGER_URL=<Ranger server URL>
     export RANGER_ADMIN_USER=<Ranger admin user name>
     export RANGER_ADMIN_PASSWORD=<Ranger admin user password>

     export HDFS_URL=<HDFS URL>
     export HDFS_SUPER_USER=<HDFS super user principal>
     export HDFS_USER_KEYTAB=<HDFS super user keytab path>
     export HDFS_KRB_FILE=<krb5.conf file path>

### 2 Run OCDP service broker in VM:
Build OCDP service broker by gradle command:

    ./gradlew build

After building, you can run service broker by run "java -jar" command like below:

    java -jar build/libs/datafoundry-ocdp-service-broker.jar

Then you can access service broker APIs like below:

    curl -H "X-Broker-API-Version: 2.8" http://<broker.username>:<broker.password>@localhost:8080/v2/catalog

### 3 Run OCDP service broker in docker container:
Overwrite krb5.conf and hdfs.keytab files in source code folder: src/main/docker/

    cp <path for krb5.conf> <path for hdfs.keytab> src/main/docker

Build OCDP service broker by gradle command:

    ./gradlew build buildDocker

Then you can start OCDP service broker container by docker command like below:

    docker run -p <local port>:8080 --add-host <hostname:ip> --env=[<env var list about connectivity properties>] -t asiainfo-ldp/datafoundry-ocdp-service-broker
