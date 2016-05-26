# Datafoundry service broker for OCDP Hadoop services
A service broker for OCDP Hadoop services by using the [Spring Cloud - Cloud Foundry Service Broker](https://github.com/spring-cloud/spring-cloud-cloudfoundry-service-broker).

# Overview

This project uses the Spring Cloud - Cloud Foundry Service Broker to implement OCDP Hadoop services.

## Getting Started

### 1 Configure connection properties
Configure some connectivity properties (LDAP, kerberos, Hadoop ...) in [application.yml](src/main/resources/application.yml):

     ocdp:
       security:
           username: <broker username>
           password: <broker password>
       etcd:
          endpoint: <etcd endpoint>
       ldap:
           url: <LDAP server URL>
           userDN: <root userdn>
           password: <password>
           base: <base dn>
       krb:
           kdcHost: <KDC hostname>
           user-principal: <admin user principal>
           keytab-location: <admin user keytab file path>
           adminPwd: <admin user password>
           realm: <kerberos realm>
       ranger:
           rangerUri: <Ranger server URL>
           rangerUser: <Ranger admin user name>
           rangerPwd: <Ranger admin user password>
       hdfs:
           hdfsSuperUser: <HDFS super user principal>
           userKeytab: <HDFS super user keytab path>
           krbFilePath: <krb5.conf file path>
           hdfsURL: <HDFS name node URL>

### 2 Run OCDP service broker in VM:
Build OCDp service broker by gradle command:

    ./gradlew build

After building, you can run service broker by run "java -jar" command like below:

    java -jar build/libs/datafoundry-ocdp-service-broker.jar

Then you can access service broker APIs like below:

    curl -H "X-Broker-API-Version: 2.8" http://<ocdp.security.username>:<ocdp.security.password>@localhost:8080/v2/catalog

### 3 Run OCDP service broker in docker container:
Copy krb5.conf and hdfs.keytab files to source code folder: src/main/docker/config/

    cd src/main/docker
    mkdir config
    cp <path for krb5.conf> <path for hdfs.keytab> src/main/docker/config/

Build OCDP service broker by gradle command:

    ./gradlew build buildDocker

Then you can start OCDP service broker container by docker command like below:

    docker run -p <local port>:8080 --add-host <hostname:ip> -t asiainfo-ldp/datafoundry-ocdp-service-broker
