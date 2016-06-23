# Datafoundry service broker for OCDP Hadoop services
A service broker for OCDP Hadoop services by using the [Spring Cloud - Cloud Foundry Service Broker](https://github.com/spring-cloud/spring-cloud-cloudfoundry-service-broker).

# Overview

This project uses the Spring Cloud - Cloud Foundry Service Broker to implement OCDP Hadoop services.

## Getting Started

### 1 Configure connection properties
Configure connectivity properties (e.g. LDAP, kerberos, Hadoop ...) in system environment variables:

     export BROKER_USERNAME=<broker username>
     export BROKER_PASSWORD=<broker password>

     export ETCD_HOST=<etcd host>
     export ETCD_PORT=<etcd port>
     export ETCD_USER=<etcd user>
     export ETCD_PWD=<etcd password>

     export LDAP_URL=<LDAP server URL>
     export LDAP_USER_DN=<root userdn>
     export LDAP_PASSWORD=<password>
     export LDAP_BASE=<base dn>
     export LDAP_GROUP=<LDAP group name>

     export KRB_KDC_HOST=<KDC hostname>
     export KRB_USER_PRINCIPAL=<admin user principal>
     export KRB_KEYTAB_LOCATION=<admin user keytab file path>
     export KRB_ADMIN_PASSWORD=<admin user password>>
     export KRB_REALM=<kerberos realm>
     export KRB_KRB5FILEPATH=<krb5.conf file path>

     export CLUSTER_NAME=<Hadoop cluster name>

     export RANGER_URL=<Ranger server URL>
     export RANGER_ADMIN_USER=<Ranger admin user name>
     export RANGER_ADMIN_PASSWORD=<Ranger admin user password>

     export HDFS_URL=<HDFS URL>
     export HDFS_SUPER_USER=<HDFS super user principal>
     export HDFS_USER_KEYTAB=<HDFS super user keytab path>

     export HBASE_MASTER_URL=<HBase master UI>
     export HBASE_MASTER_PRINCIPAL=<HBase super user principal>
     export HBASE_MASTER_USER_KEYTAB=<HBase super user keytab path>
     export HBASE_ZOOKEEPER_QUORUM=<Zookeeper hosts list>
     export HBASE_ZOOKEEPER_CLIENT_PORT=<Zookeeper port>
     export HBASE_ZOOKEEPER_ZNODE_PARENT=<Zookeeper zNode parent>

     export HIVE_HOST=<HiveServer2 hostname/ip>
     export HIVE_PORT=<HiveServer2 port>
     export HIVE_SUPER_USER=<Hive admin user>
     export HIVE_SUPER_USER_KEYTAB=<Hive admin user keytab>

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

    docker run -p <local port>:8080 --add-host <hostname:ip> (host list for ldap/kdc/hadoop) -e <env_name='env_value'> (env list about connectivity properties) -t asiainfo-ldp/datafoundry-ocdp-service-broker
