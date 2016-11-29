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
     export LDAP_GROUP_ID=<LDP group ID>

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

     export HDFS_NAME_NODE=<HDFS name node host>
     export HDFS_RPC_PORT=<HDFS RPC port>
     export HDFS_SUPER_USER=<HDFS super user principal>
     export HDFS_USER_KEYTAB=<HDFS super user keytab path>
     export HDFS_RPC_PORT=<HDFS RPC Port>
     export HDFS_PORT=<Web HDFS Port>

     export HBASE_MASTER_URL=<HBase master UI>
     export HBASE_MASTER=<HBase master>
     export HBASE_REST_PORT=<HBase rest port>
     export HBASE_MASTER_PRINCIPAL=<HBase super user principal>
     export HBASE_MASTER_USER_KEYTAB=<HBase super user keytab path>
     export HBASE_ZOOKEEPER_QUORUM=<Zookeeper hosts list>
     export HBASE_ZOOKEEPER_CLIENT_PORT=<Zookeeper port>
     export HBASE_ZOOKEEPER_ZNODE_PARENT=<Zookeeper zNode parent>

     export HIVE_HOST=<HiveServer2 hostname/ip>
     export HIVE_PORT=<HiveServer2 port>
     export HIVE_SUPER_USER=<Hive admin user>
     export HIVE_SUPER_USER_KEYTAB=<Hive admin user keytab>

     export AMBARI_HOST=<Amabari server>
     export AMBARI_ADMIN_USER=<Ambari admin username>
     export AMBARI_ADMIN_PWD=<Ambari admin password>

     export YARN_RESOURCEMANAGER_HOST=<Yarn Resource Manager host>
     export YARN_RESOURCEMANAGER_PORT=<Yarn Resource Manager Port>
     export YARN_RESOURCEMANAGER_URL=<Yarn Resource Manager URL>
     export YARN_SUPER_USER=<Yarn super user>
     export YARN_SUPER_USER_KEYTAB=<Yarn super user keytab>


     export MR_HISTORY_URL=<MapReduce History server URL>

     export SPARK_HISTORY_URL=<Spark History server URL>

### 2 Run OCDP service broker in VM:
Build OCDP service broker by gradle command:

    ./gradlew build

After building, you can run service broker by run "java -jar" command like below:

    java -jar build/libs/datafoundry-ocdp-service-broker.jar

### 3 Run OCDP service broker in docker container:
Overwrite krb5.conf and hdfs.keytab files in source code folder: src/main/docker/

    cp <path for krb5.conf> <path for hdfs.keytab> src/main/docker

Build OCDP service broker by gradle command:

    ./gradlew build buildDocker

Then you can start OCDP service broker container by docker command like below:

    docker run -p <local port>:8080 --add-host <hostname:ip> (host list for ldap/kdc/hadoop) -e <env_name='env_value'> (env list about connectivity properties) -t asiainfo-ldp/datafoundry-ocdp-service-broker

### 4 Test OCDP service broker
Service catalog:

    curl -H "X-Broker-API-Version: 2.8" http://<broker.username>:<broker.password>@localhost:8080/v2/catalog

HDFS service instance provision:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hdfs-shared-001 -d '{
      "service_id":"ae67d4ba-5c4e-4937-a68b-5b47cfe356d8",
      "plan_id":"72150b09-1025-4533-8bae-0e04ef68ac13",
      "organization_guid": "org-guid",
      "space_guid":"space-guid",
      "parameters": {"ami_id":"ami-ecb68a84"}
    }' -H "Content-Type: application/json"

HDFS service instance binding:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hdfs-shared-003/service_bindings/hdfs-binding-001 -d '{
      "plan_id":        "ae67d4ba-5c4e-4937-a68b-5b47cfe356d8"",
      "service_id":     "72150b09-1025-4533-8bae-0e04ef68ac13",
      "app_guid":       "app-guid"
    }' -H "Content-Type: application/json"

HDFS service instance unbinding:

    curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hdfs-shared-002/service_bindings/hdfs-binding-001?service_id=ae67d4ba-5c4e-4937-a68b-5b47cfe356d8&plan_id=72150b09-1025-4533-8bae-0e04ef68ac13'

HDFS service deprovision:

    curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hdfs-shared-002?service_id=ae67d4ba-5c4e-4937-a68b-5b47cfe356d8&plan_id=72150b09-1025-4533-8bae-0e04ef68ac13'

HBase service instance provision:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hbase-shared-001 -d '{
      "service_id":"d9845ade-9410-4c7f-8689-4e032c1a8450",
      "plan_id":"f658e391-b7d6-4b72-9e4c-c754e4943ae1",
      "organization_guid": "org-guid",
      "space_guid":"space-guid",
      "parameters": {"ami_id":"ami-ecb68a84"}
    }' -H "Content-Type: application/json"

HBase service instance binding:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hbase-shared-001/service_bindings/hbase-binding-001 -d '{
      "service_id":"d9845ade-9410-4c7f-8689-4e032c1a8450",
      "plan_id":"f658e391-b7d6-4b72-9e4c-c754e4943ae1",
      "app_guid":       "app-guid"
    }' -H "Content-Type: application/json"

HBase service instance unbinding:

    curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hbase-shared-001/service_bindings/hbase-binding-001?service_id=d9845ade-9410-4c7f-8689-4e032c1a8450&plan_id=f658e391-b7d6-4b72-9e4c-c754e4943ae1'

HBase service instance deprovision:

    curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hbase-shared-001?service_id=d9845ade-9410-4c7f-8689-4e032c1a8450&plan_id=f658e391-b7d6-4b72-9e4c-c754e4943ae1'

Hive service instance provision:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hive-shared-001 -d '{
      "service_id":"2ef26018-003d-4b2b-b786-0481d4ee9fa3",
      "plan_id":"aa7e364f-fdbf-4187-b60a-218b6fa398ed",
      "organization_guid": "org-guid",
      "space_guid":"space-guid",
      "parameters": {"ami_id":"ami-ecb68a84"}
    }' -H "Content-Type: application/json"

Hive service instance binding:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hive-shared-001/service_bindings/hive-binding-001 -d '{
      "service_id":"2ef26018-003d-4b2b-b786-0481d4ee9fa3",
      "plan_id":"aa7e364f-fdbf-4187-b60a-218b6fa398ed",
      "app_guid":       "app-guid"
    }' -H "Content-Type: application/json"

Hive service instance unbinding:

    curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hive-shared-001/service_bindings/hive-binding-001?service_id=2ef26018-003d-4b2b-b786-0481d4ee9fa3&plan_id=aa7e364f-fdbf-4187-b60a-218b6fa398ed'

Hive service instance deprovision:

    curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hive-shared-001?service_id=2ef26018-003d-4b2b-b786-0481d4ee9fa3&plan_id=aa7e364f-fdbf-4187-b60a-218b6fa398ed'

MapReduce service instance provision:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/mr-shared-001?accepts_incomplete=true -d '{
      "service_id":"ae0f2324-27a8-415b-9c7f-64ab6cd88d40",
      "plan_id":"6524c793-0ea5-4456-9a60-ca70271decdc",
      "organization_guid": "org-guid",
      "space_guid":"space-guid",
      "parameters": {"ami_id":"ami-ecb68a84"}
    }' -H "Content-Type: application/json"

MapReduce service instance binding:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/mr-shared-001/service_bindings/mr-binding-001 -d '{
      "service_id":"ae0f2324-27a8-415b-9c7f-64ab6cd88d40",
      "plan_id":"6524c793-0ea5-4456-9a60-ca70271decdc",
      "parameters": {"ami_id":"ami-ecb68a84"}
    }' -H "Content-Type: application/json"

MapReduce service instance unbinding:

      curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/mr-shared-001/service_bindings/mr-binding-001?service_id=ae0f2324-27a8-415b-9c7f-64ab6cd88d40&plan_id=6524c793-0ea5-4456-9a60-ca70271decdc'

MapReduce service instance deprovision:

      curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080//v2/service_instances/mr-shared-001?service_id=ae0f2324-27a8-415b-9c7f-64ab6cd88d40&plan_id=6524c793-0ea5-4456-9a60-ca70271decdc'

Spark service instance provision:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/spark-shared-001?accepts_incomplete=true -d '{
      "service_id":"d3b9a485-f038-4605-9b9b-29792f5c61d1",
      "plan_id":"5c3d471d-f94a-4bb8-b340-f783f3c15ba1",
      "organization_guid": "org-guid",
      "space_guid":"space-guid",
      "parameters": {"ami_id":"ami-ecb68a84"}
    }' -H "Content-Type: application/json"

Spark service instance binding:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/spark-shared-001/service_bindings/spark-binding-001 -d '{
      "service_id":"d3b9a485-f038-4605-9b9b-29792f5c61d1",
      "plan_id":"5c3d471d-f94a-4bb8-b340-f783f3c15ba1",
      "parameters": {"ami_id":"ami-ecb68a84"}
    }' -H "Content-Type: application/json"

Spark service instance unbinding:

      curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/spark-shared-001/service_bindings/spark-binding-001?service_id=d3b9a485-f038-4605-9b9b-29792f5c61d1&plan_id=5c3d471d-f94a-4bb8-b340-f783f3c15ba1'

Spark service instance deprovision:

      curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080//v2/service_instances/spark-shared-001?service_id=d3b9a485-f038-4605-9b9b-29792f5c61d1&plan_id=5c3d471d-f94a-4bb8-b340-f783f3c15ba1'