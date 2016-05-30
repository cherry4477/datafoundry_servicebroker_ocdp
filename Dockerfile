FROM centos

EXPOSE 8080

RUN yum update -y && \
    yum install -y java-1.8.0-openjdk-devel krb5-workstation krb5-libs git

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/

WORKDIR /root/servicebroker

COPY src/main/docker/krb5.conf /etc/krb5.conf

COPY src/main/docker/hdfs.keytab /tmp/hdfs.keytab

RUN git clone https://github.com/asiainfoLDP/datafoundry_servicebroker_ocdp.git && \
    cd datafoundry_servicebroker_ocdp && \
    ./gradlew build

ENTRYPOINT ["java", "-jar", "datafoundry_servicebroker_ocdp/build/libs/datafoundry-ocdp-service-broker.jar"]
