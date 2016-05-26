FROM centos

EXPOSE 8080

RUN yum update && \
    yum install -y java-1.8.0-openjdk-devel krb5-workstation krb5-libs

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/

#COPY hosts /tmp/hosts

COPY krb5.conf /etc/krb5.conf

COPY hdfs.keytab /tmp/hdfs.keytab

COPY datafoundry-ocdp-service-broker.jar app.jar

ENTRYPOINT ["java", "-jar", "./app.jar"]