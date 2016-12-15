FROM docker.io/kshenk/centos-7.2

EXPOSE 8080

RUN yum update -y && \
    yum install -y java-1.8.0-openjdk-devel krb5-workstation krb5-libs

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/

COPY krb5.conf /etc/krb5.conf

COPY *.keytab /tmp/

COPY datafoundry-ocdp-service-broker.jar app.jar

COPY start.sh start.sh 

#ENTRYPOINT ["java", "-jar", "./app.jar"]

ENTRYPOINT ["/bin/bash", "-c", "sh ./start.sh"]
