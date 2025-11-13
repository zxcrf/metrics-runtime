####
# This Dockerfile is used in order to build a container that runs the Quarkus application in JVM mode
#
# Before building the container image run:
#
# ./gradlew build
#
# Then, build the image with:
#
# docker build -f Dockerfile -t quarkus/dataos-metrics-runtime-jvm .
#
# Then run the container using:
#
# docker run -i --rm -p 8080:8080 quarkus/dataos-metrics-runtime-jvm
#
# If you want to include the debug port into your docker image
# you will need to expose DEBUG_PORT environment variable, for example:
# -e DEBUG_PORT=true
# -e DEBUG_HOST=0.0.0.0
# -e DEBUG_SUSPEND=true
# and then you should be able to mount a debugger to the port 5005.
#
###
FROM registry.access.redhat.com/ubi8/openjdk-21:1.19

ENV LANGUAGE='en_US:en'

# Create a dedicated directory for the application
WORKDIR /app

# Copy the built runner jar file. Using wildcards to handle version changes
COPY build/*-native-image-source-jar/*-runner.jar app.jar

EXPOSE 8080
EXPOSE 8443

# Run the application
ENTRYPOINT [ "java", "-jar", "app.jar" ]
