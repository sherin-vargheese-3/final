# Use official OpenJDK 17 image as base
FROM openjdk:17-jdk-slim

# Set workdir in container
WORKDIR /app

# Copy the application jar from host to container
# Assuming the jar is built and located at target/FinalApplication-0.0.1-SNAPSHOT.jar
COPY target/final-0.0.1-SNAPSHOT.jar app.jar

# Expose the default Spring Boot port
EXPOSE 8080

# Add metadata label
LABEL maintainer="your-email@example.com"
LABEL description="Docker container for Spring Boot FinalApplication"

# Entry point to run the jar file
ENTRYPOINT ["java","-jar","app.jar"]