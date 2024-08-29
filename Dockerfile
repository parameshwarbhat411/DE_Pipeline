FROM jenkins/jenkins:lts-jdk11

# Switch to root user to install Docker and configure permissions
USER root

# Install Docker CLI inside the container
RUN apt-get update && \
    apt-get install -y apt-transport-https ca-certificates curl gnupg2 software-properties-common && \
    apt-get install -y docker.io

# Add Jenkins user to the Docker group
RUN usermod -aG docker jenkins

# Switch back to Jenkins user
USER jenkins