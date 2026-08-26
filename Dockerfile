# syntax=docker/dockerfile:1
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /src
COPY gradle gradle
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY src src
COPY certs certs
RUN chmod +x gradlew && ./gradlew installDist --no-daemon

FROM eclipse-temurin:21-jre-jammy
WORKDIR /opt/fastpay
COPY --from=build /src/build/install/fastpay-grpc /opt/fastpay
COPY --from=build /src/certs /opt/fastpay/certs
ENV FASTPAY_AUTH_TOKEN=demo-token
ENV FASTPAY_CERT=/opt/fastpay/certs/server.crt
ENV FASTPAY_KEY=/opt/fastpay/certs/server.key
ENV FASTPAY_TRUST_CERT=/opt/fastpay/certs/ca.crt
ENV FASTPAY_TLS=false
EXPOSE 6565
ENTRYPOINT ["/opt/fastpay/bin/fastpay-grpc"]
