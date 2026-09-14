# syntax=docker/dockerfile:1
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /src
COPY gradle gradle
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY src src
COPY certs certs
RUN chmod +x gradlew && ./gradlew installDist --no-daemon

FROM eclipse-temurin:21-jre-jammy
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/fastpay
COPY --from=build /src/build/install/fastpay-grpc /opt/fastpay
COPY docker-entrypoint.sh /opt/fastpay/docker-entrypoint.sh
RUN chmod +x /opt/fastpay/docker-entrypoint.sh && mkdir -p /data /opt/fastpay/certs
ENV FASTPAY_PAY_TOKEN=pay-token
ENV FASTPAY_ADMIN_TOKEN=admin-token
ENV FASTPAY_CERT=/opt/fastpay/certs/server.crt
ENV FASTPAY_KEY=/opt/fastpay/certs/server.key
ENV FASTPAY_TRUST_CERT=/opt/fastpay/certs/ca.crt
ENV FASTPAY_DB=/data/fastpay.db
ENV FASTPAY_WEBHOOK_URL=
ENV FASTPAY_WEBHOOK_SECRET=
ENV FASTPAY_TLS=false
VOLUME ["/data"]
EXPOSE 6565
ENTRYPOINT ["/opt/fastpay/docker-entrypoint.sh"]
