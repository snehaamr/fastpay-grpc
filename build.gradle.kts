import com.google.protobuf.gradle.*

plugins {
    `java`
    id("com.google.protobuf") version "0.9.4" // keep plugin modern
    id("application")
}

group = "com.example"
version = "0.1.0"
java.sourceCompatibility = JavaVersion.VERSION_17

repositories {
    mavenCentral()
}

val grpcVersion = "1.57.0"        // pick reasonably recent gRPC Java
val protobufVersion = "3.24.3"   // example; plugin controls protoc
val nettyVersion = "4.1.99.Final" // netty version aligned with gRPC

dependencies {
    implementation("io.grpc:grpc-netty:${grpcVersion}")
    implementation("io.grpc:grpc-protobuf:${grpcVersion}")
    implementation("io.grpc:grpc-stub:${grpcVersion}")

    // For optional native transport (Epoll) for Linux high-perf
    runtimeOnly("io.netty:netty-transport-native-epoll:${nettyVersion}:linux-x86_64") {
        because("use native epoll transport on Linux for lower latency and higher throughput")
    }

    implementation("com.google.protobuf:protobuf-java:${protobufVersion}")

    // logging
    implementation("org.slf4j:slf4j-api:2.0.9")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.9")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
}

application {
    mainClass.set("fastpay.server.FastPayServer")
}

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:${protobufVersion}" }
    plugins {
        id("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:${grpcVersion}" }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                id("grpc")
            }
        }
    }
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf("-parameters"))
}
