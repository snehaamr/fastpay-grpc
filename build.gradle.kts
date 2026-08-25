plugins {
    java
    application
    id("com.google.protobuf") version "0.9.4"
}

group = "com.example"
version = "0.1.0"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

val grpcVersion = "1.68.2"
val protobufVersion = "3.25.5"

dependencies {
    implementation("io.grpc:grpc-netty:${grpcVersion}")
    implementation("io.grpc:grpc-protobuf:${grpcVersion}")
    implementation("io.grpc:grpc-stub:${grpcVersion}")
    implementation("com.google.protobuf:protobuf-java:${protobufVersion}")
    implementation("javax.annotation:javax.annotation-api:1.3.2")

    implementation("org.slf4j:slf4j-api:2.0.16")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")

    // Optional native transport on Linux x86_64; gRPC selects it when present.
    runtimeOnly("io.netty:netty-transport-native-epoll:4.1.110.Final:linux-x86_64")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.3")
    testImplementation("io.grpc:grpc-inprocess:${grpcVersion}")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    val configuredMain = findProperty("mainClass") as String?
    mainClass.set(configuredMain ?: "fastpay.server.FastPayServer")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${protobufVersion}"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${grpcVersion}"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf("-parameters"))
}

tasks.test {
    useJUnitPlatform()
}
