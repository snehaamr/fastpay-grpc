import com.google.protobuf.gradle.id

plugins {
    java
    idea
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
    // shaded Netty: ServerBuilder plus the historical NettyServerBuilder import
    implementation("io.grpc:grpc-netty-shaded:${grpcVersion}")
    implementation("io.grpc:grpc-protobuf:${grpcVersion}")
    implementation("io.grpc:grpc-services:${grpcVersion}")
    implementation("org.xerial:sqlite-jdbc:3.47.1.0")
    implementation("org.postgresql:postgresql:42.7.4")
    implementation("com.google.protobuf:protobuf-java:${protobufVersion}")
    // Required by generated gRPC stubs on Java 9+
    implementation("javax.annotation:javax.annotation-api:1.3.2")

    implementation("org.slf4j:slf4j-api:2.0.16")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")

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
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${grpcVersion}"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach { task ->
            task.plugins {
                // Braces are required or the grpc plugin is not applied.
                id("grpc") { }
            }
        }
    }
}

tasks.named("compileJava") {
    dependsOn("generateProto")
}

tasks.named("compileTestJava") {
    dependsOn("generateProto")
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf("-parameters"))
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        events("failed")
        showCauses = true
        showStackTraces = true
    }
}

fun JavaExec.allowDeprecatedUnsafeOnNewJdks() {
    if (JavaVersion.current().majorVersion.toInt() >= 24) {
        jvmArgs("--sun-misc-unsafe-memory-access=allow")
    }
}

tasks.named<JavaExec>("run") {
    allowDeprecatedUnsafeOnNewJdks()
}

tasks.register<JavaExec>("runClient") {
    group = "application"
    description = "Run the sample client; starts a local server on 6565 if none is listening"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("fastpay.client.FastPayClient")
    allowDeprecatedUnsafeOnNewJdks()
}

tasks.register<JavaExec>("runDemo") {
    group = "application"
    description = "Same as runClient: start a server if needed, run the sample, then shut down"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("fastpay.client.FastPayDemo")
    allowDeprecatedUnsafeOnNewJdks()
}
