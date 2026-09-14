package fastpay.security;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class Tls {
    private Tls() {
    }

    public static void ensureLocalhostCerts(Path certChain, Path privateKey, Path trustCert) throws IOException {
        if (Files.isRegularFile(certChain) && Files.isRegularFile(privateKey)) {
            if (trustCert != null && !Files.isRegularFile(trustCert)) {
                Files.copy(certChain, trustCert);
            }
            return;
        }
        Path parent = certChain.getParent() == null ? Path.of(".") : certChain.getParent();
        Files.createDirectories(parent);
        openssl(
                "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-days", "365",
                "-nodes",
                "-keyout", privateKey.toString(),
                "-out", certChain.toString(),
                "-subj", "/CN=localhost",
                "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"
        );
        if (trustCert != null && !Files.isRegularFile(trustCert)) {
            Files.copy(certChain, trustCert);
        }
    }

    /**
     * Dev CA + localhost server cert + client cert. Reuses existing TLS material
     * when present (self-signed server cert can sign the client cert).
     */
    public static void ensureLocalhostMtls(
            Path trustCert,
            Path serverCert,
            Path serverKey,
            Path clientCert,
            Path clientKey
    ) throws IOException {
        ensureLocalhostCerts(serverCert, serverKey, trustCert);
        if (Files.isRegularFile(clientCert) && Files.isRegularFile(clientKey)) {
            return;
        }
        Path parent = clientCert.getParent() == null ? Path.of(".") : clientCert.getParent();
        Files.createDirectories(parent);
        Path caKey = caKey(trustCert);
        Path signingKey = Files.isRegularFile(caKey) ? caKey : serverKey;
        requireReadable(trustCert, "FASTPAY_TRUST_CERT");
        requireReadable(signingKey, "FASTPAY_KEY");
        Path work = Files.createTempDirectory("fastpay-mtls");
        try {
            Path csr = work.resolve("client.csr");
            Path ext = work.resolve("client.ext");
            Files.writeString(ext, "extendedKeyUsage=clientAuth\n");
            openssl(
                    "req", "-new", "-newkey", "rsa:2048", "-nodes",
                    "-keyout", clientKey.toString(),
                    "-out", csr.toString(),
                    "-subj", "/CN=fastpay-client"
            );
            openssl(
                    "x509", "-req", "-in", csr.toString(),
                    "-CA", trustCert.toString(),
                    "-CAkey", signingKey.toString(),
                    "-CAcreateserial",
                    "-out", clientCert.toString(),
                    "-days", "365",
                    "-sha256",
                    "-extfile", ext.toString()
            );
        } finally {
            deleteQuietly(work);
        }
    }

    public static Path caKey(Path trustCert) {
        if (trustCert == null || trustCert.getParent() == null) {
            return Path.of("certs/ca.key");
        }
        return trustCert.resolveSibling("ca.key");
    }

    public static SslContext serverContext(Path certChain, Path privateKey) throws IOException {
        return serverContext(certChain, privateKey, null, false);
    }

    public static SslContext serverContext(
            Path certChain,
            Path privateKey,
            Path trustCert,
            boolean mtls
    ) throws IOException {
        requireReadable(certChain, "FASTPAY_CERT");
        requireReadable(privateKey, "FASTPAY_KEY");
        try {
            SslContextBuilder builder = GrpcSslContexts.forServer(certChain.toFile(), privateKey.toFile());
            if (mtls) {
                requireReadable(trustCert, "FASTPAY_TRUST_CERT");
                builder.trustManager(trustCert.toFile()).clientAuth(ClientAuth.REQUIRE);
            }
            return builder.build();
        } catch (Exception e) {
            throw new IOException("failed to build server TLS context", e);
        }
    }

    public static SslContext clientContext(Path trustCert) throws IOException {
        return clientContext(trustCert, null, null);
    }

    public static SslContext clientContext(Path trustCert, Path clientCert, Path clientKey) throws IOException {
        requireReadable(trustCert, "FASTPAY_TRUST_CERT");
        try {
            SslContextBuilder builder = GrpcSslContexts.forClient().trustManager(trustCert.toFile());
            if (clientCert != null && clientKey != null) {
                requireReadable(clientCert, "FASTPAY_CLIENT_CERT");
                requireReadable(clientKey, "FASTPAY_CLIENT_KEY");
                builder.keyManager(clientCert.toFile(), clientKey.toFile());
            }
            return builder.build();
        } catch (Exception e) {
            throw new IOException("failed to build client TLS context", e);
        }
    }

    private static void openssl(String... args) throws IOException {
        List<String> command = new ArrayList<>();
        command.add("openssl");
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        try {
            int code = process.waitFor();
            if (code != 0) {
                throw new IOException("openssl failed with exit " + code
                        + " (install openssl to enable TLS): " + output);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("openssl interrupted", e);
        }
    }

    private static void deleteQuietly(Path dir) {
        try (var paths = Files.walk(dir)) {
            paths.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {
                    // best-effort cleanup of openssl scratch files
                }
            });
        } catch (IOException ignored) {
            // best-effort cleanup of openssl scratch files
        }
    }

    private static void requireReadable(Path path, String envName) throws IOException {
        if (path == null || !Files.isRegularFile(path)) {
            throw new IOException(envName + " file not found: " + path);
        }
    }
}
