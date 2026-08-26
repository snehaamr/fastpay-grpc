package fastpay.security;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class Tls {
    private Tls() {
    }

    public static SslContext serverContext(Path certChain, Path privateKey) throws IOException {
        requireReadable(certChain, "FASTPAY_CERT");
        requireReadable(privateKey, "FASTPAY_KEY");
        try {
            return GrpcSslContexts.forServer(certChain.toFile(), privateKey.toFile()).build();
        } catch (Exception e) {
            throw new IOException("failed to build server TLS context", e);
        }
    }

    public static SslContext clientContext(Path trustCert) throws IOException {
        requireReadable(trustCert, "FASTPAY_TRUST_CERT");
        try {
            return GrpcSslContexts.forClient().trustManager(trustCert.toFile()).build();
        } catch (Exception e) {
            throw new IOException("failed to build client TLS context", e);
        }
    }

    private static void requireReadable(Path path, String envName) throws IOException {
        if (path == null || !Files.isRegularFile(path)) {
            throw new IOException(envName + " file not found: " + path);
        }
    }
}
