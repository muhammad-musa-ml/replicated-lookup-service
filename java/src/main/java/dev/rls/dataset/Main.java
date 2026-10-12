package dev.rls.dataset;

import dev.rls.lookup.HealthReply;
import dev.rls.lookup.HealthRequest;
import dev.rls.lookup.LookupGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Java dataset replica. Environment: DATA_PATH, PORT (default 50051), REPLICA_ID.
 * With --healthcheck it probes the local port and exits 0 when Health reports serving.
 */
public final class Main {
    private static final int MAX_MESSAGE = 32 * 1024 * 1024;

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return v == null || v.isEmpty() ? fallback : v;
    }

    static int healthcheck(int port) {
        ManagedChannel ch = NettyChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build();
        try {
            HealthReply h = LookupGrpc.newBlockingStub(ch)
                    .withDeadlineAfter(2, TimeUnit.SECONDS)
                    .health(HealthRequest.getDefaultInstance());
            return h.getServing() ? 0 : 1;
        } catch (RuntimeException e) {
            return 1;
        } finally {
            ch.shutdownNow();
        }
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(env("PORT", "50051"));
        if (args.length > 0 && args[0].equals("--healthcheck")) {
            System.exit(healthcheck(port));
        }
        String replica = env("REPLICA_ID", "java-1");
        Dataset ds = Dataset.load(Path.of(env("DATA_PATH", "/data/madison.csv.gz")));

        Server server = ServerBuilder.forPort(port)
                .maxInboundMessageSize(MAX_MESSAGE)
                .addService(new LookupService(ds, replica))
                .build()
                .start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.shutdown()));
        System.out.printf("%s: %d rows, listening on %d%n", replica, ds.size(), port);
        server.awaitTermination();
    }
}
