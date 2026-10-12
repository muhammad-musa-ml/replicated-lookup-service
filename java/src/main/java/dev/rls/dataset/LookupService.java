package dev.rls.dataset;

import dev.rls.lookup.AddressRequest;
import dev.rls.lookup.HealthReply;
import dev.rls.lookup.HealthRequest;
import dev.rls.lookup.LookupGrpc;
import dev.rls.lookup.LookupReply;
import dev.rls.lookup.ParcelRequest;
import dev.rls.lookup.Property;
import dev.rls.lookup.StreetRequest;
import dev.rls.lookup.ZipRequest;
import io.grpc.stub.StreamObserver;
import java.util.List;

public final class LookupService extends LookupGrpc.LookupImplBase {
    static final String NOT_FOUND = "no addresses found";
    static final String EMPTY_KEY = "empty key";

    private final Dataset ds;
    private final String replica;

    public LookupService(Dataset ds, String replica) {
        this.ds = ds;
        this.replica = replica;
    }

    @SuppressWarnings("deprecation")
    static LookupReply reply(List<Property> rows, String key, int limit) {
        if (key.isBlank()) {
            return LookupReply.newBuilder().setError(EMPTY_KEY).setFailed(true).build();
        }
        if (rows.isEmpty()) {
            return LookupReply.newBuilder().setError(NOT_FOUND).setFailed(true).build();
        }
        List<Property> shown = limit > 0 && limit < rows.size() ? rows.subList(0, limit) : rows;
        return LookupReply.newBuilder().addAllProperties(shown).setTotal(rows.size()).build();
    }

    private static void send(StreamObserver<LookupReply> out, LookupReply r) {
        out.onNext(r);
        out.onCompleted();
    }

    @Override
    public void byParcel(ParcelRequest req, StreamObserver<LookupReply> out) {
        send(out, reply(ds.byParcel(req.getParcel()), req.getParcel(), 0));
    }

    @Override
    public void byAddress(AddressRequest req, StreamObserver<LookupReply> out) {
        send(out, reply(ds.byAddress(req.getAddress()), req.getAddress(), 0));
    }

    @Override
    public void byStreet(StreetRequest req, StreamObserver<LookupReply> out) {
        send(out, reply(ds.byStreet(req.getStreet()), req.getStreet(), req.getLimit()));
    }

    @Override
    public void byZip(ZipRequest req, StreamObserver<LookupReply> out) {
        send(out, reply(ds.byZip(req.getZip()), req.getZip(), req.getLimit()));
    }

    @Override
    public void health(HealthRequest req, StreamObserver<HealthReply> out) {
        out.onNext(HealthReply.newBuilder()
                .setServing(true)
                .setImplementation("java")
                .setReplica(replica)
                .setRows(ds.size())
                .build());
        out.onCompleted();
    }
}
