package dev.rls.dataset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.rls.lookup.HealthReply;
import dev.rls.lookup.HealthRequest;
import dev.rls.lookup.LookupGrpc;
import dev.rls.lookup.LookupReply;
import dev.rls.lookup.ParcelRequest;
import dev.rls.lookup.Property;
import dev.rls.lookup.ZipRequest;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.Locale;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DatasetTest {
    private static Dataset ds;
    private static Server server;
    private static ManagedChannel channel;
    private static LookupGrpc.LookupBlockingStub stub;

    @BeforeAll
    static void start() throws Exception {
        ds = Dataset.load(Path.of("../testdata/sample.csv.gz"));
        server = InProcessServerBuilder.forName("test")
                .directExecutor()
                .addService(new LookupService(ds, "test-1"))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName("test").directExecutor().build();
        stub = LookupGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void readsColumnsByNameNotPosition() throws IOException {
        String csv = "extra,living_area,zip,parcel,address,street,assessed_total,assessed_land,"
                + "assessed_improvement,lot_size,year_built\n"
                + "x,800,53703,1,1 Main St,Main St,10,4,6,12.5,1999\n";
        Dataset d = Dataset.read(new StringReader(csv));
        Property p = d.byParcel("1").get(0);
        assertEquals("53703", p.getZip());
        assertEquals(800, p.getLivingArea());
        assertEquals(12.5, p.getLotSize());
    }

    @Test
    void missingColumnIsNamed() {
        IOException e = assertThrows(IOException.class, () -> Dataset.read(new StringReader("parcel,address\n1,2\n")));
        assertTrue(e.getMessage().contains("street"));
    }

    @Test
    void everyRowIsReachableByEveryKey() {
        assertTrue(ds.size() > 0);
        for (Property p : ds.all()) {
            assertTrue(ds.byParcel(p.getParcel()).contains(p));
            assertTrue(ds.byAddress(p.getAddress()).contains(p));
            assertTrue(ds.byStreet(p.getStreet()).contains(p));
            assertTrue(ds.byZip(p.getZip()).contains(p));
        }
    }

    @Test
    void addressIgnoresCaseAndSpacing() {
        Property p = ds.all().get(0);
        String messy = "  " + p.getAddress().toUpperCase(Locale.ROOT).replace(" ", "   ") + " ";
        assertEquals(ds.byAddress(p.getAddress()), ds.byAddress(messy));
        assertFalse(ds.byAddress(messy).isEmpty());
    }

    @Test
    void replyShapes() {
        Property p = ds.all().get(0);
        LookupReply ok = stub.byParcel(ParcelRequest.newBuilder().setParcel(p.getParcel()).build());
        assertEquals("", ok.getError());
        assertEquals(ok.getTotal(), ok.getPropertiesCount());

        LookupReply miss = stub.byParcel(ParcelRequest.newBuilder().setParcel("nope").build());
        assertEquals("no addresses found", miss.getError());
        assertEquals(0, miss.getPropertiesCount());

        LookupReply blank = stub.byZip(ZipRequest.newBuilder().setZip("  ").build());
        assertEquals("empty key", blank.getError());
    }

    @Test
    void zipLimitKeepsTotal() {
        String zip = ds.all().get(0).getZip();
        int full = ds.byZip(zip).size();
        LookupReply r = stub.byZip(ZipRequest.newBuilder().setZip(zip).setLimit(2).build());
        assertEquals(2, r.getPropertiesCount());
        assertEquals(full, r.getTotal());
    }

    @Test
    void health() {
        HealthReply h = stub.health(HealthRequest.getDefaultInstance());
        assertTrue(h.getServing());
        assertEquals("java", h.getImplementation());
        assertEquals(ds.size(), h.getRows());
    }
}
