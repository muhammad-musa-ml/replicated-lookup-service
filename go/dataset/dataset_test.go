package dataset

import (
	"context"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/muhammad-musa-ml/replicated-lookup-service/go/lookuppb"
)

const sample = "../../testdata/sample.csv.gz"

func loadSample(t *testing.T) *Dataset {
	t.Helper()
	ds, err := Load(sample)
	if err != nil {
		t.Fatal(err)
	}
	return ds
}

func TestReadsColumnsByName(t *testing.T) {
	csv := "extra,living_area,zip,parcel,address,street,assessed_total,assessed_land,assessed_improvement,lot_size,year_built\n" +
		"x,800,53703,1,1 Main St,Main St,10,4,6,12.5,1999\n"
	ds, err := Read(strings.NewReader(csv))
	if err != nil {
		t.Fatal(err)
	}
	rows := ds.ByParcel("1")
	if len(rows) != 1 || rows[0].Zip != "53703" || rows[0].LivingArea != 800 || rows[0].LotSize != 12.5 {
		t.Fatalf("unexpected row: %v", rows)
	}
}

func TestMissingColumnIsNamed(t *testing.T) {
	_, err := Read(strings.NewReader("parcel,address\n1,2\n"))
	if err == nil || !strings.Contains(err.Error(), "street") {
		t.Fatalf("want an error naming street, got %v", err)
	}
}

func TestBadNumberFailsTheLoad(t *testing.T) {
	csv := "parcel,address,street,zip,assessed_total,assessed_land,assessed_improvement,lot_size,year_built,living_area\n" +
		"1,a,b,53703,ten,1,1,1.0,1990,1\n"
	if _, err := Read(strings.NewReader(csv)); err == nil {
		t.Fatal("expected a parse error")
	}
}

func TestEveryRowIsReachableByEveryKey(t *testing.T) {
	ds := loadSample(t)
	if ds.Len() == 0 {
		t.Fatal("empty sample")
	}
	for _, r := range ds.rows {
		for name, found := range map[string][]*lookuppb.Property{
			"parcel": ds.ByParcel(r.Parcel), "address": ds.ByAddress(r.Address),
			"street": ds.ByStreet(r.Street), "zip": ds.ByZip(r.Zip),
		} {
			ok := false
			for _, f := range found {
				ok = ok || f == r
			}
			if !ok {
				t.Fatalf("%s lookup lost %s", name, r.Address)
			}
		}
	}
}

func TestAddressIgnoresCaseAndSpacing(t *testing.T) {
	ds := loadSample(t)
	r := ds.rows[0]
	messy := "  " + strings.ToUpper(strings.ReplaceAll(r.Address, " ", "   ")) + " "
	if len(ds.ByAddress(messy)) != len(ds.ByAddress(r.Address)) || len(ds.ByAddress(messy)) == 0 {
		t.Fatal("normalisation differs")
	}
}

func client(t *testing.T, ds *Dataset) lookuppb.LookupClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	lookuppb.RegisterLookupServer(srv, NewServer(ds, "test-1"))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return lookuppb.NewLookupClient(conn)
}

func TestReplyShapes(t *testing.T) {
	ds := loadSample(t)
	c := client(t, ds)
	ctx := context.Background()

	row := ds.rows[0]
	ok, err := c.ByParcel(ctx, &lookuppb.ParcelRequest{Parcel: row.Parcel})
	if err != nil || ok.Error != "" || ok.Failed || int(ok.Total) != len(ok.Properties) {
		t.Fatalf("found reply wrong: %v %v", ok, err)
	}
	miss, _ := c.ByParcel(ctx, &lookuppb.ParcelRequest{Parcel: "nope"})
	if miss.Error != NotFound || !miss.Failed || len(miss.Properties) != 0 {
		t.Fatalf("miss reply wrong: %v", miss)
	}
	blank, _ := c.ByZip(ctx, &lookuppb.ZipRequest{Zip: "  "})
	if blank.Error != EmptyKey {
		t.Fatalf("blank key reply wrong: %v", blank)
	}
}

func TestZipLimitKeepsTotal(t *testing.T) {
	ds := loadSample(t)
	c := client(t, ds)
	zip := ds.rows[0].Zip
	full := len(ds.ByZip(zip))
	r, _ := c.ByZip(context.Background(), &lookuppb.ZipRequest{Zip: zip, Limit: 2})
	if len(r.Properties) != 2 || int(r.Total) != full {
		t.Fatalf("limit reply wrong: %d shown, total %d, want total %d", len(r.Properties), r.Total, full)
	}
}

func TestHealth(t *testing.T) {
	ds := loadSample(t)
	h, err := client(t, ds).Health(context.Background(), &lookuppb.HealthRequest{})
	if err != nil || !h.Serving || h.Implementation != "go" || int(h.Rows) != ds.Len() {
		t.Fatalf("health wrong: %v %v", h, err)
	}
}
