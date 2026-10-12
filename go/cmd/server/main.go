// Go dataset replica. Environment: DATA_PATH, PORT (default 50051), REPLICA_ID.
// `-healthcheck` dials the local port and exits 0 when Health reports serving.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/muhammad-musa-ml/replicated-lookup-service/go/dataset"
	"github.com/muhammad-musa-ml/replicated-lookup-service/go/lookuppb"
)

const maxMessage = 32 * 1024 * 1024

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func healthcheck(port string) int {
	conn, err := grpc.NewClient("127.0.0.1:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 1
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	h, err := lookuppb.NewLookupClient(conn).Health(ctx, &lookuppb.HealthRequest{})
	if err != nil || !h.Serving {
		return 1
	}
	return 0
}

func main() {
	check := flag.Bool("healthcheck", false, "probe the local server and exit")
	flag.Parse()
	port := env("PORT", "50051")
	if *check {
		os.Exit(healthcheck(port))
	}

	replica := env("REPLICA_ID", "go-1")
	ds, err := dataset.Load(env("DATA_PATH", "/data/madison.csv.gz"))
	if err != nil {
		log.Fatalf("load: %v", err)
	}
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(maxMessage), grpc.MaxSendMsgSize(maxMessage))
	lookuppb.RegisterLookupServer(srv, dataset.NewServer(ds, replica))

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		<-sig
		srv.GracefulStop()
	}()
	log.Printf("%s: %d rows, listening on %s", replica, ds.Len(), port)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
