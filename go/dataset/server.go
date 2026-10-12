package dataset

import (
	"context"
	"strings"

	"github.com/muhammad-musa-ml/replicated-lookup-service/go/lookuppb"
)

const (
	NotFound = "no addresses found"
	EmptyKey = "empty key"
)

type Server struct {
	lookuppb.UnimplementedLookupServer
	ds      *Dataset
	replica string
}

func NewServer(ds *Dataset, replica string) *Server {
	return &Server{ds: ds, replica: replica}
}

func reply(rows []*lookuppb.Property, key string, limit int32) *lookuppb.LookupReply {
	if strings.TrimSpace(key) == "" {
		return &lookuppb.LookupReply{Error: EmptyKey, Failed: true}
	}
	if len(rows) == 0 {
		return &lookuppb.LookupReply{Error: NotFound, Failed: true}
	}
	shown := rows
	if limit > 0 && int(limit) < len(rows) {
		shown = rows[:limit]
	}
	return &lookuppb.LookupReply{Properties: shown, Total: int32(len(rows))}
}

func (s *Server) ByParcel(_ context.Context, r *lookuppb.ParcelRequest) (*lookuppb.LookupReply, error) {
	return reply(s.ds.ByParcel(r.Parcel), r.Parcel, 0), nil
}

func (s *Server) ByAddress(_ context.Context, r *lookuppb.AddressRequest) (*lookuppb.LookupReply, error) {
	return reply(s.ds.ByAddress(r.Address), r.Address, 0), nil
}

func (s *Server) ByStreet(_ context.Context, r *lookuppb.StreetRequest) (*lookuppb.LookupReply, error) {
	return reply(s.ds.ByStreet(r.Street), r.Street, r.Limit), nil
}

func (s *Server) ByZip(_ context.Context, r *lookuppb.ZipRequest) (*lookuppb.LookupReply, error) {
	return reply(s.ds.ByZip(r.Zip), r.Zip, r.Limit), nil
}

func (s *Server) Health(context.Context, *lookuppb.HealthRequest) (*lookuppb.HealthReply, error) {
	return &lookuppb.HealthReply{
		Serving:        true,
		Implementation: "go",
		Replica:        s.replica,
		Rows:           int64(s.ds.Len()),
	}, nil
}
