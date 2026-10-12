package dataset

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/muhammad-musa-ml/replicated-lookup-service/go/lookuppb"
)

var columns = []string{
	"parcel", "address", "street", "zip", "assessed_total", "assessed_land",
	"assessed_improvement", "lot_size", "year_built", "living_area",
}

// Norm collapses whitespace and lowercases; the Python and Java servers do the
// same so address and street keys behave identically everywhere.
func Norm(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), " "))
}

type Dataset struct {
	rows    []*lookuppb.Property
	parcel  map[string][]int
	address map[string][]int
	street  map[string][]int
	zip     map[string][]int
}

func Load(path string) (*Dataset, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var in io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		in = gz
	}
	return Read(in)
}

func Read(in io.Reader) (*Dataset, error) {
	r := csv.NewReader(in)
	r.ReuseRecord = true
	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}
	pos := make(map[string]int, len(header))
	for i, name := range header {
		pos[name] = i
	}
	var missing []string
	for _, c := range columns {
		if _, ok := pos[c]; !ok {
			missing = append(missing, c)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing columns: %s", strings.Join(missing, ", "))
	}

	d := &Dataset{
		parcel:  map[string][]int{},
		address: map[string][]int{},
		street:  map[string][]int{},
		zip:     map[string][]int{},
	}
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		get := func(name string) string { return rec[pos[name]] }
		p, err := parseRow(get)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", len(d.rows)+1, err)
		}
		i := len(d.rows)
		d.rows = append(d.rows, p)
		d.parcel[p.Parcel] = append(d.parcel[p.Parcel], i)
		d.address[Norm(p.Address)] = append(d.address[Norm(p.Address)], i)
		d.street[Norm(p.Street)] = append(d.street[Norm(p.Street)], i)
		d.zip[p.Zip] = append(d.zip[p.Zip], i)
	}
	return d, nil
}

func parseRow(get func(string) string) (*lookuppb.Property, error) {
	var err error
	atoi := func(name string, bits int) int64 {
		v, e := strconv.ParseInt(get(name), 10, bits)
		if e != nil && err == nil {
			err = fmt.Errorf("%s: %w", name, e)
		}
		return v
	}
	lot, e := strconv.ParseFloat(get("lot_size"), 64)
	if e != nil {
		return nil, fmt.Errorf("lot_size: %w", e)
	}
	p := &lookuppb.Property{
		Parcel:              get("parcel"),
		Address:             get("address"),
		Street:              get("street"),
		Zip:                 get("zip"),
		AssessedTotal:       atoi("assessed_total", 64),
		AssessedLand:        atoi("assessed_land", 64),
		AssessedImprovement: atoi("assessed_improvement", 64),
		LotSize:             lot,
		YearBuilt:           int32(atoi("year_built", 32)),
		LivingArea:          int32(atoi("living_area", 32)),
	}
	return p, err
}

func (d *Dataset) Len() int { return len(d.rows) }

func (d *Dataset) pick(index map[string][]int, key string) []*lookuppb.Property {
	ids := index[key]
	out := make([]*lookuppb.Property, len(ids))
	for i, id := range ids {
		out[i] = d.rows[id]
	}
	return out
}

func (d *Dataset) ByParcel(parcel string) []*lookuppb.Property {
	return d.pick(d.parcel, strings.TrimSpace(parcel))
}

func (d *Dataset) ByAddress(address string) []*lookuppb.Property {
	return d.pick(d.address, Norm(address))
}

func (d *Dataset) ByStreet(street string) []*lookuppb.Property {
	return d.pick(d.street, Norm(street))
}

func (d *Dataset) ByZip(zip string) []*lookuppb.Property {
	return d.pick(d.zip, strings.TrimSpace(zip))
}
