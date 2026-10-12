package dev.rls.dataset;

import dev.rls.lookup.Property;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/** Rows in file order plus one index per lookup key. Columns are read by name. */
public final class Dataset {
    static final List<String> COLUMNS = List.of(
            "parcel", "address", "street", "zip", "assessed_total", "assessed_land",
            "assessed_improvement", "lot_size", "year_built", "living_area");

    private final List<Property> rows = new ArrayList<>();
    private final Map<String, List<Integer>> parcel = new HashMap<>();
    private final Map<String, List<Integer>> address = new HashMap<>();
    private final Map<String, List<Integer>> street = new HashMap<>();
    private final Map<String, List<Integer>> zip = new HashMap<>();

    /** Same normalisation as the Python and Go servers. */
    static String norm(String s) {
        return s.trim().replaceAll("\\s+", " ").toLowerCase(Locale.ROOT);
    }

    public static Dataset load(Path path) throws IOException {
        InputStream in = Files.newInputStream(path);
        if (path.toString().endsWith(".gz")) {
            in = new GZIPInputStream(in);
        }
        try (Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
            return read(reader);
        }
    }

    public static Dataset read(Reader reader) throws IOException {
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get();
        Dataset ds = new Dataset();
        try (CSVParser parser = format.parse(reader)) {
            List<String> missing = new ArrayList<>(COLUMNS);
            missing.removeAll(parser.getHeaderMap().keySet());
            if (!missing.isEmpty()) {
                throw new IOException("missing columns: " + String.join(", ", missing));
            }
            for (CSVRecord rec : parser) {
                ds.add(toProperty(rec));
            }
        }
        return ds;
    }

    private static Property toProperty(CSVRecord r) {
        return Property.newBuilder()
                .setParcel(r.get("parcel"))
                .setAddress(r.get("address"))
                .setStreet(r.get("street"))
                .setZip(r.get("zip"))
                .setAssessedTotal(Long.parseLong(r.get("assessed_total")))
                .setAssessedLand(Long.parseLong(r.get("assessed_land")))
                .setAssessedImprovement(Long.parseLong(r.get("assessed_improvement")))
                .setLotSize(Double.parseDouble(r.get("lot_size")))
                .setYearBuilt(Integer.parseInt(r.get("year_built")))
                .setLivingArea(Integer.parseInt(r.get("living_area")))
                .build();
    }

    private void add(Property p) {
        int i = rows.size();
        rows.add(p);
        parcel.computeIfAbsent(p.getParcel(), k -> new ArrayList<>()).add(i);
        address.computeIfAbsent(norm(p.getAddress()), k -> new ArrayList<>()).add(i);
        street.computeIfAbsent(norm(p.getStreet()), k -> new ArrayList<>()).add(i);
        zip.computeIfAbsent(p.getZip(), k -> new ArrayList<>()).add(i);
    }

    private List<Property> pick(Map<String, List<Integer>> index, String key) {
        List<Integer> ids = index.getOrDefault(key, List.of());
        List<Property> out = new ArrayList<>(ids.size());
        for (int id : ids) {
            out.add(rows.get(id));
        }
        return out;
    }

    public int size() {
        return rows.size();
    }

    public List<Property> all() {
        return rows;
    }

    public List<Property> byParcel(String s) {
        return pick(parcel, s.trim());
    }

    public List<Property> byAddress(String s) {
        return pick(address, norm(s));
    }

    public List<Property> byStreet(String s) {
        return pick(street, norm(s));
    }

    public List<Property> byZip(String s) {
        return pick(zip, s.trim());
    }
}
