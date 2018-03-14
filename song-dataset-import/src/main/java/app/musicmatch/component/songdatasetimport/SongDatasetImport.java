package app.musicmatch.component.songdatasetimport;

import app.musicmatch.component.songdatasetimport.dto.model.Song;
import app.musicmatch.component.songdatasetimport.dto.model.Term;
import app.musicmatch.component.songdatasetimport.exception.ApplicationException;
import app.musicmatch.component.songdatasetimport.exception.DuplicateEntityException;
import app.musicmatch.component.songdatasetimport.exception.InvalidRecordStructure;
import app.musicmatch.component.songdatasetimport.exception.WrappedException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import javafx.util.Pair;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.logging.MorphiaLoggerFactory;
import org.mongodb.morphia.logging.SilentLogger;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SongDatasetImport {

    private ObjectMapper objectMapper;
    private DatastoreConfig datastoreConfig;
    private String filename;

    private Long recordCount = 0L;
    private Long storedCount = 0L;
    private Long rejectedCount = 0L;
    private List<Pair<CSVRecord, String>> errors = new LinkedList<>();

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Usage: java -jar song-dataset-import.jar pathToFile/Folder dbConnectionString databaseName");
            System.exit(-1);
        } else {
            File selectedFile = new File(args[0]);
            String dbConnection = args[1];
            String dbName = args[2];
            DatastoreConfig datastoreConfig = DatastoreConfig.open(dbConnection, dbName);

            for (File file : selectedFile.isDirectory() ? selectedFile.listFiles(File::isFile) : new File[] {selectedFile}) {
                System.out.println("Processing file: " + file.getName());

                SongDatasetImport songDatasetImport =
                        new SongDatasetImport(file.getAbsolutePath(), datastoreConfig);
                songDatasetImport.importDataset();
                songDatasetImport.printImportResult();

            }

            datastoreConfig.close();
        }
    }

    public SongDatasetImport(String filename, DatastoreConfig datastoreConfig) {
        this.filename = filename;
        this.objectMapper = new ObjectMapper();
        this.datastoreConfig = datastoreConfig;
    }

    public void importDataset() throws IOException {
        Reader in = new FileReader(filename);
        Iterable<CSVRecord> records = CSVFormat.RFC4180
                .withHeader(DatasetDef.class)
                .withSkipHeaderRecord()
                .withNullString("")
                .withIgnoreSurroundingSpaces()
                .withQuoteMode(QuoteMode.MINIMAL)
                .parse(in);

        for (CSVRecord record: records) {
            recordCount++;
            try {
                Song song = Song.Builder.aSong()
                        .withId(record.get(DatasetDef.song_id))
                        .withArtistFamiliarity(Optional.ofNullable(record.get(DatasetDef.artist_familiarity)).map(this::mapNanValue).map(Double::parseDouble).orElse(null))
                        .withArtistHotness(Optional.ofNullable(record.get(DatasetDef.artist_hotttnesss)).map(this::mapNanValue).map(Double::parseDouble).orElse(null))
                        .withArtistId(record.get(DatasetDef.artist_id))
                        .withArtistName(record.get(DatasetDef.artist_name))
                        .withArtistTerms(buildArtistTerms(record))
                        .withDanceability(Optional.ofNullable(record.get(DatasetDef.danceability)).map(this::mapNanValue).map(Double::parseDouble).orElse(null))
                        .withDuration(Optional.ofNullable(record.get(DatasetDef.duration)).map(this::mapNanValue).map(Double::parseDouble).orElse(null))
                        .withEnergy(Optional.ofNullable(record.get(DatasetDef.energy)).map(this::mapNanValue).map(Double::parseDouble).orElse(null))
                        .withKey(Optional.ofNullable(record.get(DatasetDef.key)).map(this::mapNanValue).map(Integer::parseInt).orElse(null))
                        .withKeyConfidence(Optional.ofNullable(record.get(DatasetDef.key_confidence)).map(this::mapNanValue).map(Double::parseDouble).orElse(null))
                        .withMode(Optional.ofNullable(record.get(DatasetDef.mode)).map(this::mapNanValue).map(Integer::parseInt).orElse(null))
                        .withModeConfidence(Optional.ofNullable(record.get(DatasetDef.mode_confidence)).map(this::mapNanValue).map(Double::parseDouble).orElse(null))
                        .withRelease(record.get(DatasetDef.release))
                        .withSimilarArtists(Optional.ofNullable(record.get(DatasetDef.similar_artists)).map(o -> jsonArrayToList(o, String.class)).get())
                        .withSongHotness(Optional.ofNullable(record.get(DatasetDef.song_hotttnesss)).map(this::mapNanValue).map(Double::parseDouble).orElse(null))
                        .withSongId(record.get(DatasetDef.song_id))
                        .withTempo(Optional.ofNullable(record.get(DatasetDef.tempo)).map(this::mapNanValue).map(Double::parseDouble).orElse(null))
                        .withTimeSignature(Optional.ofNullable(record.get(DatasetDef.time_signature)).map(this::mapNanValue).map(Integer::parseInt).orElse(null))
                        .withTimeSignatureConfidence(Optional.ofNullable(record.get(DatasetDef.time_signature_confidence)).map(this::mapNanValue).map(Double::parseDouble).orElse(null))
                        .withTitle(record.get(DatasetDef.title))
                        .withTrackId(record.get(DatasetDef.track_id))
                        .withYear(Optional.ofNullable(record.get(DatasetDef.year)).map(this::mapNanValue).map(Integer::parseInt).map(i -> i == 0 ? null : i).orElse(null))
                        .build();
                if (datastoreConfig.getDatastore().exists(song) == null) {
                    datastoreConfig.getDatastore().save(song);
                    storedCount++;
                } else {
                    throw new DuplicateEntityException();
                }
            } catch (ApplicationException | NumberFormatException e) {
                delegateExceptionHandling(record, e);
            } catch (WrappedException e) {
                delegateExceptionHandling(record, e.getCause());
            }
        }
    }

    public void printImportResult() {
        System.out.printf("Total number of read records: %d\n", recordCount);
        System.out.printf("Total number of stored records: %d\n", storedCount);
        System.out.printf("Total number of rejected records: %d\n", rejectedCount);
        if (rejectedCount > 0) {
            System.out.println();
            errors.forEach(item -> {
                System.out.printf("Record %d: \n", item.getKey().getRecordNumber());
                System.out.println(item.getValue());
            });
        }
    }

    private String mapNanValue(String s) {
        return s.equals("nan") ? null : s;
    }

    private void delegateExceptionHandling(CSVRecord record, Throwable e) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(stringWriter);
        e.printStackTrace(writer);
        errors.add(new Pair<CSVRecord, String>(record, stringWriter.toString()));
        rejectedCount++;
        writer.close();
    }

    private List<Term> buildArtistTerms(CSVRecord record) {
        List<String> artistTerms = jsonArrayToList(record.get(DatasetDef.artist_terms), String.class);
        List<Double> termsFreq = jsonArrayToList(record.get(DatasetDef.artist_terms_freq), Double.class);
        List<Double> weightFreq = jsonArrayToList(record.get(DatasetDef.artist_terms_weight), Double.class);
        if ( (artistTerms.size() == termsFreq.size()) && (termsFreq.size() == weightFreq.size()) ) {
            return IntStream.range(0, artistTerms.size())
                    .mapToObj(v -> new Term(artistTerms.get(v), termsFreq.get(v), weightFreq.get(v)))
                    .collect(Collectors.toList());
        } else {
            throw new WrappedException(new InvalidRecordStructure("Artist's terms are not valid"));
        }
    }

    private <T> List<T> jsonArrayToList(String json, Class<T> typeClass) {
        try {
            return objectMapper.readValue(json,
                    objectMapper.getTypeFactory().constructCollectionType(List.class, typeClass));
        } catch (IOException e) {
            throw new WrappedException(e);
        }
    }
}
