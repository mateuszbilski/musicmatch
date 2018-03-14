package app.musicmatch.component.usersimport;

import app.musicmatch.component.usersimport.dto.model.Song;
import app.musicmatch.component.usersimport.dto.model.SongPlayCount;
import app.musicmatch.component.usersimport.dto.model.User;
import app.musicmatch.component.usersimport.exception.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import javafx.util.Pair;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.Morphia;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.LinkedList;
import java.util.List;

public class UsersImport {

    public static final String SCAN_PACKAGE = "app.musicmatch.component.usersimport";

    private ObjectMapper objectMapper;
    private String filename;
    private MongoClient databaseClient;
    private Morphia morphia;
    private Datastore datastore;

    private Long recordCount = 0L;
    private Long storedCount = 0L;
    private Long rejectedCount = 0L;
    private List<Pair<CSVRecord, String>> errors = new LinkedList<>();

    public static void main(String[] args) throws IOException {
        UsersImport usersImport = new UsersImport(args[0], args[1], args[2]);
        usersImport.importDataset();
        System.out.printf("Total number of read records: %d\n", usersImport.recordCount);
        System.out.printf("Total number of stored records: %d\n", usersImport.storedCount);
        System.out.printf("Total number of rejected records: %d\n", usersImport.rejectedCount);
        if (usersImport.rejectedCount > 0) {
            System.out.println();
            usersImport.errors.stream().forEach(item -> {
                System.out.printf("Record %d: \n", item.getKey().getRecordNumber());
                System.out.println(item.getValue());
            });
        }
    }

    public UsersImport(String filename, String connectionString, String databaseName) {
        this.filename = filename;
        this.objectMapper = new ObjectMapper();
        this.morphia = new Morphia();

        databaseClient = new MongoClient(new MongoClientURI(connectionString));
        morphia.mapPackage(SCAN_PACKAGE);
        datastore = morphia.createDatastore(databaseClient, databaseName);
    }

    public void importDataset() throws IOException {
        Reader in = new FileReader(filename);
        Iterable<CSVRecord> records = CSVFormat.TDF
                .withHeader(DatasetDef.class)
                .withNullString("")
                .withIgnoreSurroundingSpaces()
                .withQuoteMode(QuoteMode.MINIMAL)
                .parse(in);

        for (CSVRecord record: records) {
            try {
                recordCount++;
                if (record.get(DatasetDef.userId) == null || record.get(DatasetDef.songId) == null) {
                    throw new InvalidRecordStructureException();
                }
                if (datastore.exists(new Key(Song.class, "songs", record.get(DatasetDef.songId))) == null) {
                    throw new EntityNotExistsException();
                }
                if (datastore.exists(new Key(User.class, "users", record.get(DatasetDef.userId))) == null) {
                    User user = new User(record.get(DatasetDef.userId), Sets.newHashSet(new SongPlayCount(record.get(DatasetDef.songId),
                            Integer.parseInt(record.get(DatasetDef.playCount)))));
                    datastore.save(user);
                } else {
                    User user = datastore.find(User.class, "id", record.get(DatasetDef.userId)).get();
                    SongPlayCount songPlayCount = new SongPlayCount(record.get(DatasetDef.songId),
                            Integer.parseInt(record.get(DatasetDef.playCount)));
                    if (!user.getSongPlayCounts().add(songPlayCount)) {
                        throw new DuplicateEntityException();
                    }
                    datastore.save(user);
                }
                storedCount++;
            } catch (ApplicationException ex) {
                rejectedCount++;
                errors.add(new Pair(record, Throwables.getStackTraceAsString(ex)));
            }
        }
    }

}
