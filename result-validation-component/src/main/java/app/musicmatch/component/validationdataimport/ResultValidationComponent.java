package app.musicmatch.component.validationdataimport;

import app.musicmatch.component.validationdataimport.exception.ApplicationException;
import app.musicmatch.component.validationdataimport.exception.InvalidRecordStructureException;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.mongodb.client.MongoCollection;
import javafx.util.Pair;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.bson.Document;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.stream.Collectors;

public class ResultValidationComponent {

    private final String filename;
    private final DatastoreConfig datastoreConfig;
    private final ImportStatistic importStatistic;
    private final String recommendationResultCollName;

    private final Map<String, Set<String>> validationDataset;
    private final Map<String, Set<String>> recommedationResult;

    private Long resultRecommendationEntriesCount;
    private Long validationEntriesCount;
    private Long correctlyRecommendedSongsCount;

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Usage: java -jar validation-data-import.jar pathToFile dbConnectionString databaseName recommendationResultCollName");
            System.exit(-1);
        } else {
            File validationFile = new File(args[0]);
            String dbConnection = args[1];
            String dbName = args[2];
            String recommendationResultCollName = args[3];
            DatastoreConfig datastoreConfig = DatastoreConfig.open(dbConnection, dbName);

            ResultValidationComponent resultValidationComponent = new ResultValidationComponent(validationFile.getAbsolutePath(), datastoreConfig, recommendationResultCollName);
            resultValidationComponent.importValidationDataset();
            resultValidationComponent.importResultsFromCollection();
            resultValidationComponent.calculateCoefficient();
            resultValidationComponent.printComputationResults();

            datastoreConfig.close();
        }
    }

    public ResultValidationComponent(String filename, DatastoreConfig datastoreConfig, String recommendationResultCollName) {
        this.filename = filename;
        this.datastoreConfig = datastoreConfig;
        this.recommendationResultCollName = recommendationResultCollName;
        validationDataset = new HashMap<>();
        recommedationResult = new HashMap<>();
        importStatistic = new ImportStatistic();
    }

    public void importValidationDataset() throws IOException {
        Reader in = new FileReader(filename);
        Iterable<CSVRecord> records = CSVFormat.TDF
                .withHeader(DatasetDef.class)
                .withNullString("")
                .withIgnoreSurroundingSpaces()
                .withQuoteMode(QuoteMode.MINIMAL)
                .parse(in);

        for (CSVRecord record : records) {
            try {
                importStatistic.increaseReadRecords();
                String userId = record.get(DatasetDef.userId);
                String songId = record.get(DatasetDef.songId);
                if (userId == null || songId == null) {
                    throw new InvalidRecordStructureException();
                } else {
                    if (validationDataset.containsKey(userId)) {
                        validationDataset.get(userId).add(songId);
                    } else {
                        validationDataset.put(userId, Sets.newHashSet(songId));
                    }
                }
                importStatistic.increaseSavedRecords();
            } catch (ApplicationException ex) {
                importStatistic.increaseRejectedRecords();
                importStatistic.logError(new Pair(record, Throwables.getStackTraceAsString(ex)));
            }
        }
    }

    public void importResultsFromCollection() {
        MongoCollection<Document> collection = datastoreConfig.getDatabaseManager().getCollection(recommendationResultCollName);
        collection.find().iterator().forEachRemaining(document -> {
            String userId = document.getString("_id");
            List<String> songs = (List<String>) document.get("songs");
            recommedationResult.put(userId, Sets.newHashSet(songs));
        });
        resultRecommendationEntriesCount = recommedationResult.entrySet().stream().mapToLong(entry -> entry.getValue().size()).sum();
    }

    public void calculateCoefficient() {
        validationEntriesCount = validationDataset.entrySet()
                .stream()
                .mapToLong(entry -> entry.getValue().size())
                .sum();
        correctlyRecommendedSongsCount =
                validationDataset.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(song -> new Pair<String, String>(entry.getKey(), song))
                        .collect(Collectors.toList()).stream())
                .filter(pair -> Optional.ofNullable(recommedationResult.get(pair.getKey()))
                        .map(songs -> songs.contains(pair.getValue())).orElseGet(() ->  false))
                .count();
    }

    public void printComputationResults() {
        System.out.println("Stage 1 - import validation set from file");
        System.out.printf("Total number of read records: %d\n", importStatistic.getReadRecords());
        System.out.printf("Total number of saved records: %d\n", importStatistic.getSavedRecords());
        System.out.printf("Total number of rejected records: %d\n", importStatistic.getRejectedRecords());
        if (importStatistic.getRejectedRecords() > 0) {
            System.out.println();
            importStatistic.getImportErrorList().forEach( item -> {
                System.out.printf("Record %d: \n", item.getKey().getRecordNumber());
                System.out.println(item.getValue());
            });
        }

        System.out.println("\nStage 2 - import recommendation results from database");
        System.out.printf("Total number of collected entries from %s: %d\n", recommendationResultCollName, resultRecommendationEntriesCount);

        System.out.println("\nStage 3 - calculate coefficient");
        System.out.printf("Total number of validation entries: %d\n", validationEntriesCount);
        System.out.printf("Total number of correctly recommended songs: %d\n", correctlyRecommendedSongsCount);
        System.out.printf("Coefficient: %f\n", (double) correctlyRecommendedSongsCount / validationEntriesCount);
    }

    static class ImportStatistic {
        private Long readRecords;
        private Long savedRecords;
        private Long rejectedRecords;
        private List<Pair<CSVRecord, String>> importErrorList;

        public ImportStatistic() {
            readRecords = 0L;
            savedRecords = 0L;
            rejectedRecords = 0L;
            importErrorList = new LinkedList<>();
        }

        public Long getReadRecords() {
            return readRecords;
        }

        public Long getSavedRecords() {
            return savedRecords;
        }

        public Long getRejectedRecords() {
            return rejectedRecords;
        }

        public List<Pair<CSVRecord, String>> getImportErrorList() {
            return importErrorList;
        }

        public void increaseReadRecords() {
            readRecords++;
        }

        public void increaseSavedRecords() {
            savedRecords++;
        }

        public void increaseRejectedRecords() {
            rejectedRecords++;
        }

        public void logError(Pair<CSVRecord, String> error) {
            importErrorList.add(error);
        }
    }
}
