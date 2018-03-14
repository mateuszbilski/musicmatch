package app.musicmatch.component.validationdataimport;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

public class DatastoreConfig {

    private static final String SCAN_PACKAGE = "app.musicmatch.component.songdatasetimport";

    private MongoClient databaseClient;
    private Morphia morphia;
    private Datastore datastore;
    private String dbConnection;
    private String databaseName;
    private MongoDatabase databaseManager;

    private DatastoreConfig() {

    }

    public static DatastoreConfig open(String dbConnection, String databaseName) {
        DatastoreConfig obj = new DatastoreConfig();
        obj.dbConnection = dbConnection;
        obj.databaseName = databaseName;
        obj.databaseClient = new MongoClient(new MongoClientURI(obj.dbConnection));
        obj.databaseManager = obj.databaseClient.getDatabase(obj.databaseName);
        obj.morphia = new Morphia();
        obj.morphia.mapPackage(SCAN_PACKAGE);
        obj.datastore = obj.morphia.createDatastore(obj.databaseClient, obj.databaseName);
        return obj;
    }

    public void close() {
        databaseClient.close();
    }

    public MongoClient getDatabaseClient() {
        return databaseClient;
    }

    public Morphia getMorphia() {
        return morphia;
    }

    public Datastore getDatastore() {
        return datastore;
    }

    public String getDbConnection() {
        return dbConnection;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public MongoDatabase getDatabaseManager() {
        return databaseManager;
    }
}
