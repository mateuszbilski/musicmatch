package app.musicmatch.component.itembasedsparkmodule;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ItemBasedSparkModule {

    private String appName = "itemBasedSparkModule";
    private Integer recommendationsNum = 500;
    private Integer songsLimit = 10000;
    private String mongoInputUri;
    private String usersCollectionName;
    private String mostPopularSongsCollectionName;
    private String mongoOutputUri;

    public static void main(String[] args) throws JsonProcessingException {
        ItemBasedSparkModule.Builder.anItemBasedSparkModule()
                .withAppName(args[0])
                .withRecommendationsNum(Integer.valueOf(args[1]))
                .withSongsLimit(Integer.valueOf(args[2]))
                .withMongoInputUri(args[3])
                .withUsersCollectionName(args[4])
                .withMostPopularSongsCollectionName(args[5])
                .withMongoOutputUri(args[6])
                .build()
                .runComputation();
    }

    @SuppressWarnings("unchecked")
    private void runComputation() throws JsonProcessingException {
        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this));

        SparkSession sparkSession = SparkSession.builder()
                .appName(appName)
                .config("spark.mongodb.input.uri", mongoInputUri)
                .config("spark.mongodb.output.uri", mongoOutputUri)
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        List<String> mostPopularSongs = MongoSpark
                .load(jsc, ReadConfig.create(jsc).withOptions(ImmutableMap.of("collection", mostPopularSongsCollectionName)))
                .mapToPair(doc -> new Tuple2<Double, String>(Optional.ofNullable(doc.getDouble("songHotness")).orElseGet(() -> 0.0), doc.getString("_id")))
                .sortByKey(false)
                .map(tuple -> tuple._2())
                .collect()
                .stream()
                .limit(recommendationsNum)
                .collect(Collectors.toList());

        List<UserPlaylist> usersPlaylists = MongoSpark
                .load(jsc, ReadConfig.create(jsc).withOptions(ImmutableMap.of("collection", usersCollectionName)))
                .map(doc -> {
                    Set<String> songs = ((List<Document>) doc.get("songPlayCounts"))
                            .stream().map(playCount -> playCount.getString("songId"))
                            .collect(Collectors.toSet());
                    return new UserPlaylist(doc.getString("_id"), songs);
                })
                .collect();

        long sl = songsLimit.longValue();
        JavaPairRDD<Document, Long> songsRdd = MongoSpark.load(jsc).zipWithUniqueId().filter(tuple -> tuple._2() < sl);
        Map<String, Long> songsLookup = songsRdd
                .mapToPair(tuple -> new Tuple2(tuple._1().getString("_id"), tuple._2()))
                .collectAsMap();
        Map<Long, String> reverseSongsLookup = songsRdd
                .mapToPair(tuple -> new Tuple2(tuple._2(), tuple._1().getString("_id")))
                .collectAsMap();

        IndexedRowMatrix indexedRowMatrix = new IndexedRowMatrix(
                songsRdd.map(tuple -> IndexedRow.apply(tuple._2(), Vectors.dense(
                        tuple._1().getDouble("tempo"),
                        tuple._1().getDouble("danceability"),
                        tuple._1().getDouble("energy"),
                        tuple._1().getInteger("key").doubleValue(),
                        tuple._1().getInteger("mode").doubleValue()
                ))).rdd()
        );
        CoordinateMatrix similaritiesMatrix = indexedRowMatrix
                .toBlockMatrix()
                .transpose()
                .toIndexedRowMatrix()
                .columnSimilarities();

        List<UserPlaylist> recommendations = usersPlaylists.stream().map(userPlaylist -> {
            Set<Long> mappedSongs = userPlaylist.getSongs().stream().map(song -> songsLookup.get(song)).collect(Collectors.toSet());
            Set<String> userRecommendations = similaritiesMatrix
                    .entries()
                    .toJavaRDD()
                    .filter(entry -> mappedSongs.contains(entry.i()) || mappedSongs.contains(entry.j()))
                    .mapToPair(entry -> new Tuple2<Double, Long>(entry.value(), (mappedSongs.contains(entry.i()) ? entry.j() : entry.i())))
                    .sortByKey(false)
                    .collect()
                    .stream()
                    .limit(recommendationsNum)
                    .map(entry -> reverseSongsLookup.get(entry._2()))
                    .collect(Collectors.toSet());
            if (userRecommendations.size() < recommendationsNum) {
                userRecommendations.addAll(mostPopularSongs.stream().limit(recommendationsNum - userRecommendations.size()).collect(Collectors.toSet()));
            }
            return new UserPlaylist(userPlaylist.getUser(), userRecommendations);
        }).collect(Collectors.toList());

        MongoSpark.save(jsc.parallelize(recommendations)
                .map(userPlaylist -> new Document(ImmutableMap.of("_id", userPlaylist.getUser(), "songs", userPlaylist.getSongs()))));
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public Integer getRecommendationsNum() {
        return recommendationsNum;
    }

    public void setRecommendationsNum(Integer recommendationsNum) {
        this.recommendationsNum = recommendationsNum;
    }

    public String getMongoInputUri() {
        return mongoInputUri;
    }

    public void setMongoInputUri(String mongoInputUri) {
        this.mongoInputUri = mongoInputUri;
    }

    public String getMongoOutputUri() {
        return mongoOutputUri;
    }

    public void setMongoOutputUri(String mongoOutputUri) {
        this.mongoOutputUri = mongoOutputUri;
    }

    public Integer getSongsLimit() {
        return songsLimit;
    }

    public void setSongsLimit(Integer songsLimit) {
        this.songsLimit = songsLimit;
    }

    public String getUsersCollectionName() {
        return usersCollectionName;
    }

    public void setUsersCollectionName(String usersCollectionName) {
        this.usersCollectionName = usersCollectionName;
    }

    public String getMostPopularSongsCollectionName() {
        return mostPopularSongsCollectionName;
    }

    public void setMostPopularSongsCollectionName(String mostPopularSongsCollectionName) {
        this.mostPopularSongsCollectionName = mostPopularSongsCollectionName;
    }

    public static final class Builder {
        private String appName = "itemBasedSparkModule";
        private Integer recommendationsNum = 500;
        private Integer songsLimit;
        private String mongoInputUri;
        private String usersCollectionName;
        private String mostPopularSongsCollectionName;
        private String mongoOutputUri;

        private Builder() {
        }

        public static Builder anItemBasedSparkModule() {
            return new Builder();
        }

        public Builder withAppName(String appName) {
            this.appName = appName;
            return this;
        }

        public Builder withRecommendationsNum(Integer recommendationsNum) {
            this.recommendationsNum = recommendationsNum;
            return this;
        }

        public Builder withSongsLimit(Integer songsLimit) {
            this.songsLimit = songsLimit;
            return this;
        }

        public Builder withMongoInputUri(String mongoInputUri) {
            this.mongoInputUri = mongoInputUri;
            return this;
        }

        public Builder withUsersCollectionName(String usersCollectionName) {
            this.usersCollectionName = usersCollectionName;
            return this;
        }

        public Builder withMostPopularSongsCollectionName(String mostPopularSongsCollectionName) {
            this.mostPopularSongsCollectionName = mostPopularSongsCollectionName;
            return this;
        }

        public Builder withMongoOutputUri(String mongoOutputUri) {
            this.mongoOutputUri = mongoOutputUri;
            return this;
        }

        public ItemBasedSparkModule build() {
            ItemBasedSparkModule itemBasedSparkModule = new ItemBasedSparkModule();
            itemBasedSparkModule.appName = this.appName;
            itemBasedSparkModule.recommendationsNum = this.recommendationsNum;
            itemBasedSparkModule.songsLimit = this.songsLimit;
            itemBasedSparkModule.mongoInputUri = this.mongoInputUri;
            itemBasedSparkModule.usersCollectionName = this.usersCollectionName;
            itemBasedSparkModule.mostPopularSongsCollectionName = this.mostPopularSongsCollectionName;
            itemBasedSparkModule.mongoOutputUri = this.mongoOutputUri;
            return itemBasedSparkModule;
        }
    }
}
