package app.musicmatch.component.alssparkmodule;

import app.musicmatch.component.alssparkmodule.model.User;
import app.musicmatch.component.alssparkmodule.model.UserSongRating;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.scalactic.Bool;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AlsSparkModule {

    private String appName = "alsSparkModule";
    private int alsIterationNum = 1;
    private int alsRank = 5;
    private boolean alsImplicitPrefs = false;
    private double alsAlpha = 1.0;
    private double alsLambda = 0.01;
    private int alsRecommendationsNum = 5;
    private String mongoInputUri;
    private String mongoOutputUri;

    public static void main(String[] args) throws JsonProcessingException {
        AlsSparkModule.Builder.anAlsSparkModule()
                .withAppName(args[0])
                .withAlsIterationNum(Integer.valueOf(args[1]))
                .withAlsRank(Integer.valueOf(args[2]))
                .withAlsImplicitPrefs(Boolean.valueOf(args[3]))
                .withAlsAlpha(Double.valueOf(args[4]))
                .withAlsLambda(Double.valueOf(args[5]))
                .withAlsRecommendationsNum(Integer.valueOf(args[6]))
                .withMongoInputUri(args[7])
                .withMongoOutputUri(args[8])
                .build()
                .runComputation();
    }

    private void runComputation() throws JsonProcessingException {
        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this));

        SparkSession sparkSession = SparkSession.builder()
                .appName(appName)
                .config("spark.mongodb.input.uri", mongoInputUri)
                .config("spark.mongodb.output.uri", mongoOutputUri)
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<Document> rdd = MongoSpark.load(jsc);

        JavaRDD<UserSongRating> userSongRating = rdd.flatMap(doc -> {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            User user = objectMapper.readValue(doc.toJson(), User.class);
            return user.getSongPlayCounts().stream()
                    .map(o -> new UserSongRating(user.getId(), o.getSongId(), 1.0f))
                    .collect(Collectors.toList())
                    .iterator();
        });

        Map<String, Long> userLookupMap = userSongRating
                .map(UserSongRating::getUserId)
                .distinct()
                .zipWithUniqueId()
                .collectAsMap();

        Map<Long, String> reverseUserLookupMap = userLookupMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        Map<String, Long> songLookupMap = userSongRating
                .map(UserSongRating::getSongId)
                .distinct()
                .zipWithUniqueId()
                .collectAsMap();

        Map<Long, String> reverseSongLookupMap = songLookupMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        JavaRDD<Rating> alsRatingRdd = userSongRating
                .map(r -> new Rating(userLookupMap.get(r.getUserId()).intValue(),
                        songLookupMap.get(r.getSongId()).intValue(),
                        r.getRating()));

        MatrixFactorizationModel model = new ALS()
                .setIterations(alsIterationNum)
                .setRank(alsRank)
                .setImplicitPrefs(alsImplicitPrefs)
                .setAlpha(alsAlpha)
                .setLambda(alsLambda)
                .run(alsRatingRdd);
        JavaRDD<Tuple2<Object, Rating[]>> recommendations = model
                .recommendProductsForUsers(alsRecommendationsNum)
                .toJavaRDD();


        MongoSpark.save(recommendations.map(tuple -> {
            Document doc = new Document();
            doc.put("_id", reverseUserLookupMap.get( ((Integer) tuple._1).longValue() ));
            doc.put("songs", Arrays.stream(tuple._2)
                    .map(i -> reverseSongLookupMap.get( ((Integer) i.product()).longValue() ))
                    .collect(Collectors.toList()));
            return doc;
        }));

    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public int getAlsIterationNum() {
        return alsIterationNum;
    }

    public void setAlsIterationNum(int alsIterationNum) {
        this.alsIterationNum = alsIterationNum;
    }

    public int getAlsRank() {
        return alsRank;
    }

    public void setAlsRank(int alsRank) {
        this.alsRank = alsRank;
    }

    public boolean isAlsImplicitPrefs() {
        return alsImplicitPrefs;
    }

    public void setAlsImplicitPrefs(boolean alsImplicitPrefs) {
        this.alsImplicitPrefs = alsImplicitPrefs;
    }

    public double getAlsAlpha() {
        return alsAlpha;
    }

    public void setAlsAlpha(double alsAlpha) {
        this.alsAlpha = alsAlpha;
    }

    public double getAlsLambda() {
        return alsLambda;
    }

    public void setAlsLambda(double alsLambda) {
        this.alsLambda = alsLambda;
    }

    public int getAlsRecommendationsNum() {
        return alsRecommendationsNum;
    }

    public void setAlsRecommendationsNum(int alsRecommendationsNum) {
        this.alsRecommendationsNum = alsRecommendationsNum;
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


    public static final class Builder {
        private String appName = "alsSparkModule";
        private int alsIterationNum = 1;
        private int alsRank = 5;
        private boolean alsImplicitPrefs = false;
        private double alsAlpha = 1.0;
        private double alsLambda = 0.01;
        private int alsRecommendationsNum = 5;
        private String mongoInputUri;
        private String mongoOutputUri;

        private Builder() {
        }

        public static Builder anAlsSparkModule() {
            return new Builder();
        }

        public Builder withAppName(String appName) {
            this.appName = appName;
            return this;
        }

        public Builder withAlsIterationNum(int alsIterationNum) {
            this.alsIterationNum = alsIterationNum;
            return this;
        }

        public Builder withAlsRank(int alsRank) {
            this.alsRank = alsRank;
            return this;
        }

        public Builder withAlsImplicitPrefs(boolean alsImplicitPrefs) {
            this.alsImplicitPrefs = alsImplicitPrefs;
            return this;
        }

        public Builder withAlsAlpha(double alsAlpha) {
            this.alsAlpha = alsAlpha;
            return this;
        }

        public Builder withAlsLambda(double alsLambda) {
            this.alsLambda = alsLambda;
            return this;
        }

        public Builder withAlsRecommendationsNum(int alsRecommendationsNum) {
            this.alsRecommendationsNum = alsRecommendationsNum;
            return this;
        }

        public Builder withMongoInputUri(String mongoInputUri) {
            this.mongoInputUri = mongoInputUri;
            return this;
        }

        public Builder withMongoOutputUri(String mongoOutputUri) {
            this.mongoOutputUri = mongoOutputUri;
            return this;
        }

        public AlsSparkModule build() {
            AlsSparkModule alsSparkModule = new AlsSparkModule();
            alsSparkModule.setAppName(appName);
            alsSparkModule.setAlsIterationNum(alsIterationNum);
            alsSparkModule.setAlsRank(alsRank);
            alsSparkModule.setAlsImplicitPrefs(alsImplicitPrefs);
            alsSparkModule.setAlsAlpha(alsAlpha);
            alsSparkModule.setAlsLambda(alsLambda);
            alsSparkModule.setAlsRecommendationsNum(alsRecommendationsNum);
            alsSparkModule.setMongoInputUri(mongoInputUri);
            alsSparkModule.setMongoOutputUri(mongoOutputUri);
            return alsSparkModule;
        }
    }
}
