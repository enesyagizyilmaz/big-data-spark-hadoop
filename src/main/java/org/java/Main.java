package org.java;

import com.google.common.collect.Iterators;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.java.Entity.Player;
import org.java.dto.PlayerDTO;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "src/main/resources/hadoop-common-2.2.0-bin-master/bin");

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSpark")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.WorldCupCollection")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.WorldCupCollection")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> rawData = sc.textFile("src/main/resources/WorldCupPlayers.csv");

        // Line Number - saving to MongoDB
        System.out.println("Line Number: " + rawData.count());
        MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoDatabase database = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("LineNumber");
        Document doc = new Document("lineCount", rawData.count());
        collection.insertOne(doc);

        JavaRDD<Player> playersRDD = rawData.map(new Function<String, Player>() {
            @Override
            public Player call(String line) throws Exception {
                String[] playersArray = line.split(",", -1);
                return new Player(playersArray[0], playersArray[1], playersArray[2], playersArray[3],
                        playersArray[4], playersArray[5], playersArray[6], playersArray[7], playersArray[8]);
            }
        });

        JavaRDD<Player> tur = playersRDD.filter(new Function<Player, Boolean>() {
            @Override
            public Boolean call(Player player) {
                return player.getTeam().equals("TUR");
            }
        });

        // Names of all players
        playersRDD.foreach(new VoidFunction<Player>() {
            @Override
            public void call(Player player) {
                // System.out.println(player.getPlayerName());
            }
        });

        // Which player played in how many games?
        JavaRDD<Player> messiRDD = playersRDD.filter(new Function<Player, Boolean>() {
            @Override
            public Boolean call(Player player) {
                return player.getPlayerName().equals("MESSI");
            }
        });

        JavaRDD<Player> rivaldoRDD = playersRDD.filter(new Function<Player, Boolean>() {
            @Override
            public Boolean call(Player player) {
                return player.getPlayerName().equals("RIVALDO");
            }
        });

        System.out.println("Messi played " + messiRDD.count() + " matches in all World Cups");
        System.out.println("Rivaldo played " + rivaldoRDD.count() + " matches in all World Cups");

        // Which player played in how many games in one list
        JavaPairRDD<String, String> mapRDD = tur.mapToPair(new PairFunction<Player, String, String>() {
            @Override
            public Tuple2<String, String> call(Player player) {
                return new Tuple2<>(player.getPlayerName(), player.getMatchId());
            }
        });

        JavaPairRDD<String, Iterable<String>> groupPlayer = mapRDD.groupByKey();

        JavaRDD<PlayerDTO> resultRDD = groupPlayer.map(new Function<Tuple2<String, Iterable<String>>, PlayerDTO>() {
            @Override
            public PlayerDTO call(Tuple2<String, Iterable<String>> array) {
                Iterator<String> iteratorRaw = array._2.iterator();
                int size = Iterators.size(iteratorRaw);
                return new PlayerDTO(array._1, size);
            }
        });

        resultRDD.foreach(new VoidFunction<PlayerDTO>() {
            @Override
            public void call(PlayerDTO playerDTO) {
                System.out.println(playerDTO.getPlayerName() + " " + playerDTO.getMatchCount());
            }
        });

        JavaRDD<Document> mongoRDD = resultRDD.map(new Function<PlayerDTO, Document>() {
            @Override
            public Document call(PlayerDTO playerDTO) {
                return new Document("PlayerName", playerDTO.getPlayerName())
                        .append("MatchCount", playerDTO.getMatchCount());
            }
        });

        MongoSpark.save(mongoRDD);

        // Messi's Goal Times - saving to MongoDB
        MongoCollection<Document> goalsOfMessiCollection = database.getCollection("goalsOfMessi");

        JavaRDD<String> messiGoalsRDD = playersRDD.filter(new Function<Player, Boolean>() {
            @Override
            public Boolean call(Player player) {
                return player.getPlayerName().equals("MESSI") && player.getEvent().contains("G");
            }
        }).map(new Function<Player, String>() {
            @Override
            public String call(Player player) {
                return player.getEvent();
            }
        });

        // Print Messi's goal events
        messiGoalsRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String event) {
                System.out.println("MESSI scored in event " + event);
            }
        });

        // Save Messi's goal events to MongoDB
        JavaRDD<Document> messiGoalsDocumentRDD = messiGoalsRDD.map(new Function<String, Document>() {
            @Override
            public Document call(String event) {
                return new Document("PlayerName", "MESSI")
                        .append("GoalEvent", event);
            }
        });

        // Convert RDD to List and insert all documents at once
        List<Document> messiGoalsDocuments = messiGoalsDocumentRDD.collect();
        goalsOfMessiCollection.insertMany(messiGoalsDocuments);

        // Print the saved documents
        System.out.println("Goals of Messi saved to MongoDB:");
        for (Document messiGoalDoc : messiGoalsDocuments) {
            System.out.println(messiGoalDoc.toJson());
        }

        // Brazilian Players' Goals - saving to MongoDB
        MongoCollection<Document> brazilianPlayerGoalsCollection = database.getCollection("BrazilianPlayerGoals");

        // Filter Brazilian players who scored goals
        JavaRDD<Player> brazilianPlayersGoalsRDD = playersRDD.filter(new Function<Player, Boolean>() {
            @Override
            public Boolean call(Player player) {
                return player.getTeam().equals("BRA") && player.getEvent().contains("G");
            }
        });

        // Map player names to their respective goals and create documents
        JavaPairRDD<String, Integer> brazilianPlayerGoalsRDD = brazilianPlayersGoalsRDD.mapToPair(new PairFunction<Player, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Player player) {
                return new Tuple2<>(player.getPlayerName(), 1);
            }
        });

        // Aggregate goals scored by each Brazilian player
        JavaPairRDD<String, Integer> aggregateBrazilianPlayerGoalsRDD = brazilianPlayerGoalsRDD.reduceByKey((goal1, goal2) -> goal1 + goal2);

        // Convert RDD to List and insert all documents at once
        List<Document> brazilianPlayerGoalsDocuments = aggregateBrazilianPlayerGoalsRDD.map(tuple -> new Document("PlayerName", tuple._1()).append("GoalsScored", tuple._2())).collect();

        brazilianPlayerGoalsCollection.insertMany(brazilianPlayerGoalsDocuments);

        System.out.println("Goals of Brazilian players saved to MongoDB:");
        for (Document brazilianPlayerGoalDoc : brazilianPlayerGoalsDocuments) {
            System.out.println(brazilianPlayerGoalDoc.toJson());
        }

        // Find players who scored exactly 3 goals in a single match
        JavaRDD<Player> playersWithThreeGoalsRDD = playersRDD.filter(new Function<Player, Boolean>() {
            @Override
            public Boolean call(Player player) {
                long goalCount = player.getEvent().chars().filter(ch -> ch == 'G').count();
                return goalCount == 3;
            }
        });

        // Print players who scored 3 goals in a single match
        playersWithThreeGoalsRDD.foreach(new VoidFunction<Player>() {
            @Override
            public void call(Player player) {
                System.out.println(player.getPlayerName() + " scored 3 goals in a single match");
            }
        });

        // Find players who scored 3 goals in a single match and save to MongoDB
        MongoCollection<Document> playersWithThreeGoalsCollection = database.getCollection("PlayersWithThreeGoals");

        JavaRDD<Document> playersWithThreeGoalsDocumentRDD = playersWithThreeGoalsRDD.map(new Function<Player, Document>() {
            @Override
            public Document call(Player player) {
                return new Document("PlayerName", player.getPlayerName())
                        .append("Event", player.getEvent());
            }
        });

        // Convert RDD to List and insert all documents at once
        List<Document> playersWithThreeGoalsDocuments = playersWithThreeGoalsDocumentRDD.collect();
        if (!playersWithThreeGoalsDocuments.isEmpty()) {
            playersWithThreeGoalsCollection.insertMany(playersWithThreeGoalsDocuments);
        }

        System.out.println("Players who scored 3 goals in a single match saved to MongoDB:");
        for (Document document : playersWithThreeGoalsDocuments) {
            System.out.println(document.toJson());
        }

        // Earliest 10 Goals
        JavaRDD<Player> goalsRDD = playersRDD.filter(new Function<Player, Boolean>() {
            @Override
            public Boolean call(Player player) {
                return player.getEvent().contains("G");
            }
        });

        // Parse the goal times and select the earliest 10
        JavaPairRDD<Integer, Player> goalsWithTimeRDD = goalsRDD.mapToPair(new PairFunction<Player, Integer, Player>() {
            @Override
            public Tuple2<Integer, Player> call(Player player) {
                // Extract the minute of the goal from the event string (assumes event format includes the minute)
                String event = player.getEvent();
                int goalTime = extractGoalTime(event);
                return new Tuple2<>(goalTime, player);
            }
        });

        List<Tuple2<Integer, Player>> earliestGoals = goalsWithTimeRDD.sortByKey().take(10);

        // Save earliest 10 goals to MongoDB
        MongoCollection<Document> earliestGoalsCollection = database.getCollection("EarliestGoals");

        List<Document> earliestGoalsDocuments = earliestGoals.stream().map(tuple -> new Document("Minute", tuple._1)
                .append("PlayerName", tuple._2.getPlayerName())
                .append("Team", tuple._2.getTeam())
                .append("Event", tuple._2.getEvent())
        ).collect(Collectors.toList());

        if (!earliestGoalsDocuments.isEmpty()) {
            earliestGoalsCollection.insertMany(earliestGoalsDocuments);
        }

        System.out.println("Earliest 10 goals saved to MongoDB:");
        for (Document earliestGoalDoc : earliestGoalsDocuments) {
            System.out.println(earliestGoalDoc.toJson());
        }

        sc.close();
    }

    /**
     * Extracts the goal time (minute) from an event string.
     *
     * @param event the event string
     * @return the goal time in minutes
     */
    private static int extractGoalTime(String event) {
        // Assuming the event string is like "G12", where "12" is the minute of the goal
        String minuteStr = event.replaceAll("[^0-9]", "");
        if (!minuteStr.isEmpty()) {
            try {
                return Integer.parseInt(minuteStr);
            } catch (NumberFormatException e) {
                return Integer.MAX_VALUE; // Handle invalid or non-numeric values
            }
        }
        return Integer.MAX_VALUE;
    }
}