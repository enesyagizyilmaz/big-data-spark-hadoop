package org.java;

import com.google.common.collect.Iterators;
import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.java.Entity.Player;
import org.java.dto.PlayerDTO;
import scala.Tuple2;

import java.util.Iterator;

public class Main
{
    public static void main(String[] args)
    {
        System.setProperty("hadoop.home.dir", "src/main/resources/hadoop-common-2.2.0-bin-master/bin");

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSpark")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.WordCupCollection")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.WordCupCollection")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> Raw_Data = sc.textFile("src/main/resources/WorldCupPlayers.csv");

        //Line Number
        System.out.println("Line Number: " + Raw_Data.count());

        JavaRDD<Player> playersRDD = Raw_Data.map(new Function<String, Player>()
        {
            @Override
            public Player call(String line) throws Exception
            {
                String[] playersArray = line.split(",", -1);
                return new Player(playersArray[0], playersArray[1], playersArray[2], playersArray[3],
                        playersArray[4], playersArray[5], playersArray[6], playersArray[7], playersArray[8]);
            }
        });

        JavaRDD<Player> tur = playersRDD.filter(new Function<Player, Boolean>()
        {
            @Override
            public Boolean call(Player player) throws Exception
            {
                return player.getTeam().equals("TUR");
            }
        });

        //Names of all players
        playersRDD.foreach(new VoidFunction<Player>()
        {
            @Override
            public void call(Player player) throws Exception
            {
                System.out.println(player.getPlayerName());
            }
        });

        //Which player played in how many games?
        JavaRDD<Player> messiRDD = playersRDD.filter(new Function<Player, Boolean>()
        {
            @Override
            public Boolean call(Player player) throws Exception
            {
                return player.getPlayerName().equals("MESSI");
            }
        });

        JavaRDD<Player> rivaldoRDD = playersRDD.filter(new Function<Player, Boolean>()
        {
            @Override
            public Boolean call(Player player) throws Exception
            {
                return player.getPlayerName().equals("RIVALDO");
            }
        });

        System.out.println("Messi played " + messiRDD.count() + " matches in all World Cup's");
        System.out.println("Rivaldo played " + rivaldoRDD.count() + " matches in all World Cup's");

        //Which player played in how many games in 1 list
        JavaPairRDD<String, String> mapRDD = tur.mapToPair(new PairFunction<Player, String, String>()
        {
            @Override
            public Tuple2<String, String> call(Player player) throws Exception
            {
                return new Tuple2<String, String>(player.getPlayerName(), player.getMatchId());
            }
        });

        JavaPairRDD<String, Iterable<String>> groupPlayer = mapRDD.groupByKey();

        JavaRDD<PlayerDTO> resultRDD = groupPlayer.map(new Function<Tuple2<String, Iterable<String>>, PlayerDTO>()
        {
            @Override
            public PlayerDTO call(Tuple2<String, Iterable<String>> array) throws Exception
            {
                Iterator<String> iteratorRaw = array._2.iterator();
                int size = Iterators.size(iteratorRaw);
                return new PlayerDTO(array._1, size);
            }
        });

        resultRDD.foreach(new VoidFunction<PlayerDTO>()
        {
            @Override
            public void call(PlayerDTO playerDTO) throws Exception
            {
                System.out.println(playerDTO.getPlayerName() + " " + playerDTO.getMatchCount());
            }
        });

        JavaRDD<Document> MongoRDD = resultRDD.map(new Function<PlayerDTO, Document>() {
            @Override
            public Document call(PlayerDTO playerDTO) throws Exception {
                // Document nesnesi kullanarak JSON oluşturma
                Document doc = new Document("PlayerName", playerDTO.getPlayerName())
                        .append("MatchCount", playerDTO.getMatchCount());
                return doc;
            }
        });



        MongoSpark.save(MongoRDD);
    }
}