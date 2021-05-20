package by.sanko.spark.two.entity.main;

import by.sanko.spark.two.entity.HotelData;
import by.sanko.spark.two.entity.StayType;
import by.sanko.spark.two.parser.HotelParser;
import by.sanko.spark.two.parser.Parser;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static final HashMap<Long, HotelData> hotelData = new HashMap<>();
    private static final HashMap<Long, HashMap<String, Double>> hotelWeatherHM = new HashMap<>();
    private static final String HOTEL_WEATHER_JOINED  = "hotel-and-weather-joined-simple";
    private static FilterByWeather filter = new FilterByWeather();

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        invokeHotelData();
        Dataset<Row> data2017 = spark.read().format("csv")
                .option("header", "true")
                .option("delimiter", ";")
                .load("/user/hadoop/task1/expedia/new_ver/year=2016/*.csv");
        long distinctHotels =  data2017.selectExpr("CAST(hotel_id AS LONG)").distinct().count();
        System.out.println("DISTINCT HOTELS ARE " + distinctHotels);
        String[] strings = data2017.columns();
        System.out.println("Expedia rows are " + data2017.count());
        readWthData(spark,HOTEL_WEATHER_JOINED);
        int iterator = 0;
        for(String part : strings){
            System.out.println("Part is     " + part + " iterator is " + iterator);
            iterator++;
        }
        System.out.println("Sorted rows is :");
        data2017.orderBy("hotel_id").filter(filter)
                .join(calculateDays(data2017, spark),data2017.col("id").equalTo(calculateDays(data2017, spark).col("id"))).show();
    }

    private static Dataset<Row> calculateDays(Dataset<Row> dataset, SparkSession sparkSession){
        List<Row> data = dataset.selectExpr("CAST(id AS LONG)","CAST(srch_ci AS STRING)", "CAST(srch_co AS STRING)").collectAsList();
        List<Row> answer = new ArrayList<>();
        for(Row row : data) {
            Long id = Long.parseLong(row.getString(0));
            String checkIN = row.getString(1);
            String checkOUT = row.getString(2);
            int stayType = StayType.calculateType(checkIN, checkOUT).getStayID();
            answer.add(RowFactory.create(id,stayType));
        }
        List<org.apache.spark.sql.types.StructField> structs = new ArrayList<>();
        structs.add(DataTypes.createStructField("id", DataTypes.LongType,false));
        structs.add(DataTypes.createStructField("stay_type", DataTypes.IntegerType,false));
        StructType structures = DataTypes.createStructType(structs);
        Dataset<Row> answerAtAll = sparkSession.createDataFrame(answer, structures);
        answerAtAll.show();
        return answerAtAll;
    }

    private static void invokeHotelData(){
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host.docker.internal:9094")
                .option("subscribe", "hw-data-topic") //weathers-data-hash
                .load();
        spark.sparkContext().setLogLevel("ERROR");
        List<String> stringList = df.selectExpr("CAST(value AS STRING)").as(Encoders.STRING()).collectAsList();
        List<String> hotels = new ArrayList<>();
        for(String value : stringList){
            int index = value.indexOf('\n');
            String tmp = value.substring(index + 1, value.indexOf('\n', index +1));
            hotels.add(tmp);
        }
        for(String hotel : hotels){
            HotelData data = HotelParser.parseData(hotel);
            hotelData.put(data.getId(), data);
        }
        System.out.println("Hotel data is " + hotelData.size());
        long numAs = df.count();
        System.out.println("Lines at all: " + numAs);
    }

    private static void readWthData(SparkSession spark, String topicName){
        Dataset<Row> df = spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host.docker.internal:9094")
                .option("subscribe", topicName) //weathers-data-hash
                .load();
        df.selectExpr("CAST(value AS STRING)").show();
        String[] strings = df.columns();
        for(String part : strings){
            System.out.println("Part is     " + part);
        }
        df.selectExpr("CAST(value AS STRING)").foreach(row -> {
            String value = row.getString(0);
            int indexOfComma = value.indexOf(Parser.comma);
            Long hotelID = Long.parseLong(value.substring(0,indexOfComma));
            indexOfComma ++;
            int indexOfNextComma = value.indexOf(Parser.comma, indexOfComma);
            String date = value.substring(indexOfComma, indexOfNextComma);
            Double avg = Double.parseDouble(value.substring(indexOfNextComma+1));
            HashMap<String, Double> map = hotelWeatherHM.get(hotelID);
            if(map == null){
                map = new HashMap<>();
                map.put(date,avg);
                hotelWeatherHM.put(hotelID, map);
            }else{
                map.put(date,avg);
            }
        });
        System.out.println("Hotel key size is " + hotelWeatherHM.keySet().size());
        AtomicInteger i = new AtomicInteger();
        hotelWeatherHM.forEach((k,v)-> i.addAndGet(v.size()));
        System.out.println("All values are " + i);
    }
    public static class FilterByWeather implements FilterFunction<Row> {

        @Override
        public boolean call(Row row) throws Exception {
            Long hotelID = Long.parseLong(row.getString(19));
            String checkIN = row.getString(12);
            System.out.println(hotelID + " date is " + checkIN);
            HashMap<String, Double> map = Main.hotelWeatherHM.get(hotelID);
            boolean firstValue = map != null;
            boolean secondValue = false;
            boolean thirdValue = false;
            if(firstValue){
                secondValue = map.get(checkIN) != null;
                if(secondValue){
                    thirdValue = map.get(checkIN) > 0;
                }
            }
            boolean anwser = firstValue && secondValue && thirdValue;
            return anwser;
        }
    }
}
