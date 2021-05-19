package by.sanko.spark.two.entity.filter;

import by.sanko.spark.two.entity.StayType;
import by.sanko.spark.two.entity.main.Main;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.HashMap;

public class FilterByWeather implements FilterFunction<Row> {
    private static FilterByWeather instance = null;

    public static FilterByWeather getInstance(){
        if(instance == null){
            instance = new FilterByWeather();
        }
        return instance;
    }

    @Override
    public boolean call(Row row) throws Exception {
        Long hotelID = Long.parseLong(row.getString(19));
        String checkIN = row.getString(12);
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
