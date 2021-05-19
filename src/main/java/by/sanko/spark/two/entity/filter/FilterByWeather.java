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
        return map != null && map.get(checkIN) != null && map.get(checkIN) > 0;
    }
}
