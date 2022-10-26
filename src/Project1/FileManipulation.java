package Project1;

import Project1.Models.Cities;
import Project1.Models.City;
import Project1.Models.Courier;
import Project1.Models.Couriers;
import com.google.gson.Gson;

import java.io.*;
import java.util.Date;

public class FileManipulation {
    static String CityDBPath = "./FileDatabase/city.json";
    static String CourierDBPath = "./FileDatabase/courier.json";
    static File CityDBFile = new File(CityDBPath);
    static File CourierDBFile = new File(CourierDBPath);

    static Couriers couriers;
    static Cities cities;

    Gson gson = new Gson();


    public static void main(String[] args) throws IOException {
        // test insert
        FileManipulation fileManipulation = new FileManipulation();
        for (int i = 0; i < 1000; i++) {
            String cityName = "TestCity" + i;
            getCities().insert(cityName);
        }

        for (City city : getCities().select(city -> true)) {
            for (int i = 0; i < 100; i++) {
                Date date = new Date();
                getCouriers().insert(
                        "Courier Of City " + city.name,
                        "男",
                        date,
                        "13888827812",
                        city.cityId
                );
            }
        }

        fileManipulation.save();

    }

    public FileManipulation() throws IOException {

        if (!CityDBFile.exists()) {
            cities = new Cities();
            cities.initialize();
            FileWriter writer = new FileWriter(CityDBFile);
            writer.write(gson.toJson(cities));
            writer.close();
        }

        if (!CourierDBFile.exists()) {
            couriers = new Couriers();
            couriers.initialize();
            FileWriter writer = new FileWriter(CourierDBFile);
            writer.write(gson.toJson(couriers));
            writer.close();
        }

        FileReader reader = new FileReader(CityDBFile);
        cities = gson.fromJson(reader, Cities.class);
        reader.close();

        reader = new FileReader(CourierDBFile);
        couriers = gson.fromJson(reader, Couriers.class);
        reader.close();
    }


    public static Couriers getCouriers() {
        return couriers;
    }

    public static Cities getCities() {
        return cities;
    }

    public void save() throws IOException {

        FileWriter writer = new FileWriter(CityDBFile);
        writer.write(gson.toJson(cities));
        writer.close();

        writer = new FileWriter(CourierDBFile);
        writer.write(gson.toJson(couriers));
        writer.close();
    }
}
