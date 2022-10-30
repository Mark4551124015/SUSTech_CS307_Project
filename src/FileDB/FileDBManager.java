package FileDB;

import FileDB.Tables.*;
import FileDB.Tables.Models.City;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.util.Date;

public class FileDBManager {
    static Couriers couriers;
    static Cities cities;
    static Companies companies;
    static FileDBManager instance;

    static BaseModel[] Tables = new BaseModel[]{
            new Cities(),
            new Companies(),
            new Containers(),
            new Couriers(),
            new Ships(),
            new PortCities(),
            new Shippings(),
            new Shipments(),
            new ImportAndExports(),
            new DeliveryAndRetrievals(),
    };

    static Gson gson;


    public static void main(String[] args) throws IOException {
        // test insert
        FileDBManager fileDBManager = new FileDBManager();
        for (int i = 0; i < 1000; i++) {
            String cityName = "TestCity" + i;
            getCities().insert(cityName);
        }

        for (City city : getCities().select(city -> true)) {
            for (int i = 0; i < 100; i++) {
                Date date = new Date();
                getCouriers().insert(
                        "Courier " + i + " Of City " + city.name,
                        "男",
                        date,
                        "13888827812",
                        city.name,
                        null
                );
            }
        }

        fileDBManager.save();

    }

    public FileDBManager() throws IOException {
        reload();
    }


    public static Cities getCities() {
        return (Cities) Tables[0];
    }

    public static Companies getCompanies() {
        return (Companies) Tables[1];
    }

    public static Containers getContainers() {
        return (Containers) Tables[2];
    }

    public static Couriers getCouriers() {
        return (Couriers) Tables[3];
    }


    public static Ships getShips() {
        return (Ships) Tables[4];
    }


    public static PortCities getPortCities() {
        return (PortCities) Tables[5];
    }

    public static Shippings getShippings() {
        return (Shippings) Tables[6];
    }

    public static Shipments getShipments() {
        return (Shipments) Tables[7];
    }

    public static ImportAndExports getImportAndExports() {
        return (ImportAndExports) Tables[8];
    }

    public static DeliveryAndRetrievals getDeliveryAndRetrievals() {
        return (DeliveryAndRetrievals) Tables[9];
    }

    public static Gson getGson() {
        if (gson == null) {
            GsonBuilder gsonBuilder = new GsonBuilder();
            gsonBuilder.disableHtmlEscaping();
            gson = gsonBuilder.create();
        }
        return gson;

    }

    public static void save() throws IOException {
        for (BaseModel model : Tables) {
            model.save();
        }
    }

    public static FileDBManager getInstance() {
        if (instance == null) {
            try {
                instance = new FileDBManager();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return instance;
    }

    public static void reload() throws IOException {
        for (int i = 0; i < Tables.length; i++) {
            BaseModel model = Tables[i];
            if (!model.getDBFile().exists()) {
                model.initialize();
                model.save();
            }

            FileReader reader = new FileReader(model.getDBFile());
            Tables[i] = getGson().fromJson(reader, model.getClass());
            reader.close();
        }

    }

    public static void initializeAll() {
        for (BaseModel model : Tables) {
            model.initialize();
        }
        try {
            save();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void shuffleAll() {
        for (BaseModel model : Tables) {
            model.shuffle();
        }
    }
}
