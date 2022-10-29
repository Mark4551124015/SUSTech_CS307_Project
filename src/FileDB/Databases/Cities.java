package FileDB.Databases;

import FileDB.Databases.Models.City;
import FileDB.Databases.Models.Constraint;

import java.util.ArrayList;

public class Cities extends BaseModel<City> {


    public void initialize() {
        data = new ArrayList<>();
    }

    @Override
    public String getDBPath() {
        return "./FileDatabase/city.json";
    }

/*    public City[] select(Constraint<City> constraint) {
        ArrayList<City> result = new ArrayList<City>();
        for (City city : cities) {
            if (constraint.check(city)) {
                result.add(city);
            }
        }

        return result.toArray(City[]::new);
    }*/

    public City insert(String cityName) {
        if (select(city -> city.name.equals(cityName)).size() > 0) {
            return null;
        }

        City newCity = new City(cityName);
        data.add(newCity);
        return newCity;
    }

/*    public int delete(Constraint<City> constraint) {
        int affectedLines = 0;

        Iterator<City> iterator = data.iterator();
        while (iterator.hasNext()) {
            City city = iterator.next();
            if (constraint.check(city)) {
                iterator.remove();
                affectedLines++;
            }
        }

        return affectedLines;
    }*/

    public int update(Constraint<City> constraint, City newModel) {
        int affectedLines = 0;

        for (City city : data) {
            if (constraint.check(city)) {
                city.name = newModel.name != null ? newModel.name : city.name;
                affectedLines++;
            }
        }

        return affectedLines;
    }
}
