package Project1.Models;

import java.util.ArrayList;
import java.util.Iterator;

public class Cities {
    protected int selfIncreasingNumber;
    protected ArrayList<City> cities;

    public void initialize() {
        selfIncreasingNumber = 1;
        cities = new ArrayList<City>();
    }

    public City[] select(Constraint<City> constraint) {
        ArrayList<City> result = new ArrayList<City>();
        for (City city : cities) {
            if (constraint.check(city)) {
                result.add(city);
            }
        }

        return result.toArray(City[]::new);
    }

    public boolean insert(String cityName) {
        City newCity = new City(selfIncreasingNumber, cityName);
        selfIncreasingNumber++;
        cities.add(newCity);
        return true;
    }

    public int delete(Constraint<City> constraint) {
        int affectedLines = 0;

        Iterator<City> iterator = cities.iterator();
        while (iterator.hasNext()) {
            City city = iterator.next();
            if (constraint.check(city)) {
                iterator.remove();
                affectedLines++;
            }
        }

        return affectedLines;
    }

    public int update(Constraint<City> constraint, City newModel) {
        int affectedLines = 0;

        for (City city : cities) {
            if (constraint.check(city)) {
                city.name = newModel.name != null ? newModel.name : city.name;
                affectedLines++;
            }
        }

        return affectedLines;
    }
}
