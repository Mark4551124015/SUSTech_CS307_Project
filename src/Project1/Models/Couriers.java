package Project1.Models;

import Project1.FileManipulation;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

public class Couriers {
    protected int selfIncreasingNumber;
    protected ArrayList<Courier> couriers;

    public void initialize() {
        this.selfIncreasingNumber = 1;
        this.couriers = new ArrayList<Courier>();
    }

    public Courier[] select(Constraint<Courier> constraint) {
        ArrayList<Courier> result = new ArrayList<>();
        for (Courier courier : couriers) {
            if (constraint.check(courier)) {
                result.add(courier);
            }
        }
        return result.toArray(Courier[]::new);
    }

    public boolean insert(String name, String gender, Date birthday, String phoneNumber, int cityId) {
        // Check Foreign Key
        if (cityForeignKeyNotExist(cityId)) {
            return false;
        }

        Courier newCourier = new Courier(selfIncreasingNumber, name, gender, birthday, phoneNumber, cityId);
        selfIncreasingNumber++;
        couriers.add(newCourier);
        return true;
    }

    public boolean delete(Constraint<Courier> constraint) {
        Iterator<Courier> iterator = couriers.iterator();
        while (iterator.hasNext()) {
            Courier courier = iterator.next();
            if (constraint.check(courier)) {
                iterator.remove();
                return true;
            }
        }

        return false;
    }


    public int update(Constraint<Courier> constraint, Courier newModel) {
        int affectedDataCount = 0;

        for (Courier courier : couriers) {
            if (constraint.check(courier)) {

                // Check Foreign Key
                if (newModel.cityId >= 0 && cityForeignKeyNotExist(newModel.cityId)) {
                    continue;
                }

                courier.phoneNumber = newModel.phoneNumber != null ? newModel.phoneNumber : courier.phoneNumber;
                courier.birthday = newModel.birthday != null ? newModel.birthday : courier.birthday;
                courier.gender = newModel.gender != null ? newModel.gender : courier.gender;
                courier.name = newModel.name != null ? newModel.name : courier.name;


                affectedDataCount++;
            }
        }

        return affectedDataCount;
    }

    protected boolean cityForeignKeyNotExist(int cityId) {
        City[] cities = FileManipulation.getCities().select((city) -> city.cityId == cityId);
        return cities.length < 1;

    }


}
