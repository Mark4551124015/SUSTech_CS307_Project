package Project1.FileDB.Databases.Models;

import java.util.Date;

public class Courier {
    public int courierId;
    public String name;
    public String gender;
    public Date birthday;
    public String phoneNumber;
    public String city;


    public Courier(int courierId, String name, String gender, Date birthday, String phoneNumber, String city) {
        this.courierId = courierId;
        this.name = name;
        this.gender = gender;
        this.birthday = birthday;
        this.phoneNumber = phoneNumber;
        this.city = city;
    }
}
