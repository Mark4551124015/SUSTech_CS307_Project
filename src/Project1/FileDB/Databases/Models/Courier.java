package Project1.FileDB.Databases.Models;

import java.util.Date;

public class Courier {
    public String name;
    public String gender;
    public Date birthday;
    public String phoneNumber;
    public String city;
    public String company;


    public Courier( String name, String gender, Date birthday, String phoneNumber, String city, String company) {
        this.name = name;
        this.gender = gender;
        this.birthday = birthday;
        this.phoneNumber = phoneNumber;
        this.city = city;
        this.company = company;
    }
}
