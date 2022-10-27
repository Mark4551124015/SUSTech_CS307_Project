package Project1.FileDB.Databases;

import Project1.FileDB.Databases.Models.Company;
import Project1.FileDB.Databases.Models.Constraint;

import java.util.ArrayList;

public class Companies extends BaseModel<Company> {
    public ArrayList<Company> companies;

    public void initialize(){
        this.companies = new ArrayList<>();
    }

    @Override
    public String getDBPath() {
        return "./FileDatabase/company.json";
    }

/*    public Company[] select(Constraint<Company> constraint) {
        ArrayList<Company> result = new ArrayList<>();
        for (Company company : companies) {
            if (constraint.check(company)) {
                result.add(company);
            }
        }

        return result.toArray(Company[]::new);
    }*/

    public boolean insert(String name) {
        if (select(company -> company.name.equals(name)).size() > 0) {
            return false;
        }
        Company newRecord = new Company(name);
        data.add(newRecord);
        return true;
    }
}
