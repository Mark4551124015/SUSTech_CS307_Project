package FileDB.Tables;

import FileDB.Tables.Models.Company;

import java.util.ArrayList;

public class Companies extends BaseTable<Company> {

    public void initialize(){
        this.data = new ArrayList<>();
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

    public Company insert(String name) {
        if (select(company -> company.name.equals(name)).size() > 0) {
            return null;
        }
        Company newRecord = new Company(name);
        data.add(newRecord);
        return newRecord;
    }
}
