package FileDB.Tables.Models;

public interface Constraint<T> {
    boolean check(T model);

}
