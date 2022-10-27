import java.io.*;
public class Client {
    static int MAXRECORD = 10000;
    static int cnt = 0;
    public static void main(String[] args) throws FileNotFoundException {
        File file = new File("test.log");

        Loader ld = new Loader();
        ld.getConnection();
        ld.insert("data/shipment_records.csv",50000);
    }

}