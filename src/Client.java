import java.io.*;
public class Client {
    static int MAXRECORD = 10000;
    static int cnt = 0;

    public static void main(String[] args) throws Exception {
        File file = new File("test.log");

        Loader ld = new Loader();

        DatabaseManipulation dm = new DatabaseManipulation();
        dm.getConnection();
//        dm.selectExportDetailByPortCity("上海");
        ld.loadFromFile("data/shipment_records.csv", 10000);
        dm.selectShipmentByName("coconut-7d2ee");
        dm.closeConnection();
    }
}
