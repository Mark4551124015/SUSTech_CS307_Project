import java.io.*;
public class Client {
    static int MAXRECORD = 10000;
    static int cnt = 0;

    public static void main(String[] args) throws Exception {
        File file = new File("test.log");

        Loader ld = new Loader();

        DatabaseManipulation dm = new DatabaseManipulation();
        dm.getConnection();
        dm.selectExportDetailByPortCity("上海");
//        dm.addFullRecords("coconut-7d2ee,coconut,359262146,北京,2016-04-03,姜友舒,女,3995-1637164230,45,2016-07-07,Detroit,井琦,女,5291-1436337968,30.0,香港,291530037.5907980,2016-05-05,Houston,165254139.7959270,2016-06-04,dabe1ba1,ISO Tank Container,划水号,唯品会,2016-07-07 14:09:47");
        dm.closeConnection();
//        ld.insert("data/shipment_records.csv", 10000);

    }
}
