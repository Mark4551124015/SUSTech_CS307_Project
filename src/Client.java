import java.io.*;
import java.util.ArrayList;

public class Client {

    public static void main(String[] args) {
        try {
            DataManipulation dm = new DataFactory().createDataManipulation("database");
            dm.addOneCompany("2;唯品会");

//            System.out.println(dm.allContinentNames());
//            System.out.println(dm.continentsWithCountryCount());
//            System.out.println(dm.FullInformationOfMoviesRuntime(65, 75));
//            System.out.println(dm.findMovieById(10));
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }



    }

    public static void insertFromFile(String filePath) {
        File csv = new File(filePath);
        csv.setReadable(true);
        csv.setWritable(true);
        InputStreamReader isr = null;
        BufferedReader br = null;
        try {
            isr = new InputStreamReader(new FileInputStream(csv), "UTF-8");
            br = new BufferedReader(isr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String line = "";

        try {
            int cnt = 0;
            while ((line = br.readLine()) != null) {
                System.out.println(line);

            }
            System.out.println("正在插入第：" + cnt +" 条数据");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void insertRecord(String str) {
        String[] Info = str.split(",");
        //get Information
        String ItemName = Info[0];
        String ItemType = Info[1];
        String ItemPrice = Info[2];
        String RetrievalCity = Info[3];
        String RetrievalStartTime = Info[4];
        String RetrievalCourier = Info[5];
        String RetrievalCourierGender = Info[6];
        String RetrievalCourierPhoneNumber = Info[7];
        String RetrievalCourierAge = Info[8];
        String DeliveryFinishTime = Info[9];
        String DeliveryCity = Info[10];
        String DeliveryCourier = Info[11];
        String DeliveryCourierGender = Info[12];
        String DeliveryCourierPhoneNumber = Info[13];
        String DeliveryCourierAge = Info[14];
        String ItemExportCity = Info[15];
        String ItemExportTax = Info[16];
        String ItemExportTime = Info[17];
        String ItemImportCity = Info[18];
        String ItemImportTax = Info[19];
        String ItemImportTime = Info[20];
        String ContainerCode = Info[21];
        String ContainerType = Info[22];
        String ShipName = Info[23];
        String CompanyName = Info[24];
        String LogTime = Info[25];
        // Order to insert records
        // city -> port city -> company -> courier -> ship -> container ->
        // retrieval and delivery -> import export detail -> ship detail ->
        // shipping -> shipment


    }

}

class login_info {
    private String host = "localhost";
    private String dbname = "cs307";
    private String user = "mark455";
    private String pwd = "314159";
    private String port = "5432";
    login_info(String host, String dbname, String username, String password, String port) {
        this.host = host;
        this.dbname = dbname;
        this.user = username;
        this.pwd = password;
        this.port = port;
    }
    public static String secret (String value, char secret) {
        byte[] bt=value.getBytes();
        for(int i=0;i < bt.length; i++)  {
            bt[i]=(byte)(bt[i]^(int)secret);
        }
        String newResult=new String(bt,0,bt.length);
        return newResult;
    }
}

