import java.io.*;
public class Client {

    public static void main(String[] args) throws FileNotFoundException {
        File file = new File("test.log");
//        PrintStream ps = new PrintStream(file);
//        System.setOut(ps);
        try {
            insertFromFile("data/shipment_records.csv");
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
            DataManipulation dm = new DataFactory().createDataManipulation("database");
            int cnt = 0;
            line = br.readLine();
            while ((line = br.readLine()) != null) {
                System.out.println("正在插入第：" + ++cnt +" 条数据");
                dm.addFullRecords(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void insertRecord(String str) {

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

