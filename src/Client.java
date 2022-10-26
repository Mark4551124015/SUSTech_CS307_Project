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
                long startTime=System.currentTimeMillis();
                dm.addFullRecords(line);
                long endTime=System.currentTimeMillis();
                System.out.println("Inserted data in line: " + ++cnt +", costs: "+(endTime-startTime)+"ms");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void insertRecord(String str) {

    }

}