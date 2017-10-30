import java.io.*;

public class TestActions {
    public static  void  main(String[] args) throws FileNotFoundException, IOException{
        File folder = new File("/home/ab/Documents/job_listings_text_small/");
        int count = 0;
        String findStr = "good";
        for (File file:
        folder.listFiles()) {
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String str = "";
            while((str = bufferedReader.readLine())!=null){
                if(str.contains(findStr)){
                    count++;
                   break;
                }
            }

        }
        System.out.println(count);

    }
}
