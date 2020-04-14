import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.util.Comparator;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestTrendsTweeter {

    private static Map<String, Integer> map = new HashMap<>();

    public static void main(String[] args) throws Exception {

        for (int j = 1; j <= 20; j++) {
            String filename ="TweetFile"+j+".txt";
            System.out.println("filename = " + filename);

            String data0 = readFileAsString("D:\\sil\\projectInput\\"+filename);
            String data = new String(data0.getBytes(), "UTF-8");
            String[] split = data.split("\\n");
            for (int i = 0; i < split.length; i++) {
                try {
                    JSONObject object = new JSONObject(split[i].toString().trim());
                    JSONObject user = object.getJSONObject("user");
                    String id_str = user.getString("id_str").trim();

                    if (map.containsKey(id_str)) {
                        int value = map.get(id_str);
                        map.put(id_str, value + 1);
                    } else {
                        map.put(id_str, 1);
                    }

                } catch (JSONException ex) {
                    ex.printStackTrace();
                    continue;
                }
            }
        }

        Map<String, Integer> reverseMap = sortMapDescendingOrder(map);

        WriteToFile(reverseMap);


    } // main

    private static void WriteToFile(Map<String, Integer> map) throws IOException {
        FileWriter writer = new FileWriter("MyFile.txt", false);

        for (String each : map.keySet()) {
            writer.write(each + "\t" + map.get(each));
            writer.write("\r\n");   // write new line
        }

        writer.close();
    }


    private  static Map<String, Integer> sortMapDescendingOrder( Map<String, Integer> unSortedMap){
//LinkedHashMap preserve the ordering of elements in which they are inserted
        LinkedHashMap<String, Integer> reverseSortedMap = new LinkedHashMap<>();

//Use Comparator.reverseOrder() for reverse ordering
        unSortedMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));

//        System.out.println("Reverse Sorted Map   : " + reverseSortedMap);
        return  reverseSortedMap;

    }


    public static String readFileAsString(String fileName) throws Exception {
        String data = "";
        data = new String(Files.readAllBytes(Paths.get(fileName)));
        return data;
    }

}
