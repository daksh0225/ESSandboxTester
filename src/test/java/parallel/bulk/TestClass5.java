package parallel.bulk;

import com.example.estester.Application;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestClass5 {
    private static StringBuilder content;
    private static String resource = "/Users/daksh0225/Intern/Ex_Files_Elasticsearch_EssT/Exercise Files/data/accounts.json";
    private static String sandboxId = null;

    @BeforeClass
    public static void loadContent() {
        File file = new File(resource);
        content = new StringBuilder();
        try(InputStream in = new FileInputStream(file)){
            int c;
            while ((c = in.read()) != -1) {
                content.append((char)c);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    public static void getSandbox(){
        try {
            URL url = new URL("http://localhost:9200/_sandbox/get");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.connect();

            int code = con.getResponseCode();
            if(code == 200){
                System.out.println("Sandbox acquired");
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();
                JSONObject jsonObject = new JSONObject(content.toString());
                String sb = jsonObject.getString("sandboxId").toString();
                sandboxId = sb;
                System.out.println(sandboxId);
            }
            else
            {
                System.out.println("Error requesting sandbox");
            }
            con.disconnect();
        }
        catch(Exception e){
            System.err.println(e.getMessage());
        }
    }

    @AfterClass
    public static void setup(){
        try {
            URL url = new URL("http://localhost:9200/companydatabase");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("DELETE");
            con.setRequestProperty("Sandbox", sandboxId);
            con.connect();

            int code = con.getResponseCode();
            if(code == 200){
                System.out.println("Index deleted successfully");
            }
            else{
                System.out.println("Index doesn't exist");
            }
            con.disconnect();
        }
        catch(Exception e){
            System.err.println(e.getMessage());
        }
    }

    @Test
    public void testBulkRequest(){
        int code = Application.bulkRequest(6, content.toString(), sandboxId);
//        System.out.println(code);
        assertEquals(200, code);
    }

    @Test
    public void testSearching() throws InterruptedException {
        Thread.sleep(1000);
        String index = "companydatabase";
        String queryPayload = "{\n  \"timeout\": \"100s\", \"query\":\n  {\n    \"match\": {\"state\": \"CA\"}\n  }\n}";
        int val = Application.searchIndex(index, queryPayload, sandboxId);
//        System.out.println("5: " + val);
        assertEquals(17, val);
    }

    @Test
    public void testUpdateRequest() throws InterruptedException {
        Thread.sleep(1000);
        String index = "companydatabase";
        Integer docId = 799;
        String queryPayload = "{\n \"script\": {\n \"source\": \"ctx._source.state = params.state\", \"lang\": \"painless\", \"params\": {\n \"state\": \"IL\"}}}";
        int code = Application.updateRequest(6, index, queryPayload, docId, sandboxId);
//        System.out.println("5: " + code);
        assertEquals(200, code);
    }

    @Test
    public void testZ() throws InterruptedException {
        Thread.sleep(1000);
        String index = "companydatabase";
        String queryPayload = "{\n  \"timeout\": \"100s\", \"query\":\n  {\n    \"match\": {\"state\": \"CA\"}\n  }\n}";
        int val = Application.searchIndex(index, queryPayload, sandboxId);
//        System.out.println("5: " + val);
        assertEquals(16, val);
    }
}
