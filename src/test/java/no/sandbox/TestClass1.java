package no.sandbox;

import com.example.estester.Application;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.Assert.assertEquals;

public class TestClass1 {

    @BeforeClass
    @AfterClass
    public static void setup(){
        try {
            URL url = new URL("http://localhost:9200/daksh0225");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("DELETE");
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
    public void testDocumentPostRequest(){
        String url = "http://localhost:9200/daksh0225/_doc";
        String payload = "{\"type\": \"table\", \"company\": \"woodtech1\"}";
        int code = Application.putDocument(1, url, payload, "POST", null);
        System.out.println("1: " + code);
        assertEquals(201, code);
    }

    @Test
    public void testSearching() throws InterruptedException {
        Thread.sleep(1000);
        String payload = "{\"type\": \"table\", \"company\": \"woodtech1\"}";
        String index = "daksh0225";
        JSONObject doc = new JSONObject(payload);
        String queryPayload = "{\n  \"query\":\n  {\n    \"match\": {\"type\": \"" + doc.getString("type") + "\"}\n  }\n}";
        int val = Application.searchIndex(index, queryPayload, null);
        System.out.println("1: " + val);
        assertEquals(1, val);
    }
}
