package com.example.estester;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Application {
    private static final int MYTHREADS = 10;

    public static void deleteIndex(String index){
        try {
            URL url = new URL("http://localhost:9200/" + index);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("DELETE");
            con.connect();

            int code = con.getResponseCode();
            if(code == 404){
                System.out.println("Index doesn't exist");
            }
            else if(code == 200){
                System.out.println("Index deleted successfully");
            }
            con.disconnect();
        }
        catch(Exception e){
            System.err.println(e.getMessage());
        }
    }

    public static int searchIndex(String index, String payload, String sandboxId){
        int val = -1;
        try {

            URL url = new URL("http://localhost:9200/" + index + "/_search");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            con.setRequestProperty("Content-Type", "application/json");
            if(sandboxId != null)
                con.setRequestProperty("Sandbox", sandboxId);
//            System.out.println(payload);
            byte[] out = payload.getBytes(StandardCharsets.UTF_8);

            OutputStream stream = con.getOutputStream();
            stream.write(out);
            con.connect();

            int code = con.getResponseCode();
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
//            System.out.println(code);
            JSONObject jsonObject = new JSONObject(content.toString());
            JSONObject hits = jsonObject.getJSONObject("hits").getJSONObject("total");
//            System.out.println(jsonObject.toString(4));
//            System.out.println(hits.getInt("value"));
            val = hits.getInt("value");
            con.disconnect();
        }
        catch (Exception e){
            System.err.println(e.getMessage());
        }
        return val;
    }

    public static int putDocument(int id, String url, String payload, String requestType, String sandboxId){
        int code = 404;
        try{
            URL siteURL = new URL(url);
            HttpURLConnection con = (HttpURLConnection) siteURL.openConnection();
            con.setRequestMethod(requestType);
            con.setRequestProperty("Content-Type", "application/json");
            if(sandboxId != null)
                con.setRequestProperty("Sandbox", sandboxId);
            con.setDoOutput(true);
            byte[] out = payload.getBytes(StandardCharsets.UTF_8);

            OutputStream stream = con.getOutputStream();
            stream.write(out);
            con.connect();

            code = con.getResponseCode();
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer content = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
//            System.out.println(code);
            JSONObject jsonObject = new JSONObject(content.toString());
//            System.out.println(id + ": "+jsonObject.toString(4));
            con.disconnect();
        }
        catch(Exception e){
            System.err.println(e.getMessage());
        }
        return code;
    }

    public static int bulkRequest(int id, String payload, String sandboxId){
        return bulkRequest(id, payload, sandboxId, null);
    }

    public static int bulkRequest(int id, String payload, String sandboxId, String index){
        int code = 404;
        try {
            URL siteURL = new URL("http://localhost:9200/"+index+(index == null ? "" : "/")+"_bulk?refresh");
            HttpURLConnection con = (HttpURLConnection) siteURL.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/x-ndjson");
            if(sandboxId != null)
                con.setRequestProperty("Sandbox", sandboxId);
            con.setDoOutput(true);

            OutputStream stream = con.getOutputStream();
            byte[] out = payload.getBytes(StandardCharsets.UTF_8);
            stream.write(out);

            con.connect();

            code = con.getResponseCode();
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer content = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
//            System.out.println(code);
            JSONObject jsonObject = new JSONObject(content.toString());
//            System.out.println(id + ": "+jsonObject.toString(4));
            con.disconnect();
        }
        catch(Exception e){
            System.err.println(e.getMessage());
        }
        return code;
    }

    public static int updateRequest(int id, String index, String payload, Integer docId, String sandboxId){
        int code = 404;
        try {
            URL siteURL = new URL("http://localhost:9200/"+index+"/_update/"+docId.toString()+"?refresh");
            HttpURLConnection con = (HttpURLConnection) siteURL.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/x-ndjson");
            if(sandboxId != null)
                con.setRequestProperty("Sandbox", sandboxId);
            con.setDoOutput(true);

            OutputStream stream = con.getOutputStream();
            byte[] out = payload.getBytes(StandardCharsets.UTF_8);
            stream.write(out);

            con.connect();

            code = con.getResponseCode();
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer content = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
//            System.out.println(code);
            JSONObject jsonObject = new JSONObject(content.toString());
//            System.out.println(id + ": "+jsonObject.toString(4));
            con.disconnect();
        }
        catch(Exception e){
            System.err.println(e.getMessage());
        }
        return code;
    }

    public static int updateByQueryRequest(int id, String index, String payload, String sandboxId){
        int code = 404;
        try {
            URL siteURL = new URL("http://localhost:9200/"+index+"/_update_by_query?refresh");
            HttpURLConnection con = (HttpURLConnection) siteURL.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/x-ndjson");
            if(sandboxId != null)
                con.setRequestProperty("Sandbox", sandboxId);
            con.setDoOutput(true);

            OutputStream stream = con.getOutputStream();
            byte[] out = payload.getBytes(StandardCharsets.UTF_8);
            stream.write(out);

            con.connect();

            code = con.getResponseCode();
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer content = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
//            System.out.println(code);
            JSONObject jsonObject = new JSONObject(content.toString());
//            System.out.println(id + ": "+jsonObject.toString(4));
            con.disconnect();
        }
        catch(Exception e){
            System.err.println(e.getMessage());
        }
        return code;
    }

    public static void main(String args[]) throws IOException {
        String url = "http://localhost:9200";
        String index = "daksh0225";
        deleteIndex(index);
        String types[] = {"table", "chair", "fan", "table", "car"};
        String companies[] = {"Havells", "Woodtech", "Havells", "Pigeon", "Tata"};
        ExecutorService executorService = Executors.newFixedThreadPool(MYTHREADS);

        for(int i=0;i<5;i++){
            String payload = "{\"type\":\"" + types[i] + "\", \"company\":\"" + companies[i]+"\"}";
            Runnable worker = new MyRunnable(i, url+"/"+index+"/_doc", payload, "POST", index);
            executorService.execute(worker);
        }
        executorService.shutdown();
        while (!executorService.isTerminated()){}
        System.out.println("Finished all threads");
    }

    public static class MyRunnable implements Runnable{
        private final int id;
        private final String url;
        private final String payload;
        private final String requestType;
        private final String index;

        public MyRunnable(int id, String url, String payload, String requestType, String index){
            this.id = id;
            this.url = url;
            this.payload = payload;
            this.requestType = requestType;
            this.index = index;
        }

        public void run(){
            int code = putDocument(id, url, payload, requestType, null);

            if(code == 201) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                JSONObject doc = new JSONObject(payload);
                String queryPayload = "{\n  \"query\":\n  {\n    \"match\": {\"type\": \"" + doc.getString("type") + "\"}\n  }\n}";
                System.out.println(id + ": " + searchIndex(index, queryPayload, null));
            }
        }

        public int runTest(){
            int code = putDocument(id, url, payload, requestType, null);
            int val = -1;
            if(code == 201) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                JSONObject doc = new JSONObject(payload);
                String queryPayload = "{\n  \"query\":\n  {\n    \"match\": {\"type\": \"" + doc.getString("type") + "\"}\n  }\n}";
                val = searchIndex(index, queryPayload, null);
                System.out.println(id + ": " + val);
            }
            else{
                System.out.println("putDocument failed");
            }
            return val;
        }
    }
}
