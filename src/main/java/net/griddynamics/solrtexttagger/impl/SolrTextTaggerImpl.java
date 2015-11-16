package net.griddynamics.solrtexttagger.impl;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by fsoloviev on 11/5/15.
 */
public class SolrTextTaggerImpl {

      /*
    HttpSolrServer is thread-safe and if you are using the following constructor,
    you *MUST* re-use the same instance for all requests.  If instances are created on
    the fly, it can cause a connection leak. The recommended practice is to keep a
    static instance of HttpSolrServer per solr server url and share it for all requests.
    See https://issues.apache.org/jira/browse/SOLR-861 for more details
  */

    private static SolrClient solr;

    public static void main(String[] args) {
       // BasicConfigurator.configure();

        String url = "http://localhost:8983/solr/techproducts";
        solr = new HttpSolrClient(url);

        List<String> tagsList = tagFileStrings(args);
        try {
            write("tags.csv", tagsList);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static List<String> tagFileStrings(String[] fields) {
        String csvFile = "/Path/to/input/csv/file.csv";
        BufferedReader br = null;
        String line = "";
        List<String> tagsList = new ArrayList<>();
        try {
            br = new BufferedReader(new FileReader(csvFile));
            ExecutorService exService = Executors.newFixedThreadPool(5);
            while ((line = br.readLine()) != null) {

                    Future<String> tagFileString = exService.submit(new TagFileLineCallable(fields, line, solr));
                    try {
                        tagsList.add(tagFileString.get());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }

            }

            exService.shutdown();
            try {
                exService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
      } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return tagsList;
    }



    private static void write (String filename, List<String> strings) throws IOException{
        BufferedWriter outputWriter = null;
        outputWriter = new BufferedWriter(new FileWriter(filename));

        for(String string : strings) {
            outputWriter.write(string);
            outputWriter.newLine();
        }

        outputWriter.flush();
        outputWriter.close();
    }

}
