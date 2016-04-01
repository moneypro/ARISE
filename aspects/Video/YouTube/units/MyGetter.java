import controllers.wrapper.sourceWrapper.interfaces.Getter;

import java.lang.String;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONObject;
import util.Constants;
import util.MyHTTP;

// Headless Browser imports
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.*;
import net.sf.json.JSON;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.lang.Iterable;
import java.util.List;

import java.util.ArrayList;
package com.google.api.services.samples.youtube.cmdline.data;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.samples.youtube.cmdline.Auth;
import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.model.ResourceId;
import com.google.api.services.youtube.model.SearchListResponse;
import com.google.api.services.youtube.model.SearchResult;
import com.google.api.services.youtube.model.Thumbnail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

    /**
     * Define a global variable that identifies the name of a file that
     * contains the developer's API key.
     */
    private static final String PROPERTIES_FILENAME = "youtube.properties";

    private static final long NUMBER_OF_VIDEOS_RETURNED = 25;

    /**
     * Define a global instance of a Youtube object, which will be used
     * to make YouTube Data API requests.
     */
    private static YouTube youtube;

    /**
     * Initialize a YouTube object to search for videos on YouTube. Then
     * display the name and thumbnail image of each video in the result set.
     *
     * @param args command line args.
     */
    public static void main(String[] args) {

    }

    /*
     * Prompt the user to enter a query term and return the user-specified term.
     */


    /*
     * Prints out all results in the Iterator. For each result, print the
     * title, video ID, and thumbnail.
     *
     * @param iteratorSearchResults Iterator of SearchResults to print
     *
     * @param query Search query (String)
     */

    }

public class MyGetter implements Getter {

    public Object getResult(JSONObject searchConditions) {
        // Read the developer key from the properties file.
        Properties properties = new Properties();
        try {
            InputStream in = Search.class.getResourceAsStream("/" + PROPERTIES_FILENAME);
            properties.load(in);

        } catch (IOException e) {
            System.err.println("There was an error reading " + PROPERTIES_FILENAME + ": " + e.getCause()
                    + " : " + e.getMessage());
            System.exit(1);
        }

        try {
            // This object is used to make YouTube Data API requests. The last
            // argument is required, but since we don't need anything
            // initialized when the HttpRequest is initialized, we override
            // the interface and provide a no-op function.
            youtube = new YouTube.Builder(Auth.HTTP_TRANSPORT, Auth.JSON_FACTORY, new HttpRequestInitializer() {
                public void initialize(HttpRequest request) throws IOException {
                }
            }).setApplicationName("youtube-cmdline-search-sample").build();

            // Prompt the user to enter a query term.
            String queryTerm = searchConditions.getString("fullName")+","+ searchConditions.getString("affiliation");

            // Define the API request for retrieving search results.
            YouTube.Search.List search = youtube.search().list("id,snippet");

            // Set your developer key from the Google Developers Console for
            // non-authenticated requests. See:
            // https://console.developers.google.com/
            String apiKey = properties.getProperty("youtube.apikey");
            search.setKey("AIzaSyCnqiqaoPyvrnzpdRAszXhzRS2RfnmWRhw");
            search.setQ(queryTerm);

            // Restrict the search results to only include videos. See:
            // https://developers.google.com/youtube/v3/docs/search/list#type
            search.setType("video");

            // To increase efficiency, only retrieve the fields that the
            // application uses.
            search.setFields("items(id/kind,id/videoId,snippet/title,snippet/thumbnails/default/url)");
            search.setMaxResults(NUMBER_OF_VIDEOS_RETURNED);

            // Call the API and print results.
            SearchListResponse searchResponse = search.execute();
            List<SearchResult> searchResultList = searchResponse.getItems();
            return searchResultList;
        } catch (GoogleJsonResponseException e) {
            System.err.println("There was a service error: " + e.getDetails().getCode() + " : "
                    + e.getDetails().getMessage());
        } catch (IOException e) {
            System.err.println("There was an IO error: " + e.getCause() + " : " + e.getMessage());
        } catch (Throwable t) {
            t.printStackTrace();
        }
        // turn off htmlunit warnings

            return (JSON)answer;

        }
        catch(Exception e){

            System.out.println("Could not find anything on 'DBLP' for the Search Query");
            System.out.println("ERROR REPORT: " + e.toString());
        }

        // Failure Case
        return null;
    }


    /**
     * This method checks if in the result section of dblp an exact author exists or not. The dblp
     * website depending on the search query finds correct related authors and displays them on the top.
     * It showcases exact author match and then similar author match. If the exact author match exists then we would like
     * to search the information on that exact match else not. This method checks if that exact match exists or not. And
     * if the exact match exists the author page is returned else the normal result page is returned.
     *
     * @param nextPage :: Initial Result Page of dblp
     * @param firstName :: first name of the author
     * @param lastName :: last name of the author
     * @return page :: Author/Result Page
     */
    public static HtmlPage dblpAuthorExists(HtmlPage nextPage, String firstName, String lastName) throws Exception{

        WebClient webClient = new WebClient();

        webClient.getOptions().setThrowExceptionOnScriptError(false);
        webClient.setJavaScriptTimeout(10000);
        webClient.getOptions().setJavaScriptEnabled(true);
        webClient.getOptions().setTimeout(10000);


        boolean exactNamePresent = false;
        HtmlDivision authorSection = (HtmlDivision)nextPage.getElementById("completesearch-authors");

        boolean openValve = false;

        ArrayList<HtmlElement> exactMatches = new ArrayList<HtmlElement>();

        for(HtmlElement authorEle : authorSection.getHtmlElementDescendants()){

            // while valve is open
            if(openValve){

                if(authorEle.getClass().toString().equalsIgnoreCase("class com.gargoylesoftware.htmlunit.html.HtmlListItem")){

                    exactMatches.add(authorEle);
                    break;
                }

            }

            if(authorEle.getTextContent().equalsIgnoreCase("Exact matches")){

                openValve = true;
            }

            else if(authorEle.getTextContent().equalsIgnoreCase("Likely matches")){

                openValve = false;
                break;
            }
        }

        // Check if there were any matches
        if(exactMatches.size() >= 1)
            exactNamePresent = true;

        HtmlPage page = nextPage;

        if(exactNamePresent){

            page = webClient.getPage("http://dblp.uni-trier.de/pers/hd/" + lastName.toLowerCase().charAt(0) + "/" + lastName + ":" + firstName);
        }

        return page;
    }

    /**
     * This method goes through all the rows inside the Table presented as results for a search query
     * on the Federal reporter website (http://dblp.uni-trier.de/). And then it collects the valuable
     * data and in the end presents a JSON.
     *
     * @param allResults :: Unordered list of DBLP's result
     * @return JSON of results
     */
    public static JSON jsonCreator(HtmlElement allResults) {

        JSONArray results = new JSONArray();

        List<HtmlElement> units = allResults.getElementsByAttribute("ul", "class", "publ-list");


        for (HtmlElement unit : units) {

            int count = 0;

            List<HtmlListItem> allLi = unit.getHtmlElementsByTagName("li");

            for (HtmlElement li : allLi) {

                if(count >= 30){
                     break;
                }

                String currentYear = "";

                String liClass = li.getAttribute("class").toString();

                if (liClass.equalsIgnoreCase("year")) {

                    currentYear = li.getTextContent();
                }

                else if(liClass.equalsIgnoreCase("drop-down") || liClass.equalsIgnoreCase("select-on-click")){

                }
                else if(li.toString().equalsIgnoreCase("HtmlListItem[<li>]")){

                }

                // Else it is an article
                else {

                    count++;

                    HtmlDivision data = li.getOneHtmlElementByAttribute("div", "class", "data");
                    List<HtmlSpan> allSpans = data.getHtmlElementsByTagName("span");

                    String authors = "";
                    String title = "";
                    String publisher = "";

                    for (HtmlSpan span : allSpans) {

                        String itemProp = span.getAttribute("itemprop");
                        String className = span.getAttribute("class");

                        if (itemProp.equalsIgnoreCase("author")) {

                            authors = authors + span.getTextContent() + "; ";
                        } else if (className.equalsIgnoreCase("title")) {

                            title = span.getTextContent();
                        }else if (itemProp.equalsIgnoreCase("name")) {

                            publisher = span.getTextContent();
                        } else if (itemProp.equalsIgnoreCase("datePublished")) {

                            currentYear = span.getTextContent();
                        }
                    }

                    // Create a new unit
                    JSONObject jsonUnit = new JSONObject();

                    jsonUnit.put("Title", title);
                    jsonUnit.put("Authors", authors);
                    jsonUnit.put("Year", currentYear);
                    jsonUnit.put("Publisher", publisher);

                    results.add(jsonUnit);

                }

            }


        }
        return results;
    }

}