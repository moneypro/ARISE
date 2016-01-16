package controllers;

import controllers.schema.Field;
import controllers.wrapper.GeneralWrapper;
import controllers.wrapper.aspectWrapper.GeneralAspectWrapper;
import controllers.wrapper.aspectWrapper.indirectWrappers.LocalAspectWrapper;
import database.EmbeddedDB;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import util.Constants;
import util.MyHTTP;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

import static util.Constants.localServerPortNumber;

public class SearchHandler{

    private Set<GeneralAspectWrapper> registeredAspects;
    private Map<String, Boolean> activation;
    private boolean isValid;
    private LocalSearchServer server;
    private JSONObject lastQuery;
    private EmbeddedDB db;

    /*
    * Constructor of a search handler:
    *   1.  Read all registered aspects.
    *   2.  Start local background service.
    * */
    public SearchHandler() {
        db = new EmbeddedDB("dbbackup");
        db.createKWTable();
        if (!db.isValid()) db = new EmbeddedDB();
        this.reset();
        server = new LocalSearchServer();
        server.start();
    }

    /*
    * Reset aspect registration.
    * Default activation state of all sources is ACTIVATED.
    * */
    public void reset() {
        this.lastQuery = null;
        File aspDir = new File(GeneralWrapper.basePath);
        this.registeredAspects = new HashSet<GeneralAspectWrapper>();
        this.activation = new HashMap<String, Boolean>();
        if (aspDir.exists() && aspDir.isDirectory()) {
            File[] aspects = aspDir.listFiles();
            try {
                for (File aspect : aspects != null ? aspects : new File[0]) {
                    if (aspect.getName().startsWith(".") || !(aspect.isDirectory())) {
                        continue;
                    }
                    GeneralAspectWrapper aWrapper = new LocalAspectWrapper(aspect.getName(), db);
                    if (aWrapper.isValid()) {
                        this.registeredAspects.add(aWrapper);
                        this.activation.put(aspect.getName(), true);
                    }
                }
                this.isValid = true;
            } catch (Exception e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
                this.isValid = false;
            }
        } else {
            this.isValid = false;
        }
    }

    /*
    * Stop search handler/background service.
    * */
    public void stop() {
        server.stop();
        db.backUpDB("dbbackup");
        //  Possibly cleaning up
    }

    public JSONObject search(JSONObject searchConditions) {
        searchConditions.put("kws", db.getKWs(searchConditions));
        JSONObject results = new JSONObject();
        for (GeneralAspectWrapper registeredAspect : this.registeredAspects) {
            if (registeredAspect.isActivated()) {
                JSONObject temp = new JSONObject();
                temp.put("schema", registeredAspect.getSchema().toJSONArray());
                temp.put("results", registeredAspect.timedGetResultAsJSON(searchConditions));
                results.put(registeredAspect.name, temp);
            }
        }
        this.lastQuery = searchConditions;
        return results;
    }

    public JSONObject redoSearch(JSONObject searchConditions) {
        JSONObject kws = searchConditions.getJSONObject("kws");
        Iterator<String> kwIter = kws.keys();
        while (kwIter.hasNext()) {
            String kw = kwIter.next();
            db.insertAllRecords(kw, searchConditions, kws.getJSONObject(kw));
        }
        Map<String, Integer> wordImportance = new HashMap<String, Integer>();
        for (GeneralAspectWrapper aspect : registeredAspects) {
            JSONObject aspectFeedBack = kws.getJSONObject(aspect.name);
            if (aspectFeedBack.containsKey("positive")) {
                JSONArray arr = aspectFeedBack.getJSONArray("positive");
                for (Object r : arr) {
                    JSONObject rec = JSONObject.fromObject(r);
                    for (Field field : aspect.getSchema().fields) {
                        if ((field.dataType.equalsIgnoreCase("Text") ||
                                field.dataType.equalsIgnoreCase("Location")) &&
                                rec.containsKey(field.fieldName)) {
                            if (rec.containsKey(field.fieldName)) {
                                String val = rec.getString(field.fieldName);
                                for (String word : val.split("\\W+?")) {
                                    boolean ignored = false;
                                    for (String ignore : Constants.kwIgnore) {
                                        if (word.equalsIgnoreCase(ignore)) ignored = true;
                                    }
                                    if (ignored) continue;
                                    wordImportance.put(
                                            word,
                                            wordImportance.getOrDefault(word, 0) + 1
                                    );

                                }
                            }
                        }
                    }
                }
            }
            if (aspectFeedBack.containsKey("negative")) {
                JSONArray arr = aspectFeedBack.getJSONArray("negative");
                for (Object r : arr) {
                    JSONObject rec = JSONObject.fromObject(r);
                    for (Field field : aspect.getSchema().fields) {
                        if (field.dataType.equalsIgnoreCase("Text") ||
                                field.dataType.equalsIgnoreCase("Location")) {
                            if (rec.containsKey(field.fieldName)) {
                                String val = rec.getString(field.fieldName);
                                for (String word : val.split("\\W+?")) {
                                    boolean ignored = false;
                                    for (String ignore : Constants.kwIgnore) {
                                        if (word.equalsIgnoreCase(ignore)) ignored = true;
                                    }
                                    if (ignored) continue;
                                    wordImportance.put(
                                            word,
                                            wordImportance.getOrDefault(word, 0) - 1
                                    );

                                }
                            }
                        }
                    }
                }
            }
        }
        Comparator<Map.Entry<String, Integer>> cmp = new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
                return e1.getValue() - e2.getValue();
            }
        };
        Set<Map.Entry<String, Integer>> entries = wordImportance.entrySet();
        String[][] ret = new String[2][5];
        for (int i = 0; i < 5; i++) {
            try {
                Map.Entry<String, Integer> max = Collections.max(entries, cmp);
                if (max.getValue() <= 0) {
                    for (int j = i; j < 5; j++) {
                        ret[0][j] = null;
                    }
                    break;
                }
                ret[0][i] = max.getKey();
                entries.remove(max);
            } catch (NoSuchElementException e) {
                for (int j = i; j < 5; j++) {
                    ret[0][j] = null;
                }
                break;
            }
        }
        for (int i = 0; i < 5; i++) {
            try {
                Map.Entry<String, Integer> min = Collections.min(entries, cmp);
                if (min.getValue() >= 0) {
                    for (int j = i; j < 5; j++) {
                        ret[1][j] = null;
                    }
                    break;
                }
                ret[1][i] = min.getKey();
                entries.remove(min);
            } catch (NoSuchElementException e) {
                for (int j = i; j < 5; j++) {
                    ret[1][j] = null;
                }
                break;
            }
        }
        JSONArray positiveKWs, negativeKWs;
        if (lastQuery.containsKey("kws")) {
            JSONObject kwObj = lastQuery.getJSONObject("kws");
            if (kwObj.containsKey("positive")) positiveKWs = kwObj.getJSONArray("positive");
            else positiveKWs = new JSONArray();
            if (kwObj.containsKey("negative")) negativeKWs = kwObj.getJSONArray("negative");
            else negativeKWs = new JSONArray();
        } else {
            positiveKWs = new JSONArray();
            negativeKWs = new JSONArray();
        }
        for (int i = 0; i < 5; i++) {
            String kw = ret[0][i];
            if (kw == null) break;
            if (negativeKWs.contains(kw)) {
                negativeKWs.remove(kw);
                db.deleteKW(searchConditions, kw);
            }
            else if (!positiveKWs.contains(kw)) {
                positiveKWs.add(kw);
                db.insertKW(searchConditions, kw, true);
            }
        }
        for (int i = 0; i < 5; i++) {
            String kw = ret[1][i];
            if (kw == null) break;
            if (positiveKWs.contains(kw)) {
                positiveKWs.remove(kw);
                db.deleteKW(searchConditions, kw);
            }
            else if (!negativeKWs.contains(kw)) {
                negativeKWs.add(kw);
                db.insertKW(searchConditions, kw, false);
            }
        }
        JSONObject kwObj = new JSONObject();
        kwObj.put("positive", positiveKWs);
        kwObj.put("negative", negativeKWs);
        searchConditions.replace("kws", kwObj);
        return search(searchConditions);
    }

    @Deprecated
    public JSONObject restartSearch(JSONObject searchConditions) {
        JSONArray positiveKWs, negativeKWs;
        if (lastQuery.containsKey("kws")) {
            JSONObject kwObj = lastQuery.getJSONObject("kws");
            if (kwObj.containsKey("positive")) positiveKWs = kwObj.getJSONArray("positive");
            else positiveKWs = new JSONArray();
            if (kwObj.containsKey("negative")) negativeKWs = kwObj.getJSONArray("negative");
            else negativeKWs = new JSONArray();
        } else {
            positiveKWs = new JSONArray();
            negativeKWs = new JSONArray();
        }
        JSONArray newPositive = searchConditions.getJSONObject("kws").getJSONArray("positive");
        JSONArray newNegative = searchConditions.getJSONObject("kws").getJSONArray("negative");
        for (int i = 0; i < newPositive.size(); i++) {
            String kw = newPositive.getString(i);
            if (kw == null) break;
            if (negativeKWs.contains(kw)) negativeKWs.remove(kw);
            else if (!positiveKWs.contains(kw)) positiveKWs.add(kw);
        }
        for (int i = 0; i < newNegative.size(); i++) {
            String kw = newNegative.getString(i);
            if (kw == null) break;
            if (positiveKWs.contains(kw)) positiveKWs.remove(kw);
            else if (!negativeKWs.contains(kw)) negativeKWs.add(kw);
        }
        JSONObject kwObj = new JSONObject();
        kwObj.put("positive", positiveKWs);
        kwObj.put("negative", negativeKWs);
        searchConditions.replace("kws", kwObj);
        return search(searchConditions);
    }

    /*
    * Change the activation state of a give source.
    * */
    public void setActivation(String aspect, String source, boolean newIsActive) {
        for (GeneralAspectWrapper registeredAspect : registeredAspects) {
            if (registeredAspect.name.equals(aspect)) {
                registeredAspect.setActivation(source, newIsActive);
            }
        }
    }

    /*
    * Report readiness to handle requests.
    * This function only reports whether any error occurred
    * during the constructing/resetting phase of the handler/registration.
    * */
    public boolean isValid() {
        return this.isValid;
    }

    /*
    * For debugging purposes: print registered aspects.
    * */
    public void print() {
        for (GeneralAspectWrapper registeredAspect : registeredAspects) {
            registeredAspect.print();
        }
    }

    /*
    * Report registration.
    * */
    public JSONObject getRegisteredAspects() {
        JSONObject ret = new JSONObject();
        for (GeneralAspectWrapper registeredAspect : this.registeredAspects) {
            ret.put(registeredAspect.name, registeredAspect.getRegisteredSources());
        }
        return ret;
    }

    /*
    * Add a new aspect:
    *   1.  Create corresponding file(s)/directory(s) in the file system, which contains only indirect sources.
    *   2.  Inform the local background service.
    * */
    public void addAspect(JSONObject description) {
        String name = description.getString("name");
        File f = new File(GeneralWrapper.basePath + "/" + name);
        if (f.exists()) {
            System.out.println("Aspect " + name + " already exists!");
        } else {
            boolean success = f.mkdir();
            if (!success) {
                System.out.println("Error generating directory " + GeneralAspectWrapper.basePath + "/" + name);
                return;
            }
            File schemaFile = new File(GeneralWrapper.basePath + "/" + name + "/schema.tsv");
            BufferedWriter writer;
            try {
                success = schemaFile.createNewFile();
                if (!success) {
                    boolean ignore = f.delete();
                    System.out.println("Error generating schema file at " + GeneralAspectWrapper.basePath + "/" + name + "/schema.tsv");
                    return;
                }
                writer = new BufferedWriter(new FileWriter(schemaFile.getAbsoluteFile()));
                writer = new BufferedWriter(new FileWriter(schemaFile.getAbsoluteFile()));
                writer.write("Field Name\tIs Primary Key\tSyntax");
                JSONArray fields = description.getJSONArray("fields");
                for (Object o : fields) {
                    JSONObject field = (JSONObject)o;
                    writer.write('\n');
                    writer.write(field.getString("name") + "\t" + field.getString("isPK") + "\t" + field.getString("syntax"));
                }
                writer.close();
                LocalAspectWrapper awrapper = new LocalAspectWrapper(name, db);
                if (awrapper.isValid()) {
                    registeredAspects.add(awrapper);
                }
                //  Inform the background service
                MyHTTP.post("http://localhost:" + localServerPortNumber + "/reset", null);
            } catch (Exception e) {
                if (schemaFile.exists()) {
                    boolean ignore = schemaFile.delete();
                }
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
