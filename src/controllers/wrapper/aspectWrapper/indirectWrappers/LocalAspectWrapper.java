package controllers.wrapper.aspectWrapper.indirectWrappers;

import controllers.relevanceFiltering.RelevanceFilter;
import controllers.schema.SchemaReader;
import controllers.wrapper.aspectWrapper.GeneralAspectWrapper;
import controllers.wrapper.sourceWrapper.GeneralSourceWrapper;
import controllers.wrapper.sourceWrapper.IndirectSourceWrapper;
import database.EmbeddedDB;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import static controllers.groupings.Grouper.group;
import static controllers.intraAspectMatchers.InclusionCounter.countInclusion;

public class LocalAspectWrapper extends GeneralAspectWrapper {

    private Set<GeneralSourceWrapper> registeredSources;
    private RelevanceFilter filter;
    private EmbeddedDB db;

    public LocalAspectWrapper(String aspectName, EmbeddedDB db) {
        super(aspectName);
        this.db = db;
        String aspectBasePath = basePath + "/" + aspectName;
        SchemaReader sReader = new SchemaReader(aspectBasePath + "/schema.tsv");
        if (!sReader.isValid()) {
            System.out.println("Error occurred while reading schema for aspect " + aspectName);
            this.schema = null;
            this.registeredSources = null;
            this.isValid = false;
        } else {
            this.schema = sReader.getSchemaObject();
            this.registeredSources = new HashSet<GeneralSourceWrapper>();
            File[] sourceNames = (new File(aspectBasePath)).listFiles();
            if (sourceNames != null) {
                for (File source : sourceNames) {
                    if (source.getName().startsWith(".") || !(source.isDirectory())) {
                        continue;
                    }
                    this.registeredSources.add(new IndirectSourceWrapper(
                            schema,
                            source.getName(),
                            this.name
                    ));
                }
                File remoteList = new File(aspectBasePath + "/remoteServers.tsv");
                if (remoteList.exists() && remoteList.isFile()) {
                    try {
                        Scanner scanner = new Scanner(new BufferedReader(new FileReader(remoteList.getAbsoluteFile())));
                        scanner.useDelimiter("\\n");
                        while (scanner.hasNext()) {
                            String line = scanner.next();
                            int tabIndex = line.indexOf('\t');
                            String sourceName = line.substring(0, tabIndex);
                            String sourceAddress = line.substring(tabIndex+1);
                            this.registeredSources.add(new IndirectSourceWrapper(
                                    schema,
                                    sourceName,
                                    this.name
                            ));
                        }
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                        e.printStackTrace();
                        this.isValid = false;
                    }
                }
                this.isValid = true;
            } else {
                this.isValid = false;
            }
        }
        //  Add aspect table in DB if it does not exist
        db.createEmptyRecordTable(name);
        filter = new RelevanceFilter(name, schema, db);
    }

    public void setActivation(String source, boolean newIsActive) {
        for (GeneralSourceWrapper registeredSource : registeredSources) {
            if (registeredSource.name.equals(source)) {
                System.out.println(registeredSource.name);
                System.out.println(newIsActive);
                registeredSource.isActive = newIsActive;
            }
        }
    }

    @Override
    public JSONArray getRegisteredSources() {
        Set<String> ret = new HashSet<String>();
        for (GeneralSourceWrapper registeredSource : this.registeredSources) {
            ret.add(registeredSource.name);
        }
        return JSONArray.fromObject(ret);
    }

    @Override
    public JSONObject getResultAsJSON(JSONObject searchConditions) {
        JSONObject resultFromEachSource = new JSONObject();
        for (GeneralSourceWrapper registeredSource : this.registeredSources) {
            if (registeredSource.isActive) {
                JSONArray result = registeredSource.getResultAsJSON(searchConditions);
                if (result != null) {
                    resultFromEachSource.put(registeredSource.name, result);
                }
            }
        }
        JSONArray merged = countInclusion(this.schema, resultFromEachSource);
        JSONArray relevant = new JSONArray();
        JSONArray irrelevant = new JSONArray();
        filter.train(searchConditions);
        for (Object o : merged) {
            JSONObject rec = JSONObject.fromObject(o);
            if (filter.predict(rec)) relevant.add(rec);
            else irrelevant.add(rec);
        }
        JSONObject ret = new JSONObject();
        ret.put("irrelevant", irrelevant);
        ret.put("relevant", group(relevant, schema));
        return ret;
    }

    @Override
    public boolean isActivated() {
        for (GeneralSourceWrapper registeredSource : registeredSources) {
            if (registeredSource.isActive) {
                return true;
            }
        }
        return false;
    }

    @Override
    public JSONObject timedGetResultAsJSON(JSONObject searchConditions) {
        JSONObject resultFromEachSource = new JSONObject();
        for (GeneralSourceWrapper registeredSource : this.registeredSources) {
            if (registeredSource.isActive) {
                JSONArray result = registeredSource.timedGetResultAsJSON(searchConditions);
                if (result != null) {
                    resultFromEachSource.put(registeredSource.name, result);
                }
            }
        }
        JSONArray merged = countInclusion(this.schema, resultFromEachSource);
        JSONArray relevant = new JSONArray();
        JSONArray irrelevant = new JSONArray();
        filter.train(searchConditions);
        for (Object o : merged) {
            JSONObject rec = JSONObject.fromObject(o);
            if (filter.predict(rec)) relevant.add(rec);
            else irrelevant.add(rec);
        }
        JSONObject ret = new JSONObject();
        ret.put("irrelevant", irrelevant);
        ret.put("relevant", group(relevant, schema));
        return ret;
    }

    @Override
    public void print() {
        System.out.println(name + ":");
        for (GeneralSourceWrapper registeredSource : registeredSources) {
            registeredSource.print();
        }
    }

}
