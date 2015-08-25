package controllers.wrapper.aspectWrapper.indirectWrappers;

import controllers.SearchHandler;
import controllers.schema.SchemaReader;
import controllers.wrapper.aspectWrapper.GeneralAspectWrapper;
import controllers.wrapper.sourceWrapper.GeneralSourceWrapper;
import controllers.wrapper.sourceWrapper.indirectWrappers.LocalSourceWrapper;
import controllers.wrapper.sourceWrapper.indirectWrappers.RemoteSourceWrapper;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import static controllers.intraAspectMatchers.InclusionCounter.countInclusion;

public class LocalAspectWrapper extends GeneralAspectWrapper {

    private Set<GeneralSourceWrapper> registeredSources;
    private SearchHandler handler;

    public LocalAspectWrapper() {
        super();
        this.schema = null;
        this.registeredSources = null;
        this.isValid = false;
    }

    public LocalAspectWrapper(String aspectName, SearchHandler handler) {
        super(aspectName);
        this.handler = handler;
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
                    this.registeredSources.add(new LocalSourceWrapper(schema, source.getName(), this.name, handler));
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
                            this.registeredSources.add(new RemoteSourceWrapper(schema, sourceName, sourceAddress));
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
    public JSONArray getResultAsJSONArray(JSONObject searchConditions) {
        JSONObject resultFromEachSource = new JSONObject();
        for (GeneralSourceWrapper registeredSource : this.registeredSources) {
            JSONArray result = registeredSource.getResultAsJSONArray(searchConditions);
            if (result != null) {
                resultFromEachSource.put(registeredSource.name, result);
            }
        }
        return countInclusion(this.schema, resultFromEachSource);
    }

    @Override
    public JSONArray timedGetResultAsJSONArray(JSONObject searchConditions) {
        JSONObject resultFromEachSource = new JSONObject();
        for (GeneralSourceWrapper registeredSource : this.registeredSources) {
            JSONArray result = registeredSource.timedGetResultAsJSONArray(searchConditions);
            if (result != null) {
                resultFromEachSource.put(registeredSource.name, result);
            }
        }
        return countInclusion(this.schema, resultFromEachSource);
    }

    @Override
    public void print() {
        System.out.println(name + ":");
        for (GeneralSourceWrapper registeredSource : registeredSources) {
            registeredSource.print();
        }
    }
}