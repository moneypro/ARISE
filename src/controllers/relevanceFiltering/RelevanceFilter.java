package controllers.relevanceFiltering;

import controllers.schema.Field;
import controllers.schema.SchemaObj;
import database.EmbeddedDB;
import net.sf.json.JSONObject;
import weka.classifiers.meta.FilteredClassifier;
import weka.classifiers.trees.RandomForest;
import weka.core.*;
import weka.filters.unsupervised.attribute.StringToWordVector;
import java.util.ArrayList;

public class RelevanceFilter {

    private String aspectName;
    private SchemaObj schema;
    private EmbeddedDB db;
    private FilteredClassifier classifier;
    private Instances traininSet;

    public RelevanceFilter(String aspectName, SchemaObj schema, EmbeddedDB db) {
        this.aspectName = aspectName;
        this.schema = schema;
        this.db = db;
    }

    public void train(JSONObject searchConditions) {
        JSONObject examples = db.getAllRecords(aspectName, searchConditions);
        ArrayList<Field> fields = schema.getAllFields();
        FastVector attrs = new FastVector(fields.size() + 1);
        String indices = "";
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            if (field.dataType.equalsIgnoreCase("Text") ||
                    field.dataType.equalsIgnoreCase("Location") ||
                    field.dataType.equalsIgnoreCase("Name")) {
                indices += Integer.toString(i+1) + ",";
                attrs.addElement(new Attribute(field.fieldName, (FastVector) null));
            } else {
                attrs.addElement(new Attribute(field.fieldName));
            }
        }
        final FastVector classValues = new FastVector(2);
        classValues.addElement("positive");
        classValues.addElement("negative");
        Attribute classLabel = new Attribute("class", classValues);
        attrs.addElement(classLabel);
        traininSet = new Instances("trainingSet", attrs, examples.size());
        traininSet.setClass(classLabel);

        for (int i = 0; i < examples.getJSONArray("positive").size(); i++) {
            Instance toAdd = new Instance(fields.size() + 1);
            toAdd.setDataset(traininSet);
            toAdd.setClassValue("positive");
            JSONObject dataPoint = examples.getJSONArray("positive").getJSONObject(i);
            for (int j = 0; j < fields.size(); j++) {
                if (dataPoint.containsKey(fields.get(j).fieldName)) {
                    if (fields.get(j).dataType.equalsIgnoreCase("Text") ||
                            fields.get(j).dataType.equalsIgnoreCase("Location") ||
                            fields.get(j).dataType.equalsIgnoreCase("Name")) {
                        toAdd.setValue(j, dataPoint.getString(fields.get(j).fieldName));
                    }
                    else {
                        toAdd.setValue(j, dataPoint.getDouble(fields.get(j).fieldName));
                    }
                }
                else {
                    toAdd.setMissing(j);
                }
            }
            traininSet.add(toAdd);
        }

        for (int i = 0; i < examples.getJSONArray("negative").size(); i++) {
            Instance toAdd = new Instance(fields.size() + 1);
            toAdd.setDataset(traininSet);
            toAdd.setClassValue("negative");
            JSONObject dataPoint = examples.getJSONArray("negative").getJSONObject(i);
            for (int j = 0; j < fields.size(); j++) {
                if (dataPoint.containsKey(fields.get(j).fieldName)) {
                    if (fields.get(j).dataType.equalsIgnoreCase("Text") ||
                            fields.get(j).dataType.equalsIgnoreCase("Location") ||
                            fields.get(j).dataType.equalsIgnoreCase("Name")) {
                        toAdd.setValue(j, dataPoint.getString(fields.get(j).fieldName));
                    }
                    else {
                        toAdd.setValue(j, dataPoint.getDouble(fields.get(j).fieldName));
                    }
                }
                else {
                    toAdd.setMissing(j);
                }
            }
            traininSet.add(toAdd);
        }

        this.classifier = new FilteredClassifier();
        RandomForest base = new RandomForest();
        StringToWordVector filter = new StringToWordVector();
        if (indices.length() >= 0) filter.setAttributeIndices(indices.substring(0, indices.length()-1));
        classifier.setFilter(filter);
        classifier.setClassifier(base);

        if (traininSet.numInstances() > 0) {
            try {
                classifier.buildClassifier(traininSet);
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Failed to train classifier.");
            }
        } else {
            classifier = null;
        }

    }

    public boolean predict(JSONObject example) {
        ArrayList<Field> fields = schema.getAllFields();
        Instance toAdd = new Instance(fields.size() + 1);
        toAdd.setDataset(traininSet);
        for (int j = 0; j < fields.size(); j++) {
            if (example.containsKey(fields.get(j).fieldName)) {
                if (fields.get(j).dataType.equalsIgnoreCase("Text") ||
                        fields.get(j).dataType.equalsIgnoreCase("Location") ||
                        fields.get(j).dataType.equalsIgnoreCase("Name")) {
                    toAdd.setValue(j, example.getString(fields.get(j).fieldName));
                }
                else {
                    toAdd.setValue(j, example.getDouble(fields.get(j).fieldName));
                }
            }
            else {
                toAdd.setMissing(j);
            }
        }
        try {
            return traininSet.classAttribute().value(
                    (int)classifier.classifyInstance(toAdd)
            ).equalsIgnoreCase("positive");
        } catch (Exception e) {
            return false;
        }
    }


//    Map<String, Double> weights;
//    double threshold;
//    SchemaObj schema;
//
//    public RelevanceFilter(SchemaObj s) {
//        weights = new HashMap<String, Double>();
//        threshold = 0;
//        schema = s;
//    }
//
//    public void dump(String file) {
//        try {
//            BufferedWriter out = new BufferedWriter(new FileWriter(file));
//            out.write(Double.toString(threshold) + "\n");
//            for (Map.Entry<String, Double> entry : weights.entrySet()) {
//                out.write(entry.getKey() + "\t" + entry.getValue().toString() + "\n");
//            }
//            out.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//

//
//    public void train(JSONObject record, boolean isRelevant) {
//        if (isRelevant != predict(record)) {
//            double factor = isRelevant ? 1 : -1;
//            for (Field field : schema.fields) {
//                if (record.containsKey(field.fieldName)) {
//                    if (field.dataType.equalsIgnoreCase("Name") ||
//                            field.dataType.equalsIgnoreCase("Location") ||
//                            field.dataType.equalsIgnoreCase("Text") ||
//                            field.dataType.equalsIgnoreCase("Date")) {
//                        String val = record.getString(field.fieldName);
//                        String[] words = val.split("\\W+?");
//                        for (String word : words) {
//                            boolean ignored = false;
//                            for (String ignore : Constants.kwIgnore) {
//                                if (word.equalsIgnoreCase(ignore)) ignored = true;
//                            }
//                            if (ignored) continue;
//                            if (weights.containsKey(word))
//                                weights.put(word, weights.get(word) + factor);
//                            else weights.put(word, factor);
//                        }
//                    }
//                    else if (field.dataType.equalsIgnoreCase("LongInteger")) {
//                        weights.put(
//                                convertFieldName(field.fieldName),
//                                weights.getOrDefault(convertFieldName(field.fieldName), 0.0) + factor * record.getLong(field.fieldName)
//                        );
//                    }
//                    else if (field.dataType.equalsIgnoreCase("Integer")) {
//                        weights.put(
//                                convertFieldName(field.fieldName),
//                                weights.getOrDefault(convertFieldName(field.fieldName), 0.0) + factor * record.getInt(field.fieldName)
//                        );
//                    }
//                }
//            }
//            threshold += factor;
//        }
//    }
//
//    public boolean predict(JSONObject record) {
//        double predict = 0;
//        for (Field field : schema.fields) {
//            if (record.containsKey(field.fieldName)) {
//                if (field.dataType.equalsIgnoreCase("Name") ||
//                        field.dataType.equalsIgnoreCase("Location") ||
//                        field.dataType.equalsIgnoreCase("Text") ||
//                        field.dataType.equalsIgnoreCase("Date")) {
//                    String val = record.getString(field.fieldName);
//                    String[] words = val.split("\\W+?");
//                    for (String word : words) {
//                        boolean ignored = false;
//                        for (String ignore : Constants.kwIgnore) {
//                            if (word.equalsIgnoreCase(ignore)) ignored = true;
//                        }
//                        if (ignored) continue;
//                        if (weights.containsKey(word))
//                            predict += weights.get(word);
//                    }
//                }
//                else if (field.dataType.equalsIgnoreCase("LongInteger") &&
//                        weights.containsKey(convertFieldName(field.fieldName))) {
//                    predict +=
//                            weights.get(convertFieldName(field.fieldName)) *
//                                    (double)record.getLong(field.fieldName);
//                }
//                else if (field.dataType.equalsIgnoreCase("Integer") &&
//                        weights.containsKey(convertFieldName(field.fieldName))) {
//                    predict +=
//                            weights.get(convertFieldName(field.fieldName)) *
//                                    (double)record.getInt(field.fieldName);
//                }
//            }
//        }
//        return predict > threshold;
//    }
//
//    private String convertFieldName(String name) {
//        return "___FIELD___" + name + "___FIELD___";
//    }


}
