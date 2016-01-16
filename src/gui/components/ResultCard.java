package gui.components;

import controllers.graphAnalyzer.GlobalGraphInfo;
import controllers.schema.Field;
import controllers.schema.SchemaObj;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import util.Constants;

import javax.imageio.ImageIO;
import javax.swing.*;
import javax.swing.border.Border;
import javax.swing.tree.*;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.image.BufferedImage;
import java.util.*;


public class ResultCard extends JPanel{

    private class RecordNode implements MutableTreeNode {

        private JSONObject data;
        public int relevance;   //  -1 for irrelevant, 1 for relevant, 0 for unclassified

        public RecordNode(JSONObject data) {
            this.data = data;
            this.relevance = 0;
        }

        @Override
        public String toString() {
            String txt = "<html>";
            for (Field field : schema.getAllFields()) {
                if (data.containsKey(field.fieldName) && field.toString(data).trim().length() > 0 ) {
                    txt += "<b>" + field.fieldName + "</b>: " + field.toString(data) + "<p>";
                }
            }
            txt += "Record provided by source(s): " + data.getString("Included by") + "</html>";
            return txt;
        }

        @Override
        public void insert(MutableTreeNode child, int index) {}

        @Override
        public void remove(int index) {}

        @Override
        public void remove(MutableTreeNode node) {}

        @Override
        public void setUserObject(Object object) {
            this.data = JSONObject.fromObject(object);
        }

        public JSONObject getUserObject() {
            return this.data;
        }

        @Override
        public void removeFromParent() {}

        @Override
        public void setParent(MutableTreeNode newParent) {}

        @Override
        public TreeNode getChildAt(int childIndex) {
            return null;
        }

        @Override
        public int getChildCount() {
            return 0;
        }

        @Override
        public TreeNode getParent() {
            return null;
        }

        @Override
        public int getIndex(TreeNode node) {
            return 0;
        }

        @Override
        public boolean getAllowsChildren() {
            return false;
        }

        @Override
        public boolean isLeaf() {
            return true;
        }

        @Override
        public Enumeration children() {
            return null;
        }
    }

    private JTree tree;
    private SchemaObj schema;
    private JScrollPane scrollPane;

    public ResultCard(JSONObject data) {
        super();
        this.schema = SchemaObj.fromJSONArray(data.getJSONArray("schema"));
        this.setSize(800, 540);
        this.setPreferredSize(new Dimension(800, 540));
        this.setBackground(new Color(255, 255, 255));
        display(data.getJSONObject("results"));
    }

    public void display(JSONObject results) {

        DefaultMutableTreeNode top = new DefaultMutableTreeNode("Keywords:");


        for (Object e : results.entrySet()) {
            Map.Entry<String, JSONArray> ent = (Map.Entry)e;
            String[] kwAndUniqueKWs = ent.getKey().split(Constants.uniquekwDelimiter);
            String[] kws = kwAndUniqueKWs[0].split(Constants.kwDelimiter);
            JSONArray group = ent.getValue();
            if (group.size() == 0) continue;
            String groupName = "<html><b>Frequent words</b>:";
            if(ent.getKey().equalsIgnoreCase("About the Researcher")){

                groupName = "<html><b>About the Researcher</b></html>";
            }
            else {
                for (int i = 0; i < kws.length; i++) {
                    groupName += kws[i] + ", ";
                }
                groupName = groupName.substring(0, groupName.length() - 2);
                groupName += ";  <b>Identifying words</b>:";
                kws = kwAndUniqueKWs[1].split(Constants.kwDelimiter);
                for (int i = 0; i < kws.length; i++) {
                    groupName += kws[i] + ", ";
                }
                groupName = groupName.substring(0, groupName.length() - 2);
                groupName += "</html>";
            }
            DefaultMutableTreeNode currentGroupNode = new DefaultMutableTreeNode(groupName);
            top.add(currentGroupNode);
            for (int j = 0; j < group.size(); j++) {
                JSONObject current = group.getJSONObject(j);
                RecordNode node = new RecordNode(current);
                currentGroupNode.add(node);

                // Graph Analyzing Info Extraction
                // -------------------------------------------------------------
                String key = current.getString("Included by");
                if(GlobalGraphInfo.sourceToCount.containsKey(key) == true){
                    int currentCount = GlobalGraphInfo.sourceToCount.get(key);
                    GlobalGraphInfo.sourceToCount.put(key, currentCount + 1);
                }
                else{
                    if(!key.contains(",")){
                        GlobalGraphInfo.sourceToCount.put(key, 0);
                        GlobalGraphInfo.keys.add(key);
                    }
                    else{
                        key = key.split("(,)")[0];

                        if(GlobalGraphInfo.sourceToCount.containsKey(key) == true){
                            int currentCount = GlobalGraphInfo.sourceToCount.get(key);
                            GlobalGraphInfo.sourceToCount.put(key, currentCount + 1);
                        }
                        else{
                            GlobalGraphInfo.sourceToCount.put(key, 0);
                            GlobalGraphInfo.keys.add(key);
                        }
                    }
                }
                // --------------------------------------------------------------
            }
        }

        this.tree = new JTree(top);
        MouseListener ml = new MouseAdapter() {

            public void mouseClicked(MouseEvent e) {
                TreePath selPath = tree.getPathForLocation(e.getX(), e.getY());
                if (selPath == null || !tree.getModel().isLeaf(selPath.getLastPathComponent())) return;
                if (e.getClickCount() >= 2) {
                    int choice = JOptionPane.showConfirmDialog(null, "Is this record relevant?",
                            "Relevance Feedback", JOptionPane.YES_NO_OPTION);
                    if ( choice == JOptionPane.YES_OPTION) {
                        ((RecordNode)selPath.getLastPathComponent()).relevance = 1;
                    } else if (choice == JOptionPane.NO_OPTION) {
                        ((RecordNode)selPath.getLastPathComponent()).relevance = -1;
                    }
                }
            }

        };
        tree.addMouseListener(ml);
        tree.setRootVisible(false);
        tree.setScrollsOnExpand(false);
        tree.setRowHeight(0);
        tree.setCellRenderer(new DefaultTreeCellRenderer() {
            private Border border = BorderFactory.createEmptyBorder ( 6, 6, 6, 6 );
            private ImageIcon loadIcon(String iconName) throws Exception{
                Image img = ImageIO.read(getClass().getResource(iconName));
                BufferedImage scaledImg = new BufferedImage(15,15,BufferedImage.TYPE_INT_ARGB);
                scaledImg.createGraphics().drawImage(img, 0, 0, 15, 15, null);
                return new ImageIcon(scaledImg);
            }
            public Component getTreeCellRendererComponent ( JTree tree, Object value, boolean sel,
                                                            boolean expanded, boolean leaf, int row,
                                                            boolean hasFocus )
            {
                JLabel label = ( JLabel ) super
                        .getTreeCellRendererComponent ( tree, value, sel, expanded, leaf, row,
                                hasFocus );
                label.setBorder ( border );
                if (leaf) {
                    try {
                        RecordNode node = (RecordNode)value;
                        if (node.relevance == 1) {
                            setIcon(loadIcon("checked.png"));
                        } else if (node.relevance == -1) {
                            setIcon(loadIcon("crossed.png"));
                        } else {
                            setIcon(loadIcon("unknown.png"));
                        }
                    } catch (Exception e) {}

                }
                return label;
            }
        });
        this.tree.putClientProperty("JTree.lineStyle", "Horizontal");
        this.scrollPane = new JScrollPane(this.tree);
        this.add(this.scrollPane);
        this.scrollPane.setSize(800, 535);
        this.scrollPane.setPreferredSize(new Dimension(800, 535));
        this.tree.setVisible(true);
        this.scrollPane.setVisible(true);
    }

    public Map<String, Double> getRelevanceVector() {
        Map<String, Double> ret = new HashMap<String, Double>();
        TreeModel model = this.tree.getModel();
        Object cur = model.getRoot();
        Stack<Object> toVisit = new Stack<Object>();
        toVisit.push(cur);
        while (!toVisit.empty()) {
            cur = toVisit.pop();
            int childrenCount = model.getChildCount(cur);
            for (int i = 0; i < childrenCount; i++) {
                toVisit.add(model.getChild(cur, i));
            }
            if (model.isLeaf(cur)) {
                RecordNode node = (RecordNode)cur;
                JSONObject curData = node.getUserObject();
                for (Field field : schema.getAllFields()) {
                    if (field.dataType.equalsIgnoreCase("Text") && curData.containsKey(field.fieldName)) {
                        String curVal = curData.getString(field.fieldName).trim();
                        String[] words = curVal.split("\\W");
                        for (String word : words) {
                            if (word.length() == 1) continue;
                            boolean ignored = false;
                            for (String ignore : Constants.kwIgnore) {
                                if (word.equalsIgnoreCase(ignore)) ignored = true;
                            }
                            if (ignored) continue;
                            if (ret.containsKey(word)) {
                                ret.replace(word, ret.get(word) + node.relevance);
                            } else {
                                ret.put(word, (double)node.relevance);
                            }
                        }
                    }
                }
            }
        }
        return ret;
    }

}
