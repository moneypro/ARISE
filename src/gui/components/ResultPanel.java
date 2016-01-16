package gui.components;

import net.sf.json.JSONObject;
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.*;

public class ResultPanel extends JPanel{

    private CardLayout layout;
    private JPanel infoArea;
    private JPanel switchArea;
    private ButtonGroup switchButtons;
    private Set<ResultCard> cards;

    public ResultPanel() {
        super();
        this.cards = new HashSet<ResultCard>();
        this.setSize(800, 610);
        this.setPreferredSize(new Dimension(800, 610));
        this.infoArea = new JPanel();
        this.infoArea.setSize(800, 540);
        this.infoArea.setSize(new Dimension(800, 540));
        this.layout = new CardLayout();
        this.infoArea.setLayout(this.layout);
        this.infoArea.setVisible(true);
        this.switchArea = new JPanel(new FlowLayout());
        this.switchArea.setSize(800, 60);
        this.switchArea.setSize(new Dimension(800, 60));
        this.switchArea.setVisible(true);
        this.setLayout(new FlowLayout());
        this.add(this.infoArea);
        this.add(this.switchArea);
        this.setVisible(true);
    }

    public void display(JSONObject results) {
        this.layout = new CardLayout();
        this.infoArea.setLayout(this.layout);
        this.switchArea.setLayout(new FlowLayout());
        this.switchButtons = new ButtonGroup();
        for (Object k : results.keySet()) {
            String cardName = (String)k;
            ResultCard card = new ResultCard(results.getJSONObject(cardName));
            card.setName(cardName);
            this.cards.add(card);
            this.infoArea.add(card, cardName);
            final JRadioButton button = new JRadioButton(cardName);
            button.setVisible(true);
            button.setSize(100, 30);
            button.setPreferredSize(new Dimension(100, 30));
            button.addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    switchCard(button.getText());
                }
            });
            this.switchButtons.add(button);
            this.switchArea.add(button);
        }
        layout.first(this.infoArea);
    }

    public JSONObject getFeedBack() {
        JSONObject ret = new JSONObject();
        for (ResultCard card : cards) {
            ret.put(card.getName(), card.getFeedBack());
        }
        return ret;
    }

    @Deprecated
    public String[][] getNewKeywords() {
        Map<String, Double> total = new HashMap<String, Double>();
        for (ResultCard card : cards) {
            Map<String, Double> cur = card.getRelevanceVector();
            for (Map.Entry<String, Double> entry : cur.entrySet()) {
                if (total.containsKey(entry.getKey())) {
                    total.replace(entry.getKey(), entry.getValue() + total.get(entry.getKey()));
                } else {
                    total.put(entry.getKey(), entry.getValue());
                }
            }
        }
        Comparator<Map.Entry<String, Double>> cmp = new Comparator<Map.Entry<String, Double>>() {
            @Override
            public int compare(Map.Entry<String, Double> e1, Map.Entry<String, Double> e2) {
                return (int)(e1.getValue() - e2.getValue());
            }
        };
        Set<Map.Entry<String, Double>> entries = total.entrySet();
        String[][] ret = new String[2][5];
        for (int i = 0; i < 5; i++) {
            try {
                Map.Entry<String, Double> max = Collections.max(entries, cmp);
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
                Map.Entry<String, Double> min = Collections.min(entries, cmp);
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
        return ret;
    }

    private void switchCard(String name) {
        this.layout.show(this.infoArea, name);
    }

}
