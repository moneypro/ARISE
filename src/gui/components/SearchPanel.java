package gui.components;

import controllers.graphAnalyzer.GlobalGraphInfo;
import controllers.graphAnalyzer.GraphAnalyzer;
import gui.MainFrame;
import javafx.application.Application;
import javafx.stage.Stage;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.awt.image.BufferedImage;
import java.io.IOException;

public class SearchPanel extends JPanel {

    private JTextField nameInput;
    private JTextField affiliationInput;
    private JButton restartSearchButton;
    private JButton searchButton;
    private JButton analyzeButton;          // Button used to show the Graph Analyze Window
    private MainFrame parentWindow;

    public SearchPanel(MainFrame parentWindow) {
        this.parentWindow = parentWindow;
        setSize(800, 80);
        setBackground(new Color(231, 231, 231));
        setPreferredSize(new Dimension(800, 80));
        setLayout(new FlowLayout(FlowLayout.CENTER, 5, 5));
        //  Set up and add name input
        nameInput = new JTextField();
        nameInput.setPreferredSize(new Dimension((this.getWidth() / 2) - 5, this.getHeight() - 40));
        nameInput.addFocusListener(new FocusAdapter() {

            @Override
            public void focusGained(FocusEvent e) {
                super.focusGained(e);
                if (nameInput.getText().equals("Name, i.e. Jiawei Han")) {
                    nameInput.setText("");
                }
            }

            @Override
            public void focusLost(FocusEvent e) {
                super.focusLost(e);
                if (nameInput.getText().length() == 0) {
                    nameInput.setText("Name, i.e. Jiawei Han");
                }
            }
        });
        add(this.nameInput);
        //  Set up and add affiliation input
        affiliationInput = new JTextField();
        affiliationInput.addFocusListener(new FocusAdapter() {
            @Override
            public void focusGained(FocusEvent e) {
                super.focusGained(e);
                if (affiliationInput.getText().equals("Affiliation, i.e. UIUC")) {
                    affiliationInput.setText("");
                }
            }

            @Override
            public void focusLost(FocusEvent e) {
                super.focusLost(e);
                if (affiliationInput.getText().length() == 0) {
                    affiliationInput.setText("Affiliation, i.e. UIUC");
                }
            }
        });
        affiliationInput.setPreferredSize(new Dimension((this.getWidth() / 2) - 5, this.getHeight() - 40));
        add(this.affiliationInput);
        //  Ste up and add restart search button
        restartSearchButton = new JButton("Redo search");
        restartSearchButton.setEnabled(false);
        restartSearchButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                // Clear Graph Analyzer Data
                GlobalGraphInfo.sourceToCount.clear();
                GlobalGraphInfo.keys.clear();
                restartSearch();
            }
        });
        restartSearchButton.setSize(120, 30);
        add(restartSearchButton);
        //  Set up and add search button
        searchButton = new JButton("Get Profile");
        searchButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                // Clear Graph Analyzer Data
                restartSearchButton.setEnabled(true);
                analyzeButton.setEnabled(true);
                GlobalGraphInfo.sourceToCount.clear();
                GlobalGraphInfo.keys.clear();
                startSearch();
            }
        });
        searchButton.setSize(120, 30);
        add(searchButton);
        analyzeButton = new JButton();
        try {
            Image img = ImageIO.read(getClass().getResource("graph.png"));

            BufferedImage scaledImg = new BufferedImage(15,15,BufferedImage.TYPE_INT_ARGB);
            scaledImg.createGraphics().drawImage(img, 0, 0, 15, 15, null);
            analyzeButton.setIcon(new ImageIcon(scaledImg));
        } catch (IOException ex) {
        }
        analyzeButton.setEnabled(false);
        analyzeButton.setSize(15, 15);
        analyzeButton.setBackground(Color.white);
        analyzeButton.setAlignmentX(50);
        analyzeButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String[] args = new String[0];
                GraphAnalyzer.launchGraph(args);
            }
        });
        add(analyzeButton);
        addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                super.mouseClicked(e);
                requestFocus();
            }
        });
    }

    public String getInputName() {
        return this.nameInput.getText();
    }

    public String getInputAffiliation() {
        return this.affiliationInput.getText();
    }

    public void startSearch() {
        this.parentWindow.startSearch(this.getInputName(), this.getInputAffiliation());
    }

    public void restartSearch() {
        this.parentWindow.redoSearch(this.getInputName(), this.getInputAffiliation());
    }

    public void promptAndFocus() {
        this.searchButton.requestFocus();
        this.nameInput.setText("Name, i.e. Jiawei Han");
        this.affiliationInput.setText("Affiliation, i.e. UIUC");
    }

}

