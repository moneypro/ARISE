package database;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import util.Constants;

import java.sql.*;
import java.util.ArrayList;

public class EmbeddedDB {

    private static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";

    private Connection conn;
    private boolean isValid;

    public EmbeddedDB() {
        try {
            Class.forName(DRIVER);
            conn = DriverManager.getConnection("jdbc:derby:arise;create=true");
            isValid = true;
        } catch (SQLException e) {
            e.printStackTrace();
            conn = null;
            isValid = false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            conn = null;
            isValid = false;
        }
    }

    public EmbeddedDB(String fromBackUp) {
        try {
            Class.forName(DRIVER);
            conn = DriverManager.getConnection(
                    "jdbc:derby:arise;restoreFrom=" + fromBackUp + "/arise"
            );
            isValid = true;
        } catch (SQLException e) {
            System.out.println("Failed to restore database. Creating a new one.");
            conn = null;
            isValid = false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            conn = null;
            isValid = false;
        }
    }

    public boolean isValid() {
        return isValid;
    }

    public boolean backUpDB(String toFolder)
    {
        //  Code from http://www.ibm.com/developerworks/data/library/techarticle/dm-0502thalamati/
        //  with small changes.
        String sqlstmt = "CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)";
        try {
            CallableStatement cs = conn.prepareCall(sqlstmt);
            cs.setString(1, toFolder);
            cs.execute();
            cs.close();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void createEmptyRecordTable(String name) {
        name = escapeTableName(name);
        String stmt = "CREATE TABLE \"" + name + "\" (" +
                "NAME VARCHAR(255), " +
                "AFFILIATION VARCHAR(255), " +
                "DATA LONG VARCHAR, " +
                "RELEVANCE BOOLEAN)";
        try {
            conn.createStatement().execute(stmt);
        } catch (SQLException e) {
            if (e.getSQLState().equals("X0Y32")) return;
            e.printStackTrace();
        }
    }

    public void createKWTable() {
        String stmt = "CREATE TABLE \"" + Constants.kwTableName + "\" (" +
                "NAME VARCHAR(255), " +
                "AFFILIATION VARCHAR(255), " +
                "WORD VARCHAR(255), " +
                "POSITIVE BOOLEAN)";
        try {
            conn.createStatement().execute(stmt);
        } catch (SQLException e) {
            if (e.getSQLState().equals("X0Y32")) return;
            e.printStackTrace();
        }
    }

    private static String escapeTableName(String name) {
        return name.replace(' ', '_').replaceAll("'", "\\\'").replaceAll("\"", "\\\"");
    }

    public JSONObject getAllRecords(String name, JSONObject search) {
        name = escapeTableName(name);
        try {
            PreparedStatement stmt = conn.prepareStatement(
                    "SELECT DATA, RELEVANCE FROM  \"" + name + "\" "  +
                            "WHERE NAME = ? AND AFFILIATION = ?"
            );
            stmt.setString(1, search.getString("fullName"));
            if (search.containsKey("affiliation")) stmt.setString(2, search.getString("affiliation"));
            else stmt.setString(2, "");
            ResultSet rs = stmt.executeQuery();
            JSONArray pos = new JSONArray();
            JSONArray neg = new JSONArray();
            while (rs.next()) {
                if (rs.getBoolean("RELEVANCE")) {
                    pos.add(
                            JSONObject.fromObject(rs.getObject("DATA"))
                    );
                } else {
                    neg.add(
                            JSONObject.fromObject(rs.getObject("DATA"))
                    );
                }
            }
            JSONObject ret = new JSONObject();
            ret.put("positive", JSONArray.fromObject(pos));
            ret.put("negative", JSONArray.fromObject(neg));
            return ret;
        } catch (SQLException e) {
            e.printStackTrace();
            JSONObject ret = new JSONObject();
            ret.put("positive", new JSONArray());
            ret.put("negative", new JSONArray());
            return ret;
        }
    }

    public void insertAllRecords(String name, JSONObject search, JSONObject data) {
        name = escapeTableName(name);
        JSONArray section = data.getJSONArray("positive");
        try {
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO \"" + name + "\" " +
                    "(NAME, AFFILIATION, DATA, RELEVANCE) VALUES (? , ? , ? , ?)");
            for (Object record : section) {
                stmt.setString(1, search.getString("fullName"));
                if (search.containsKey("affiliation")) stmt.setString(2, search.getString("affiliation"));
                else stmt.setString(2, "");
                stmt.setString(3, record.toString());
                stmt.setBoolean(4, true);
                stmt.executeUpdate();
            }
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        section = data.getJSONArray("negative");
        try {
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO \"" + name + "\" " +
                    "(NAME, AFFILIATION, DATA, RELEVANCE) VALUES (? , ? , ? , ?)");
            for (Object record : section) {
                stmt.setString(1, search.getString("fullName"));
                if (search.containsKey("affiliation")) stmt.setString(2, search.getString("affiliation"));
                else stmt.setString(2, "");
                stmt.setString(3, record.toString());
                stmt.setBoolean(4, false);
                stmt.executeUpdate();
            }
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertKW(JSONObject search, String word, boolean positive) {
        try {
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO \"" + Constants.kwTableName + "\" " +
                    "(NAME, AFFILIATION, WORD, POSITIVE) VALUES (? , ? , ? , ?)");
            stmt.setString(1, search.getString("fullName"));
            if (search.containsKey("affiliation")) stmt.setString(2, search.getString("affiliation"));
            else stmt.setString(2, "");
            stmt.setString(3, word);
            stmt.setBoolean(4, positive);
            stmt.executeUpdate();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void deleteKW(JSONObject search, String word) {
        try {
            PreparedStatement stmt = conn.prepareStatement("DELETE FROM \"" + Constants.kwTableName + "\" " +
                    "WHERE NAME = ? AND AFFILIATION = ? AND WORD = ?");
            stmt.setString(1, search.getString("fullName"));
            if (search.containsKey("affiliation")) stmt.setString(2, search.getString("affiliation"));
            else stmt.setString(2, "");
            stmt.setString(3, word);
            stmt.executeUpdate();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public JSONObject getKWs(JSONObject search) {
        ArrayList<String> pos = new ArrayList<String>();
        ArrayList<String> neg = new ArrayList<String>();
        try {
            PreparedStatement stmt = conn.prepareStatement(
                    "SELECT WORD, POSITIVE FROM \"" + Constants.kwTableName + "\" " +
                    "WHERE NAME = ? AND AFFILIATION = ?"
            );
            stmt.setString(1, search.getString("fullName"));
            if (search.containsKey("affiliation")) stmt.setString(2, search.getString("affiliation"));
            else stmt.setString(2, "");
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                if (rs.getBoolean("POSITIVE")) {
                    pos.add(rs.getString("WORD"));
                } else {
                    neg.add(rs.getString("WORD"));
                }
            }
            JSONObject ret = new JSONObject();
            ret.put("positive", JSONArray.fromObject(pos));
            ret.put("negative", JSONArray.fromObject(neg));
            return ret;
        } catch (SQLException e) {
            e.printStackTrace();
            JSONObject ret = new JSONObject();
            ret.put("positive", new JSONArray());
            ret.put("negative", new JSONArray());
            return ret;
        }
    }

}
