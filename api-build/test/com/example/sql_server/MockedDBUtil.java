package com.example.sql_server;


import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 9/24/2021<br/>
 * Time: 11:49 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class MockedDBUtil {

    private static Map<String, List<Action>> historyMap = new HashMap<>();
    private static Stack<String> queryStack = new Stack<>();

    static class Action {
        public Action() {
        }

        public Action(Instant time, String source) {
            this.time = time;
            this.source = source;
        }

        private Instant time;
        private String source;

        public Instant getTime() {
            return time;
        }
    }

    public static boolean isHistoryAvl() {
        return !historyMap.isEmpty();
    }

    public static boolean isHistoryAvl(String tbl) {
        if (!historyMap.containsKey(tbl)) {
            return false;
        }

        return !historyMap.get(tbl).isEmpty();
    }

    public static void processRecord(String tbl) {
        if (!historyMap.containsKey(tbl)) {
            return;
        }

        if (historyMap.get(tbl).isEmpty()) {
            return;
        }

        // remove the last element of the list
        historyMap.get(tbl).remove(historyMap.get(tbl).size() - 1);
    }

    public static Timestamp getTimestamp(String tbl) {
        if (!historyMap.containsKey(tbl)) {
            return null;
        }

        if (historyMap.get(tbl).isEmpty()) {
            return null;
        }

        Action lastAction = historyMap.get(tbl).get(historyMap.get(tbl).size() - 1);
        // remove the last element of the list
        historyMap.get(tbl).remove(historyMap.get(tbl).size() - 1);
        if (lastAction != null) {
            return Timestamp.from(lastAction.getTime());
        }

        return null;
    }

    public static void updateTable(String table, String source) {
        if (historyMap.containsKey(table)) {
            historyMap.get(table).add(new Action(Instant.now(), source));
        } else {
            List<Action> actions = new ArrayList<>();
            actions.add(new Action(Instant.now(), source));
            historyMap.put(table, actions);
        }
    }

    public static void addQuery(String query) {
        queryStack.push(query);
    }

    public static String getQuery() {
        if (queryStack.isEmpty()) {
            return null;
        }

        return queryStack.pop();
    }
}