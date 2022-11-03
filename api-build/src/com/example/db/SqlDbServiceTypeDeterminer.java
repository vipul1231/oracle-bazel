package com.example.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

// This only exists temporarily because not all DB connectors have moved over to the standard interface yet.
// This logic should exist in DbInformer#inferServiceType, and this interface should be removed.
public interface SqlDbServiceTypeDeterminer<ServiceT extends DbServiceType> {
    ServiceT determine(Connection connection);

    static boolean checkQueryResult(Connection connection, String query, Function<String, Boolean> check) {
        try (PreparedStatement ps = connection.prepareStatement(query);
                ResultSet results = ps.executeQuery()) {

            if (!results.next()) return false;

            return checkQueryResult(check, results, 1);
        } catch (SQLException e) {
            return false;
        }
    }

    static boolean checkQueryResult(Function<String, Boolean> check, ResultSet results, int index) {
        try {
            return check.apply(results.getString(index));
        } catch (SQLException e) {
            return false;
        }
    }

    static boolean checkQueryResult(Connection connection, String query) {
        return checkQueryResult(connection, query, result -> true);
    }
}
