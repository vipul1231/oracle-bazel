package com.example.oracle;

import com.example.core.TableRef;
import com.example.logger.ExampleLogger;
import com.example.oracle.exceptions.QuickBlockRangeFailedException;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static com.example.oracle.Constants.SYSTEM_SCHEMAS;
import static com.example.oracle.Util.doubleQuote;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/8/2021<br/>
 * Time: 9:23 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class SqlUtil {

    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    public static String buildSchemaFilter(Set<TableRef> selected) {
        if (!selected.isEmpty()) {
//            String userSchema =
//                    Joiner.on(",").join(selected.stream().map(s -> "'" + s.schema + "'").collect(Collectors.toSet()));
            String userSchema = "";
            return "IN (" + userSchema + ") ";
        } else {
            return "NOT IN (" + SYSTEM_SCHEMAS + ") ";
        }
    }

    public static Set<TablespaceType> getTablespaceTypes(TableRef table) throws QuickBlockRangeFailedException {
//        return ImmutableSet.copyOf(getTablespaceTypes(getTablespaces(table)).values());
        return null;
    }

    /**
     * convert collumns into a comma seperated string
     * @param columns
     * @return
     */
    public static String getSelectCols(List<OracleColumn> columns) {
        return columns.stream().map(c -> doubleQuote(c.name)).collect(Collectors.joining(","));
    }

    /* This method takes BigDecimal values and tries to fit them into smaller type containers. This is so Core doesn't make all numeric columns into Float columns. */
    static Object minimizeType(BigDecimal value) {
        try {
            if (value.precision() >= OracleType.DEFAULT_PRECISION) {
                // for really big numbers, we need to send them to core as a String. Then core will type-promote the
                // column.
                // doing .stripTrailingZeros().toPlainString() "normalizes" the values (0.100 and 0.10 become 0.1) and
                // spits them out without scientific notation. Since the column type is going to be Unknown, this lets
                // core decide how to adjust the column to fit the value.
                return value.stripTrailingZeros().toPlainString();
            }

            // If we leave non-fractional (round) numbers as BigDecimal, core will make the column a float. We only want
            // to make the column a float if it actually contains a fraction value.
            if (value.scale() == 0) {
                // Round numbers
                if (value.precision() < 5) {
                    return value.shortValueExact();
                } else if (value.precision() < 10) {
                    return value.intValueExact();
                } else if (value.precision() < 19) {
                    return value.longValueExact();
                }
                // BigInteger is the next smalled type, but core turns them into BigDecimal anyway
            }
        } catch (ArithmeticException ex) {
            LOG.log(Level.INFO, "Failed to shrink BigDecimal to a smaller format", ex);
        }
        // doing .stripTrailingZeros().toPlainString() "normalizes" the values (0.100 and 0.10 become 0.1) and spits
        // them out without scientific notation. Since the column type is going to be Unknown, this lets core decide how
        // to adjust the column to fit the value.
        return new BigDecimal(value.stripTrailingZeros().toPlainString());
    }

    public static Optional<Long> scnXHoursBeforeScn(long hoursAgo, long scn) {
        String query =
                "SELECT timestamp_to_scn("
                        + "scn_to_timestamp("
                        + scn
                        + ") - interval '"
                        + hoursAgo
                        + "' hour) as SCN FROM dual";

        Transactions.RetryFunction<Optional<Long>> action =
                (c) -> {
                    try (Statement stmt = c.createStatement();
                         ResultSet rs = stmt.executeQuery(query)) {
                        if (!rs.next()) {
                            return Optional.empty();
                        }
                        return Optional.of(rs.getBigDecimal("SCN").longValue());
                    }
                };

        return ConnectionFactory.getInstance().retry(
                "scnXHoursBeforeScn",
                t -> new RuntimeException("Error in getting SCN from " + hoursAgo + " hours before " + scn, t),
                action);
    }
}