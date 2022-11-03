package com.example.oracle.warnings;

import com.example.core.PathName;
import com.example.core.warning.Warning;
import com.example.donkey.Markdowns;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;

import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/18/2021<br/>
 * Time: 7:49 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class NoTableAccessWarning implements Warning {
    public String connectorName;
    public Set<NoTableAccessType> tablesMissingAccess;

    public NoTableAccessWarning() {}

    public NoTableAccessWarning(String connectorName, Set<NoTableAccessType> tablesMissingAccess) {
        this.connectorName = connectorName;
        this.tablesMissingAccess = ImmutableSortedSet.copyOf(tablesMissingAccess);
    }

    public static final WarningType<NoTableAccessWarning> TYPE =
            new WarningType<NoTableAccessWarning>() {
                @Override
                public String title() {
                    return "Connecting User Cannot Access needed tables";
                }

                @Override
                public String type() {
                    return "no_table_access";
                }

                @Override
                public PathName keyTail(NoTableAccessWarning warning) {
                    return PathName.of(
                            warning.tablesMissingAccess.stream().map(Enum::toString).collect(Collectors.joining(".")));
                }

                @Override
                public Class<NoTableAccessWarning> instanceClass() {
                    return NoTableAccessWarning.class;
                }

                @Override
                public String renderBody(List<NoTableAccessWarning> warnings) {
                    List<String> warningTexts =
                            warnings.stream()
                                    .map(
                                            w ->
                                                    "We need access to "
                                                            + w.tablesMissingAccess
                                                            .stream()
                                                            .map(Enum::toString)
                                                            .collect(Collectors.joining(", "))
                                                            + " on "
                                                            + w.connectorName)
                                    .collect(Collectors.toList());
                    return Markdowns.markdownVelocity(
                            Paths.get("/integrations/oracle/resources/warnings/NoTableAccess.md.vm"),
                            ImmutableMap.of("warningTexts", warningTexts));
                }
            };

    @Override
    public WarningType getType() {
        return TYPE;
    }
}