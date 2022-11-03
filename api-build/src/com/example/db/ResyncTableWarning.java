package com.example.db;

import com.example.core.PathName;
import com.example.core.warning.Warning;
import com.example.oracle.warnings.NoTableAccessWarning;
import com.example.oracle.warnings.WarningType;

import java.util.List;

public class ResyncTableWarning implements Warning {
    public static final WarningType<ResyncTableWarning> TYPE =
            new WarningType<ResyncTableWarning>() {
                @Override
                public String title() {
                    return "Resync Table Warning";
                }

                @Override
                public String type() {
                    return "resync_table_warning";
                }

                @Override
                public PathName keyTail(NoTableAccessWarning warning) {
                    return null;
                }

                @Override
                public Class<NoTableAccessWarning> instanceClass() {
                    return null;
                }

                @Override
                public String renderBody(List<NoTableAccessWarning> warnings) {
                    return null;
                }

                //                @Override
                public PathName keyTail(ResyncTableWarning warning) {
//                    return PathName.of(warning.group, warning.schemaPrefix, warning.table);
                    return null;
                }

//                @Override
//                public Class<ResyncTableWarning> instanceClass() {
//                    return ResyncTableWarning.class;
//                }

//                @Override
//                public String renderBody(List<NoTableAccessWarning> warnings) {
//                    return null;
//                }

//                @Override
//                public String renderBody(List<ResyncTableWarning> warnings) {
//                    return null;
//                    return Markdowns.markdownVelocity(
//                            Paths.get("/integrations/db/resources/warnings/ResyncTableWarning.md.vm"),
//                            ImmutableMap.of("warnings", warnings));
//                }
            };

    public String group;
    public String schemaPrefix;
    public String table;
    public String reason;

    public ResyncTableWarning() {}

    public ResyncTableWarning(String group, String schemaPrefix, String table, String reason) {
        this.group = group;
        this.schemaPrefix = schemaPrefix;
        this.table = table;
        this.reason = reason;
    }

    @Override
    public WarningType<ResyncTableWarning> getType() {
        return TYPE;
    }
}
