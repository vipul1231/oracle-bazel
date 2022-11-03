package com.example.ibf;

import com.google.common.collect.ImmutableMap;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;


import java.io.StringWriter;
import java.util.*;

public class IbfTemplate {
    public enum Output {
        TEST("test"),
        INVERTIBLE_BLOOM_FILTER("invertibleBloomFilter"),
        COMMON_INVERTIBLE_BLOOM_FILTER("commonInvertibleBloomFilter"),
        COMMON_INTERMEDIATE_ROWS("commonIntermediateRows"),
        PRIMARY_KEY_STRATA_ESTIMATOR("primaryKeyStrataEstimator"),
        SETUP("setup");

        private final String macroName;

        Output(String macroName) {
            this.macroName = macroName;
        }

        public String getMacroName() {
            return macroName;
        }
    }

    private static boolean reduceWhitespace = true;
    private static boolean obfuscate = true;

    private static final Random rand = new Random();

    /**
     * Renders a macro from a Velocity template file that contains a SQL query
     *
     * @param dbTemplateFilename
     * @param macro
     * @param params
     * @return
     */
    public static String get(String dbTemplateFilename, Output macro, Map<String, Object> params) {
        Map<String, Object> combinedParams =
                ImmutableMap.<String, Object>builder()
                        .putAll(params)
                        .put("output", "#" + macro.getMacroName() + "()")
                        .build();

        String text =
                template("output_handler.vm", combinedParams, Arrays.asList(dbTemplateFilename));

        return obfuscate ? obfuscate(text) : text;
    }

    private static String template(String name, Map<String, Object> params, List<String> macroLibraries) {
        VelocityEngine ve = new VelocityEngine();
        ve.setProperty(RuntimeConstants.RESOURCE_LOADER, "file");
        ve.setProperty(RuntimeConstants.FILE_RESOURCE_LOADER_PATH, System.getProperty("work.dir"));
        ve.setProperty(RuntimeConstants.FILE_RESOURCE_LOADER_CACHE, "false");
        System.out.println(System.getProperty("work.dir"));
        //ve.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
        //ve.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
        StringWriter writer = new StringWriter();
        org.apache.velocity.Template template = ve.getTemplate(name);
        VelocityContext context = new VelocityContext();
        params.forEach(context::put);
        template.merge(context, writer);
        return writer.toString();
    }

    private static String obfuscate(String query) {
        List<String> tokensToObfuscate = new ArrayList<>();
        tokensToObfuscate.add("_ibf_hash_index");
        tokensToObfuscate.add("_ibf_row_hash");
        for (int i = 0; i < 99; i++) {
            tokensToObfuscate.add("_ibf_column" + i);
        }

        for (String token : tokensToObfuscate) {
            String replacementToken = getRandomHexString(16);

            query = query.replace(token, replacementToken);
        }

        if (reduceWhitespace) {
            query = query.replaceAll("\\s+", " ");
        }

        return query;
    }

    private static String getRandomHexString(int numchars) {
        StringBuilder sb = new StringBuilder();
        // must start with a character
        sb.append("abcdef".charAt(rand.nextInt(6)));
        while (sb.length() < numchars) {
            sb.append(Integer.toHexString(rand.nextInt()));
        }

        return sb.substring(0, numchars);
    }

    public static void setReduceWhitespace(boolean val) {
        reduceWhitespace = val;
    }

    public static void setObfuscate(boolean val) {
        IbfTemplate.obfuscate = val;
    }
}
