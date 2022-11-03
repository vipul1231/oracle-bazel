package com.example.snowflakecritic.scripts;

import com.example.core2.ConnectionParameters;
import com.example.snowflakecritic.SnowflakeSource;
import com.example.snowflakecritic.SnowflakeSourceCredentials;
import com.example.snowflakecritic.SnowflakeSourceWithSession;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.TimeZone;

public class SnowflakeConnectorScriptRunner {

    public static long DEFAULT_ROW_COUNT = 1000L;

    public static void main(String[] args) {
        if (args.length < 2) {
            printUsageAndExit();
        }

        // Generate a "process id"
        String processId = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(LocalDateTime.now());


        printSectionTitle("begin " + args[0]);

        try {
            Class<?> scriptKlass =
                    Class.forName(SnowflakeConnectorScriptRunner.class.getPackage().getName() + "." + args[0]);

            if (!BaseScriptRunner.class.isAssignableFrom(scriptKlass)) {
                throw new IllegalArgumentException(scriptKlass.getName() + " must extend BaseScriptRunner.");
            }

            File connectorDir = new File(args[1]);

            if (!connectorDir.exists() || !connectorDir.isDirectory()) {
                System.err.println(
                        "\n[ERROR] Connector directory doesn't exist or is not a directory: " + connectorDir);
                System.err.println();
                printUsageAndExit();
            }

            printSectionTitle(
                    "|   begin "
                            + args[0]
                            + "\n|\n|   connector dir: "
                            + connectorDir.toString()
                            + "\n|   processId: "
                            + processId);

            JsonFileHelper jsonFileHelper = new JsonFileHelper(connectorDir, processId);

            System.setProperty("work.dir", args[1]);

            if (args.length > 3) {
              System.setProperty("application.mode", args[3]);
            }

            SnowflakeSourceCredentials credentials = jsonFileHelper.loadCredentials();

            if (args.length > 2) {
                credentials.password = args[2];
            }

            if (credentials.password == null || credentials.password.isEmpty()) {
                System.out.println("\nError: Missing password\n");
                printUsageAndExit();
            }

            try (SnowflakeSource snowflakeSource =
                         new SnowflakeSourceWithSession(
                                 credentials, new ConnectionParameters(null, "PUBLIC", TimeZone.getDefault()))) {
                ((BaseScriptRunner)
                        scriptKlass
                                .getConstructor(SnowflakeSource.class, JsonFileHelper.class)
                                .newInstance(snowflakeSource, jsonFileHelper))
                        .run();
            }
        } catch (IOException e) {
            System.err.println(e);
            System.out.println(
                    "Make sure to create a credentials.json file in your data directory. Use the following as a template:");
            printExampleCredentialsFile();
        } catch (Exception e) {
            System.err.println(e);
        } finally {
            printSectionTitle("end " + args[0]);
        }

        // Check for threads that may block exit
        checkForBlockingUserThreads();
    }

    private static void printSectionTitle(String title) {
        System.out.println(
                "--------------------------------------------------\n"
                        + title
                        + "\n--------------------------------------------------");
    }

    private static void printUsageAndExit() {
        System.out.println(
                "Usage: bazel run //integrations/snowflake:script_runner -- <script-class-name> <path-to-data-dir> [<password>]"
                        + "\n   E.g.   bazel run //integrations/snowflake:script_runner -- GetSystemTime ~/testing_playground/test01 'mypassword'"
                        + "\n   Password is optional: you can also specify the password in the credentials.json file."
                        + "\n   There must be a file named credentials.json in this directory. Use the following as a template:");

        printExampleCredentialsFile();
        System.exit(0);
    }

    private static void printExampleCredentialsFile() {
        System.out.println(
                "{\n"
                        + "   \"host\":\"example.snowflakecomputing.com\",\n"
                        + "   \"port\":443,\n"
                        + "   \"database\":\"EXAMPLE_CONNECTOR_DEV\",\n"
                        + "   \"user\":\"EXAMPLE_CONNECTOR_USER\",\n"
                        + "   \"role\":\"EXAMPLE_CONNECTOR_ROLE\"\n"
                        + "}");

        System.out.println("\nYou may also add the optional \"password\" field and optional \"updateMethod\" field");
        System.out.println("updateMethod values are \"ibf\" (default) or \"TIME_TRAVEL\"");
        System.out.println();
    }

    static void checkForBlockingUserThreads() {
        System.out.println("==== THREAD REPORT ====");
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        int userThreadCount = 0;
        for (Thread t : threadSet) {
            if (t != Thread.currentThread() && t.isAlive()) {
                if (!t.isDaemon()) {
                    ++userThreadCount;
                }
                System.out.println((t.isDaemon() ? "Daemon " : "User ") + t);
            }
        }

        if (userThreadCount > 0) {
            System.exit(0);
        }
    }
}
