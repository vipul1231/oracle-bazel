What you do need are the credentials for a snowflake account.

To run a script use the following command template

bazel run //api-build/test/com/example/snowflakecritic:script_runner -- GetSystemTime ~/Desktop/qx/oracle-api-build/api-build/src/com/example/snowflakecritic/scripts 'Newyork12345678'

    bazel run //integrations/snowflake:script_runner -- SCRIPT_CLASS_NAME PATH_TO_DATA_FOLDER PASSWORD MODE

*Note: MODE = script-runner 
SCRIPT_CLASS_NAME: the name of a class in the package.

PATH_TO_DATA_FOLDER: Path to a directory on your workstation that contains data files necessary for the given script 
you are running. Minimally it requires a `credentials.json` file:

    {
        "host":"example.snowflakecomputing.com",
        "port":443,
        "database":"EXAMPLE_CONNECTOR_DEV",
        "user":"EXAMPLE_CONNECTOR_USER",
        "role":"EXAMPLE_CONNECTOR_ROLE"
    }


If you need different sets of credentials then create separate directories with their own files.

PASSWORD: pass the password

If you get this error:

    java.lang.RuntimeException: net.snowflake.client.jdbc.SnowflakeSQLException: 
    User temporarily locked. Try again later, or contact your local system administrator.

Then you may be passing the password incorrectly. Also you will need to unlock the user by
going to snowflake.com, logging on with the credentials
and then running this command to reset the user:

    alter user example_CONNECTOR_USER set mins_to_unlock = 0;

## Useful Bash Scripts
Here are some handy bash scripts to make calling script runner easier. Don't forget `chmod 755 *.sh`. Change
the `~/example/engineering` if necessary.

### runscript.sh
Just create this in your test connector directory (the one with the credentials.json file) and

    #!/bin/bash
    currentDir=$(PWD)
    pushd ~/example/engineering
    bazel run //integrations/snowflake:script_runner -- $1 $currentDir 'password'
    popd

Provide 'password' if you don't provide it in the credentials.json file.

if you have multiple test connectors in subdirectories you could do something like this:

    #!/bin/bash
    connectorDir=$(PWD)/$1
    pushd ~/example/engineering
    bazel run //integrations/snowflake:script_runner -- $2 $connectorDir
    popd


Which you can call like `./runscript.sh testconnector1 RunSync` from the parent directory
of the directory named `testconnector1`. (This sample script assumes that the password is in
the credentials.json file in the test connector dir).

# Script Classes

## GetStandardConfig

This tests our connector's service class (and informer, etc) to retrieve the metadata and construct a StandardConfig.

This is interactive and it will prompt you whether you want to include each table.

It prints the StandardConfig to the console and optionally allows to you save it.

## GetSchema
This extends GetStandardConfig and also retrieves and display column information, which
is saved in the standardConfig.json file. Invoke this if we want to see the column metadata
or if you intend to exclude any specific columns. You'll have to manually edit the standardConfig.json
after saving in order to manually exclude columns.

## RunSync
Invoke GetStandardConfig before this at least once so that you can make sure
to exclude any tables you do not want to sync.

This instantiates the Service object and calls the update method. This runs the entire sync
and is stateful: it loads the current state.json file (if it exists) and updates it whenever
checkpoint is called. This allows you to sync (initial import), modify some tables on the source,
and sync again to get incremental updates.

Some handy shell scripts you could use: just create these files directly in your test connector directory
(the one that contains a credentials.json file).

## ReSync
Invoke GetStandardConfig before this at least once so that you can make sure
to exclude any tables you do not want to sync.

Resets the connector state just like a real full connector resync request, then
runs a full sync. This extends RunSync.

## GetSystemTime

Simple test: connects to the snowflake account and queries the system for its current time.

## SnowflakeTableImporterScript

Handy for testing just the importer. Uses the `updateMethod` from the `credentials.json` file
(or defaults to `Ibf`).

## IbfBenchmarkRunner

This connects to the snowflake account and uses the Informer to get a list of tables. It displays the
list of tables and then a prompt for which table you want to perform the Ibf IBF Encode benchmark test against.