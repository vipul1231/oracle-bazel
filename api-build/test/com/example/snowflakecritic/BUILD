load("@rules_java//java:defs.bzl", "java_binary")

COMMON_DEPS = [
    "@maven//:net_snowflake_snowflake_jdbc",
]

java_test(
    name = "SnowflakeConnectorServiceSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflakecritic.SnowflakeConnectorServiceSpec",
    visibility = ["//visibility:public"],
    deps = COMMON_DEPS + [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        #"//dockerized/sql_server:main",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_apache_commons_commons_lang3",
    ],
)

java_test(
    name = "SnowflakeTimeTravelTableImporterSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflakecritic.SnowflakeTimeTravelTableImporterSpec",
    visibility = ["//visibility:public"],
    deps = COMMON_DEPS + [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        #"//dockerized/sql_server:main",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_apache_commons_commons_lang3",
    ],
)

java_test(
    name = "SnowflakeScriptImportTableSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflakecritic.SnowflakeScriptImportTableSpec",
    visibility = ["//visibility:public"],
    deps = COMMON_DEPS + [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        #"//dockerized/sql_server:main",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_apache_commons_commons_lang3",
    ],
)

java_test(
    name = "SnowflakeSourceCredentialsSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflakecritic.SnowflakeSourceCredentialsSpec",
    visibility = ["//visibility:public"],
    deps = COMMON_DEPS + [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        #"//dockerized/sql_server:main",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_apache_commons_commons_lang3",
    ],
)

java_test(
    name = "SnowflakeIbfTableUpdaterSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflakecritic.SnowflakeIbfTableUpdaterSpec",
    visibility = ["//visibility:public"],
    deps = COMMON_DEPS + [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        #"//dockerized/sql_server:main",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_apache_commons_commons_lang3",
    ],
)

java_test(
    name = "SnowflakeUpdateScenarioSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflakecritic.SnowflakeUpdateScenarioSpec",
    visibility = ["//visibility:public"],
    deps = COMMON_DEPS + [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        #"//dockerized/sql_server:main",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_apache_commons_commons_lang3",
    ],
)

java_binary(
    name = "script_runner",
    srcs = glob(["**/*.java"]),
    main_class = "com.example.snowflakecritic.scripts.SnowflakeConnectorScriptRunner",
    resources = glob(["resources/**"]),
    visibility = ["//visibility:public"],
    deps = COMMON_DEPS + [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        #"//dockerized/sql_server:main",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:com_fasterxml_jackson_core_jackson_annotations",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_google_guava_guava",
        "@maven//:commons_collections_commons_collections",
    ],
)

java_test(
    name = "SnowflakeInformationSchemaDaoSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflakecritic.ibf.SnowflakeInformationSchemaDaoSpec",
    visibility = ["//visibility:public"],
    deps = COMMON_DEPS + [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        #"//dockerized/sql_server:main",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_apache_commons_commons_lang3",
    ],
)

java_test(
    name = "SnowflakeTestsQueryBuilderSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflakecritic.SnowflakeTestsQueryBuilderSpec",
    visibility = ["//visibility:public"],
    deps = COMMON_DEPS + [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        #"//dockerized/sql_server:main",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_apache_commons_commons_lang3",
    ],
)

java_test(
    name = "SnowflakeIbfAdapterSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflakecritic.ibf.SnowflakeIbfAdapterSpec",
    visibility = ["//visibility:public"],
    deps = COMMON_DEPS + [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        #"//dockerized/sql_server:main",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:com_fasterxml_jackson_core_jackson_annotations",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_google_guava_guava",
        "@maven//:commons_collections_commons_collections",
        "@maven//:org_apache_commons_commons_exec",
    ],
)

java_test(
    name = "SnowflakeSimpleTableImporterSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflakecritic.SnowflakeSimpleTableImporterSpec",
    visibility = ["//visibility:public"],
    deps = COMMON_DEPS + [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        #"//dockerized/sql_server:main",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:com_fasterxml_jackson_core_jackson_annotations",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_google_guava_guava",
        "@maven//:commons_collections_commons_collections",
        "@maven//:org_apache_commons_commons_exec",
    ],
)

java_test(
    name = "InvertibleBloomFilterSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflakecritic.ibf.InvertibleBloomFilterSpec",
    visibility = ["//visibility:public"],
    deps = COMMON_DEPS + [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        #"//dockerized/sql_server:main",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:com_fasterxml_jackson_core_jackson_annotations",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_google_guava_guava",
        "@maven//:commons_collections_commons_collections",
        "@maven//:org_apache_commons_commons_exec",
    ],
)

java_test(
    name = "InvertibleBloomFilterDiffSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflakecritic.ibf.InvertibleBloomFilterDiffSpec",
    visibility = ["//visibility:public"],
    deps = COMMON_DEPS + [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        #"//dockerized/sql_server:main",
        "//fire:main",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:com_fasterxml_jackson_core_jackson_annotations",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_google_guava_guava",
        "@maven//:commons_collections_commons_collections",
        "@maven//:org_apache_commons_commons_exec",
    ],
)
