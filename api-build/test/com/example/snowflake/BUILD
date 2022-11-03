load("@rules_java//java:defs.bzl", "java_binary")

java_test(
    name = "SnowflakeClientSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflake.SnowflakeClientSpec",
    visibility = ["//visibility:public"],
    deps = [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        "//dockerized/sql_server:main",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
    ],
)

java_test(
    name = "SnowflakeImporterSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflake.SnowflakeImporterSpec",
    visibility = ["//visibility:public"],
    deps = [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        "//dockerized/sql_server:main",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
    ],
)

java_test(
    name = "SnowflakeStreamsSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflake.SnowflakeStreamsSpec",
    visibility = ["//visibility:public"],
    deps = [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        "//dockerized/sql_server:main",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
    ],
)

java_test(
    name = "SnowflakeServiceSpec",
    timeout = "eternal",  # 60 minutes
    srcs = glob(["**/*.java"]),
    resources = glob(["resources/**"]),
    test_class = "com.example.snowflake.SnowflakeServiceSpec",
    visibility = ["//visibility:public"],
    deps = [
        "//api-build/src/com/example:ProjectLibs",
        "//dockerized/common:main",
        "//dockerized/sql_server:main",
        "//fire:main",
        "//metal:main",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
    ],
)
