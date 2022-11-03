package com.example.snowflakecritic.scripts;

public class ScriptArguments {
    private String[] args;

    public ScriptArguments(String[] args) {
        this.args = args;
    }

    public String argumentAt(int index) {
        return args[index];
    }
}
