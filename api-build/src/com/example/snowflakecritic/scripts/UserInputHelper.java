package com.example.snowflakecritic.scripts;

import java.io.IOException;

public class UserInputHelper {

    private static final String HR = "===============================================================================";

    public static boolean getYesOrNo(String prompt) throws IOException {
        prompt = prompt + "  [Y/n]: ";
        while (true) {
            print(TextColor.YELLOW, prompt);
            byte[] input = new byte[4];
            System.in.read(input);
            String inputString = new String(input).trim();
            if ("Y".equals(inputString)) {
                return true;
            }

            if ("n".equals(inputString)) {
                return false;
            }

            System.err.println(inputString + " is not a valid choice.");
        }
    }

    public static void printHeader(String msg) {
        String rule = msg.length() < HR.length() ? HR.substring(0, msg.length()) : HR;

        println(TextColor.BLUE, "\n" + rule);
        println(TextColor.CYAN, msg + "\n");
    }

    public static Object getUserChoice(String prompt, Object... choices) throws IOException {
        int numberOfChoices = 0;
        String choiceString = prompt + " [";
        for (Object label : choices) {
            if (numberOfChoices > 0) {
                choiceString += " ";
            }
            ++numberOfChoices;
            choiceString += String.format("%d=%s", numberOfChoices, label);
        }

        choiceString += "]: ";
        newLine();

        while (true) {
            print(TextColor.CYAN, choiceString);
            byte[] input = new byte[4];
            System.in.read(input);
            String inputString = new String(input).trim();
            try {
                int choice = Integer.parseInt(inputString);

                if (choice > 0 && choice <= numberOfChoices) {
                    return choices[choice - 1];
                }

                throw new Exception("Bad choice: Try again");
            } catch (Exception ex) {
                printBadUserChoice(inputString);
            }
        }
    }

    private static void printBadUserChoice(String message) {
        printWarning(message + " is not a valid choice.");
    }

    public static void printWarning(String message) {
        println(TextColor.MAGENTA, message.toUpperCase());
    }

    public enum TextColor {
        BLACK("\u001B[30m"),
        RED("\u001B[31m"),
        GREEN("\u001B[32m"),
        YELLOW("\u001B[33m"),
        BLUE("\u001B[34m"),
        MAGENTA("\u001B[35m"),
        CYAN("\u001B[36m"),
        WHITE("\u001B[37m");

        private String ansiCode;

        TextColor(String ansi) {
            this.ansiCode = ansi;
        }

        public String getAnsiCode() {
            return ansiCode;
        }
    }

    private static final String ANSI_RESET = "\u001B[0m";

    public static void println(TextColor color, String text) {
        System.out.println(color.getAnsiCode() + text + ANSI_RESET);
    }

    public static void print(TextColor color, String text) {
        System.out.print(color.getAnsiCode() + text + ANSI_RESET);
    }

    public static void newLine() {
        System.out.println();
    }
}
