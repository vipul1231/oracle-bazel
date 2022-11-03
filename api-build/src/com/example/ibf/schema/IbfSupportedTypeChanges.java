package com.example.ibf.schema;

import com.example.core.annotations.DataType;

class IbfSupportedTypeChanges {

    private static boolean booleanChange(DataType newType) {
        switch (newType) {
            case String:
            case Short:
            case Int:
            case Long:
            case Float:
            case Double:
            case BigDecimal:
            case Json:
                return true;
            default:
                return false;
        }
    }

    private static boolean shortChange(DataType newType) {
        switch (newType) {
            case String:
            case Int:
            case Long:
            case Float:
            case Double:
            case BigDecimal:
            case Json:
                return true;
            default:
                return false;
        }
    }

    private static boolean intChange(DataType newType) {
        switch (newType) {
            case String:
            case Long:
            case Float:
            case Double:
            case BigDecimal:
            case Json:
                return true;
            default:
                return false;
        }
    }

    private static boolean longChange(DataType newType) {
        switch (newType) {
            case String:
            case Float:
            case Double:
            case BigDecimal:
            case Json:
                return true;
            default:
                return false;
        }
    }

    private static boolean floatChange(DataType newType) {
        switch (newType) {
            case String:
            case Json:
                return true;
            default:
                return false;
        }
    }

    private static boolean doubleChange(DataType newType) {
        switch (newType) {
            case String:
            case Json:
                return true;
            default:
                return false;
        }
    }

    private static boolean bigDecimalChange(DataType newType) {
        switch (newType) {
            case String:
            case Json:
                return true;
            default:
                return false;
        }
    }

    static boolean isTypeChangeSupported(DataType oldType, DataType newType) {
        switch (oldType) {
            case Boolean:
                return booleanChange(newType);
            case String:
                return newType.equals(DataType.Json);
            case Short:
                return shortChange(newType);
            case Int:
                return intChange(newType);
            case Long:
                return longChange(newType);
            case Float:
                return floatChange(newType);
            case Double:
                return doubleChange(newType);
            case BigDecimal:
                return bigDecimalChange(newType);
            case Instant:
            case LocalDate:
            case LocalDateTime:
            case Json:
                return newType.equals(DataType.String);
            case Unknown:
                return true;
            default:
                throw new RuntimeException("Unsupported type: " + oldType);
        }
    }
}
