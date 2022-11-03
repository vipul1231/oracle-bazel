package com.example.ibf.exception;

import java.io.IOException;

public class IbfException extends IOException {
    IbfException(String message) {
        super(message);
    }
}
