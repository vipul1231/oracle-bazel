package com.example.snowflakecritic.setup;

import com.example.core.SetupTest;
import com.example.forms.Form;
import com.example.oracle.SetupForm;
import com.example.oracle.WizardContext;
import com.example.snowflakecritic.SnowflakeServiceType;
import com.example.snowflakecritic.SnowflakeSourceCredentials;

import java.net.URI;
import java.util.List;

public class SnowflakeConnectorSetupForm implements SetupForm<SnowflakeSourceCredentials> {

    private static final String SETUP_URI = "/docs/applications/snowflake/setup-guide";

    private final WizardContext wizardContext;
    private final SnowflakeServiceType serviceType;

    public SnowflakeConnectorSetupForm(WizardContext wizardContext, SnowflakeServiceType serviceType) {
        this.wizardContext = wizardContext;
        this.serviceType = serviceType;
    }

//    @Override
    public Form form(SnowflakeSourceCredentials snowflakeCredentials) {
        Form.Builder form = new Form.Builder(snowflakeCredentials);

        return form.build();
    }

//    @Override
    public Form form() {
        return form(new SnowflakeSourceCredentials());
    }

    @Override
    public URI instructions() {
        switch (serviceType) {
            case SNOWFLAKE:
            default:
                return URI.create(SETUP_URI);
        }
    }

//    @Override
    public List<SetupTest<SnowflakeSourceCredentials>> tests() {
        return null;
    }
}
