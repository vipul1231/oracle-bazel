package com.example.sql_server;

//import com.example.core.OnboardingCategory;
//import com.example.core.OnboardingSettings;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.example.sql_server.SqlServerServiceType.DYNAMICS_365_FO;

public class Dynamics365FOService extends SqlServerServiceAzure {

    public Dynamics365FOService() {
        serviceType = DYNAMICS_365_FO;
    }

    @Override
    public Path largeIcon() {
        return Paths.get("/integrations/dynamics365/resources/logo.svg");
    }

    @Override
    public URI linkToDocs() {
        return URI.create("/docs/applications/microsoft-dynamics365-fo/setup-guide");
    }

//    @Override
//    public OnboardingSettings onboardingSettings() {
//        return new OnboardingSettings(OnboardingCategory.Databases, OnboardingCategory.FinanceAndOpsAnalytics);
//    }
}
