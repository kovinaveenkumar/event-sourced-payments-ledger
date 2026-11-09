package com.ledger.command.bdd;

import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasspathResource;
import org.junit.platform.suite.api.Suite;

import static io.cucumber.junit.platform.engine.Constants.GLUE_PROPERTY_NAME;
import static io.cucumber.junit.platform.engine.Constants.PLUGIN_PROPERTY_NAME;

/** Runs the Cucumber scenarios in src/test/resources/features when I run mvn test. */
@Suite
@IncludeEngines("cucumber")
@SelectClasspathResource("features")
@ConfigurationParameter(key = GLUE_PROPERTY_NAME, value = "com.ledger.command.bdd")
@ConfigurationParameter(key = PLUGIN_PROPERTY_NAME, value = "summary, html:target/cucumber-report.html")
public class RunCucumberTest {
}
