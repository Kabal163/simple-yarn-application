package com.epam.yarn.master;

public enum  MasterOptions {
    APP_ATTEMPT_ID ("app_attempt_id", ""),
    SHELL_ENV ("shell_env", ""),
    CONTAINER_MEMORY ("container_memory", "32"),
    CONTAINER_VCORES ("container_vcores", "1"),
    NUM_CONTAINERS ("num_containers", "1"),
    PRIORITY ("priority", "0"),
    HELP ("help", "");

    private String name;
    private String defaultValue;

    MasterOptions(String name, String defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return name;
    }

    public String getDefault() {
        return defaultValue;
    }
}
