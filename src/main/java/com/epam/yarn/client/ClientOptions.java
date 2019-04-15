package com.epam.yarn.client;

public enum ClientOptions {

    APP_NAME ("appname", "HelloYarn"),
    PRIORITY ("priority", "0"),
    QUEUE ("queue", "default"),
    MASTER_MEMORY ("master_memory", "32"),
    MASTER_VCORES ("master_vcores", "1"),
    TIMEOUT ("timeout", "60000"),
    JAR ("jar", ""),
    NUM_CONTAINERS ("num_containers", "1"),
    HELP ("help", ""),
    CONTAINER_MEMORY ("container_memory", "32"),
    CONTAINER_VCORES ("container_vcores", "1");

    private String name;
    private String defaultValue;

    ClientOptions(String name, String defaultValue) {
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
