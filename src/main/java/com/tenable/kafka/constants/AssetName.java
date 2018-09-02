package com.tenable.kafka.constants;

public enum AssetName {
    ONE("rhel5x86.target.tenablesecurity.com"),
    TWO("solaris10.target.tenablesecurity.com"),
    THREE("VCENTER");

    private final String text;

    AssetName(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
