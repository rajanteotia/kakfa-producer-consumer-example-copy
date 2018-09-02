package com.tenable.kafka.constants;

public enum Fqdn {
    ONE("wsus.ad.demo.io"),
    TWO("adam.ad.demo.io"),
    THREE("solaris9.dc.demo.io"),
    FOUR("winserver2012.dc.demo.io"),
    FIVE("esxi51.dc.demo.io"),
    SIX("win2k8r2.dc.demo.io"),
    SEVEN("slackware13.dc.demo.io"),
    EIGHT("esxi6.dc.demo.io"),
    NINE("ubuntu1710-desktop.dc.demo.io"),
    TEN("sharepoint2016.dc.demo.io"),
    ELEVEN("debian832.dc.demo.io"),
    TWELVE("jboss-630.dc.demo.io"),
    THIRTEEN("osx1010.dc.demo.io"),
    FOURTEEN("win1032.dc.demo.io"),
    FIFTEEN("sccm.dc.demo.io"),
    SIXTEEN("vcenter.dc.demo.io"),
    SEVENTEEN("solaris11.dc.demo.io"),
    EIGHTEEN("splunk.demo.io"),
    NINETEEN("rita.ad.demo.io"),
    TWNETY("denise.ad.demo.io"),
    TWENTY_ONE("adam-linux.demo.io"),
    TWENTY_TWO("ids.demo.io");

    private final String text;

    Fqdn(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
