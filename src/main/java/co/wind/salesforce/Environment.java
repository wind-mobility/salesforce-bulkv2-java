package co.wind.salesforce;

public enum Environment {

    SANDBOX("https://test.salesforce.com/"),
    PRODUCTION("https://login.salesforce.com/");

    public final String url;

    Environment(String url) {
        this.url = url;
    }

    public String getAuthUrl() {
        return url + "services/oauth2/token";
    }

    public String getSoapUrl() {
        return url + "services/Soap/u/54.0";
    }
}
