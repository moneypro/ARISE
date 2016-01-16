package util;

public class Constants {

    public static final String fullSourceNameDelimiter = "__NICE_LITTLE_DELIMITER__";
    public static final String kwDelimiter = "__NICE_LITTLE_DELIMITER__";
    public static final String uniquekwDelimiter = "__NICE_LITTLE_UNIQUE_KW_DELIMITER__";
    public static final int localServerPortNumber = 8888;
    public static final int connectTimeout = 5000;  //  Timeout if connection has not been established after 5 seconds.
    public static final int readTimeout = 5000; //  Timeout if input has not been available after 5 seconds.
    public static final int sourceExecutionTimeout = 5000; //    Timeout if a source executed for more than 5 seconds without returning.
    public static final String[] kwIgnore = {"a", "an", "the", "is", "and", "of", "at", "from", "to", "or", "for",
            "in", "on", "onto", "into", "based", "with", "within", "journal", ",", ".", ":", ";", "-", "", " ", "  ",
            "   ", "    ", "\t", "\t\t", "\t\t\t", "la", "de", "da", "no", "et", "univ", "university", "college",
            "department", "dept", "school"};
    public static final String kwTableName = "KWS";

}
