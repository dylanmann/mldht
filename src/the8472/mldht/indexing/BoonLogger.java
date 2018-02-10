package the8472.mldht.indexing;

public class BoonLogger {

    private BoonLogger() {

    }

    public static BoonLogger getLogger() {
        return logger;
    }

    private static boolean initialized = false;
    private static BoonLogger logger = new BoonLogger();


    public void log(CharSequence str) {
        StringBuilder s = new StringBuilder();
        s.append("[BOON]");
        s.append(str);
        System.out.println(s);
    }

    public static void initialize() {
//        logger.
    }
}