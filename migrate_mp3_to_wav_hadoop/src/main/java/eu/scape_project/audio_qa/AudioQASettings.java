package eu.scape_project.audio_qa;

/**
 * A class containing constants for use elsewhere in the code.
 * eu.scape_project
 * User: baj@statsbiblioteket.dk
 * Date: 8/6/13
 * Time: 11:37 AM
 */
public class AudioQASettings {

    /**
     * String constants.
     */
    public static final String MPG321 = "mpg321";
    public static final String SLASH = "/";
    public static final String UNDERSCORE = "_";
    public static final String DOTLOG = ".log";
    public static final String DOTWAV = ".wav";

    /**
     * Default workflow output directory on HDFS.
     */
    public static String OUTPUT_DIR = "hdfs:///user/bolette/output/test-output/MigrateMp3ToWav/";
            //TODO fix local hadoop working directory bolette-ubuntu
            //"/net/zone1.isilon.sblokalnet/ifs/data/hdfs/user/scape/mapred-write/test-output/MigrateMp3ToWav/";
    public static String DEFAULT_JOBID = "test-default-jobid";
}
