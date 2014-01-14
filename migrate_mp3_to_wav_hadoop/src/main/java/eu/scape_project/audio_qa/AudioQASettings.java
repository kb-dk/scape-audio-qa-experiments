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
     * Default workflow output directory on HDFS.
     */
    public static String OUTPUT_DIR =
            //SB SCAPE isilon hadoop cluster default output dir
            "/net/zone1.isilon.sblokalnet/ifs/data/hdfs/user/scape/mapred-write/test-output/MigrateMp3ToWav/";
}
