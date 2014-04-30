package eu.scape_project.audio_qa.ffprobe_extract_compare;

/**
 * The map function of FfprobeExtractCompareMapper compares properties of the original mp3 file and the
 * migrated wav file specified in input.
 * The map function returns the path to the preservation event file with the output of the comparison.
 *
 * The input is a line number as key (not used) and a Text line, which we assume is a path to an original mp3 and
 * a migrated wav file (tab-separated).
 * The output is an exit code (not used), and the path to an output file.
 *
 * eu.scape_project.audio_qa.ffprobe_extract_compare
 * User: baj@statsbiblioteket.dk
 * Date: 4/28/14
 * Time: 12:53 PM
 */
public class FfprobeExtractCompareMapper {
}
