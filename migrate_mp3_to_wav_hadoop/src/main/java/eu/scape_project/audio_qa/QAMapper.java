package eu.scape_project.audio_qa;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * eu.scape_project.audio_qa
 * User: baj@statsbiblioteket.dk
 * Date: 8/30/13
 *
 * The map function performs QA on the wav file in the input directory referenced in input.
 * The map function works in the input directory and returns only "QA passed true/false".
 *
 * The input is a line number as key (not used) and a Text line, which we assume is the path to the
 * original mp3. We further assume that the input migrated wav is in the AudioQASettings.MAPPER_OUTPUT_DIR/"mp3-name"
 * directory.
 * TODO Text Text input is better (input mp3, working directory with migrated wav and mp3 ffprobe output)
 * The output is a boolean as Text "QA passed true/false", and an exit code (not used).
 *
 */
public class QAMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private Log log = new Log4JLogger("QAMapper Log");

    @Override
    protected void map(LongWritable lineNo, Text inputMp3path, Context context) throws IOException, InterruptedException {

        if (inputMp3path.toString().equals("")) return;

        //what is the inputMp3Name?
        String[] inputSplit = inputMp3path.toString().split("/");
        String inputMp3 = inputSplit.length > 0 ? inputSplit[inputSplit.length - 1] : inputMp3path.toString();
        String[] inputMp3Split = inputMp3.split("\\.");
        String inputMp3Name = inputMp3Split.length > 0 ? inputMp3Split[0] : inputMp3;

        //what is the file-specific working directory
        String outputDirPath = context.getConfiguration().get("map.outputdir", AudioQASettings.MAPPER_OUTPUT_DIR) + inputMp3Name;
        //write output directory and input mp3 to the output key text
        Text output = new Text(outputDirPath);
        log.debug("outputDir="+outputDirPath);
        System.out.println(outputDirPath);

        //where is the wav, that we are performing qa on?
        String migratedWavPath = outputDirPath + "/" + inputMp3 + "_ffmpeg.wav";
        log.debug("wav="+migratedWavPath);
        System.out.println(migratedWavPath);

        //ffprobe the wav file
        String wavFfprobeLogFileString = migratedWavPath + "_ffprobe.log";
        log.debug("wavFfprobeLogFileString="+wavFfprobeLogFileString);
        System.out.println(wavFfprobeLogFileString);
        String [] ffprobeCommand = new String[2];
        ffprobeCommand[0] = "ffprobe";
        ffprobeCommand[1] = migratedWavPath;
        FileSystem fs = FileSystem.get(context.getConfiguration());
        int exitCode = CLIToolRunner.runCLItool(ffprobeCommand, wavFfprobeLogFileString, fs, new Text());
        File outputFile = new File(wavFfprobeLogFileString);
        outputFile.setReadable(true, false);
        outputFile.setWritable(true, false);

        if (exitCode == 0) {
            //where is the ffprobe output of the original mp3?
            String mp3FfprobeLogFileString = outputDirPath + "/" + inputMp3 + "_ffprobe.log";

            //compare the two ffprobe characterisations
            exitCode = ffprobeExtractCompare(wavFfprobeLogFileString, mp3FfprobeLogFileString);
            //TODO are there updates to the output Text?
        }
        //TODO run JHove2 on the wav file
        //TODO check JHove2 result. File format valid?
        //Skip JHove2 for first version...

        //convert original mp3 to wav using mpg321 for comparison
        if (exitCode == 0) {
            String mpg321log = outputDirPath + "/" + inputMp3 + "_mpg321.log";
            log.debug("mpg321log="+mpg321log);
            System.out.println(mpg321log);
            //logFile = new File(mpg321log);
            //logFile.setReadable(true, false);
            //logFile.setWritable(true, false);
            String[] mpg321command = new String[4];
            mpg321command[0] = "mpg321";
            mpg321command[1] = "-w";
            File outputwav = new File(outputDirPath + "/", inputMp3 + "_mpg321.wav");
            outputwav.setReadable(true, false);
            outputwav.setWritable(true, false);
            mpg321command[2] = outputwav.getAbsolutePath();
            mpg321command[3] = inputMp3path.toString();
            System.out.println(Arrays.toString(mpg321command));
            exitCode = CLIToolRunner.runCLItool(mpg321command, mpg321log, fs, new Text());
        }

        //TODO run xcorrSound waveform-compare to compare the migrated wav and the converted comparison wav

        context.write(new LongWritable(exitCode), output);
    }

    private int ffprobeExtractCompare(String wavFFprobeOutputPath, String mp3FFprobeOutputPath) throws IOException {
        int exitCode = 0;
        String wavFFprobeOutput = "";
        BufferedReader in = new BufferedReader(new FileReader(wavFFprobeOutputPath));
        while (in.ready()) {
            wavFFprobeOutput += in.readLine();
        }
        Map<String, String> wavFFprobeProperties = extractFromFFprobe(wavFFprobeOutput);

        String mp3FFprobeOutput = "";
        in = new BufferedReader(new FileReader(mp3FFprobeOutputPath));
        while (in.ready()) {
            mp3FFprobeOutput += in.readLine();
        }
        Map<String, String> mp3FFprobeProperties = extractFromFFprobe(mp3FFprobeOutput);

        //first check if durations are close enough
        String wavDuration = wavFFprobeProperties.remove("duration");
        String mp3Duration = mp3FFprobeProperties.remove("duration");
        try {
            if (!compareDuration(wavDuration, mp3Duration, 100)) {
                exitCode = 1;
                //write error message to log
                log.info("Error in FFprobe property duration comparison for file "+
                        "\nProperties not close enough."+
                        "\nProperty for mp3 file: "+mp3Duration+
                        "\nProperty for wav file: "+wavDuration);
            }
        } catch (ParseException e) {
            exitCode = 1;
            //write error message to log
            log.info("Error in FFprobe property duration comparison for file "+
                    "\nProperty could not be parsed."+
                    "\nProperty for mp3 file: "+mp3Duration+
                    "\nProperty for wav file: "+wavDuration);
        }

        //now check number of channels equal
        String wavChannels = wavFFprobeProperties.remove("channels");
        String mp3Channels = mp3FFprobeProperties.remove("channels");
        if (!compareChannels(wavChannels, mp3Channels)) {
            exitCode = 1;
            //write error message to log
            log.info("Error in FFprobe property channel comparison for file "+
                    "\nChannel properties do not match."+
                    "\nProperty for mp3 file: "+mp3Channels+
                    "\nProperty for wav file: "+wavChannels);
        }

        //bitrate does not have to be equal
        wavFFprobeProperties.remove("bitrate");
        mp3FFprobeProperties.remove("bitrate");

        //check all other properties equal
        for (String property: mp3FFprobeProperties.keySet()) {
            if (!mp3FFprobeProperties.get(property).trim().equalsIgnoreCase(wavFFprobeProperties.get(property).trim())) {
                exitCode = 1;
                //write error message to log
                log.info("Error in FFprobe property "+property+" comparison for file "+
                        "\nProperty for mp3 file: "+mp3FFprobeProperties.get(property)+
                        "\nProperty for wav file: "+wavFFprobeProperties.get(property));
            }
        }
        log.info("FFprobe property comparison finished.");
        return exitCode;
    }

    /**
     * This method extracts properties from FFprobe output.
     * Matches the ExtractFromFFprobe beanshell script of the FFprobe_Extract_Compare Taverna workflow.
     * @param ffprobe_output
     * @return Map containing duration, hz, channels, bitsPerSample, bitrate
     */
    public Map<String, String> extractFromFFprobe(String ffprobe_output) {
        if (ffprobe_output!=null && ffprobe_output.length()>0) {
            Map<String, String> propertyMap = new HashMap<String, String>();
            int blockBegin = ffprobe_output.indexOf("  Duration: ")+"  Duration: ".length();
            String block = ffprobe_output.substring(blockBegin);

            //getDuration
            int durationStringEnd = block.indexOf(",");
            String durationString = block.substring(0,durationStringEnd).trim();
            //Insert into Map
            propertyMap.put("duration", durationString);

            //getHz
            int audioBeginIndex = block.indexOf("Audio: ");
            int hzBeginIndex = block.indexOf(", ",audioBeginIndex)+2;
            int hzEndIndex = block.indexOf(",",hzBeginIndex)-3;
            String hzString = block.substring(hzBeginIndex,hzEndIndex).trim();
            propertyMap.put("hz", hzString);

            //getChannels
            int channelsBeginIndex = block.indexOf(" ",hzEndIndex+2);
            int channelsEndIndex = block.indexOf(" ",channelsBeginIndex+1);
            String channelsString = block.substring(channelsBeginIndex,channelsEndIndex).trim();
            propertyMap.put("channels", channelsString);

            //getBitsPerSample
            int sBeginIndex = block.indexOf(" s",channelsEndIndex)+2;
            int sEndIndex = block.indexOf(",",sBeginIndex+1);
            String s = block.substring(sBeginIndex,sEndIndex);
            propertyMap.put("bitsPerSample", s);

            //bitrate
            int bitrateIndexBegin = sEndIndex+1;
            int bitrateIndexEnd = block.indexOf(" ",bitrateIndexBegin+1);
            String bitrateString = block.substring(bitrateIndexBegin,bitrateIndexEnd).trim();
            propertyMap.put("bitrate", bitrateString);

            return propertyMap;
        }
        return null; //todo error recovery?
    }

    /**
     * Compare durations as Dates. Similar if less than 10 seconds difference.
     * Matches the compareDuration beanshell script of the FFprobe_Extract_Compare Taverna workflow.
     * @param first first duration as String, format "HH:mm:ss.SS"
     * @param second second duration as String, format "HH:mm:ss.SS"
     * @param maxDiff maximum accepted difference in milliseconds
     * @return true if durations close enough, false otherwise
     * @throws ParseException
     */
    public boolean compareDuration(String first, String second, int maxDiff) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

        Date firstDate = dateFormat.parse(first+"0");
        Date secondDate = dateFormat.parse(second+"0");

        long firstLong = firstDate.getTime();
        long secondLong = secondDate.getTime();
        if (firstLong < 0) {
            firstLong = -firstLong;
        }
        if (secondLong < 0) {
            secondLong = -secondLong;
        }

        long diff = firstLong-secondLong;
        if (diff < 0) {
            diff = -diff;
        }

        long maxDiffLong = Long.valueOf(maxDiff);
        return (diff < maxDiffLong);
    }

    /**
     * Compare number of channels.
     * Matches the compareChannels beanshell script of the FFprobe_Extract_Compare Taverna workflow.
     * @param first first channels as String
     * @param second second channels as String
     * @return true if number of channels equal, false otherwise
     */
    public boolean compareChannels(String first, String second) {
        if (first.trim().equalsIgnoreCase("stereo,") &&
                second.trim().equalsIgnoreCase("2")) {
            return true;
        } else
        if (second.trim().equalsIgnoreCase("stereo,") &&
                first.trim().equalsIgnoreCase("2")) {
            return true;
        } else {
            return first.trim().equalsIgnoreCase(second.trim());
        }

    }
}
