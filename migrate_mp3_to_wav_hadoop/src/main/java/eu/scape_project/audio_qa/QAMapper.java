package eu.scape_project.audio_qa;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
 * original mp3. We further assume that the input migrated wav is in the AudioQASettings.OUTPUT_DIR/"mp3-name"
 * directory.
 * TODO Text Text input is better (input mp3, working directory with migrated wav and mp3 ffprobe output)
 * The output is a boolean as Text "QA passed true/false", and an exit code (not used).
 *
 */
public class QAMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable lineNo, Text inputMp3path, Context context) throws IOException, InterruptedException {

        if (inputMp3path.toString().equals("")) return;

        //what is the inputMp3Name?
        String[] inputSplit = inputMp3path.toString().split("/");
        String inputMp3 = inputSplit.length > 0 ? inputSplit[inputSplit.length - 1] : inputMp3path.toString();
        String[] inputMp3Split = inputMp3.split("\\.");
        String inputMp3Name = inputMp3Split.length > 0 ? inputMp3Split[0] : inputMp3;

        //what is the file-specific working directory
        File outputDir = new File(context.getConfiguration().get("map.outputdir", AudioQASettings.OUTPUT_DIR), inputMp3Name);
        //write output directory to the output key text
        Text output = new Text(outputDir.toString());
        System.out.println(outputDir);

        //where is the wav, that we are performing qa on?
        File wav = new File(outputDir.toString() + "/", inputMp3 + "_ffmpeg.wav");
        System.out.println(wav);

        //ffprobe the wav file
        String wavFfprobeLogFileString = wav.getAbsolutePath() + "_ffprobe.log";
        System.out.println(wavFfprobeLogFileString);
        String [] ffprobeCommand = new String[2];
        ffprobeCommand[0] = "ffprobe";
        ffprobeCommand[1] = wav.toString();
        int exitCode = CLIToolRunner.runCLItool(ffprobeCommand, wavFfprobeLogFileString);
        File outputFile = new File(wavFfprobeLogFileString);
        System.out.println(outputFile);
        outputFile.setReadable(true, false);
        outputFile.setWritable(true, false);

        if (exitCode == 0) {
            //where is the ffprobe output of the original mp3?
            String mp3FfprobeLogFileString = outputDir.toString() + "/" + inputMp3 + "_ffprobe.log";

            //TODO compare the two ffprobe characterisations
            exitCode = ffprobeExtractCompare(wavFfprobeLogFileString, mp3FfprobeLogFileString, output);

        }
        //TODO run JHove2 on the wav file

        //TODO check JHove2 result. File format valid?

        //TODO convert original mp3 to wav using mpg321 for comparison

        //TODO run xcorrSound waveform-compare to compare the migrated wav and the converted comparison wav

        context.write(output, new LongWritable(exitCode));
    }

    private int ffprobeExtractCompare(String wavFFprobeOutputPath, String mp3FFprobeOutputPath, Text output) throws IOException {
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

        for (String property: mp3FFprobeProperties.keySet()) {
            System.out.println(property);
            System.out.println(mp3FFprobeProperties.get(property));
            System.out.println(wavFFprobeProperties.get(property));
            try {
                if (property.equals("duration") && !compareDuration(mp3FFprobeProperties.get(property), wavFFprobeProperties.get(property), 10)) {
                    exitCode = 1;
                    //write error message to the output key text
                    output = new Text("Error in FFprobe property "+property+" comparison for file "+inputMp3+
                            "\nProperties not close enough."+
                            "\nProperty for mp3 file: "+mp3FFprobeProperties.get(property)+
                            "\nProperty for wav file: "+wavFFprobeProperties.get(property));
                }
            } catch (ParseException e) {
                exitCode = 1;
                //write error message to the output key text
                output = new Text("Error in FFprobe property "+property+" comparison for file "+inputMp3+
                        "\nProperty could not be parsed."+
                        "\nProperty for mp3 file: "+mp3FFprobeProperties.get(property)+
                        "\nProperty for wav file: "+wavFFprobeProperties.get(property));
            }
            if (!mp3FFprobeProperties.get(property).equals(wavFFprobeProperties.get(property))) {
                exitCode = 1;
                //write error message to the output key text
                output = new Text("Error in FFprobe property "+property+" comparison for file "+inputMp3+
                        "\nProperty for mp3 file: "+mp3FFprobeProperties.get(property)+
                        "\nProperty for wav file: "+wavFFprobeProperties.get(property));
            }
        }
        //todo update output Text?
        //output = new Text(output.toString()+"\nFFprobe property comparison successful");
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
}
