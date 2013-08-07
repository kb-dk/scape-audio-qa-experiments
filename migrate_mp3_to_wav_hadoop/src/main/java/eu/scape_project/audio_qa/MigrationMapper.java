package eu.scape_project.audio_qa;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The map function characterises the mp3 files referenced in input and migrates them to wav.
 * The map function returns the path to output directory with the result files.
 */
public class MigrationMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable lineNo, Text inputMp3path, Context context) throws IOException, InterruptedException {

        if (inputMp3path.toString().equals("")) return;

        //create a file-specific output dir
        String[] inputSplit = inputMp3path.toString().split("/");
        String inputMp3 = inputSplit.length > 0 ? inputSplit[inputSplit.length - 1] : inputMp3path.toString();
        String[] inputMp3Split = inputMp3.split("\\.");
        String inputMp3Name = inputMp3Split.length > 0 ? inputMp3Split[0] : inputMp3;

        //just this once
        //File test_output = new File("/net/zone1.isilon.sblokalnet/ifs/data/hdfs/user/scape/mapred-write/test-output/");
        //recursiveDeleteDir(test_output);

        //todo fix output directory + owner??? + permissions
        File outputDir = new File(context.getConfiguration().get("map.outputdir", AudioQASettings.OUTPUT_DIR), inputMp3Name);
        recursiveDeleteDir(outputDir);
        outputDir.mkdirs();
        outputDir.setReadable(true, false);
        outputDir.setWritable(true, false);
        File parent = outputDir.getParentFile();
        parent.setReadable(true, false);
        parent.setWritable(true, false);
        File grandparent = parent.getParentFile();
        grandparent.setReadable(true, false);
        grandparent.setWritable(true, false);

        System.out.println(outputDir);//todo logging

        //write output directory to the output key text
        Text output = new Text(outputDir.toString());

        //start with ffprobe
        String outputFileString = outputDir.getAbsolutePath() + "/" + inputMp3 + "_ffprobe.log";
        String [] ffprobeCommand = new String[2];
        ffprobeCommand[0] = "ffprobe";
        ffprobeCommand[1] = inputMp3path.toString();
        int exitCode = CLIToolRunner.runCLItool(ffprobeCommand, outputFileString);
        File outputFile = new File(outputFileString);
        outputFile.setReadable(true, false);
        outputFile.setWritable(true, false);

        //next migrate with ffmpeg
        if (exitCode == 0) {
            String log = outputDir.getAbsolutePath() + "/" + inputMp3 + "_ffmpeg.log";
            String[] ffmpegcommand = new String[5];
            ffmpegcommand[0] = "ffmpeg";
            ffmpegcommand[1] = "-y";
            ffmpegcommand[2] = "-i";
            ffmpegcommand[3] = inputMp3path.toString();
            File outputwav = new File(outputDir.toString() + "/", inputMp3 + "_ffmpeg.wav");
            ffmpegcommand[4] = outputwav.getAbsolutePath();
            exitCode = CLIToolRunner.runCLItool(ffmpegcommand, log);
        }

        context.write(output, new LongWritable(exitCode));
    }

    public boolean recursiveDeleteDir(File outputDir) {
        if (outputDir.exists()) {
            if (outputDir.isDirectory()) {
                File[] outputDirFiles = outputDir.listFiles();
                if (outputDirFiles != null) {
                    for (File outputDirFile : outputDirFiles) recursiveDeleteDir(outputDirFile);
                }
            }
            return outputDir.delete();
        }
        return true;
    }

}