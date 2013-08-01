package eu.scape_project.audio_qa;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.*;

public class MigrationMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable lineNo, Text inputMp3path, Context context) throws IOException, InterruptedException {

            //create a file-specific output dir
            String[] inputSplit = inputMp3path.toString().split("/");
            String inputMp3 = inputSplit.length > 0 ? inputSplit[inputSplit.length - 1] : inputMp3path.toString();
            String[] inputMp3Split = inputMp3.split("\\.");
            String inputMp3Name = inputMp3Split.length > 0 ? inputMp3Split[0] : inputMp3;

            //todo fix output directory
            File outputDir = new File(MigrateMp3ToWav.getOutputdirPath(), inputMp3Name);
            outputDir.delete();
            outputDir.mkdirs();
            outputDir.setReadable(true, false);
            outputDir.setWritable(true, false);

            //write output directory to the output key text
            Text output = new Text(outputDir.toString());

            //start with ffprobe
            String outputFile = outputDir.getAbsolutePath() + "/" + inputMp3 + "_ffprobe.log";
            String [] ffprobeCommand = new String[2];
            ffprobeCommand[0] = "ffprobe";
            ffprobeCommand[1] = inputMp3path.toString();
            int exitCode = runCLIcommand(ffprobeCommand, outputFile);

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
                exitCode = runCLIcommand(ffmpegcommand, log);
            }

            context.write(output, new LongWritable(exitCode));
        }

    private int runCLIcommand(String[] commandline, String logFile) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(commandline);
        //start the executable
        Process proc = pb.start();
        BufferedReader stdout = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        BufferedReader stderr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        try {
            //wait for process to end before continuing
            proc.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        int exitCode = proc.exitValue();
        String stdoutString = "";
        while (stdout.ready()) {
            stdoutString += stdout.readLine() + "\n";
        }
        String stderrString = "";
        while (stderr.ready()) {
            stderrString += stderr.readLine() + "\n";
        }

        BufferedWriter logFileWriter = new BufferedWriter(new FileWriter(logFile, true));
        //write log of stdout and stderr to the log file
        logFileWriter.write(stdoutString + stderrString);
        logFileWriter.newLine();
        logFileWriter.close();
        return exitCode;
    }
}