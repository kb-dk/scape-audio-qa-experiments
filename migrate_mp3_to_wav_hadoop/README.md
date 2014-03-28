# Migrate mp3 files to wav files and perform audio qa using Hadoop

The migrate-mp3-to-wav-hadoop module contains a number of Hadoop Jobs. These are

* FfmpegMigration
* Mpg321Conversion
* WaveformCompare

which all perform exactly one task. And

* MigrateMp3ToWav, which uses the Hadoop Chainmapper to include
  * MigrationMepper, which does both the ffmpeg migration and the ffprobe characterisation of the mp3 file and
  * QAMapper, which does the ffprobe characterisation of the migrated wav file, the property comparison,
  the mpg321 conversion, and the waveform-compare.

More about these later.

The module also contains a number of Taverna workflows using these Hadoop jobs.

* TavernaWorkflowUsingFfmpegMigrateHadoopJob.t2flow
* TavernaWorkflowUsingMpg321ConvertHadoopJob.t2flow
* TavernaWorkflowUsingWaveformCompareHadoopJob.t2flow

and

* SlimMigrateAndQAmp3toWavUsingHadoopJobs.t2flow, which combines the three above jobs to do migration, conversion
and content comparison

The commandline to run the workflow looks something like this:

~/tools/taverna-commandline-2.4.0/executeworkflow.sh SlimMigrateAndQAmp3toWavUsingHadoopJobs.t2flow -inmemory
-inputvalue mapreduce_output_path baj/out/exp-140216-01
-inputvalue jar_input_path /scape/shared/jars/
-inputvalue mp3_list_on_hdfs_input_path baj/data/mp3-file-list-medium.txt
-inputvalue hdfs_output_path_2 baj/out/exp-140216
-inputvalue nfs_output_path /scape/shared/out/wav/
-inputvalue max_split_size 256
-logfile log.txt

## Ffmpeg Migration

The input to this job is a list of mp3 file paths to mp3 files on NFS. The text file with
the list of mp3 file paths should be available on HDFS.

TBD

## Mpg321Conversion

The input to this job is a list of mp3 file paths to mp3 files on NFS. The text file with
the list of mp3 file paths should be available on HDFS.

TBD

## WaveformCompare

The input to this job is a list of pairs of file paths to wav files on NFS. The text file with
the list of pairs of file paths should be available on HDFS.

TBD

## MigrateMp3ToWav

The input to this job is a list of mp3 file paths to mp3 files already available on HDFS. The text file with
the list of mp3 file paths should also be available on HDFS.

To run the job first run 'mvn package' to get 'migrate_mp3_to_wav_hadoop-0.1-SNAPSHOT-jar-with-dependencies.jar'

Then run
'hadoop jar migrate_mp3_to_wav_hadoop-0.1-SNAPSHOT-jar-with-dependencies.jar eu.scape_project.audio_qa.MigrateMp3ToWav
-Dmapred.max.split.size=1024 <input/mp3list.txt> <output/map-reduce-result-output-dir> <output/mp3-to-wav-output-dir>'

where <input/mp3list.txt> is the path on hdfs to the text file with the list of mp3 file paths (on hdfs),
<output/map-reduce-result-output-dir> is the output directory for the map-reduce result (on hdfs)
and <output/mp3-to-wav-output-dir> is the output directory for the migration and qa results (on hdfs).

The -Dmapred.max.split.size=1024 parameter is the easy way of splitting the input text files into smaller pieces.
The actual value can be adjusted. This will be changed to use number of lines (i.e. input files) instead.

The reason for two output directories is that to have the actual migrated files on NFS, and the qa results on HDFS,
such that we can reduce these / do statistics on them using MR. The reason for three output directories is mostly
historic.
