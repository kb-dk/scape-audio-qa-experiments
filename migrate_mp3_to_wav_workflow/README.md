# Migrate mp3 files to wav files and perform audio qa using Taverna

Use mvn package to create migrate_mp3_to_wav_workflow-0.1-SNAPSHOT-bundle.tar.gz

Move the package to the desired destination and unpack using

tar -zxvf migrate_mp3_to_wav_workflow-0.1-SNAPSHOT-bundle.tar.gz

Change directory to migrate_mp3_to_wav_workflow-0.1-SNAPSHOT

cd migrate_mp3_to_wav_workflow-0.1-SNAPSHOT

Run the full migration workflow on the test data with this command

./bin/migrateMP3ToWAV.sh $PWD/samples/filelist.txt $PWD

To run the "Validate Compare Compare" QA workflow, you need a list of migrated files
and a list of 'compare to files'. You can then run

./bin/validateCompareCompare.sh <comparetowav_list> <migratedwav_list> $PWD

##Dependencies and further instructions

These workflows depend on

taverna (obviously)
migrationQA (later changed to waveform-compare) from scape-xcorrsound
ffmpeg >= 0.10
mpeg321
jhove2.sh

This means that these commands must be available and functions from the $PATH
ffmpeg
ffprobe
mpeg321
jhove2.sh
migrationQA
executeWorkflow.sh

$PATH can be updated in setenv.sh

Note the jhove2 "not in install dir fix script" expects jhove2 to be installed in $HOME/tools/jhove2-2.0.0

checkinstall.sh checks if the required tools are installed.

migrateMP3ToWAV.sh <input-file-list> <output-dir> uses the
/src/main/taverna/Mp3toWav_migrateValidateCompareCompare.t2flow workflow
to migrate and qa the files in <input-file-list>.

The WAV output files and the qa result summary are written to <output-dir>.
The detailed results and logs are written to <output-dir>/logs.

NOTE <input-file-list> and <output-dir> must be full paths.