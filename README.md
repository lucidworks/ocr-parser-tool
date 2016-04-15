# ocr-parser-tool

To build, use `./gradlew clean build shadowJar` on Linux, or `gradlew.bat clean build shadowJar` on Windows. This will produce a jar file in the directory `build/libs/` called `ocr-parser-tool-1.0-SNAPSHOT-all.jar`. 

## Usage

`java -jar ocr-parser-tool-1.0-SNAPSHOT-all.jar -f folder-to-scan -w 8` to scan the folder `folder-to-scan`  using `8` threads.

For additional settings, use `java -jar ocr-parser-tool-1.0-SNAPSHOT-all.jar -h`
