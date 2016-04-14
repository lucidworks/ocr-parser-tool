package com.lucidworks.ocr;

import com.google.gson.Gson;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.pdf.PDFParserConfig;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {

  static int workers = Runtime.getRuntime().availableProcessors();
  static String outputFileName = "parsed.json";
  static Parser parser = new AutoDetectParser();
  static ParseContext context = new ParseContext();

  static ExecutorService executorService;

  public static void main(String[] args) throws Exception {
    OptionParser op = new OptionParser();
    op.accepts("w", "number of concurrent parsers, defaults to "+workers).withRequiredArg().ofType(Integer.class);
    op.accepts("o", "output file, defaults to '"+outputFileName+"'").withRequiredArg();
    op.accepts("help", "prints this message").isForHelp();
    op.accepts("h", "prints this message").isForHelp();
    op.accepts("f", "folder to parse").requiredUnless("h", "help").withRequiredArg();

    OptionSet options = null;
    try {
       options = op.parse(args);
    } catch (Exception e) {
      System.err.println("Missing required parameters");
      op.printHelpOn(System.err);
      System.exit(1);
    }

    long startTime = System.currentTimeMillis();
    if(options.has("h") || options.has("help")) {
      op.printHelpOn(System.out);
      System.exit(0);
    }

    if(options.has("o")) {
      outputFileName = options.valueOf("o").toString();
    }

    TesseractOCRConfig tesseractConfig = new TesseractOCRConfig();
    PDFParserConfig pdfConfig = new PDFParserConfig();
    pdfConfig.setExtractInlineImages(true);
    context.set(TesseractOCRConfig.class, tesseractConfig);
    context.set(PDFParserConfig.class, pdfConfig);
    context.set(Parser.class, parser);


    if(options.has("w")) {
      workers = (Integer) options.valueOf("w");
    }

    System.out.println("Starting up "+workers+" concurrent processors");

    File dir = new File(options.valueOf("f").toString());
    List<Map<String, Object>> list;
    if(dir.isDirectory()) {
      File[] files = dir.listFiles();
      list = new ArrayList<>(files.length);
      System.out.println("Found "+ files.length+" files in folder "+dir.getAbsolutePath());
      executorService = Executors.newFixedThreadPool(workers);
      BlockingQueue<File> workQueue = new LinkedBlockingQueue<>();
      List<Callable<List<Map<String, Object>>>> ocrParsers = new ArrayList<>();
      for(int i=0; i<workers; i++) {
        ocrParsers.add(new OcrParser(parser, context, workQueue));
      }

      for(File f : files) {
        workQueue.offer(f);
      }
      for(int i=0; i<workers; i++) {
        workQueue.offer(OcrParser.POISON);
      }

      try {
        List<Future<List<Map<String, Object>>>> futures = executorService.invokeAll(ocrParsers);
        for(Future<List<Map<String, Object>>> future : futures) {
          list.addAll(future.get());
        }
        executorService.shutdownNow();
      } catch(InterruptedException e) {
        workQueue.clear();
        executorService.shutdownNow();
      }


    } else {
      throw new UnsupportedOperationException("Specified -f parameter is a file, not a folder");
    }

    System.out.println("\n\nFinished parsing "+list.size() +" pdfs");
    File outputFile = new File(outputFileName);
    System.out.println("Writing json to file: "+outputFile.getAbsolutePath());
    FileOutputStream fos = new FileOutputStream(outputFile);
    BufferedOutputStream bos = new BufferedOutputStream(fos);
    bos.write(new Gson().toJson(list).getBytes(Charset.forName("UTF8")));
    System.out.println("Time elapsed: "+(System.currentTimeMillis()-startTime)/1000+" seconds");
  }
}
