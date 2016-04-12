package com.lucidworks.ocr;

import com.google.gson.Gson;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.DefaultParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.sax.BodyContentHandler;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

  static Parser parser = new AutoDetectParser();
  static ParseContext context = new ParseContext();

  public static void main(String[] args) throws Exception {
    if(args.length == 0) {
      System.err.println("Missing directory");
      System.exit(1);
    }
    TesseractOCRConfig tesseractConfig = new TesseractOCRConfig();
    PDFParserConfig pdfConfig = new PDFParserConfig();
    pdfConfig.setExtractInlineImages(true);
    context.set(TesseractOCRConfig.class, tesseractConfig);
    context.set(PDFParserConfig.class, pdfConfig);
    context.set(Parser.class, parser);

    File dir = new File(args[0]);
    List<Map<String, Object>> list;
    if(dir.isDirectory()) {
      File[] files = dir.listFiles();
      list = new ArrayList<>(files.length);
      System.out.println("Found "+ files.length+" files in folder "+dir.getAbsolutePath());
      for(File f : files) {
        list.add(parse(f));
        System.out.print('.');
      }
    } else {
      list = new ArrayList<>();
      list.add(parse(dir));
    }

    System.out.println("\n\nFinished parsing "+list.size() +" pdfs");
    File outputFile = args.length > 1 ? new File(args[1]) : new File("parsed.json");
    System.out.println("Writing json to file: "+outputFile.getAbsolutePath());
    FileOutputStream fos = new FileOutputStream(outputFile);
    BufferedOutputStream bos = new BufferedOutputStream(fos);
    bos.write(new Gson().toJson(list).getBytes(Charset.forName("UTF8")));

  }

  private static Map<String, Object> parse(File f) throws Exception {
    Map<String, Object> map = new HashMap<>();
    Metadata metadata = new Metadata();
    FileInputStream stream = new FileInputStream(f);
    BodyContentHandler handler = new BodyContentHandler();
    parser.parse(stream, handler, metadata, context);

    map.put("filename", f.getName());
    map.put("body", handler.toString());
    for(String s : metadata.names()) {
      map.put(s.replaceAll("[^\\w\\d]", "_"), metadata.get(s));
    }
    return map;
  }
}
