package com.lucidworks.ocr;


import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Created by joelwestberg on 4/13/16.
 */
public class OcrParser implements Callable<List<Map<String, Object>>> {
  public static final File POISON = new File("/");
  private final ParseContext context;
  private final BlockingQueue<File> workQueue;
  private final Parser parser;
  private List<Map<String, Object>> list = new ArrayList<>();

  private int processedCount = 0;

  public OcrParser(Parser parser, ParseContext context, BlockingQueue<File> workQueue) {
    this.parser = parser;
    this.context = context;
    this.workQueue = workQueue;
  }

  private Map<String, Object> parse(Map<String, Object> map, File f) throws Exception {
    Metadata metadata = new Metadata();
    FileInputStream stream = new FileInputStream(f);
    BodyContentHandler handler = new BodyContentHandler(10_000_000);
    parser.parse(stream, handler, metadata, context);

    map.put("body", handler.toString());
    for(String s : metadata.names()) {
      map.put(s.replaceAll("[^\\w\\d]", "_"), metadata.get(s));
    }
    return map;
  }

  public List<Map<String, Object>> getList() {
    return list;
  }

  @Override
  public List<Map<String, Object>> call() throws Exception {
    try {
      while(!Thread.currentThread().isInterrupted()) {
        File f = workQueue.poll(5, TimeUnit.MINUTES);
        if(f.getAbsolutePath().equals(POISON.getAbsolutePath())) {
          System.out.println(Thread.currentThread().getName()+" is shutting down after "+processedCount+" documents processed");
          break;
        }
        Map<String, Object> map = new HashMap<>();
        map.put("filename", f.getName());
        try {
          parse(map, f);
          processedCount++;
          System.out.print('.');
        } catch (Exception e) {
          System.out.println("Caught exception while parsing file "+f.getName()+": "+e.getMessage());
        }
        list.add(map);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return list;
  }
}
