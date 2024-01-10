/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.kafka.eventstreams.impl;

import java.lang.IllegalStateException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import org.ojai.Document;
import org.ojai.DocumentListener;
import org.ojai.DocumentReader;
import org.ojai.DocumentStream;
import org.ojai.FieldPath;
import org.ojai.exceptions.StreamInUseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ojai.store.QueryResult;
import org.ojai.store.exceptions.StoreException;
import java.nio.file.AccessDeniedException;
import com.mapr.fs.proto.Marlinserver.MarlinInternalDefaults;

public class MarlinDocumentStream implements QueryResult {
  static final Logger LOG = LoggerFactory.getLogger(MarlinDocumentStream.class);

  private class DocumentListNode {
    Document[] docs;
    int totalDocs;
    int consumedDocs;
    int batchSize;
    DocumentListNode next;

    public DocumentListNode(int maxMsgsPerRow) {
      docs = new Document[maxMsgsPerRow];
      totalDocs = 0;
      consumedDocs = 0;
      batchSize = 0;
      next = null;
    }

    public void addDocument(Document doc) {
      assert(totalDocs < docs.length);
      docs[totalDocs ++] = doc;
    }

    public int getNumTotalDocuments() {
      return totalDocs - consumedDocs;
    }

    public Document getNextDocument() {
      if (consumedDocs >= totalDocs) {
        return null;
      }

      Document doc = docs[consumedDocs];
      docs[consumedDocs] = null;
      consumedDocs ++;
      return doc;
    }

    public void setBatchSize(int size) {
      batchSize = size;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public void setNextDocumentList(DocumentListNode node) {
      next = node;
    }

    public DocumentListNode getNextDocumentList() {
      return next;
    }
  }

  private class PartitionScanner implements Runnable {

    DocumentStream docStream;
    MarlinDocumentStream marlinStream;
    List<Integer> pathIds;

    PartitionScanner(DocumentStream dStream,
                  MarlinDocumentStream mStream,
                  List<Integer> pIds) {
      docStream = dStream;
      marlinStream = mStream;
      pathIds = pIds;
    }

    public void run() {
      try {
        Iterator<Document> iter = docStream.iterator();
        MarlinInternalDefaults mdef = MarlinInternalDefaults.getDefaultInstance();

        while (iter.hasNext()) {
          Document dbDoc = iter.next();
          StreamsDocumentTranslator t = new StreamsDocumentTranslator(dbDoc,
                                                                      pathIds);
          DocumentListNode newNode = new DocumentListNode(mdef.getMaxMsgsPerRow());
          int totalBatchSize = 0;

          while (t.hasNext()) {
            StreamsDocument doc = (StreamsDocument)t.next();
            totalBatchSize += doc.size();
            newNode.addDocument(doc);
          }

          newNode.setBatchSize(totalBatchSize);
          marlinStream.addScannedDocuments(newNode);
        }

        docStream.close();
        int numActiveScans = marlinStream.markScanComplete();
        LOG.debug("Num scanners active = " + numActiveScans );
      } catch (Exception e) {
        if (e.getCause() instanceof com.mapr.db.exceptions.AccessDeniedException ||
            e.getCause() instanceof org.apache.hadoop.security.AccessControlException) {
          synchronized(marlinStream) {
            marlinStream.accessExceptionOccurred = true;
            marlinStream.notifyAll();
          }
        } else {
          LOG.error("Error while scanning stream: " + e.getCause());
          marlinStream.exception = new StoreException(e);
        }
      }
    }
  }

  // background thread pool
  private final ExecutorService pool;

  // Linked-list of all the decoded StreamsDocument's
  DocumentListNode first;
  DocumentListNode last;

  // max scanners that can be iterated over in parallel
  int scansInProgress;

  boolean iteratorOpened;
  boolean closed;

  // set accessExceptionOccured flag only for those exceptions for which DocumentStream
  // should be empty.
  boolean accessExceptionOccurred;

  long totalCachedMemory;
  long maxCacheMemory;
  StoreException exception;

  public MarlinDocumentStream(List<DocumentStream> dbDocumentStreams,
                              List<FieldPath> ps,
                              int parallelScans,
                              long maxMemory) {

    List<Integer> pathIds = StreamsDocument.getProjectionIdList(ps);
    iteratorOpened = false;
    closed = false;
    pool = Executors.newFixedThreadPool(parallelScans);
    first = null;
    last  = null;
    scansInProgress = dbDocumentStreams.size();
    totalCachedMemory = 0;
    maxCacheMemory = maxMemory;
    accessExceptionOccurred = false;
    exception = null;

    for (DocumentStream dStream : dbDocumentStreams) {
      Runnable pScanObj = new PartitionScanner(dStream, this, pathIds);
      pool.execute(pScanObj);
    }
    //Initiates an orderly shutdown in which previously submitted tasks are
    //executed, but no new tasks will be accepted.
    pool.shutdown();
  }

  // We get the `first' DocumentListNode, and reset `first' and `last' to null
  // The caller will then go through all the DocumentListNode's in the 
  // linked-list. This is to reduce lock contention by the reader.
  private synchronized DocumentListNode getAllScannedDocuments() {
    while (true) {
      if (first != null) {
        DocumentListNode ret = first;
        first = null;
        last = null;
        totalCachedMemory = 0;

        // Notify all the waiters who were waiting for the data
        // to be consumed
        notifyAll();

        return ret;
      }

      if (scansInProgress == 0) {
        return null;
      }

      try {
          synchronized(MarlinDocumentStream.this) {
            if (MarlinDocumentStream.this.accessExceptionOccurred)
              return null;

            wait();
          }
      } catch (InterruptedException e) {
        LOG.error("getScannedDocuments: Interrupted: " + e.getMessage());
        return null;
      }
    }
  }

  private synchronized int markScanComplete() {
    scansInProgress --;
    notifyAll();
    return scansInProgress;
  }

  private synchronized void addScannedDocuments(DocumentListNode node) {

    if (first == null) {
      first = node;
      last  = node;
    } else {
      last.setNextDocumentList(node);
      node.setNextDocumentList(null);
      last = node;
    }

    totalCachedMemory += node.getBatchSize();

    // Notify the reader that we have more Documents available
    notifyAll();

    // If we go beyond the max cache memory size, we wait to get notified
    while (totalCachedMemory > maxCacheMemory) {
      try  {
        wait();
      } catch (InterruptedException e) {
        return;
      }
    }

    return;
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      pool.shutdownNow();
      closed = true;
    }
  }

  @Override
  public void streamTo(DocumentListener l) {
    try {
      for (Document record : this) {
        l.documentArrived(record);
      }
    } catch (Exception e) {
      try {
        close();
      } catch (Exception e1) {}
      l.failed(e);
    }
  }

  private void checkDocStreamIteratorOpened() {
    if (iteratorOpened) {
      throw new StreamInUseException("An iterator has already been opened on " +
          "this document stream.");
    }
  }

  private synchronized void checkDocStreamClosed() {
    if (closed) {
      throw new IllegalStateException("DocumentStream already closed.");
    }
  }

  @Override
  public Iterator<Document> iterator() {
    checkDocStreamIteratorOpened();
    checkDocStreamClosed();
    iteratorOpened = true;

    return new Iterator<Document>() {

      DocumentListNode allScannedDocs = null;
      boolean  done = false;

      @Override
      public boolean hasNext() {
        if (done) {
          return false;
        }

        if (MarlinDocumentStream.this.accessExceptionOccurred) {
          return false;
        }

        if (MarlinDocumentStream.this.exception != null)
          throw MarlinDocumentStream.this.exception;

        checkDocStreamClosed();

        while (allScannedDocs != null &&
               allScannedDocs.getNumTotalDocuments() == 0) {
          allScannedDocs = allScannedDocs.getNextDocumentList();
        }

        if (allScannedDocs == null) {
          allScannedDocs = MarlinDocumentStream.this.getAllScannedDocuments();

          // Once done, no other calls will go to backend
          if (allScannedDocs == null) {
            done = true;
            try {
              MarlinDocumentStream.this.close();
            } catch (Exception e) {
              LOG.error("Error while closing stream: " + e.getMessage());
            }
          }
        }

        return !done;

      }

      @Override
      public Document next() {
        if (!hasNext()) {
          throw new NoSuchElementException("next() called after hasNext() " +
              "return false.");
        }

        return allScannedDocs.getNextDocument();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public Iterable<DocumentReader> documentReaders() {
    throw new UnsupportedOperationException("documentReaders() not supported for MarlinDocumentStream");
  }

  @Override
  public Document getQueryPlan() {
    throw new UnsupportedOperationException();
  }
}
