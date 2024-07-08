package ly.persona.academic.data.decoder;

import ly.persona.academic.data.DataReader;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

//
// TODO: It is proposed to implement a Reader that reads and transforms data using several CPU cores to improve the processing speed.
//
public class MultiThreadDecoder<R, V> extends DataDecoder<R, V> {

    private final int maxParallelismLevel;

    private final ExecutorService worker;
    private final LinkedBlockingQueue<Future<V>> bufferOrdered;
    private volatile Thread backgroundReader;

    private final AtomicBoolean waitingOnFullQueue;
    private final AtomicBoolean closed;
    private final Object internalMonitor;

    public MultiThreadDecoder(DataReader<R> reader, Function<R, V> decoder, int maxParallelismLevel) {
        super(reader, decoder);
        this.maxParallelismLevel = maxParallelismLevel;
        bufferOrdered = new LinkedBlockingQueue<>(maxParallelismLevel);
        worker = Executors.newCachedThreadPool();
        waitingOnFullQueue = new AtomicBoolean(false);
        closed = new AtomicBoolean(false);
        internalMonitor = new Object();
    }

    /**
     * the order is guaranteed even while concurrent calls
     */
    @Override
    public V read() {
        checkReadingStarted();
        final Future<V> future = bufferOrdered.poll();
        try {
            return future == null ? null : future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkReadingStarted() {

        if (backgroundReader == null) {
            synchronized (internalMonitor) {
                if (backgroundReader == null) {
                    CompletableFuture<Void> firstResultPresented = new CompletableFuture<>();
                    backgroundReader = new Thread(() -> backgroundJob(firstResultPresented));
                    backgroundReader.start();
                    firstResultPresented.join();
                }
            }
        }

        if (backgroundReader.isAlive()) {
            while (bufferOrdered.isEmpty() && backgroundReader.isAlive()) {
                releaseBackgroundThread();
                try {
                    // happens when the data is being consumed faster that produced
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            releaseBackgroundThread();
        }
    }

    private void backgroundJob(CompletableFuture<Void> firstResultPresented) {

        CompletableFuture<Void> first = firstResultPresented;

        do {
            Throwable error = null;
            try {
                final R readRecord = readRecord();
                if (readRecord == null) {
                    break;
                }
                final Future<V> future = worker.submit(() -> decodeRecord(readRecord));
                bufferOrdered.put(future);
            } catch (Throwable e) {
                error = e;
            } finally {
                if (first != null) {
                    if (error == null) {
                        first.complete(null);
                    } else {
                        first.completeExceptionally(error);
                    }
                    first = null;
                }
            }

            pauseBackGroundThread();

            if (error != null) {
                error.printStackTrace();
                break;
            }
        } while (!closed.get());
    }

    private void releaseBackgroundThread() {
        if (waitingOnFullQueue.compareAndSet(true, false) || closed.get()) {
            synchronized (internalMonitor) {
                internalMonitor.notify();
            }
        }
    }

    private void pauseBackGroundThread() {
        while (bufferOrdered.size() >= maxParallelismLevel && !closed.get()) {
            synchronized (internalMonitor) {
                waitingOnFullQueue.set(true);
                try {
                    internalMonitor.wait(1000);
                } catch (InterruptedException ignored) {
                    // no one interrupts the thread
                }
            }
        }
    }

    @Override
    public void close() {
        super.close();
        closed.set(true);
        releaseBackgroundThread();
        try {
            if (backgroundReader != null && backgroundReader.isAlive()) {
                backgroundReader.join();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            worker.shutdown();
        }
    }
}
