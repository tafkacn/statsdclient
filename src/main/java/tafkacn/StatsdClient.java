package tafkacn.statsdclient;

import com.lmax.disruptor.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A java statsd client that makes use of LMAX disruptor for smart batching
 * of counter updates to a statsd server
 * @author raymond.mak
 */
public class StatsdClient {
    
    final static private byte COUNTER_METRIC_TYPE = 0;
    final static private byte GAUGE_METRIC_TYPE = 1;
    final static private byte HISTOGRAM_METRIC_TYPE = 2;
    final static private byte METER_METRIC_TYPE = 3;
    final static private byte TIMER_METRIC_TYPE = 4;
    final static private short HUNDRED_PERCENT = (short)10000;
    
    final static private String[] METRIC_TYPE_STRINGS = new String[] {
        "|c",
        "|g",
        "|h",
        "|m",
        "|ms",        
    };
    
    static private class CounterEvent {
        private byte metricsType;
        private short samplingRate = HUNDRED_PERCENT; // In hundredth of a percentage point, 10000 being 100% 
        private String key;
        private long magnitude;

        public CounterEvent() {
            super();
        }
        
        public byte getMetricsType() {
            return metricsType;
        }
        
        public void setMetricsType(byte value) {
            metricsType = value;
        }
        
        public short getSamplingRate() {
            return samplingRate;
        }
        
        public void setSamplingRate(short value) {
            samplingRate = value;
        }
        
        public String getKey() {
            return key;
        }
        
        public void setKey(String value) {
            key = value;
        }
        
        public long getMagnitude() {
            return magnitude;
        }
        
        public void setMagnitude(long value) {
            magnitude = value;
        }
        
        final static public EventFactory<CounterEvent> EVENT_FACTORY = new EventFactory<CounterEvent>() 
        {
            @Override
            public CounterEvent newInstance() {
                return new CounterEvent();
            }
        };
    }
    
    final static private ThreadLocal<Random> threadLocalSampler = new ThreadLocal<Random>() {
        @Override
        public Random initialValue() {
            return new Random(System.currentTimeMillis());
        }
    };    
    
    private class CounterEventHandler implements EventHandler<CounterEvent> {
        final static private int MAX_MESSAGE_SIZE = 1000; // Assuming MTU being 1500 for typical datacenter Eternet implementation, cap message size to 1000 bytes to avoid fragmentation
        private DatagramChannel datagramChannel;
        private SocketChannel socketChannel;
        private ByteBuffer counterMessageBuffer;
        private int messagesInBuffer = 0;
        private CharsetEncoder encoder = Charset.forName("UTF-8").newEncoder();
        private WritableByteChannel outputChannel;
        private String statsdHostName;
        private int statsdPort;
        private long tcpConnectionRetryInterval = Long.MAX_VALUE;
        private long lastSuccessfulFlushTimestamp;
        private int timeDelayInMillisAfterIncompleteFlush;
        
        public CounterEventHandler(
            String statsdHostName,
            int statsdPort,
            boolean useTcp,
            int tcpMessageSize,
            long tcpConnectionRetryInterval,
            int timeDelayInMillisAfterIncompleteFlush) throws IOException {
            this.statsdHostName = statsdHostName;
            this.statsdPort = statsdPort;
            this.timeDelayInMillisAfterIncompleteFlush = timeDelayInMillisAfterIncompleteFlush;
            if (useTcp) {
                this.counterMessageBuffer = ByteBuffer.allocate(tcpMessageSize);
                this.tcpConnectionRetryInterval = tcpConnectionRetryInterval;
                openSocket();
            }
            else {
                this.counterMessageBuffer = ByteBuffer.allocate(MAX_MESSAGE_SIZE);
                this.datagramChannel = DatagramChannel.open();
                this.datagramChannel.connect(
                    new InetSocketAddress(statsdHostName, statsdPort));
                this.outputChannel = datagramChannel;
            }
            
        }

        private void openSocket() throws IOException {
            this.socketChannel = SocketChannel.open();
            this.socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
            this.socketChannel.connect(new InetSocketAddress(statsdHostName, statsdPort));
            this.outputChannel = socketChannel;
            this.socketChannel.shutdownInput();
        }
        
        private void flush() throws IOException {
            onFlushMessageBuffer();
            counterMessageBuffer.flip();
            try {
                outputChannel.write(counterMessageBuffer);
                lastSuccessfulFlushTimestamp = System.currentTimeMillis();
            }
            catch (Throwable e) {
                onFlushMessageFailure(e);
                if (socketChannel != null) {
                    if (System.currentTimeMillis() - lastSuccessfulFlushTimestamp < tcpConnectionRetryInterval) {
                        lastSuccessfulFlushTimestamp = System.currentTimeMillis();                             
                        if (socketChannel.isOpen()) {
                            socketChannel.close();
                        }
                        try {                        
                            openSocket();
                            onReconnection();
                        }
                        catch (Throwable innerException) {}
                    }
                }
            }
            finally {
                counterMessageBuffer.clear();
                messagesInBuffer = 0;
            }
        }

        private void addMessageToBuffer(String message) throws Exception {
            counterMessageBuffer.mark(); // Remember current position 
            onCounterMessageReceived();
            if (encoder.encode(CharBuffer.wrap(message), counterMessageBuffer, true) == CoderResult.OVERFLOW) {
                counterMessageBuffer.reset();
                flush();
                if (encoder.encode(CharBuffer.wrap(message), counterMessageBuffer, true) == CoderResult.OVERFLOW) {
                    // Well the message is too big to fit in a single packet, throw it away
                    counterMessageBuffer.clear();
                    onOversizedMessage(message);
                }
                else {
                    messagesInBuffer++;
                }
            }
            else {
                messagesInBuffer++;                    
            }
        }

        @Override
        public void onEvent(CounterEvent t, long l, boolean bln) throws Exception {
            // Construct counter message
            String message = t.getKey().replace(":", "_") + ":" + 
                    Long.toString(t.getMagnitude()) + 
                    METRIC_TYPE_STRINGS[t.getMetricsType()] + 
                    (t.getSamplingRate() < HUNDRED_PERCENT ? 
                        String.format("|@0.%4d\n", t.getSamplingRate()/HUNDRED_PERCENT, t.getSamplingRate()%HUNDRED_PERCENT)
                        :
                        "\n");

            addMessageToBuffer(message);
            if (bln && messagesInBuffer > 0) {
                flush();
                if (timeDelayInMillisAfterIncompleteFlush > 0) {
                    Thread.sleep(timeDelayInMillisAfterIncompleteFlush);
                }
            }
        }

        public void close() throws Exception {
            if (datagramChannel != null) {
                datagramChannel.close();
            }
            if (socketChannel != null) {
                socketChannel.close();
            }
        }
    }
    
    private RingBuffer<CounterEvent> ringBuffer;
    private ExecutorService eventProcessorExecutor;
    private BatchEventProcessor<CounterEvent> counterEventProcessor;
    private CounterEventHandler counterEventHandler;
    private short defaultSamplingRate;
    
    protected StatsdClient() {}
    public StatsdClient(
        int ringBufferSize,
        final String statsdHostName,
        final int statsdPort,
        final boolean useTcp,
        final int tcpMessageSize,
        final int tcpConnectionRetryInterval,
        final int timeDelayInMillisAfterIncompletFlush,
        short defaultSamplingRate) throws Exception {
        
        this(ringBufferSize, 
             statsdHostName, 
             statsdPort, 
             useTcp, 
             tcpMessageSize, 
             tcpConnectionRetryInterval, 
             timeDelayInMillisAfterIncompletFlush, 
             defaultSamplingRate,
             new BlockingWaitStrategy());
    }
    
    static private class NullClient extends StatsdClient {
        
        public NullClient() {}
        
        @Override
        public void sendCounterMessage(byte metricsType, short samplingRate, String key, long magnitude) {}
                
    }    
    static final public StatsdClient NULL = new NullClient();
    
    public StatsdClient(
        int ringBufferSize,
        final String statsdHostName,
        final int statsdPort,
        final boolean useTcp,
        final int tcpMessageSize,
        final int tcpConnectionRetryInterval,
        final int timeDelayInMillisAfterIncompletFlush,
        short defaultSamplingRate,
        WaitStrategy waitStrategy
        ) throws Exception {
        
        this.defaultSamplingRate = defaultSamplingRate;
        ringBuffer = new RingBuffer<>(CounterEvent.EVENT_FACTORY, 
                new MultiThreadedClaimStrategy(ringBufferSize),
                waitStrategy);
        
        counterEventHandler = new CounterEventHandler(statsdHostName, 
            statsdPort, useTcp, tcpMessageSize, tcpConnectionRetryInterval,
            timeDelayInMillisAfterIncompletFlush);
        counterEventProcessor = new BatchEventProcessor<>(ringBuffer, 
            ringBuffer.newBarrier(), counterEventHandler);        
        ringBuffer.setGatingSequences(counterEventProcessor.getSequence());
        eventProcessorExecutor = Executors.newSingleThreadExecutor();
        eventProcessorExecutor.submit(counterEventProcessor);
        
    }
    
    public void shutdown() throws Exception {
        if (counterEventProcessor != null) {
            counterEventProcessor.halt();
            counterEventProcessor = null;
        }
        if (counterEventHandler != null) {
            counterEventHandler.close();
        }
        if (eventProcessorExecutor != null) {
            eventProcessorExecutor.shutdown();
            eventProcessorExecutor = null;
        }        
    }
    
    public void incrementCounter(String key, long magnitude) {
        incrementCounter(key, magnitude, defaultSamplingRate);
    }
    
    public void incrementCounter(String key, long magnitude, short samplingRate) {
        sendCounterMessage(COUNTER_METRIC_TYPE, samplingRate, key, magnitude);
    }
    
    public void updateGauge(String key, long magnitude) {
        updateGauge(key, magnitude, defaultSamplingRate);
    }
    
    public void updateGauge(String key, long magnitude, short samplingRate) {
        sendCounterMessage(GAUGE_METRIC_TYPE, samplingRate, key, magnitude);        
    }    
    
    public void updateHistogram(String key, long magnitude) {
        updateHistogram(key, magnitude, defaultSamplingRate);
    }
    
    public void updateHistogram(String key, long magnitude, short samplingRate) {
        sendCounterMessage(HISTOGRAM_METRIC_TYPE, samplingRate, key, magnitude);        
    }    

    public void updateMeter(String key, long magnitude) {
        updateMeter(key, magnitude, defaultSamplingRate);
    }
    
    public void updateMeter(String key, long magnitude, short samplingRate) {
        sendCounterMessage(METER_METRIC_TYPE, samplingRate, key, magnitude);        
    }    

    public void updateTimer(String key, long magnitude) {
        updateTimer(key, magnitude, defaultSamplingRate);
    }
    
    public void updateTimer(String key, long magnitude, short samplingRate) {
        sendCounterMessage(TIMER_METRIC_TYPE, samplingRate, key, magnitude);        
    }    

    // Extension points for intercepting interesting events in the statsd client
    // such as a counter message being picked up by the consumer thread and
    // when the message buffer is flushed down the wire.
    protected void onCounterMessageReceived() {}
    protected void onFlushMessageBuffer() {}
    protected void onFlushMessageFailure(Throwable exception) {}
    protected void onReconnection() {}
    protected void onOversizedMessage(String message) {}
    protected void onRingBufferOverflow() {}
    
    public void sendCounterMessage(byte metricsType, short samplingRate, String key, long magnitude) {
                
        if (samplingRate < HUNDRED_PERCENT 
            && threadLocalSampler.get().nextInt(HUNDRED_PERCENT) >= samplingRate) {
            return;
        }

        try {
            long sequence = ringBuffer.tryNext(1);
            CounterEvent event = ringBuffer.get(sequence);
            event.setMetricsType(metricsType);
            event.setSamplingRate(samplingRate);
            event.setKey(key);
            event.setMagnitude(magnitude);            
            ringBuffer.publish(sequence);
        }
        catch (InsufficientCapacityException e) {
            onRingBufferOverflow();
        }
    }
}
