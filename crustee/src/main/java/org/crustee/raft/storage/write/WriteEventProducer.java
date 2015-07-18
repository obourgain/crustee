package org.crustee.raft.storage.write;

import java.nio.ByteBuffer;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;

public class WriteEventProducer {

    private final EventTranslatorTwoArg<WriteEvent, ByteBuffer, ByteBuffer> translator = new WriteEventTranslator();
    private final RingBuffer<WriteEvent> ringBuffer;

    public WriteEventProducer(RingBuffer<WriteEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void onWriteRequest(ByteBuffer key, ByteBuffer value) {
        ringBuffer.publishEvent(translator, key, value);
    }

    private static class WriteEventTranslator implements EventTranslatorTwoArg<WriteEvent, ByteBuffer, ByteBuffer> {

        @Override
        public void translateTo(WriteEvent event, long sequence, ByteBuffer key, ByteBuffer value) {
            event.setKey(key);
            event.setValue(value);
        }
    }
}
