package org.crustee.raft.storage.write;

import java.nio.ByteBuffer;
import java.util.Map;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.RingBuffer;

public class WriteEventProducer {

    private final EventTranslatorThreeArg<WriteEvent, ByteBuffer, ByteBuffer, Map> translator = new WriteEventTranslator();
    private final RingBuffer<WriteEvent> ringBuffer;

    public WriteEventProducer(RingBuffer<WriteEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void onWriteRequest(ByteBuffer command, ByteBuffer key, Map values) {
        ringBuffer.publishEvent(translator, command, key, values);
    }

    private static class WriteEventTranslator implements EventTranslatorThreeArg<WriteEvent, ByteBuffer, ByteBuffer, Map> {

        @Override
        public void translateTo(WriteEvent event, long sequence, ByteBuffer command, ByteBuffer rowKey, Map values) {
            event.publish(command, rowKey, values);
        }
    }
}
