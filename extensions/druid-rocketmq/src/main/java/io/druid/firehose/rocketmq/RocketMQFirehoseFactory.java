package io.druid.firehose.rocketmq;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import com.metamx.common.exception.FormattedException;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

public class RocketMQFirehoseFactory implements FirehoseFactory<ByteBufferInputRowParser> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMQFirehoseFactory.class);

    private static final int BLOCKING_QUEUE_SIZE = 1000;

    /**
     * Passed in configuration for consumer client.
     */
    @JsonProperty
    private final Properties consumerProps;

    @JsonProperty
    private final String consumerGroup;

    /**
     * Topic to consume.
     */
    @JsonProperty
    private final String feed;

    @JsonProperty
    private final ByteBufferInputRowParser parser;

    private final DefaultMQPushConsumer defaultMQPushConsumer;

    private final LinkedBlockingQueue<Message> blockingQueue;

    private volatile boolean notified = true;

    @JsonCreator
    public RocketMQFirehoseFactory(@JsonProperty("consumerProps") Properties consumerProps,
                                   @JsonProperty("consumerGroup") String consumerGroup,
                                   @JsonProperty("feed") String feed,
                                   @JsonProperty ByteBufferInputRowParser parser) {
        this.consumerProps = consumerProps;
        for (Map.Entry<Object, Object> configItem : consumerProps.entrySet()) {
            System.setProperty(configItem.getKey().toString(), configItem.getValue().toString());
        }
        this.consumerGroup = consumerGroup;
        this.feed = feed;
        this.parser = parser;
        defaultMQPushConsumer = new DefaultMQPushConsumer(consumerGroup);
        defaultMQPushConsumer.setPersistConsumerOffsetInterval(Integer.MAX_VALUE);
        defaultMQPushConsumer.setMessageModel(MessageModel.CLUSTERING);
        blockingQueue = new LinkedBlockingQueue<>(BLOCKING_QUEUE_SIZE);
    }

    @Override
    public Firehose connect(ByteBufferInputRowParser byteBufferInputRowParser) throws IOException {

        Set<String> newDimExclus = Sets.union(
                byteBufferInputRowParser.getParseSpec().getDimensionsSpec().getDimensionExclusions(),
                Sets.newHashSet("feed")
        );

        final ByteBufferInputRowParser theParser = byteBufferInputRowParser.withParseSpec(
                byteBufferInputRowParser.getParseSpec()
                        .withDimensionsSpec(
                                byteBufferInputRowParser.getParseSpec()
                                        .getDimensionsSpec()
                                        .withDimensionExclusions(
                                                newDimExclus
                                        )
                        )
        );

        try {
            defaultMQPushConsumer.subscribe(feed, "*");
            defaultMQPushConsumer.setMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                ConsumeConcurrentlyContext context) {
                    try {
                        for (Message message : msgs) {
                            blockingQueue.put(message);

                            if (!notified) {
                                synchronized (RocketMQFirehoseFactory.this) {
                                    RocketMQFirehoseFactory.this.notify();
                                    notified = true;
                                }
                            }
                        }
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    } catch (InterruptedException e) {
                        LOGGER.error("Exception raised while putting message into blocking queue", e);
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
            });
            defaultMQPushConsumer.start();
        } catch (MQClientException e) {
            throw new IOException("Unable to start RocketMQ client", e);
        }


        return new Firehose() {
            @Override
            public boolean hasMore() {
                if (blockingQueue.size() <= 0) {
                    try {
                        synchronized (RocketMQFirehoseFactory.this) {
                            notified = false;
                            RocketMQFirehoseFactory.this.wait();
                        }
                    } catch (InterruptedException e) {
                        LOGGER.warn("No Messages available but wait() method got interrupted.");
                        return false;
                    }
                }
                return true;
            }

            @Override
            public InputRow nextRow() throws FormattedException {
                Message message = null;
                try {
                    message = blockingQueue.take();
                    return theParser.parse(ByteBuffer.wrap(message.getBody()));
                } catch (Exception e) {
                    throw new FormattedException.Builder()
                            .withErrorCode(FormattedException.ErrorCode.UNPARSABLE_ROW)
                            .withMessage(String.format("Error parsing[%s], got [%s]",
                                    null == message ? null : ByteBuffer.wrap(message.getBody()),
                                    e.toString()))
                            .build();
                }
            }

            @Override
            public Runnable commit() {
                return new Runnable() {
                    @Override
                    public void run() {
                        LOGGER.info("To commit offsets.");
                        defaultMQPushConsumer.getDefaultMQPushConsumerImpl().persistConsumerOffset();
                        LOGGER.info("Offsets committed.");
                    }
                };
            }

            @Override
            public void close() throws IOException {
                defaultMQPushConsumer.shutdown();
            }
        };
    }

    @Override
    public InputRowParser getParser() {
        return parser;
    }
}
