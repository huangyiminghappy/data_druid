package io.druid.firehose.rocketmq;

import com.alibaba.rocketmq.client.consumer.*;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.consumer.store.ReadOffsetType;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.metamx.common.exception.FormattedException;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class RocketMQFirehoseFactory implements FirehoseFactory<ByteBufferInputRowParser> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMQFirehoseFactory.class);

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

    /**
     * Topic-Queue mapping.
     */
    private final ConcurrentHashMap<String, Set<MessageQueue>> topicQueueMap = new ConcurrentHashMap<>();;

    /**
     * Store messages that are fetched from brokers but not yet delivered to druid via fire hose.
     */
    private final ConcurrentHashMap<MessageQueue, ConcurrentSkipListSet<MessageExt>> messageQueueTreeSetMap = new ConcurrentHashMap<>();

    /**
     * Store message consuming status.
     */
    private final ConcurrentHashMap<MessageQueue, ConcurrentSkipListSet<Long>> windows = new ConcurrentHashMap<>();

    private final ScheduledExecutorService pullMessageThreadPool = Executors.newSingleThreadScheduledExecutor();

    private final Map<String, String> topicTagMap = new HashMap<String, String>();

    /**
     * Default pull batch size.
     */
    private static final int PULL_BATCH_SIZE = 32;

    private static final int MESSAGE_COUNT_THRESHOLD = 10000;

    private static final long THRESHOLD_SLEEP_INTERVAL = 1000L;

    @JsonCreator
    public RocketMQFirehoseFactory(@JsonProperty("consumerProps") Properties consumerProps,
                                   @JsonProperty("consumerGroup") String consumerGroup,
                                   @JsonProperty("feed") String feed,
                                   @JsonProperty ByteBufferInputRowParser parser) {
        this.consumerProps = consumerProps;
        for (Map.Entry<Object, Object> configItem : this.consumerProps.entrySet()) {
            System.setProperty(configItem.getKey().toString(), configItem.getValue().toString());
        }
        this.consumerGroup = consumerGroup;
        this.feed = feed;
        this.parser = parser;
    }

    private void executePullRequest(final DefaultMQPullConsumer pullConsumer, final DruidPullRequest pullRequest) {
        pullMessageThreadPool.execute(new PullMessageTask(pullConsumer, pullRequest));
    }

    private void executePullRequestLater(final DefaultMQPullConsumer pullConsumer,
                                         final DruidPullRequest pullRequest, long interval) {
        pullMessageThreadPool.schedule(new PullMessageTask(pullConsumer, pullRequest), interval, TimeUnit.MILLISECONDS);
    }

    private class PullMessageTask implements Runnable {

        private final DefaultMQPullConsumer pullConsumer;
        private final DruidPullRequest pullRequest;

        PullMessageTask(DefaultMQPullConsumer pullConsumer, DruidPullRequest pullRequest) {
            this.pullRequest = pullRequest;
            this.pullConsumer = pullConsumer;
        }

        @Override
        public void run() {
            try {
                if (!pullRequest.isLongPull()) {
                    pullConsumer.pull(
                            pullRequest.getMessageQueue(),
                            topicTagMap.get(pullRequest.getMessageQueue().getTopic()),
                            pullRequest.getNextBeginOffset(),
                            pullRequest.getPullBatchSize(),
                            new DruidPullCallback(pullConsumer, pullRequest, messageQueueTreeSetMap)
                    );
                } else {
                    pullConsumer.pullBlockIfNotFound(
                            pullRequest.getMessageQueue(),
                            topicTagMap.get(pullRequest.getMessageQueue().getTopic()),
                            pullRequest.getNextBeginOffset(),
                            pullRequest.getPullBatchSize(),
                            new DruidPullCallback(pullConsumer, pullRequest, messageQueueTreeSetMap)
                    );
                }
            } catch (Throwable e) {
                LOGGER.error("Error while pulling", e);
            }
        }
    }

    /**
     * Check if there are locally pending messages to consume.
     * @return true if there are some; false otherwise.
     */
    private boolean hasMessagesPending() {

        for (ConcurrentHashMap.Entry<MessageQueue, ConcurrentSkipListSet<MessageExt>> entry : messageQueueTreeSetMap.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                return true;
            }
        }

        return false;
    }

    private boolean isThrottling(int threshold) {
        int count = 0;
        for (Map.Entry<MessageQueue, ConcurrentSkipListSet<MessageExt>> next : messageQueueTreeSetMap.entrySet()) {
            count += next.getValue().size();
        }

        return count >= threshold;
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



        /*
         * Default Pull-style client for RocketMQ.
         */
        final DefaultMQPullConsumer defaultMQPullConsumer;

        topicQueueMap.clear();
        messageQueueTreeSetMap.clear();
        windows.clear();

        try {
            defaultMQPullConsumer = new DefaultMQPullConsumer(this.consumerGroup);
            defaultMQPullConsumer.setMessageModel(MessageModel.CLUSTERING);

            String[] subscriptions = feed.split(",");
            for (String subscription : subscriptions) {
                subscription = subscription.trim();
                if (subscription.isEmpty()) {
                    continue;
                }

                String topic;
                String tags;
                if (subscription.contains(":")) {
                    String[] segments = subscription.split(":");
                    topic = segments[0];
                    tags = Strings.isNullOrEmpty(segments[1]) ? "*" : segments[1];
                } else {
                    topic = subscription;
                    tags = "*";
                }

                topicTagMap.put(topic, tags);
                topicQueueMap.put(topic, new ConcurrentSet<MessageQueue>());
            }

            DruidMessageQueueListener druidMessageQueueListener = new DruidMessageQueueListener(Sets.newHashSet(subscriptions), topicQueueMap, defaultMQPullConsumer);
            defaultMQPullConsumer.setMessageQueueListener(druidMessageQueueListener);
            defaultMQPullConsumer.start();

        } catch (MQClientException e) {
            LOGGER.error("Failed to start DefaultMQPullConsumer", e);
            throw new IOException("Failed to start RocketMQ client", e);
        }


        return new Firehose() {
            @Override
            public boolean hasMore() {
                return hasMessagesPending();
            }

            @Override
            public InputRow nextRow() throws FormattedException {
                for (Map.Entry<MessageQueue, ConcurrentSkipListSet<MessageExt>> entry : messageQueueTreeSetMap.entrySet()) {
                    if (!entry.getValue().isEmpty()) {
                        MessageExt message = entry.getValue().pollFirst();
                        InputRow inputRow = theParser.parse(ByteBuffer.wrap(message.getBody()));

                        if (!windows.keySet().contains(entry.getKey())) {
                            windows.put(entry.getKey(), new ConcurrentSkipListSet<Long>());
                        }
                        windows.get(entry.getKey()).add(message.getQueueOffset());
                        return inputRow;
                    }
                }

                // should never happen.
                throw new IllegalStateException("Unexpected State");
            }

            @Override
            public Runnable commit() {
                return new Runnable() {
                    @Override
                    public void run() {
                        OffsetStore offsetStore = defaultMQPullConsumer.getOffsetStore();
                        Set<MessageQueue> updated = new HashSet<>();
                        // calculate offsets according to consuming windows.
                        for (ConcurrentHashMap.Entry<MessageQueue, ConcurrentSkipListSet<Long>> entry : windows.entrySet()) {
                            while (!entry.getValue().isEmpty()) {

                                long offset = offsetStore.readOffset(entry.getKey(), ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                                if (offset + 1 > entry.getValue().first()) {
                                    entry.getValue().pollFirst();
                                } else if (offset + 1 == entry.getValue().first()) {
                                    entry.getValue().pollFirst();
                                    offsetStore.updateOffset(entry.getKey(), offset + 1, true);
                                    updated.add(entry.getKey());
                                } else {
                                    break;
                                }

                            }
                        }
                        offsetStore.persistAll(updated);
                    }
                };
            }

            @Override
            public void close() throws IOException {
                defaultMQPullConsumer.shutdown();
            }
        };
    }


    /**
     * Pull request.
     */
    private class DruidPullRequest {
        private MessageQueue messageQueue;
        private long nextBeginOffset;
        private int pullBatchSize;
        private boolean longPull;

        DruidPullRequest() {
            pullBatchSize = PULL_BATCH_SIZE;
        }

        MessageQueue getMessageQueue() {
            return messageQueue;
        }

        void setMessageQueue(MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        long getNextBeginOffset() {
            return nextBeginOffset;
        }

        void setNextBeginOffset(long nextBeginOffset) {
            this.nextBeginOffset = nextBeginOffset;
        }

        int getPullBatchSize() {
            return pullBatchSize;
        }

        void setPullBatchSize(int pullBatchSize) {
            this.pullBatchSize = pullBatchSize;
        }

        boolean isLongPull() {
            return longPull;
        }

        void setLongPull(boolean longPull) {
            this.longPull = longPull;
        }
    }

    /**
     * Compare messages pulled from same message queue according to queue offset.
     */
    private class MessageComparator implements Comparator<MessageExt> {
        @Override
        public int compare(MessageExt lhs, MessageExt rhs) {
            return lhs.getQueueOffset() < rhs.getQueueOffset() ? -1 : (lhs.getQueueOffset() == rhs.getQueueOffset() ? 0 : 1);
        }
    }


    /**
     * Handle message queues re-balance operations.
     */
    private class DruidMessageQueueListener implements MessageQueueListener {

        private Set<String> topics;

        private final ConcurrentHashMap<String, Set<MessageQueue>> topicQueueMap;

        private final DefaultMQPullConsumer defaultMQPullConsumer;


        DruidMessageQueueListener(final Set<String> topics,
                                  final ConcurrentHashMap<String, Set<MessageQueue>> topicQueueMap,
                                  final DefaultMQPullConsumer defaultMQPullConsumer) {
            this.topics = topics;
            this.topicQueueMap = topicQueueMap;
            this.defaultMQPullConsumer = defaultMQPullConsumer;
        }

        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            if (topics.contains(topic)) {

                Set<MessageQueue> previous = topicQueueMap.get(topic);

                // Update topicQueueMap
                topicQueueMap.put(topic, mqDivided);

                for (MessageQueue messageQueue : mqDivided) {
                    if (null == previous || !previous.contains(messageQueue)) {
                        LOGGER.info("New message queue allocated to this client: {}", messageQueue.toString());

                        DruidPullRequest druidPullRequest = new DruidPullRequest();
                        druidPullRequest.setMessageQueue(messageQueue);
                        try {
                            long offset = defaultMQPullConsumer.fetchConsumeOffset(messageQueue, false);
                            druidPullRequest.setNextBeginOffset(offset);
                        } catch (MQClientException e) {
                            LOGGER.error("Failed to fetch consume offset for queue: {}", messageQueue);
                            continue;
                        }
                        druidPullRequest.setLongPull(true);
                        messageQueueTreeSetMap.putIfAbsent(messageQueue, new ConcurrentSkipListSet<MessageExt>());
                        windows.putIfAbsent(messageQueue, new ConcurrentSkipListSet<Long>());
                        executePullRequest(defaultMQPullConsumer, druidPullRequest);
                    }
                }

                // Remove message queues that are re-assigned to other clients.
                Iterator<ConcurrentHashMap.Entry<MessageQueue, ConcurrentSkipListSet<MessageExt>>> it =
                        messageQueueTreeSetMap.entrySet().iterator();
                while (it.hasNext()) {
                    if (!mqDivided.contains(it.next().getKey())) {
                        it.remove();
                    }
                }

                // Remove message queue windows that are re-assigned to other clients
                Iterator<ConcurrentHashMap.Entry<MessageQueue, ConcurrentSkipListSet<Long>>> windowIterator =
                        windows.entrySet().iterator();
                while (windowIterator.hasNext()) {
                    if (!mqDivided.contains(windowIterator.next().getKey())) {
                        windowIterator.remove();
                    }
                }


                StringBuilder stringBuilder = new StringBuilder();
                for (MessageQueue messageQueue : mqDivided) {
                    stringBuilder.append(messageQueue.getBrokerName())
                            .append("#")
                            .append(messageQueue.getQueueId())
                            .append(", ");
                }

                if (LOGGER.isDebugEnabled() && stringBuilder.length() > 2) {
                    LOGGER.debug(String.format("%s@%s is consuming the following message queues: %s",
                            defaultMQPullConsumer.getClientIP(),
                            defaultMQPullConsumer.getInstanceName(),
                            stringBuilder.substring(0, stringBuilder.length() - 2) /*Remove the trailing comma*/));
                }
            }

        }
    }

    private class DruidPullCallback implements PullCallback {

        private final DefaultMQPullConsumer pullConsumer;
        private final DruidPullRequest pullRequest;
        private final ConcurrentHashMap<MessageQueue, ConcurrentSkipListSet<MessageExt>> messageQueueTreeSetMap;

        DruidPullCallback(final DefaultMQPullConsumer pullConsumer,
                                 final DruidPullRequest pullRequest,
                                 final ConcurrentHashMap<MessageQueue, ConcurrentSkipListSet<MessageExt>> messageQueueTreeSetMap) {
            this.pullConsumer = pullConsumer;
            this.pullRequest = pullRequest;
            this.messageQueueTreeSetMap = messageQueueTreeSetMap;
        }

        @Override
        public void onSuccess(PullResult pullResult) {
            try {
                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        boolean valid = false;
                        for (Map.Entry<String, Set<MessageQueue>> next : topicQueueMap.entrySet()) {
                            for (MessageQueue messageQueue : next.getValue()) {
                                if (messageQueue.equals(pullRequest.getMessageQueue())) {
                                    valid = true;
                                    break;
                                }
                            }
                        }

                        if (!valid) {
                            LOGGER.warn("Message Queue {} is removed, pullResult dropped.", pullRequest.getMessageQueue().toString());
                            return;
                        }

                        if (!messageQueueTreeSetMap.keySet().contains(pullRequest.getMessageQueue())) {
                            messageQueueTreeSetMap.putIfAbsent(pullRequest.getMessageQueue(), new ConcurrentSkipListSet<>(new MessageComparator()));
                        }

                        messageQueueTreeSetMap.get(pullRequest.getMessageQueue()).addAll(pullResult.getMsgFoundList());
                        break;

                    case NO_NEW_MSG:
                        LOGGER.debug("No new messages found");
                        break;

                    case NO_MATCHED_MSG:
                        LOGGER.debug("No matched messages after filtering");
                        break;

                    case OFFSET_ILLEGAL:
                        LOGGER.error("Offset illegal");
                        break;

                    case SLAVE_LAG_BEHIND:
                        LOGGER.error("Slave lags behind");
                        break;

                    case SUBSCRIPTION_NOT_LATEST:
                        LOGGER.error("Subscription not latest");
                        break;

                    default:
                        LOGGER.error("Unknown pull status");
                        break;
                }
            } catch (Exception e) {
                LOGGER.error("Error while processing pullCallback", e);
            } finally {
                scheduleNextPull(pullResult.getNextBeginOffset());
            }
        }

        @Override
        public void onException(Throwable e) {
            LOGGER.error("Pull Failed", e);
            scheduleNextPull(pullRequest.getNextBeginOffset());
        }

        private void scheduleNextPull(long nextBeginOffset) {
            String topic = pullRequest.getMessageQueue().getTopic();
            if (topicQueueMap.get(topic).contains(pullRequest.getMessageQueue())) {
                DruidPullRequest nextPull = new DruidPullRequest();
                nextPull.setMessageQueue(pullRequest.getMessageQueue());
                nextPull.setLongPull(pullRequest.isLongPull());
                nextPull.setNextBeginOffset(nextBeginOffset);
                nextPull.setPullBatchSize(pullRequest.getPullBatchSize());

                if (isThrottling(MESSAGE_COUNT_THRESHOLD)) {
                    executePullRequestLater(pullConsumer, nextPull, THRESHOLD_SLEEP_INTERVAL);
                } else {
                    executePullRequest(pullConsumer, nextPull);
                }
            } else {
                LOGGER.info("Message Queue: {} is dropped", pullRequest.getMessageQueue().toString());
            }
        }
    }

    @Override
    public InputRowParser getParser() {
        return parser;
    }
}
