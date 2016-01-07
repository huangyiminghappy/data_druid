package io.druid.firehose.rocketmq;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MessageQueueListener;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import org.relaxng.datatype.DatatypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

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
     * Topics to consume.
     * Multiple topics are separated by comma ",".
     */
    @JsonProperty
    private final String feed;

    private final DefaultMQPullConsumer defaultMQPullConsumer;

    private final ConcurrentHashMap<String, Set<MessageQueue>> topicQueueMap;

    /**
     *
     */
    private final ConcurrentHashMap<MessageQueue, AtomicLong> offsets = new ConcurrentHashMap<>();

    /**
     * Store messages that are fetched from brokers but not yet delivered to druid via fire hose.
     */
    private ConcurrentHashMap<MessageQueue, ConcurrentSkipListSet<MessageExt>> messageQueueTreeSetMap = new ConcurrentHashMap<>();

    /**
     * Store message consuming status.
     */
    private ConcurrentHashMap<MessageQueue, ConcurrentSkipListSet<Long>> windows = new ConcurrentHashMap<>();

    /**
     * Default pull batch size.
     */
    private static final int PULL_BATCH_SIZE = 32;

    private final DruidPullMessageService pullMessageService;

    @JsonCreator
    public RocketMQFirehoseFactory(@JsonProperty("consumerProps") Properties consumerProps,
                                   @JsonProperty("consumerGroup") String consumerGroup,
                                   @JsonProperty("feed") String feed) {
        this.consumerProps = consumerProps;
        for (Map.Entry<Object, Object> configItem : this.consumerProps.entrySet()) {
            System.setProperty(configItem.getKey().toString(), configItem.getValue().toString());
        }
        this.consumerGroup = consumerGroup;
        this.feed = feed;
        defaultMQPullConsumer = new DefaultMQPullConsumer(this.consumerGroup);
        defaultMQPullConsumer.setMessageModel(MessageModel.CLUSTERING);
        topicQueueMap = new ConcurrentHashMap<>();

        pullMessageService = new DruidPullMessageService();
    }

    private boolean hasMessagesPending() {

        for (ConcurrentHashMap.Entry<MessageQueue, ConcurrentSkipListSet<MessageExt>> entry : messageQueueTreeSetMap.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Firehose connect(ByteBufferInputRowParser byteBufferInputRowParser) throws IOException, ParseException {

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
            if (!feed.contains(",")) {
                defaultMQPullConsumer.fetchSubscribeMessageQueues(feed);
                defaultMQPullConsumer.setMessageQueueListener(new DruidMessageQueueListener(Sets.newHashSet(feed)));
                topicQueueMap.put(feed, defaultMQPullConsumer.fetchMessageQueuesInBalance(feed));
            } else {
                String[] topics = feed.split(",");
                for (String topic : topics) {
                    defaultMQPullConsumer.fetchSubscribeMessageQueues(topic);
                    topicQueueMap.put(topic, defaultMQPullConsumer.fetchMessageQueuesInBalance(topic));
                }
                defaultMQPullConsumer.setMessageQueueListener(new DruidMessageQueueListener(Sets.newHashSet(topics)));
            }

            defaultMQPullConsumer.start();
            pullMessageService.start();
        } catch (MQClientException e) {
            throw new IOException("Unable to start RocketMQ client", e);
        }

        return new Firehose() {

            @Override
            public boolean hasMore() {
                boolean hasMore = false;
                DruidPullRequest earliestPullRequest = null;

                for (Map.Entry<String, Set<MessageQueue>> entry : topicQueueMap.entrySet()) {
                    for (MessageQueue messageQueue : entry.getValue()) {
                        if (messageQueueTreeSetMap.keySet().contains(messageQueue)
                                && !messageQueueTreeSetMap.get(messageQueue).isEmpty()) {
                            hasMore = true;
                        } else {
                            DruidPullRequest newPullRequest = new DruidPullRequest();
                            newPullRequest.setMessageQueue(messageQueue);

                            if (offsets.containsKey(messageQueue)) {
                                newPullRequest.setNextBeginOffset(offsets.get(messageQueue).get());
                            } else {
                                try {
                                    long offset = defaultMQPullConsumer.fetchConsumeOffset(messageQueue, false);
                                    newPullRequest.setNextBeginOffset(offset);
                                } catch (MQClientException e) {
                                    LOGGER.error("Failed to fetch consume offset for queue: {}", entry.getKey());
                                    continue;
                                }
                            }
                            newPullRequest.setLongPull(!hasMessagesPending());

                            // notify pull message service to pull messages from brokers.
                            pullMessageService.putRequest(newPullRequest);

                            // set the earliest pull in case we need to block.
                            if (null == earliestPullRequest) {
                                earliestPullRequest = newPullRequest;
                            }
                        }
                    }
                }

                // Block only when there is no locally pending messages.
                if (!hasMore && null != earliestPullRequest) {
                    try {
                        earliestPullRequest.getCountDownLatch().await();
                        hasMore = true;
                    } catch (InterruptedException e) {
                        LOGGER.error("CountDownLatch await got interrupted", e);
                    }
                }
                return hasMore;
            }

            @Override
            public InputRow nextRow() {
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
                return null;
            }

            @Override
            public Runnable commit() {
                return new Runnable() {
                    @Override
                    public void run() {

                        // calculate offsets according to consuming windows.
                        for (ConcurrentHashMap.Entry<MessageQueue, ConcurrentSkipListSet<Long>> entry : windows.entrySet()) {
                            while (!entry.getValue().isEmpty()) {
                                if (offsets.get(entry.getKey()).get() + 1 == entry.getValue().first()) {
                                    entry.getValue().pollFirst();
                                    offsets.get(entry.getKey()).incrementAndGet();
                                } else {
                                    break;
                                }
                            }
                        }

                        OffsetStore offsetStore = defaultMQPullConsumer.getOffsetStore();
                        for (Map.Entry<MessageQueue, AtomicLong> entry : offsets.entrySet()) {
                            offsetStore.updateOffset(entry.getKey(), entry.getValue().get(), true);
                        }

                        offsetStore.persistAll(offsets.keySet());
                    }
                };
            }

            @Override
            public void close() throws IOException {
                defaultMQPullConsumer.shutdown();
                pullMessageService.shutdown(false);
            }
        };
    }

    class DruidPullRequest {
        private MessageQueue messageQueue;
        private String tag;
        private long nextBeginOffset;
        private int pullBatchSize;
        private boolean longPull;
        private CountDownLatch countDownLatch;
        private PullResult pullResult;
        private boolean successful;

        public DruidPullRequest() {
            countDownLatch  = new CountDownLatch(1);
            tag = "*";
            pullBatchSize = PULL_BATCH_SIZE;
            successful = false;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public void setMessageQueue(MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        public long getNextBeginOffset() {
            return nextBeginOffset;
        }

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }

        public void setNextBeginOffset(long nextBeginOffset) {
            this.nextBeginOffset = nextBeginOffset;
        }

        public int getPullBatchSize() {
            return pullBatchSize;
        }

        public void setPullBatchSize(int pullBatchSize) {
            this.pullBatchSize = pullBatchSize;
        }

        public boolean isLongPull() {
            return longPull;
        }

        public void setLongPull(boolean longPull) {
            this.longPull = longPull;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public PullResult getPullResult() {
            return pullResult;
        }

        public void setPullResult(PullResult pullResult) {
            this.pullResult = pullResult;
        }

        public boolean isSuccessful() {
            return successful;
        }

        public void setSuccessful(boolean successful) {
            this.successful = successful;
        }
    }


    class DruidPullMessageService extends ServiceThread {

        private volatile List<DruidPullRequest> requestsWrite = new ArrayList<DruidPullRequest>();
        private volatile List<DruidPullRequest> requestsRead = new ArrayList<DruidPullRequest>();

        public void putRequest(final DruidPullRequest request) {
            synchronized (this) {
                this.requestsWrite.add(request);
                if (!hasNotified) {
                    hasNotified = true;
                    notify();
                }
            }
        }

        private void swapRequests() {
            List<DruidPullRequest> tmp = requestsWrite;
            requestsWrite = requestsRead;
            requestsRead = tmp;
        }

        @Override
        public String getServiceName() {
            return getClass().getSimpleName();
        }

        private void doPull() {
            for (DruidPullRequest pullRequest : requestsRead) {
                PullResult pullResult = null;
                try {
                    if (!pullRequest.isLongPull()) {
                        pullResult = defaultMQPullConsumer.pull(
                                pullRequest.getMessageQueue(),
                                pullRequest.getTag(),
                                pullRequest.getNextBeginOffset(),
                                pullRequest.getPullBatchSize());
                    } else {
                        pullResult = defaultMQPullConsumer.pullBlockIfNotFound(
                                pullRequest.getMessageQueue(),
                                pullRequest.getTag(),
                                pullRequest.getNextBeginOffset(),
                                pullRequest.getPullBatchSize()
                        );
                    }
                    pullRequest.setPullResult(pullResult);
                    pullRequest.setSuccessful(true);

                    if (!messageQueueTreeSetMap.keySet().contains(pullRequest.getMessageQueue())) {
                        messageQueueTreeSetMap.putIfAbsent(pullRequest.getMessageQueue(),
                                new ConcurrentSkipListSet<>(new MessageComparator()));
                    }
                    messageQueueTreeSetMap.get(pullRequest.getMessageQueue()).addAll(pullResult.getMsgFoundList());

                } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
                    LOGGER.error("Failed to pull message from broker.", e);
                } finally {
                    pullRequest.getCountDownLatch().countDown();
                }

            }
            requestsRead.clear();
        }

        @Override
        public void run() {
            LOGGER.info(getServiceName() + " starts.");
            while (!isStoped()) {
                waitForRunning(0);
                doPull();
            }

            // 在正常shutdown情况下，等待请求到来，然后再刷盘
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                LOGGER.error("", e);
            }


            synchronized (this) {
                swapRequests();
            }

            doPull();
            LOGGER.info(getServiceName() + " terminated.");

        }

        @Override
        protected void onWaitEnd() {
            swapRequests();
        }
    }

    class MessageComparator implements Comparator<MessageExt> {
        @Override
        public int compare(MessageExt lhs, MessageExt rhs) {
            return lhs.getQueueOffset() < rhs.getQueueOffset() ? -1 : (lhs.getQueueOffset() == rhs.getQueueOffset() ? 0 : 1);
        }
    }

    class DruidMessageQueueListener implements MessageQueueListener {

        private Set<String> topics;

        public DruidMessageQueueListener(Set<String> topics) {
            this.topics = topics;
        }

        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            if (topics.contains(topic)) {
                topicQueueMap.put(topic, mqDivided);

                // Remove message queues that are re-assigned to other clients.
                Iterator<ConcurrentHashMap.Entry<MessageQueue, AtomicLong>> iterator = offsets.entrySet().iterator();
                while (iterator.hasNext()) {
                    if (!mqDivided.contains(iterator.next().getKey())) {
                        iterator.remove();
                    }
                }

                Iterator<ConcurrentHashMap.Entry<MessageQueue, ConcurrentSkipListSet<MessageExt>>> it =
                        messageQueueTreeSetMap.entrySet().iterator();
                while (it.hasNext()) {
                    if (!mqDivided.contains(it.next().getKey())) {
                        it.remove();
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
}
