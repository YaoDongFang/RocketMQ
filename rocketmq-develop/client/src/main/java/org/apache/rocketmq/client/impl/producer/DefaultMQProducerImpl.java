/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.producer;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.hook.CheckForbiddenHook;
import org.apache.rocketmq.client.hook.EndTransactionContext;
import org.apache.rocketmq.client.hook.EndTransactionHook;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionExecuter;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.RequestCallback;
import org.apache.rocketmq.client.producer.RequestFutureHolder;
import org.apache.rocketmq.client.producer.RequestResponseFuture;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.compression.CompressionType;
import org.apache.rocketmq.common.compression.Compressor;
import org.apache.rocketmq.common.compression.CompressorFactory;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.CorrelationIdUtil;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class DefaultMQProducerImpl implements MQProducerInner {

    private final Logger log = LoggerFactory.getLogger(DefaultMQProducerImpl.class);
    private final Random random = new Random();
    private final DefaultMQProducer defaultMQProducer;
    private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable =
        new ConcurrentHashMap<>();
    private final ArrayList<SendMessageHook> sendMessageHookList = new ArrayList<>();
    private final ArrayList<EndTransactionHook> endTransactionHookList = new ArrayList<>();
    private final RPCHook rpcHook;
    private final BlockingQueue<Runnable> asyncSenderThreadPoolQueue;
    private final ExecutorService defaultAsyncSenderExecutor;
    protected BlockingQueue<Runnable> checkRequestQueue;
    protected ExecutorService checkExecutor;
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory;
    private ArrayList<CheckForbiddenHook> checkForbiddenHookList = new ArrayList<>();
    private MQFaultStrategy mqFaultStrategy = new MQFaultStrategy();
    private ExecutorService asyncSenderExecutor;

    // compression related
    private int compressLevel = Integer.parseInt(System.getProperty(MixAll.MESSAGE_COMPRESS_LEVEL, "5"));
    private CompressionType compressType = CompressionType.of(System.getProperty(MixAll.MESSAGE_COMPRESS_TYPE, "ZLIB"));
    private final Compressor compressor = CompressorFactory.getCompressor(compressType);

    // backpressure related
    // 反压力相关
    private Semaphore semaphoreAsyncSendNum;
    private Semaphore semaphoreAsyncSendSize;

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer) {
        this(defaultMQProducer, null);
    }

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer, RPCHook rpcHook) {
        //保存defaultMQProducer和rpcHook
        this.defaultMQProducer = defaultMQProducer;
        this.rpcHook = rpcHook;

        /*
         * 异步发送消息的线程池队列
         */
        this.asyncSenderThreadPoolQueue = new LinkedBlockingQueue<>(50000);
        /*
         * 默认的异步发送消息的线程池
         * 核心线程和最大线程数量都是当前服务器的可用线程数
         */
        this.defaultAsyncSenderExecutor = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.asyncSenderThreadPoolQueue,
            new ThreadFactoryImpl("AsyncSenderExecutor_"));
        if (defaultMQProducer.getBackPressureForAsyncSendNum() > 10) {
            semaphoreAsyncSendNum = new Semaphore(Math.max(defaultMQProducer.getBackPressureForAsyncSendNum(), 10), true);
        } else {
            semaphoreAsyncSendNum = new Semaphore(10, true);
            log.info("semaphoreAsyncSendNum can not be smaller than 10.");
        }

        if (defaultMQProducer.getBackPressureForAsyncSendNum() > 1024 * 1024) {
            semaphoreAsyncSendSize = new Semaphore(Math.max(defaultMQProducer.getBackPressureForAsyncSendNum(), 1024 * 1024), true);
        } else {
            semaphoreAsyncSendSize = new Semaphore(1024 * 1024, true);
            log.info("semaphoreAsyncSendSize can not be smaller than 1M.");
        }
    }

    public void registerCheckForbiddenHook(CheckForbiddenHook checkForbiddenHook) {
        this.checkForbiddenHookList.add(checkForbiddenHook);
        log.info("register a new checkForbiddenHook. hookName={}, allHookSize={}", checkForbiddenHook.hookName(),
            checkForbiddenHookList.size());
    }

    public void setSemaphoreAsyncSendNum(int num) {
        semaphoreAsyncSendNum = new Semaphore(num, true);
    }

    public void setSemaphoreAsyncSendSize(int size) {
        semaphoreAsyncSendSize = new Semaphore(size, true);
    }

    public void initTransactionEnv() {
        TransactionMQProducer producer = (TransactionMQProducer) this.defaultMQProducer;
        if (producer.getExecutorService() != null) {
            this.checkExecutor = producer.getExecutorService();
        } else {
            this.checkRequestQueue = new LinkedBlockingQueue<>(producer.getCheckRequestHoldMax());
            this.checkExecutor = new ThreadPoolExecutor(
                producer.getCheckThreadPoolMinSize(),
                producer.getCheckThreadPoolMaxSize(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.checkRequestQueue);
        }
    }

    public void destroyTransactionEnv() {
        if (this.checkExecutor != null) {
            this.checkExecutor.shutdown();
        }
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register sendMessage Hook, {}", hook.hookName());
    }

    public void registerEndTransactionHook(final EndTransactionHook hook) {
        this.endTransactionHookList.add(hook);
        log.info("register endTransaction Hook, {}", hook.hookName());
    }

    public void start() throws MQClientException {
        this.start(true);
    }

    public void start(final boolean startFactory) throws MQClientException {
        //根据服务状态选择走不同的代码分支
        switch (this.serviceState) {
            /**
             * 服务仅仅创建，而不是启动状态，那么启动服务
             */
            case CREATE_JUST:
                //首先修改服务状态为服务启动失败，如果最终启动成功则再修改为RUNNING
                this.serviceState = ServiceState.START_FAILED;
                /*
                 * 1 检查生产者的配置信息
                 * 主要是检查ProducerGroup是否符合规范，
                 * 如果ProducerGroup为空，
                 * 或者长度大于255个字符，
                 * 或者包含非法字符（正常的匹配模式为 ^[%|a-zA-Z0-9_-]+$），
                 * 或者生产者组名为默认组名DEFAULT_PRODUCER
                 * 满足以上任意条件都校验不通过抛出异常。
                 */
                this.checkConfig();

                //如果ProducerGroup不是CLIENT_INNER_PRODUCER，那么将修改当前的instanceName为当前进程pid，PID就是服务的进程号。
                //CLIENT_INNER_PRODUCER是客户端内部的生产者组名，该生产者用于发送消息回退请求
                if (!this.defaultMQProducer.getProducerGroup().equals(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
                    this.defaultMQProducer.changeInstanceNameToPID();
                }
                /*
                 * 2 获取MQClientManager实例，然后根据clientId获取或者创建CreateMQClientInstance实例，并赋给mQClientFactory变量
                 */
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQProducer, rpcHook);
                /*
                 * 3 将当前生产者注册到MQClientInstance实例的producerTable属性中
                 */
                boolean registerOK = mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
                //如果注册失败，那么设置服务属性为CREATE_JUST，并抛出异常
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    throw new MQClientException("The producer group[" + this.defaultMQProducer.getProducerGroup()
                        + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                        null);
                }
                /*
                 * 4 启动CreateMQClientInstance客户端通信实例
                 * netty服务、各种定时任务、拉取消息服务、rebalanceService服务
                 */
                if (startFactory) {
                    mQClientFactory.start();
                }

                log.info("the producer [{}] start OK. sendMessageWithVIPChannel={}", this.defaultMQProducer.getProducerGroup(),
                    this.defaultMQProducer.isSendMessageWithVIPChannel());
                //服务状态改为RUNNING
                this.serviceState = ServiceState.RUNNING;
                break;
            /**
             * 服务状态是其他的，那么抛出异常，即start方法仅能调用一次
             */
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The producer service state not OK, maybe started once, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            default:
                break;
        }
        /*
         * 5 发送心跳信息给所有broker
         */
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
        /*
         * 6 启动一个定时任务，移除超时的请求，并执行异常回调，任务间隔1s
         */
        RequestFutureHolder.getInstance().startScheduledTask(this);

    }

    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQProducer.getProducerGroup());

        if (this.defaultMQProducer.getProducerGroup().equals(MixAll.DEFAULT_PRODUCER_GROUP)) {
            throw new MQClientException("producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP + ", please specify another one.",
                null);
        }
    }

    public void shutdown() {
        this.shutdown(true);
    }

    public void shutdown(final boolean shutdownFactory) {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.mQClientFactory.unregisterProducer(this.defaultMQProducer.getProducerGroup());
                this.defaultAsyncSenderExecutor.shutdown();
                if (shutdownFactory) {
                    this.mQClientFactory.shutdown();
                }
                RequestFutureHolder.getInstance().shutdown(this);
                log.info("the producer [{}] shutdown OK", this.defaultMQProducer.getProducerGroup());
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    @Override
    public Set<String> getPublishTopicList() {
        return new HashSet<>(this.topicPublishInfoTable.keySet());
    }

    @Override
    public boolean isPublishTopicNeedUpdate(String topic) {
        TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);

        return null == prev || !prev.ok();
    }

    /**
     * @deprecated This method will be removed in the version 5.0.0 and {@link DefaultMQProducerImpl#getCheckListener} is recommended.
     */
    @Override
    @Deprecated
    public TransactionCheckListener checkListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionCheckListener();
        }

        return null;
    }

    @Override
    public TransactionListener getCheckListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionListener();
        }
        return null;
    }

    @Override
    public void checkTransactionState(final String addr, final MessageExt msg,
        final CheckTransactionStateRequestHeader header) {
        Runnable request = new Runnable() {
            private final String brokerAddr = addr;
            private final MessageExt message = msg;
            private final CheckTransactionStateRequestHeader checkRequestHeader = header;
            private final String group = DefaultMQProducerImpl.this.defaultMQProducer.getProducerGroup();

            @Override
            public void run() {
                TransactionCheckListener transactionCheckListener = DefaultMQProducerImpl.this.checkListener();
                TransactionListener transactionListener = getCheckListener();
                if (transactionCheckListener != null || transactionListener != null) {
                    LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
                    Throwable exception = null;
                    try {
                        if (transactionCheckListener != null) {
                            localTransactionState = transactionCheckListener.checkLocalTransactionState(message);
                        } else {
                            log.debug("TransactionCheckListener is null, used new check API, producerGroup={}", group);
                            localTransactionState = transactionListener.checkLocalTransaction(message);
                        }
                    } catch (Throwable e) {
                        log.error("Broker call checkTransactionState, but checkLocalTransactionState exception", e);
                        exception = e;
                    }

                    this.processTransactionState(
                        localTransactionState,
                        group,
                        exception);
                } else {
                    log.warn("CheckTransactionState, pick transactionCheckListener by group[{}] failed", group);
                }
            }

            private void processTransactionState(
                final LocalTransactionState localTransactionState,
                final String producerGroup,
                final Throwable exception) {
                final EndTransactionRequestHeader thisHeader = new EndTransactionRequestHeader();
                thisHeader.setCommitLogOffset(checkRequestHeader.getCommitLogOffset());
                thisHeader.setProducerGroup(producerGroup);
                thisHeader.setTranStateTableOffset(checkRequestHeader.getTranStateTableOffset());
                thisHeader.setFromTransactionCheck(true);
                thisHeader.setBname(checkRequestHeader.getBname());

                String uniqueKey = message.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                if (uniqueKey == null) {
                    uniqueKey = message.getMsgId();
                }
                thisHeader.setMsgId(uniqueKey);
                thisHeader.setTransactionId(checkRequestHeader.getTransactionId());
                switch (localTransactionState) {
                    case COMMIT_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                        break;
                    case ROLLBACK_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                        log.warn("when broker check, client rollback this transaction, {}", thisHeader);
                        break;
                    case UNKNOW:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                        log.warn("when broker check, client does not know this transaction state, {}", thisHeader);
                        break;
                    default:
                        break;
                }

                String remark = null;
                if (exception != null) {
                    remark = "checkLocalTransactionState Exception: " + UtilAll.exceptionSimpleDesc(exception);
                }
                doExecuteEndTransactionHook(msg, uniqueKey, brokerAddr, localTransactionState, true);

                try {
                    DefaultMQProducerImpl.this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, thisHeader, remark,
                        3000);
                } catch (Exception e) {
                    log.error("endTransactionOneway exception", e);
                }
            }
        };

        this.checkExecutor.submit(request);
    }

    @Override
    public void updateTopicPublishInfo(final String topic, final TopicPublishInfo info) {
        if (info != null && topic != null) {
            TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
            if (prev != null) {
                log.info("updateTopicPublishInfo prev is not null, " + prev);
            }
        }
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQProducer.isUnitMode();
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.makeSureStateOK();
        Validators.checkTopic(newTopic);
        Validators.isSystemTopic(newTopic);

        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag, null);
    }

    /**
     * DefaultMQProducerImpl的方法
     */
    private void makeSureStateOK() throws MQClientException {
        //服务状态不是RUNNING，那么抛出MQClientException异常。
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The producer service state not OK, "
                + this.serviceState
                + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                null);
        }
    }

    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public MessageExt viewMessage(
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.makeSureStateOK();

        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey)
        throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    /**
     * DEFAULT ASYNC -------------------------------------------------------
     */
    public void send(Message msg,
        SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        //该方法内部又调用另一个send方法，设置超时时间参数，默认3000ms。
        send(msg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * @param msg
     * @param sendCallback
     * @param timeout      the <code>sendCallback</code> will be invoked at most time
     * @throws RejectedExecutionException
     * @deprecated It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     */
    @Deprecated
    public void send(final Message msg, final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        //调用起始时间
        final long beginStartTime = System.currentTimeMillis();
        // 构建Runnable
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                /*
                 * 发送之前计算超时时间，如果超时则不发送，直接执行回调函数onException方法
                 */
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeout > costTime) {
                    try {
                        //调用sendDefaultImpl方法执行发送操作
                        sendDefaultImpl(msg, CommunicationMode.ASYNC, sendCallback, timeout - costTime);
                    } catch (Exception e) {
                        //抛出异常，执行回调函数onException方法
                        sendCallback.onException(e);
                    }
                } else {
                    //超时，执行回调函数onException方法
                    sendCallback.onException(
                        new RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout"));
                }
            }
        };
        executeAsyncMessageSend(runnable, msg, sendCallback, timeout, beginStartTime);
    }

    public void executeAsyncMessageSend(Runnable runnable, final Message msg, final SendCallback sendCallback,
        final long timeout, final long beginStartTime)
        throws MQClientException, InterruptedException {
        ExecutorService executor = this.getAsyncSenderExecutor();
        boolean isEnableBackpressureForAsyncMode = this.getDefaultMQProducer().isEnableBackpressureForAsyncMode();
        boolean isSemaphoreAsyncNumAquired = false;
        boolean isSemaphoreAsyncSizeAquired = false;
        int msgLen = msg.getBody() == null ? 1 : msg.getBody().length;

        try {
            // 拥堵时会阻止异步消息发送
            if (isEnableBackpressureForAsyncMode) {
                // 计算超时时间，如果超时则不执行，
                // 如果最大执行数大于semaphoreAsyncSendNum（默认10），则不执行
                long costTime = System.currentTimeMillis() - beginStartTime;
                isSemaphoreAsyncNumAquired = timeout - costTime > 0
                    && semaphoreAsyncSendNum.tryAcquire(timeout - costTime, TimeUnit.MILLISECONDS);
                // 超时不执行
                if (!isSemaphoreAsyncNumAquired) {
                    sendCallback.onException(
                        new RemotingTooMuchRequestException("send message tryAcquire semaphoreAsyncNum timeout"));
                    return;
                }
                costTime = System.currentTimeMillis() - beginStartTime;
                // 计算超时时间，如果超时则不执行，
                // 如果排队消息大小 大于semaphoreAsyncSendSize（默认1024 * 1024 1M），则不执行
                isSemaphoreAsyncSizeAquired = timeout - costTime > 0
                    && semaphoreAsyncSendSize.tryAcquire(msgLen, timeout - costTime, TimeUnit.MILLISECONDS);
                if (!isSemaphoreAsyncSizeAquired) {
                    sendCallback.onException(
                        new RemotingTooMuchRequestException("send message tryAcquire semaphoreAsyncSize timeout"));
                    return;
                }
            }

            // 线程池执行
            executor.submit(runnable);
        } catch (RejectedExecutionException e) {
            if (isEnableBackpressureForAsyncMode) {
                runnable.run();
            } else {
                throw new MQClientException("executor rejected ", e);
            }
        } finally {
            if (isSemaphoreAsyncSizeAquired) {
                semaphoreAsyncSendSize.release(msgLen);
            }
            if (isSemaphoreAsyncNumAquired) {
                semaphoreAsyncSendNum.release();
            }
        }

    }

    /**
     * DefaultMQProducerImpl的方法
     *
     * 选择一个消息队列
     * @param tpInfo topic信息
     * @param lastBrokerName 上次使用过的broker
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        //调用mqFaultStrategy#selectOneMessageQueue方法
        return this.mqFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName);
    }

    /**
     * DefaultMQProducerImpl的方法
     * @param brokerName brokerName
     * @param currentLatency 当前延迟
     * @param isolation 是否使用默认隔离
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        //调用MQFaultStrategy#updateFaultItem方法
        this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation);
    }

    private void validateNameServerSetting() throws MQClientException {
        List<String> nsList = this.getMqClientFactory().getMQClientAPIImpl().getNameServerAddressList();
        if (null == nsList || nsList.isEmpty()) {
            throw new MQClientException(
                "No name server address, please set it." + FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL), null).setResponseCode(ClientErrorCode.NO_NAME_SERVER_EXCEPTION);
        }

    }

    /**
     * DefaultMQProducerImpl的方法
     *
     * @param msg               方法
     * @param communicationMode 通信模式
     * @param sendCallback      回调方法
     * @param timeout           超时时间
     */
    private SendResult sendDefaultImpl(
        Message msg,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback,
        final long timeout
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        /*
         * 1 确定此producer的服务状态正常，如果服务状态不是RUNNING，那么抛出异常
         */
        this.makeSureStateOK();
        /*
         * 2 校验消息的合法性
         */
        Validators.checkMessage(msg, this.defaultMQProducer);
        //生成本次调用id
        final long invokeID = random.nextLong();
        //开始时间戳
        long beginTimestampFirst = System.currentTimeMillis();
        long beginTimestampPrev = beginTimestampFirst;
        //结束时间戳
        long endTimestamp = beginTimestampFirst;
        /*
         * 3 尝试查找消息的一个topic路由，用以发送消息
         */
        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        // topic存在且messageQueueList不为空
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            boolean callTimeout = false;
            MessageQueue mq = null;
            Exception exception = null;
            SendResult sendResult = null;
            /*
             * 4 计算发送消息的总次数
             * 同步模式为3，即默认允许重试2次，可更改重试次数；其他模式为1，即不允许重试，不可更改
             */
            // 如果是同步发送记录，允许重试次数为最大失败次数为2
            // 如果不是同步步，允许重试次数为1
            // 注意，异步发送同样有重试，并且也是两次，只不过它的重试不在这个循环里面，而是是在MQClientAPIImpl#sendMessage方法中
            int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1;
            int times = 0;
            //记录每一次重试时候发送消息目标Broker名字的数组
            String[] brokersSent = new String[timesTotal];
            // 重试机制
            /*
             * 在循环中，发送消息，包含消息重试的逻辑，总次数默认不超过3
             */
            for (; times < timesTotal; times++) {
                // 上次使用过的broker，第一次发送mq为空，lastBrokerName也为空
                String lastBrokerName = null == mq ? null : mq.getBrokerName();
                // 选择一个MessageQueue
                MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName);
                if (mqSelected != null) {
                    // 指定mq = MessageQueue
                    mq = mqSelected;
                    // 记录第n次发送的brokerName
                    brokersSent[times] = mq.getBrokerName();
                    try {
                        //调用的开始时间
                        beginTimestampPrev = System.currentTimeMillis();
                        // 如果还有调用次数
                        if (times > 0) {
                            //Reset topic with namespace during resend.
                            // 在重新发送重置主题与名称空间。
                            msg.setTopic(this.defaultMQProducer.withNamespace(msg.getTopic()));
                        }
                        //现在调用的开始时间 减去 开始时间，判断时候在调用发起之前就超时了
                        long costTime = beginTimestampPrev - beginTimestampFirst;
                        // 超时直接停止
                        //即超时的时候，即使还剩下重试次数，也不会再继续重试
                        if (timeout < costTime) {
                            callTimeout = true;
                            break;
                        }
                        /*
                         * 6 异步、同步、单向发送消息
                         */
                        sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback, topicPublishInfo, timeout - costTime);
                        //方法调用结束时间戳
                        endTimestamp = System.currentTimeMillis();
                        /*
                         * 7 更新本地错误表缓存数据，用于延迟时间的故障转移的功能
                         */
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        /*
                         * 8 根据发送模式执行不同的处理
                         */
                        switch (communicationMode) {
                            //异步和单向模式直接返回null
                            case ASYNC:
                                return null;
                            case ONEWAY:
                                return null;
                            case SYNC:
                                //同步模式，如果开启了retryAnotherBrokerWhenNotStoreOK开关，那么如果不是返回SEND_OK状态，则仍然会执行重试发送
                                if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                                    if (this.defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK()) {
                                        continue;
                                    }
                                }
                                //如果发送成功，则返回
                                return sendResult;
                            default:
                                break;
                        }
                    } catch (RemotingException | MQClientException e) {
                        //RemotingException异常，会执行重试
                        //MQClientException异常，会执行重试
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq, e);
                        log.warn(msg.toString());
                        exception = e;
                        continue;
                    } catch (MQBrokerException e) {
                        //MQBrokerException异常
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq, e);
                        log.warn(msg.toString());
                        exception = e;
                        //如果返回的状态码属于一下几种，则支持重试：
                        //ResponseCode.TOPIC_NOT_EXIST,
                        //ResponseCode.SERVICE_NOT_AVAILABLE,
                        //ResponseCode.SYSTEM_ERROR,
                        //ResponseCode.NO_PERMISSION,
                        //ResponseCode.NO_BUYER_ID,
                        //ResponseCode.NOT_IN_CURRENT_UNIT
                        if (this.defaultMQProducer.getRetryResponseCodes().contains(e.getResponseCode())) {
                            continue;
                        } else {
                            //其他状态码不支持重试，如果有结果则返回，否则直接抛出异常
                            if (sendResult != null) {
                                return sendResult;
                            }

                            throw e;
                        }
                    } catch (InterruptedException e) {
                        //InterruptedException异常，不会执行重试
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        log.warn("sendKernelImpl exception, throw exception, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq, e);
                        log.warn(msg.toString());
                        throw e;
                    }
                } else {
                    break;
                }
            }
            // 重试次数用完 || 没有找到合适的MessageQueue..直接返回
            if (sendResult != null) {
                return sendResult;
            }
            // 发送失败
            String info = String.format("Send [%d] times, still failed, cost [%d]ms, Topic: %s, BrokersSent: %s",
                times,
                System.currentTimeMillis() - beginTimestampFirst,
                msg.getTopic(),
                Arrays.toString(brokersSent));

            info += FAQUrl.suggestTodo(FAQUrl.SEND_MSG_FAILED);

            MQClientException mqClientException = new MQClientException(info, exception);
            if (callTimeout) {
                throw new RemotingTooMuchRequestException("sendDefaultImpl call timeout");
            }

            if (exception instanceof MQBrokerException) {
                mqClientException.setResponseCode(((MQBrokerException) exception).getResponseCode());
            } else if (exception instanceof RemotingConnectException) {
                mqClientException.setResponseCode(ClientErrorCode.CONNECT_BROKER_EXCEPTION);
            } else if (exception instanceof RemotingTimeoutException) {
                mqClientException.setResponseCode(ClientErrorCode.ACCESS_BROKER_TIMEOUT);
            } else if (exception instanceof MQClientException) {
                mqClientException.setResponseCode(ClientErrorCode.BROKER_NOT_EXIST_EXCEPTION);
            }

            throw mqClientException;
        }
        // 校验name Server是否正常
        validateNameServerSetting();

        throw new MQClientException("No route info of this topic: " + msg.getTopic() + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO),
            null).setResponseCode(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION);
    }

    /**
     * DefaultMQProducerImpl的方法
     * <p>
     * 查找指定topic的推送信息
     */
    private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
        //尝试直接从producer的topicPublishInfoTable中获取topic信息
        TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
        //如果没有获取到有效信息，
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            //那么立即创建一个TopicPublishInfo
            this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
            //立即从nameServer同步此topic的路由配置信息，并且更新本地缓存
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            //再次获取topicPublishInfo
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
        }
        //如果找到的路由信息是可用的，直接返回
        if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
            return topicPublishInfo;
        } else {
            //再次从nameServer同步topic的数据，不过这次使用默认的topic “TBW102”去找路由配置信息作为本topic参数信息
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic, true, this.defaultMQProducer);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
            return topicPublishInfo;
        }
    }

    /**
     * DefaultMQProducerImpl的方法
     * 发送消息
     * 1.首先调用findBrokerAddressInPublish方法从brokerAddrTable中查找Master broker地址。如果找不到，那么再次调用tryToFindTopicPublishInfo方法从nameServer远程拉取配置，并更新本地缓存，随后再次尝试获取Master broker地址。
     * 2.调用brokerVIPChannel判断是否开启vip通道，如果开启了，那么将brokerAddr的port – 2，因为vip通道的端口为普通端口 – 2。
     * 3.如果不是批量消息，那么设置唯一的uniqId。
     * 4.如果不是批量消息，并且消息体大于4K，那么进行消息压缩。
     * 5.如果存在CheckForbiddenHook，则执行checkForbidden钩子方法。如果存在SendMessageHook，则执行sendMessageBefore钩子方法。
     * 6.设置请求头信息SendMessageRequestHeader，请求头包含各种基本属性，例如producerGroup、topic、queueId等，并且针对重试消息的处理，将消息重试次数和最大重试次数存入请求头中。
     * 7.根据不同的发送模式发送消息。如果是异步发送模式，则需要先克隆并还原消息。最终异步、单向、同步模式都是调用MQClientAPIImpl#sendMessage方法发送消息的。
     * 8.如果MQClientAPIImpl#sendMessage方法正常发送或者抛出RemotingException、MQBrokerException、InterruptedException异常，那么会判断如果存在SendMessageHook，则执行sendMessageAfter钩子方法。
     * 9.在finally块中，对原始消息进行恢复。
     * @param msg               消息
     * @param mq                mq
     * @param communicationMode 发送模式
     * @param sendCallback      发送回调
     * @param topicPublishInfo  topic信息
     * @param timeout           超时时间
     * @return 发送结果
     */
    private SendResult sendKernelImpl(final Message msg,
        final MessageQueue mq,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback,
        final TopicPublishInfo topicPublishInfo,
        final long timeout //剩余超时时间
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        //开始时间
        long beginStartTime = System.currentTimeMillis();
        String brokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(mq);
        /*
         * 1 根据brokerName从brokerAddrTable中查找broker地址
         */
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(brokerName);
        // 如果无法获取broker地址，尝试重新获取
        if (null == brokerAddr) {
            /*
             * 2 从nameServer远程拉取配置，并更新本地缓存
             * 该方法此前就学习过了
             */
            tryToFindTopicPublishInfo(mq.getTopic());
            //再次获取名字和地址
            brokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(mq);
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(brokerName);
        }

        SendMessageContext context = null;
        if (brokerAddr != null) {
            /*
             * 3 vip通道判断
             */
            brokerAddr = MixAll.brokerVIPChannel(this.defaultMQProducer.isSendMessageWithVIPChannel(), brokerAddr);

            byte[] prevBody = msg.getBody();
            try {
                //for MessageBatch,ID has been set in the generating process
                // 为消息批量,ID被设置在生成过程中
                /*
                 * 4 如果不是批量消息，那么尝试生成唯一uniqId，即UNIQ_KEY属性。MessageBatch批量消息在生成时就已经设置uniqId
                 * uniqId也被称为客户端生成的msgId，从逻辑上代表唯一一条消息
                 */
                if (!(msg instanceof MessageBatch)) {
                    MessageClientIDSetter.setUniqID(msg);
                }
                // 设置消息实例
                /*
                 * 设置nameSpace为实例Id
                 */
                boolean topicWithNamespace = false;
                if (null != this.mQClientFactory.getClientConfig().getNamespace()) {
                    msg.setInstanceId(this.mQClientFactory.getClientConfig().getNamespace());
                    topicWithNamespace = true;
                }
                //消息标识符
                int sysFlag = 0;
                //消息压缩标识
                boolean msgBodyCompressed = false;
                /*
                 * 5 尝试压缩消息
                 */
                if (this.tryToCompressMessage(msg)) {
                    sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
                    sysFlag |= compressType.getCompressionFlag();
                    msgBodyCompressed = true;
                }
                //事务消息标志，prepare消息
                final String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (Boolean.parseBoolean(tranMsg)) {
                    sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
                }

                // 检查消息钩子
                /*
                 * 6 如果存在CheckForbiddenHook，则执行checkForbidden方法
                 * 为什么叫禁止钩子呢，可能是想要使用者将不可发送消息的检查放在这个钩子函数里面吧（猜测）
                 */
                if (hasCheckForbiddenHook()) {
                    CheckForbiddenContext checkForbiddenContext = new CheckForbiddenContext();
                    checkForbiddenContext.setNameSrvAddr(this.defaultMQProducer.getNamesrvAddr());
                    checkForbiddenContext.setGroup(this.defaultMQProducer.getProducerGroup());
                    checkForbiddenContext.setCommunicationMode(communicationMode);
                    checkForbiddenContext.setBrokerAddr(brokerAddr);
                    checkForbiddenContext.setMessage(msg);
                    checkForbiddenContext.setMq(mq);
                    checkForbiddenContext.setUnitMode(this.isUnitMode());
                    this.executeCheckForbiddenHook(checkForbiddenContext);
                }
                /*
                 * 7 如果存在SendMessageHook，则执行sendMessageBefore方法
                 */
                if (this.hasSendMessageHook()) {
                    context = new SendMessageContext();
                    context.setProducer(this);
                    context.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                    context.setCommunicationMode(communicationMode);
                    context.setBornHost(this.defaultMQProducer.getClientIP());
                    context.setBrokerAddr(brokerAddr);
                    context.setMessage(msg);
                    context.setMq(mq);
                    context.setNamespace(this.defaultMQProducer.getNamespace());
                    String isTrans = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                    if (isTrans != null && isTrans.equals("true")) {
                        context.setMsgType(MessageType.Trans_Msg_Half);
                    }

                    if (msg.getProperty("__STARTDELIVERTIME") != null || msg.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null) {
                        context.setMsgType(MessageType.Delay_Msg);
                    }
                    this.executeSendMessageHookBefore(context);
                }
                // 设置请求头
                SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
                requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                requestHeader.setTopic(msg.getTopic());
                requestHeader.setDefaultTopic(this.defaultMQProducer.getCreateTopicKey());
                requestHeader.setDefaultTopicQueueNums(this.defaultMQProducer.getDefaultTopicQueueNums());
                requestHeader.setQueueId(mq.getQueueId());
                requestHeader.setSysFlag(sysFlag);
                requestHeader.setBornTimestamp(System.currentTimeMillis());
                requestHeader.setFlag(msg.getFlag());
                requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
                requestHeader.setReconsumeTimes(0);
                requestHeader.setUnitMode(this.isUnitMode());
                requestHeader.setBatch(msg instanceof MessageBatch);
                requestHeader.setBname(brokerName);
                //针对重试消息的处理
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    //获取消息重新消费次数属性值
                    String reconsumeTimes = MessageAccessor.getReconsumeTime(msg);
                    if (reconsumeTimes != null) {
                        //将重新消费次数设置到请求头中，并且清除该属性
                        requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTimes));
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME);
                    }
                    //获取消息的最大重试次数属性值
                    String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(msg);
                    if (maxReconsumeTimes != null) {
                        //将最大重新消费次数设置到请求头中，并且清除该属性
                        requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
                    }
                }
                /*
                 * 9 根据不同的发送模式，发送消息
                 */
                SendResult sendResult = null;
                switch (communicationMode) {
                    /*
                     * 异步发送模式
                     */
                    case ASYNC:
                        /*
                         * 首先克隆并还原消息
                         *
                         * 该方法的finally中已经有还原消息的代码了，为什么在异步发送消息之前，还要先还原消息呢？
                         *
                         * 因为异步发送时 finally 重新赋值的时机并不确定，有很大概率是在第一次发送结束前就完成了 finally 中的赋值，
                         * 因此在内部重试前 msg.body 大概率已经被重新赋值过，而 onExceptionImpl 中的重试逻辑 MQClientAPIImpl.sendMessageAsync 不会再对数据进行压缩，
                         * 简言之，在异步发送的情况下，如果调用 onExceptionImpl 内部的重试，有很大概率发送的是无压缩的数据
                         */
                        Message tmpMessage = msg;
                        boolean messageCloned = false;
                        //如果开启了消息压缩
                        if (msgBodyCompressed) {
                            //If msg body was compressed, msgbody should be reset using prevBody.
                            //Clone new message using commpressed message body and recover origin massage.
                            //Fix bug:https://github.com/apache/rocketmq-externals/issues/66
                            //克隆一个message
                            tmpMessage = MessageAccessor.cloneMessage(msg);
                            messageCloned = true;
                            //恢复原来的消息体
                            msg.setBody(prevBody);
                        }
                        //如果topic整合了namespace
                        if (topicWithNamespace) {
                            if (!messageCloned) {
                                tmpMessage = MessageAccessor.cloneMessage(msg);
                                messageCloned = true;
                            }
                            //还原topic
                            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
                        }
                        /*
                         * 发送消息之前，进行超时检查，如果已经超时了那么取消本次发送操作，抛出异常
                         */
                        long costTimeAsync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeAsync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        /*
                         * 10 发送异步消息
                         */
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                            brokerAddr,
                            brokerName,
                            tmpMessage,
                            requestHeader,
                            timeout - costTimeAsync,
                            communicationMode,
                            sendCallback,
                            topicPublishInfo,
                            this.mQClientFactory,
                            this.defaultMQProducer.getRetryTimesWhenSendAsyncFailed(),
                            context,
                            this);
                        break;
                    /*
                     * 单向、同步发送模式
                     */
                    case ONEWAY:
                    case SYNC:
                        /*
                         * 发送消息之前，进行超时检查，如果已经超时了那么取消本次发送操作，抛出异常
                         */
                        long costTimeSync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeSync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        /*
                         * 10 发送单向、同步消息
                         */
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                            brokerAddr,
                            brokerName,
                            msg,
                            requestHeader,
                            timeout - costTimeSync,
                            communicationMode,
                            context,
                            this);
                        break;
                    default:
                        assert false;
                        break;
                }
                // 消息发送后置处理
                /*
                 * 9 如果存在SendMessageHook，则执行sendMessageAfter方法
                 */
                if (this.hasSendMessageHook()) {
                    context.setSendResult(sendResult);
                    this.executeSendMessageHookAfter(context);
                }
                //返回执行结果
                return sendResult;
                //如果抛出了异常，如果存在SendMessageHook，则执行sendMessageAfter方法
            } catch (RemotingException | InterruptedException | MQBrokerException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } finally {
                /*
                 * 对消息进行恢复
                 * 1、因为客户端可能还需要查看原始的消息内容，如果是压缩消息，则无法查看
                 * 2、另外如果第一次压缩后消息还是大于4k，如果不恢复消息，那么客户端使用该message重新发送的时候，还会进行一次消息压缩
                 */
                msg.setBody(prevBody);
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
            }
        }

        // 获取不到broker地址，报错
        throw new MQClientException("The broker[" + brokerName + "] not exist", null);
    }

    public MQClientInstance getMqClientFactory() {
        return mQClientFactory;
    }

    @Deprecated
    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    private boolean tryToCompressMessage(final Message msg) {
        //如果是批量消息，那么不进行压缩
        if (msg instanceof MessageBatch) {
            //batch does not support compressing right now
            return false;
        }
        byte[] body = msg.getBody();
        if (body != null) {
            //如果消息长度大于4K
            if (body.length >= this.defaultMQProducer.getCompressMsgBodyOverHowmuch()) {
                try {
                    //进行压缩，使用的JDK自带的压缩类
                    byte[] data = compressor.compress(body, compressLevel);
                    if (data != null) {
                        //重新设置到body中
                        msg.setBody(data);
                        return true;
                    }
                } catch (IOException e) {
                    log.error("tryToCompressMessage exception", e);
                    log.warn(msg.toString());
                }
            }
        }

        return false;
    }

    public boolean hasCheckForbiddenHook() {
        return !checkForbiddenHookList.isEmpty();
    }

    public void executeCheckForbiddenHook(final CheckForbiddenContext context) throws MQClientException {
        if (hasCheckForbiddenHook()) {
            for (CheckForbiddenHook hook : checkForbiddenHookList) {
                hook.checkForbidden(context);
            }
        }
    }

    public boolean hasSendMessageHook() {
        return !this.sendMessageHookList.isEmpty();
    }

    public void executeSendMessageHookBefore(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageBefore(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookBefore", e);
                }
            }
        }
    }

    public void executeSendMessageHookAfter(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageAfter(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookAfter", e);
                }
            }
        }
    }

    public boolean hasEndTransactionHook() {
        return !this.endTransactionHookList.isEmpty();
    }

    public void executeEndTransactionHook(final EndTransactionContext context) {
        if (!this.endTransactionHookList.isEmpty()) {
            for (EndTransactionHook hook : this.endTransactionHookList) {
                try {
                    hook.endTransaction(context);
                } catch (Throwable e) {
                    log.warn("failed to executeEndTransactionHook", e);
                }
            }
        }
    }

    public void doExecuteEndTransactionHook(Message msg, String msgId, String brokerAddr, LocalTransactionState state,
        boolean fromTransactionCheck) {
        if (hasEndTransactionHook()) {
            EndTransactionContext context = new EndTransactionContext();
            context.setProducerGroup(defaultMQProducer.getProducerGroup());
            context.setBrokerAddr(brokerAddr);
            context.setMessage(msg);
            context.setMsgId(msgId);
            context.setTransactionId(msg.getTransactionId());
            context.setTransactionState(state);
            context.setFromTransactionCheck(fromTransactionCheck);
            executeEndTransactionHook(context);
        }
    }

    /**
     * DEFAULT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        try {
            //调用sendDefaultImpl方法，设置消息发送模式为ONEWAY，即单向；设置回调函数为null；设置超时时间参数，默认3000ms
            this.sendDefaultImpl(msg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * KERNEL SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueue mq)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, mq, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueue mq, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        if (!msg.getTopic().equals(mq.getTopic())) {
            throw new MQClientException("message's topic not equal mq's topic", null);
        }

        long costTime = System.currentTimeMillis() - beginStartTime;
        if (timeout < costTime) {
            throw new RemotingTooMuchRequestException("call timeout");
        }

        return this.sendKernelImpl(msg, mq, CommunicationMode.SYNC, null, null, timeout);
    }

    /**
     * KERNEL ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        send(msg, mq, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * @param msg
     * @param mq
     * @param sendCallback
     * @param timeout      the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     * @deprecated It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     */
    @Deprecated
    public void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException {

        final long beginStartTime = System.currentTimeMillis();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    makeSureStateOK();
                    Validators.checkMessage(msg, defaultMQProducer);

                    if (!msg.getTopic().equals(mq.getTopic())) {
                        throw new MQClientException("Topic of the message does not match its target message queue", null);
                    }
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeout > costTime) {
                        try {
                            sendKernelImpl(msg, mq, CommunicationMode.ASYNC, sendCallback, null,
                                timeout - costTime);
                        } catch (MQBrokerException e) {
                            throw new MQClientException("unknown exception", e);
                        }
                    } else {
                        sendCallback.onException(new RemotingTooMuchRequestException("call timeout"));
                    }
                } catch (Exception e) {
                    sendCallback.onException(e);
                }
            }

        };

        executeAsyncMessageSend(runnable, msg, sendCallback, timeout, beginStartTime);
    }

    /**
     * KERNEL ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg,
        MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        try {
            this.sendKernelImpl(msg, mq, CommunicationMode.ONEWAY, null, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * SELECT SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, selector, arg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.sendSelectImpl(msg, selector, arg, CommunicationMode.SYNC, null, timeout);
    }

    private SendResult sendSelectImpl(
        Message msg,
        MessageQueueSelector selector,
        Object arg,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback, final long timeout
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            MessageQueue mq = null;
            try {
                List<MessageQueue> messageQueueList =
                    mQClientFactory.getMQAdminImpl().parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());
                Message userMessage = MessageAccessor.cloneMessage(msg);
                String userTopic = NamespaceUtil.withoutNamespace(userMessage.getTopic(), mQClientFactory.getClientConfig().getNamespace());
                userMessage.setTopic(userTopic);

                mq = mQClientFactory.getClientConfig().queueWithNamespace(selector.select(messageQueueList, userMessage, arg));
            } catch (Throwable e) {
                throw new MQClientException("select message queue threw exception.", e);
            }

            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeout < costTime) {
                throw new RemotingTooMuchRequestException("sendSelectImpl call timeout");
            }
            if (mq != null) {
                return this.sendKernelImpl(msg, mq, communicationMode, sendCallback, null, timeout - costTime);
            } else {
                throw new MQClientException("select message queue return null.", null);
            }
        }

        validateNameServerSetting();
        throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
    }

    /**
     * SELECT ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        send(msg, selector, arg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param selector
     * @param arg
     * @param sendCallback
     * @param timeout      the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    @Deprecated
    public void send(final Message msg, final MessageQueueSelector selector, final Object arg,
        final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException {

        final long beginStartTime = System.currentTimeMillis();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeout > costTime) {
                    try {
                        try {
                            sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, sendCallback,
                                timeout - costTime);
                        } catch (MQBrokerException e) {
                            throw new MQClientException("unknown exception", e);
                        }
                    } catch (Exception e) {
                        sendCallback.onException(e);
                    }
                } else {
                    sendCallback.onException(new RemotingTooMuchRequestException("call timeout"));
                }
            }

        };
        executeAsyncMessageSend(runnable, msg, sendCallback, timeout, beginStartTime);
    }

    /**
     * SELECT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, InterruptedException {
        try {
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    public TransactionSendResult sendMessageInTransaction(final Message msg,
        final LocalTransactionExecuter localTransactionExecuter, final Object arg)
        throws MQClientException {
        TransactionListener transactionListener = getCheckListener();
        if (null == localTransactionExecuter && null == transactionListener) {
            throw new MQClientException("tranExecutor is null", null);
        }

        // ignore DelayTimeLevel parameter
        if (msg.getDelayTimeLevel() != 0) {
            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        }

        Validators.checkMessage(msg, this.defaultMQProducer);

        SendResult sendResult = null;
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, this.defaultMQProducer.getProducerGroup());
        try {
            sendResult = this.send(msg);
        } catch (Exception e) {
            throw new MQClientException("send message Exception", e);
        }

        LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
        Throwable localException = null;
        switch (sendResult.getSendStatus()) {
            case SEND_OK: {
                try {
                    if (sendResult.getTransactionId() != null) {
                        msg.putUserProperty("__transactionId__", sendResult.getTransactionId());
                    }
                    String transactionId = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                    if (null != transactionId && !"".equals(transactionId)) {
                        msg.setTransactionId(transactionId);
                    }
                    if (null != localTransactionExecuter) {
                        localTransactionState = localTransactionExecuter.executeLocalTransactionBranch(msg, arg);
                    } else {
                        log.debug("Used new transaction API");
                        localTransactionState = transactionListener.executeLocalTransaction(msg, arg);
                    }
                    if (null == localTransactionState) {
                        localTransactionState = LocalTransactionState.UNKNOW;
                    }

                    if (localTransactionState != LocalTransactionState.COMMIT_MESSAGE) {
                        log.info("executeLocalTransactionBranch return: {} messageTopic: {} transactionId: {} tag: {} key: {}",
                            localTransactionState, msg.getTopic(), msg.getTransactionId(), msg.getTags(), msg.getKeys());
                    }
                } catch (Throwable e) {
                    log.error("executeLocalTransactionBranch exception, messageTopic: {} transactionId: {} tag: {} key: {}",
                        msg.getTopic(), msg.getTransactionId(), msg.getTags(), msg.getKeys(), e);
                    localException = e;
                }
            }
            break;
            case FLUSH_DISK_TIMEOUT:
            case FLUSH_SLAVE_TIMEOUT:
            case SLAVE_NOT_AVAILABLE:
                localTransactionState = LocalTransactionState.ROLLBACK_MESSAGE;
                break;
            default:
                break;
        }

        try {
            this.endTransaction(msg, sendResult, localTransactionState, localException);
        } catch (Exception e) {
            log.warn("local transaction execute " + localTransactionState + ", but end broker transaction failed", e);
        }

        TransactionSendResult transactionSendResult = new TransactionSendResult();
        transactionSendResult.setSendStatus(sendResult.getSendStatus());
        transactionSendResult.setMessageQueue(sendResult.getMessageQueue());
        transactionSendResult.setMsgId(sendResult.getMsgId());
        transactionSendResult.setQueueOffset(sendResult.getQueueOffset());
        transactionSendResult.setTransactionId(sendResult.getTransactionId());
        transactionSendResult.setLocalTransactionState(localTransactionState);
        return transactionSendResult;
    }

    /**
     * DEFAULT SYNC -------------------------------------------------------
     */
    public SendResult send(
        Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        //调用另一个send方法，设置超时时间参数，默认3000ms
        return send(msg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public void endTransaction(
        final Message msg,
        final SendResult sendResult,
        final LocalTransactionState localTransactionState,
        final Throwable localException) throws RemotingException, MQBrokerException, InterruptedException, UnknownHostException {
        final MessageId id;
        if (sendResult.getOffsetMsgId() != null) {
            id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
        } else {
            id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
        }
        String transactionId = sendResult.getTransactionId();
        final String destBrokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(defaultMQProducer.queueWithNamespace(sendResult.getMessageQueue()));
        final String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(destBrokerName);
        EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
        requestHeader.setTransactionId(transactionId);
        requestHeader.setCommitLogOffset(id.getOffset());
        requestHeader.setBname(destBrokerName);
        switch (localTransactionState) {
            case COMMIT_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                break;
            case ROLLBACK_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                break;
            case UNKNOW:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                break;
            default:
                break;
        }

        doExecuteEndTransactionHook(msg, sendResult.getMsgId(), brokerAddr, localTransactionState, false);
        requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
        requestHeader.setTranStateTableOffset(sendResult.getQueueOffset());
        requestHeader.setMsgId(sendResult.getMsgId());
        String remark = localException != null ? ("executeLocalTransactionBranch exception: " + localException.toString()) : null;
        this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, requestHeader, remark,
            this.defaultMQProducer.getSendMsgTimeout());
    }

    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.mQClientFactory.getMQClientAPIImpl().getRemotingClient().setCallbackExecutor(callbackExecutor);
    }

    public ExecutorService getAsyncSenderExecutor() {
        return null == asyncSenderExecutor ? defaultAsyncSenderExecutor : asyncSenderExecutor;
    }

    public void setAsyncSenderExecutor(ExecutorService asyncSenderExecutor) {
        this.asyncSenderExecutor = asyncSenderExecutor;
    }

    /**
     * DefaultMQProducerImpl的方法
     *
     * @param msg     消息
     * @param timeout 超时时间，毫秒值
     */
    public SendResult send(Message msg,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        //调用另一个sendDefaultImpl方法，设置消息发送模式为SYNC，即同步；设置回调函数为null
        return this.sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeout);
    }

    public Message request(final Message msg,
        long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            return waitResponse(msg, timeout, requestResponseFuture, cost);
        } finally {
            RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(Message msg, final RequestCallback requestCallback, long timeout)
        throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
                requestResponseFuture.executeRequestCallback();
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);
    }

    public Message request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final long timeout) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException, RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            return waitResponse(msg, timeout, requestResponseFuture, cost);
        } finally {
            RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final RequestCallback requestCallback, final long timeout)
        throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);

    }

    public Message request(final Message msg, final MessageQueue mq, final long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, null, timeout - cost);

            return waitResponse(msg, timeout, requestResponseFuture, cost);
        } finally {
            RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        }
    }

    private Message waitResponse(Message msg, long timeout, RequestResponseFuture requestResponseFuture,
        long cost) throws InterruptedException, RequestTimeoutException, MQClientException {
        Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost);
        if (responseMessage == null) {
            if (requestResponseFuture.isSendRequestOk()) {
                throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
                    "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
            } else {
                throw new MQClientException("send request message to <" + msg.getTopic() + "> fail", requestResponseFuture.getCause());
            }
        }
        return responseMessage;
    }

    public void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout)
        throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, null, timeout - cost);
    }

    private void requestFail(final String correlationId) {
        RequestResponseFuture responseFuture = RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        if (responseFuture != null) {
            responseFuture.setSendRequestOk(false);
            responseFuture.putResponseMessage(null);
            try {
                responseFuture.executeRequestCallback();
            } catch (Exception e) {
                log.warn("execute requestCallback in requestFail, and callback throw", e);
            }
        }
    }

    private void prepareSendRequest(final Message msg, long timeout) {
        String correlationId = CorrelationIdUtil.createCorrelationId();
        String requestClientId = this.getMqClientFactory().getClientId();
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, requestClientId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TTL, String.valueOf(timeout));

        boolean hasRouteData = this.getMqClientFactory().getTopicRouteTable().containsKey(msg.getTopic());
        if (!hasRouteData) {
            long beginTimestamp = System.currentTimeMillis();
            this.tryToFindTopicPublishInfo(msg.getTopic());
            this.getMqClientFactory().sendHeartbeatToAllBrokerWithLock();
            long cost = System.currentTimeMillis() - beginTimestamp;
            if (cost > 500) {
                log.warn("prepare send request for <{}> cost {} ms", msg.getTopic(), cost);
            }
        }
    }

    public ConcurrentMap<String, TopicPublishInfo> getTopicPublishInfoTable() {
        return topicPublishInfoTable;
    }

    public int getCompressLevel() {
        return compressLevel;
    }

    public void setCompressLevel(int compressLevel) {
        this.compressLevel = compressLevel;
    }

    public CompressionType getCompressType() {
        return compressType;
    }

    public void setCompressType(CompressionType compressType) {
        this.compressType = compressType;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    public void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public long[] getNotAvailableDuration() {
        return this.mqFaultStrategy.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.mqFaultStrategy.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.mqFaultStrategy.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.mqFaultStrategy.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.mqFaultStrategy.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.mqFaultStrategy.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }
}
