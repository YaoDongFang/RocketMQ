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
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ResponseFuture {
    private final Channel channel;
    private final int opaque;
    private final RemotingCommand request;
    private final long timeoutMillis;
    private final InvokeCallback invokeCallback;
    private final long beginTimestamp = System.currentTimeMillis();
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    private final SemaphoreReleaseOnlyOnce once;

    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);
    private volatile RemotingCommand responseCommand;
    private volatile boolean sendRequestOK = true;
    private volatile Throwable cause;
    private volatile boolean interrupted = false;

    public ResponseFuture(Channel channel, int opaque, long timeoutMillis, InvokeCallback invokeCallback,
                          SemaphoreReleaseOnlyOnce once) {
        this(channel, opaque, null, timeoutMillis, invokeCallback, once);
    }

    public ResponseFuture(Channel channel, int opaque, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback,
                          SemaphoreReleaseOnlyOnce once) {
        this.channel = channel;
        this.opaque = opaque;
        this.request = request;
        this.timeoutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
        this.once = once;
    }

    public void executeInvokeCallback() {
        if (invokeCallback != null) {
            //通过cas保证只允许一次调用
            if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                //执行回调器的回调方法operationComplete
                invokeCallback.operationComplete(this);
            }
        }
    }

    public void interrupt() {
        interrupted = true;
        executeInvokeCallback();
    }

    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }

    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    /**
     * ResponseFuture的方法
     * <p>
     * 同步等待响应结果
     *
     * @param timeoutMillis 超时时间
     */
    public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
        //使用countDownLatch等待给定的时间
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }

    /**
     * ResponseFuture的方法
     * <p>
     * 存入响应结果并且唤醒等待的线程
     *
     * @param responseCommand 响应结果
     */
    public void putResponse(final RemotingCommand responseCommand) {
        //存入结果
        this.responseCommand = responseCommand;
        //倒计数减去1，唤醒等待的线程
        this.countDownLatch.countDown();
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public RemotingCommand getResponseCommand() {
        return responseCommand;
    }

    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }

    public int getOpaque() {
        return opaque;
    }

    public RemotingCommand getRequestCommand() {
        return request;
    }

    public Channel getChannel() {
        return channel;
    }

    public boolean isInterrupted() {
        return interrupted;
    }

    @Override
    public String toString() {
        return "ResponseFuture [responseCommand=" + responseCommand + ", sendRequestOK=" + sendRequestOK
            + ", cause=" + cause + ", opaque=" + opaque + ", timeoutMillis=" + timeoutMillis
            + ", invokeCallback=" + invokeCallback + ", beginTimestamp=" + beginTimestamp
            + ", countDownLatch=" + countDownLatch + "]";
    }
}
