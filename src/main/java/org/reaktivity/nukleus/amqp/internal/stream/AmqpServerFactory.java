/**
 * Copyright 2016-2019 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.amqp.internal.stream;

import static java.util.Objects.requireNonNull;

import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpFrameFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpHeaderFW;
import org.reaktivity.nukleus.amqp.internal.types.stream.*;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.amqp.internal.AmqpConfiguration;
import org.reaktivity.nukleus.amqp.internal.types.OctetsFW;
import org.reaktivity.nukleus.amqp.internal.types.control.RouteFW;

public final class AmqpServerFactory implements StreamFactory
{
    private static final int MAXIMUM_HEADER_SIZE = 14;

    private final RouteFW routeRO = new RouteFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();
    private final SignalFW signalRO = new SignalFW();

    private final AmqpBeginExFW.Builder amqpBeginExRW = new AmqpBeginExFW.Builder();
    private final AmqpDataExFW.Builder amqpDataExRW = new AmqpDataExFW.Builder();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final OctetsFW payloadRO = new OctetsFW();

    private final AmqpDataExFW amqpDataExRO = new AmqpDataExFW();

    private final AmqpHeaderFW amqpHeaderRO = new AmqpHeaderFW();
    private final AmqpHeaderFW.Builder amqpHeaderRW = new AmqpHeaderFW.Builder();
    private final AmqpFrameFW amqpFrameRO = new AmqpFrameFW();
    private final AmqpFrameFW.Builder amqpFrameRW = new AmqpFrameFW.Builder();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;

    private final Long2ObjectHashMap<AmqpServerConnection> correlations;
    private final Long2ObjectHashMap<AmqpServerSession> sessions;
    private final MessageFunction<RouteFW> wrapRoute;

    private final BufferPool bufferPool;

    public AmqpServerFactory(
        AmqpConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.bufferPool = bufferPool;
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.correlations = new Long2ObjectHashMap<>();
        this.sessions = new Long2ObjectHashMap<>();
        this.wrapRoute = this::wrapRoute;
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long streamId = begin.streamId();

        MessageConsumer newStream = null;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newInitialStream(begin, throttle);
        }
        else
        {
            newStream = newReplyStream(begin, throttle);
        }
        return newStream;
    }

    private MessageConsumer newInitialStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long routeId = begin.routeId();
        final long initialId = begin.streamId();

        final MessagePredicate filter = (t, b, o, l) -> true;
        final RouteFW route = router.resolve(routeId, begin.authorization(), filter, this::wrapRoute);
        MessageConsumer newStream = null;

        if (route != null)
        {
            final AmqpServerConnection connection = new AmqpServerConnection(sender, routeId, initialId);
            correlations.put(supplyReplyId.applyAsLong(initialId), connection);
            newStream = connection::onNetwork;
        }
        return newStream;
    }

    private MessageConsumer newReplyStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long replyId = begin.streamId();
        final AmqpServerConnection connection = correlations.remove(replyId);
        MessageConsumer newStream = null;
        if (connection != null)
        {
            newStream = connection::onNetwork;
        }
        return newStream;
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

    private final class AmqpServerConnection
    {
        private final MessageConsumer receiver;
        private final long routeId;
        private final long initialId;
        private final long replyId;

        private int initialBudget;
        private int initialPadding;
        private int replyBudget;
        private int replyPadding;

        private long decodeTraceId;
        private DecoderState decodeState;

        private AmqpServerConnection(
            MessageConsumer receiver,
            long routeId,
            long initialId)
        {
            this.receiver = receiver;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.decodeState = this::decodeHeader;
        }

        private void doBegin(
            long traceId)
        {
            final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(replyId)
                .trace(traceId)
                .build();
            receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
            router.setThrottle(replyId, this::onNetwork);
        }

        private void doReset(
            long traceId)
        {
            final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(initialId)
                .trace(traceId)
                .build();

            receiver.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
        }

        private void doWindow(
            long traceId,
            int initialCredit)
        {
            if (initialCredit > 0)
            {
                initialBudget += initialCredit;

                final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(initialId)
                    .trace(traceId)
                    .credit(initialCredit)
                    .padding(initialPadding)
                    .groupId(0)
                    .build();

                receiver.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
            }
        }

        private void doAmqpHeader(
            DirectBuffer header,
            int offset,
            int length)
        {
            final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(replyId)
                .trace(supplyTraceId.getAsLong())
                .groupId(0)
                .padding(replyPadding)
                .payload(header, offset, length)
                .build();
            receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
        }

        private void onNetwork(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    onBegin(begin);
                    break;
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    onData(data);
                    break;
                case EndFW.TYPE_ID:
                    final EndFW end = endRO.wrap(buffer, index, index + length);
                    onEnd(end);
                    break;
                case AbortFW.TYPE_ID:
                    final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                    onAbort(abort);
                    break;
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    onWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    onReset(reset);
                    break;
                case SignalFW.TYPE_ID:
                    final SignalFW signal = signalRO.wrap(buffer, index, index + length);
                    onSignal(signal);
                    break;
                default:
                    break;
            }
        }

        private void onBegin(
            BeginFW begin)
        {
            doBegin(supplyTraceId.getAsLong());
        }

        private void onData(
            DataFW data)
        {
            initialBudget -= data.length() + data.padding();

            if (initialBudget < 0)
            {
                doReset(supplyTraceId.getAsLong());
            }
            else
            {
                decodeTraceId = data.trace();
                final OctetsFW payload = data.payload();
                final DirectBuffer buffer = payload.buffer();

                int offset = payload.offset();
                int length = payload.sizeof();

                while (length > 0)
                {
                    int consumed = decodeState.decode(buffer, offset, length);
                    offset += consumed;
                    length -= consumed;
                }
            }
        }

        private void onEnd(
            EndFW end)
        {
            // TODO
        }

        private void onAbort(
            AbortFW abort)
        {
            // TODO
        }

        private void onWindow(
            WindowFW window)
        {
            final int replyCredit = window.credit();

            replyBudget += replyCredit;
            replyPadding += window.padding();

            final int initialCredit = bufferPool.slotCapacity() - initialBudget;
            doWindow(supplyTraceId.getAsLong(), initialCredit);
        }

        private void onReset(
            ResetFW reset)
        {
            // TODO
        }

        private void onSignal(
            SignalFW signal)
        {
            // TODO
        }

        private void onAmqpHeader(AmqpHeaderFW header)
        {
            doAmqpHeader(header.buffer(), header.offset(), header.limit());
        }

        private void onAmqpOpen(
            AmqpFrameFW open)
        {
            // TODO
        }

        private void onAmqpBegin(
            AmqpFrameFW begin)
        {
            // TODO
            /*
            * get a session from map with channel
            * */
        }

        private void onAmqpAttach(
            AmqpFrameFW attach)
        {
            // TODO
        }

        private void onAmqpFlow(
            AmqpFrameFW flow)
        {
            // TODO
        }

        private void onAmqpTransfer(
            AmqpFrameFW transfer)
        {
            // TODO
        }

        private void onAmqpDisposition(
            AmqpFrameFW disposition)
        {
            // TODO
        }

        private void onAmqpDetach(
            AmqpFrameFW detach)
        {
            // TODO
        }

        private void onAmqpEnd(
            AmqpFrameFW end)
        {
            // TODO
        }

        private void onAmqpClose(
            AmqpFrameFW close)
        {
            // TODO
        }

        private int decodeHeader(
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            final AmqpHeaderFW amqpHeader = amqpHeaderRO.wrap(buffer, offset, offset + length);
            onAmqpHeader(amqpHeader);
            decodeState = this::decodeFrame;
            return amqpHeader.sizeof(); // should be 8
        }

        // TODO
        private int decodeFrame(
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            int consumed;

            final AmqpFrameFW amqpFrame = amqpFrameRO.wrap(buffer, offset, offset + length); // TODO

            switch (amqpFrame.performative())
            {
                case 0x00:
                    onAmqpOpen(amqpFrame);
                    break;
                case 0x01:
                    onAmqpBegin(amqpFrame);
                    break;
                case 0x02:
                    onAmqpAttach(amqpFrame);
                    break;
                case 0x03:
                    onAmqpFlow(amqpFrame);
                    break;
                case 0x04:
                    onAmqpTransfer(amqpFrame);
                    break;
                case 0x05:
                    onAmqpDisposition(amqpFrame);
                    break;
                case 0x06:
                    onAmqpDetach(amqpFrame);
                    break;
                case 0x07:
                    onAmqpEnd(amqpFrame);
                    break;
                case 0x08:
                    onAmqpClose(amqpFrame);
                    break;
            }
            return amqpFrame.sizeof();
        }
    }

    @FunctionalInterface
    private interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int length);
    }

    // TODO
    private final class AmqpServerSession
    {
        private final Long2ObjectHashMap<AmqpServerSession> links;

        private AmqpServerSession()
        {
            this.links = new Long2ObjectHashMap<>();
        }
    }

    private final class AmqpServerStream
    {
        private void onAmqpAttach()
        {
            // TODO: write BEGIN with AmqpBeginEx
        }
    }
}
