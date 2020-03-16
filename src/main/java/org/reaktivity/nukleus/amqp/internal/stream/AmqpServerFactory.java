/**
 * Copyright 2016-2020 The Reaktivity Project
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.budget.BudgetCreditor.NO_CREDITOR_INDEX;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.amqp.internal.AmqpConfiguration;
import org.reaktivity.nukleus.amqp.internal.types.Flyweight;
import org.reaktivity.nukleus.amqp.internal.types.OctetsFW;
import org.reaktivity.nukleus.amqp.internal.types.String8FW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpBeginFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpFrameHeaderFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpFrameType;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpOpenFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpProtocolHeaderFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.PerformativeFW;
import org.reaktivity.nukleus.amqp.internal.types.control.RouteFW;
import org.reaktivity.nukleus.amqp.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.amqp.internal.types.stream.AmqpBeginExFW;
import org.reaktivity.nukleus.amqp.internal.types.stream.AmqpDataExFW;
import org.reaktivity.nukleus.amqp.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.amqp.internal.types.stream.DataFW;
import org.reaktivity.nukleus.amqp.internal.types.stream.EndFW;
import org.reaktivity.nukleus.amqp.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.amqp.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.amqp.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.budget.BudgetCreditor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class AmqpServerFactory implements StreamFactory
{
    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(new UnsafeBuffer(new byte[0]), 0, 0);

    private static final int MAXIMUM_HEADER_SIZE = 14;

    private final RouteFW routeRO = new RouteFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();
    private final SignalFW signalRO = new SignalFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final SignalFW.Builder signalRW = new SignalFW.Builder();

    private final AmqpBeginExFW amqpBeginExRO = new AmqpBeginExFW();
    private final AmqpDataExFW amqpDataExRO = new AmqpDataExFW();

    private final AmqpBeginExFW.Builder amqpBeginExRW = new AmqpBeginExFW.Builder();
    private final AmqpDataExFW.Builder amqpDataExRW = new AmqpDataExFW.Builder();

    private final OctetsFW payloadRO = new OctetsFW();
    private final OctetsFW.Builder payloadRW = new OctetsFW.Builder();

    private final AmqpProtocolHeaderFW amqpProtocolHeaderRO = new AmqpProtocolHeaderFW();
    private final AmqpOpenFW amqpOpenRO = new AmqpOpenFW();
    private final AmqpBeginFW amqpBeginRO = new AmqpBeginFW();
    private final AmqpFrameHeaderFW amqpFrameHeaderRO = new AmqpFrameHeaderFW();

    private final AmqpProtocolHeaderFW.Builder amqpProtocolHeaderRW = new AmqpProtocolHeaderFW.Builder();
    private final AmqpFrameHeaderFW.Builder amqpFrameHeaderRW = new AmqpFrameHeaderFW.Builder();
    private final AmqpOpenFW.Builder amqpOpenRW = new AmqpOpenFW.Builder();
    private final AmqpBeginFW.Builder amqpBeginRW = new AmqpBeginFW.Builder();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer frameBuffer;
    private final MutableDirectBuffer encodeBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final LongSupplier supplyBudgetId;

    private final Long2ObjectHashMap<AmqpServer> correlations;
    private final Long2ObjectHashMap<AmqpServerSession> sessions;
    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final BufferPool bufferPool;
    private final BudgetCreditor creditor;

    private final AmqpServerDecoder decodeFrameType = this::decodeFrameType;
    private final AmqpServerDecoder decodeHeader = this::decodeHeader;
    private final AmqpServerDecoder decodeOpen = this::decodeOpen;
    private final AmqpServerDecoder decodeBegin = this::decodeBegin;
    private final AmqpServerDecoder decodeIgnoreAll = this::decodeIgnoreAll;
    private final AmqpServerDecoder decodeUnknownType = this::decodeUnknownType;

    private final int outgoingWindow;
    private final String containerId;

    private final Map<AmqpFrameType, AmqpServerDecoder> decodersByFrameType;
    {
        final Map<AmqpFrameType, AmqpServerDecoder> decodersByFrameType = new EnumMap<>(AmqpFrameType.class);
        decodersByFrameType.put(AmqpFrameType.OPEN, decodeOpen);
        decodersByFrameType.put(AmqpFrameType.BEGIN, decodeBegin);
        // decodersByFrameType.put(AmqpFrameType.ATTACH, decodeAttach);
        // decodersByFrameType.put(AmqpFrameType.FLOW, decodeFlow);
        // decodersByFrameType.put(AmqpFrameType.TRANSFER, decodeTransfer);
        // decodersByFrameType.put(AmqpFrameType.DISPOSITION, decodeDisposition);
        // decodersByFrameType.put(AmqpFrameType.DETACH, decodeDetach);
        // decodersByFrameType.put(AmqpFrameType.END, decodeEnd);
        this.decodersByFrameType = decodersByFrameType;
    }

    public AmqpServerFactory(
        AmqpConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        BudgetCreditor creditor,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyBudgetId,
        LongSupplier supplyTraceId)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.bufferPool = bufferPool;
        this.creditor = creditor;
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyBudgetId = requireNonNull(supplyBudgetId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.correlations = new Long2ObjectHashMap<>();
        this.sessions = new Long2ObjectHashMap<>();
        this.encodeBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.frameBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.containerId = config.containerId();
        this.outgoingWindow = config.outgoingWindow();
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

        final MessagePredicate filter = (t, b, o, l) -> true;
        final RouteFW route = router.resolve(routeId, begin.authorization(), filter, wrapRoute);
        MessageConsumer newStream = null;

        if (route != null)
        {
            final long initialId = begin.streamId();
            final long affinity = begin.affinity();
            final long replyId = supplyReplyId.applyAsLong(initialId);
            final long budgetId = supplyBudgetId.getAsLong();

            final AmqpServer connection = new AmqpServer(sender, routeId, initialId, replyId, affinity, budgetId);
            newStream = connection::onNetwork;
        }
        return newStream;
    }

    private MessageConsumer newReplyStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long replyId = begin.streamId();
        // TODO: Application stream
        MessageConsumer newStream = null;
        return newStream;
    }

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long replyId,
        long traceId,
        long authorization,
        long affinity,
        Flyweight extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .routeId(routeId)
            .streamId(replyId)
            .traceId(traceId)
            .authorization(authorization)
            .affinity(affinity)
            .extension(extension.buffer(), extension.offset(), extension.sizeof())
            .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doData(
        MessageConsumer receiver,
        long routeId,
        long replyId,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int index,
        int length,
        Flyweight extension)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .routeId(routeId)
            .streamId(replyId)
            .traceId(traceId)
            .authorization(authorization)
            .budgetId(budgetId)
            .reserved(reserved)
            .payload(buffer, index, length)
            .extension(extension.buffer(), extension.offset(), extension.sizeof())
            .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private void doEnd(
        MessageConsumer receiver,
        long routeId,
        long replyId,
        long traceId,
        long authorization,
        Flyweight extension)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .routeId(routeId)
            .streamId(replyId)
            .traceId(traceId)
            .authorization(authorization)
            .extension(extension.buffer(), extension.offset(), extension.sizeof())
            .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doAbort(
        MessageConsumer receiver,
        long routeId,
        long replyId,
        long traceId,
        long authorization,
        Flyweight extension)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .routeId(routeId)
            .streamId(replyId)
            .traceId(traceId)
            .authorization(authorization)
            .extension(extension.buffer(), extension.offset(), extension.sizeof())
            .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doWindow(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        long budgetId,
        int credit,
        int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .routeId(routeId)
            .streamId(streamId)
            .traceId(traceId)
            .authorization(authorization)
            .budgetId(budgetId)
            .credit(credit)
            .padding(padding)
            .build();

        receiver.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        Flyweight extension)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity()).routeId(routeId)
            .streamId(streamId)
            .traceId(traceId)
            .authorization(authorization)
            .extension(extension.buffer(), extension.offset(), extension.sizeof())
            .build();

        receiver.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doSignal(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId)
    {
        final SignalFW signal = signalRW.wrap(writeBuffer, 0, writeBuffer.capacity()).routeId(routeId)
            .streamId(streamId)
            .traceId(traceId)
            .build();

        receiver.accept(signal.typeId(), signal.buffer(), signal.offset(), signal.sizeof());
    }

    @FunctionalInterface
    private interface AmqpServerDecoder
    {
        int decode(
            AmqpServer server,
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit);
    }

    private int decodeFrameType(
        AmqpServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        int progress = offset;
        final AmqpFrameHeaderFW frameHeader = amqpFrameHeaderRO.tryWrap(buffer, offset, limit);

        if (frameHeader != null)
        {
            final AmqpFrameType frameType = AmqpFrameType.valueOf(frameHeader.performative().descriptorId().getAsUint8());
            final AmqpServerDecoder decoder = decodersByFrameType.getOrDefault(frameType, decodeUnknownType);

            if (limit - frameHeader.limit() >= 0)
            {
                server.onDecodeFrameHeader(frameHeader);
                server.decoder = decoder;
            }
            progress = frameHeader.limit();
        }

        return progress;
    }

    private int decodeHeader(
        AmqpServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        final AmqpProtocolHeaderFW protocolHeader = amqpProtocolHeaderRO.tryWrap(buffer, offset, limit);

        int progress = offset;
        if (protocolHeader != null)
        {
            server.onDecodeHeader(traceId, authorization, protocolHeader);
            server.decoder = decodeFrameType;
            progress = protocolHeader.limit();
        }

        return progress;
    }

    private int decodeOpen(
        AmqpServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        AmqpOpenFW amqpOpen = amqpOpenRO.tryWrap(buffer, offset, limit);

        int progress = offset;

        if (amqpOpen != null)
        {
            server.onDecodeOpen(traceId, authorization);
            server.decoder = decodeFrameType;
            progress = amqpOpen.limit();
        }
        return progress;
    }

    private int decodeBegin(
        AmqpServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        AmqpBeginFW amqpBegin = amqpBeginRO.tryWrap(buffer, offset, limit);

        int progress = offset;

        if (amqpBegin != null)
        {
            server.onDecodeBegin(traceId, authorization);
            server.decoder = decodeFrameType;
            progress = amqpBegin.limit();
        }
        return progress;
    }

    private int decodeIgnoreAll(
        AmqpServer server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        return limit;
    }

    private int decodeUnknownType(
        AmqpServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        server.onDecodeError(traceId, authorization);
        server.decoder = decodeIgnoreAll;
        return limit;
    }

    private final class AmqpServer
    {
        private final MessageConsumer network;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long affinity;
        private final long replySharedBudgetId;

        // private final Int2ObjectHashMap<MqttServerStream> streams;
        // private final Int2ObjectHashMap<MutableInteger> activeStreams;
        // private final Int2ObjectHashMap<Subscription> subscriptionsByPacketId;

        private int initialBudget;
        private int initialPadding;
        private int replyBudget;
        private int replyPadding;

        private long replyBudgetIndex = NO_CREDITOR_INDEX;
        private int sharedBudget;

        private int decodeSlot = NO_SLOT;
        private int decodeSlotLimit;

        private int encodeSlot = NO_SLOT;
        private int encodeSlotOffset;
        private long encodeSlotTraceId;
        private int encodeSlotMaxLimit = Integer.MAX_VALUE;

        private AmqpServerDecoder decoder;

        private boolean connected;

        private AmqpServer(
            MessageConsumer network,
            long routeId,
            long initialId,
            long replyId,
            long affinity,
            long budgetId)
        {
            this.network = network;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = replyId;
            this.affinity = affinity;
            this.replySharedBudgetId = budgetId;
            this.decoder = decodeHeader;
            // this.streams = new Int2ObjectHashMap<>();
            // this.activeStreams = new Int2ObjectHashMap<>();
            // this.subscriptionsByPacketId = new Int2ObjectHashMap<>();
        }

        private void doEncodeHeader(
            int major,
            int minor,
            int revision,
            long traceId,
            long authorization)
        {
            final AmqpProtocolHeaderFW protocolHeader = amqpProtocolHeaderRW
                .wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .name(n -> n.set("AMQP".getBytes(StandardCharsets.US_ASCII)))
                .id(0)
                .major(major)
                .minor(minor)
                .revision(revision)
                .build();

            doNetworkData(traceId, authorization, 0L, protocolHeader);
        }

        private void doEncodeFrameHeader(
            int doff,
            int type,
            int channel,
            PerformativeFW performative)
        {
            amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                .size(0)
                .doff(doff)
                .type(type)
                .channel(channel)
                .performative(b -> b.domainId(d -> d.set(performative.domainId()))
                                    .descriptorId(d2 -> d2.set(performative.descriptorId().get())));
        }

        private void doEncodeOpen(
            long traceId,
            long authorization)
        {
            int offsetOpenFrame = amqpFrameHeaderRW.limit();

            final String8FW containerIdRO = new String8FW.Builder()
                .wrap(writeBuffer, 0, writeBuffer.capacity())
                .set(containerId, UTF_8)
                .build();

            final AmqpOpenFW open = amqpOpenRW
                .wrap(frameBuffer, offsetOpenFrame, frameBuffer.capacity())
                .containerId(containerIdRO)
                .build();

            AmqpFrameHeaderFW originalFrameHeader = amqpFrameHeaderRW.build();

            final AmqpFrameHeaderFW updatedFrameHeader =
                amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                    .size(originalFrameHeader.sizeof() + open.sizeof())
                    .doff(originalFrameHeader.doff())
                    .type(originalFrameHeader.type())
                    .channel(originalFrameHeader.channel())
                    .performative(b -> b.domainId(d -> d.set(originalFrameHeader.performative().domainId()))
                        .descriptorId(d2 -> d2.set(originalFrameHeader.performative().descriptorId().get())))
                    .build();

            OctetsFW openWithFrameHeader = payloadRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .put(updatedFrameHeader.buffer(), 0, updatedFrameHeader.sizeof())
                .put(open.buffer(), offsetOpenFrame, open.sizeof())
                .build();

            doNetworkData(traceId, authorization, 0L, openWithFrameHeader);
        }

        private void doEncodeBegin(
            long traceId,
            long authorization)
        {
            int offsetBeginFrame = amqpFrameHeaderRW.limit();

            final AmqpBeginFW begin = amqpBeginRW
                .wrap(frameBuffer, offsetBeginFrame, frameBuffer.capacity())
                .remoteChannel(1)
                .nextOutgoingId(1)
                .incomingWindow(writeBuffer.capacity())
                .outgoingWindow(outgoingWindow)
                .build();

            AmqpFrameHeaderFW originalFrameHeader = amqpFrameHeaderRW.build();

            final AmqpFrameHeaderFW updatedFrameHeader =
                amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                    .size(originalFrameHeader.sizeof() + begin.sizeof())
                    .doff(originalFrameHeader.doff())
                    .type(originalFrameHeader.type())
                    .channel(originalFrameHeader.channel())
                    .performative(b -> b.domainId(d -> d.set(originalFrameHeader.performative().domainId()))
                        .descriptorId(d2 -> d2.set(originalFrameHeader.performative().descriptorId().get())))
                    .build();

            OctetsFW openWithFrameHeader = payloadRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .put(updatedFrameHeader.buffer(), 0, updatedFrameHeader.sizeof())
                .put(begin.buffer(), offsetBeginFrame, begin.sizeof())
                .build();

            doNetworkData(traceId, authorization, 0L, openWithFrameHeader);
        }

        private void encodeNetwork(
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit,
            int maxLimit)
        {
            encodeNetworkData(traceId, authorization, budgetId, buffer, offset, limit, maxLimit);
        }

        private void encodeNetworkData(
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit,
            int maxLimit)
        {
            final int length = Math.max(Math.min(replyBudget - replyPadding, limit - offset), 0);

            if (length > 0)
            {
                final int reserved = length + replyPadding;

                replyBudget -= reserved;

                assert replyBudget >= 0;

                doData(network, routeId, replyId, traceId, authorization, budgetId,
                    reserved, buffer, offset, length, EMPTY_OCTETS);
            }

            final int maxLength = maxLimit - offset;
            final int remaining = maxLength - length;
            if (remaining > 0)
            {
                if (encodeSlot == NO_SLOT)
                {
                    encodeSlot = bufferPool.acquire(replyId);
                }
                else
                {
                    encodeSlotMaxLimit -= length;
                    assert encodeSlotMaxLimit >= 0;
                }

                if (encodeSlot == NO_SLOT)
                {
                    cleanupNetwork(traceId, authorization);
                }
                else
                {
                    final MutableDirectBuffer encodeBuffer = bufferPool.buffer(encodeSlot);
                    encodeBuffer.putBytes(0, buffer, offset + length, remaining);
                    encodeSlotOffset = remaining;
                }
            }
            else
            {
                cleanupEncodeSlotIfNecessary();

                // TODO
                // if (streams.isEmpty() && decoder == decodeIgnoreAll)
                // {
                //     doNetworkEnd(traceId, authorization);
                // }
            }
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
                onNetworkBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onNetworkData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onNetworkEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onNetworkAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onNetworkWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onNetworkReset(reset);
                break;
            case SignalFW.TYPE_ID:
                final SignalFW signal = signalRO.wrap(buffer, index, index + length);
                onNetworkSignal(signal);
                break;
            default:
                break;
            }
        }

        private void onNetworkBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();
            final long authorization = begin.authorization();
            doNetworkBegin(traceId, authorization);
        }

        private void onNetworkData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final long authorization = data.authorization();

            initialBudget -= data.reserved();

            if (initialBudget < 0)
            {
                doNetworkReset(supplyTraceId.getAsLong(), authorization);
            }
            else
            {
                final long streamId = data.streamId();
                final long budgetId = data.budgetId();
                final OctetsFW payload = data.payload();

                DirectBuffer buffer = payload.buffer();
                int offset = payload.offset();
                int limit = payload.limit();

                if (decodeSlot != NO_SLOT)
                {
                    final MutableDirectBuffer slotBuffer = bufferPool.buffer(decodeSlot);
                    slotBuffer.putBytes(decodeSlotLimit, buffer, offset, limit - offset);
                    decodeSlotLimit += limit - offset;
                    buffer = slotBuffer;
                    offset = 0;
                    limit = decodeSlotLimit;
                }

                decodeNetwork(traceId, authorization, budgetId, buffer, offset, limit);
            }
        }

        private void releaseBufferSlotIfNecessary()
        {
            if (decodeSlot != NO_SLOT)
            {
                bufferPool.release(decodeSlot);
                decodeSlot = NO_SLOT;
                decodeSlotLimit = 0;
            }
        }

        private void onNetworkEnd(
            EndFW end)
        {
            final long authorization = end.authorization();
            if (decodeSlot == NO_SLOT)
            {
                final long traceId = end.traceId();

                cleanupDecodeSlotIfNecessary();

                doNetworkEnd(traceId, authorization);
            }
            decoder = decodeIgnoreAll;
        }

        private void onNetworkAbort(
            AbortFW abort)
        {
            // TODO
            releaseBufferSlotIfNecessary();
        }

        private void onNetworkWindow(
            WindowFW window)
        {
            final long traceId = window.traceId();
            final long authorization = window.authorization();
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            replyBudget += credit;
            replyPadding += padding;

            if (encodeSlot != NO_SLOT)
            {
                final MutableDirectBuffer buffer = bufferPool.buffer(encodeSlot);
                final int limit = Math.min(encodeSlotOffset, encodeSlotMaxLimit);
                final int maxLimit = encodeSlotOffset;

                encodeNetwork(encodeSlotTraceId, authorization, budgetId, buffer, 0, limit, maxLimit);
            }

            // TODO
            // if (encodeSlot == NO_SLOT)
            // {
            //    streams.values().forEach(s -> s.flushReplyWindow(traceId, authorization));
            // }

            doNetworkWindow(traceId, authorization, credit, padding, 0L);

            final int slotCapacity = bufferPool.slotCapacity();
            final int sharedBudgetCredit = Math.min(slotCapacity, replyBudget - encodeSlotOffset);

            if (sharedBudgetCredit > 0)
            {
                final long replySharedPrevious = creditor.credit(traceId, replyBudgetIndex, credit);

                assert replySharedPrevious <= slotCapacity
                    : String.format("%d <= %d, replyBudget = %d",
                    replySharedPrevious, slotCapacity, replyBudget);

                assert credit <= slotCapacity
                    : String.format("%d <= %d", credit, slotCapacity);
            }
        }

        private void onNetworkReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final long authorization = reset.authorization();

            cleanupBudgetCreditorIfNecessary();
            cleanupEncodeSlotIfNecessary();

            // TODO: streams.values().forEach(s -> s.cleanup(traceId, authorization));

            doNetworkReset(traceId, authorization);
        }

        private void onNetworkSignal(
            SignalFW signal)
        {
            final long traceId = signal.traceId();
            doNetworkSignal(traceId);
        }

        private void onDecodeError(
            long traceId,
            long authorization)
        {
            cleanupStreams(traceId, authorization);
            // TODO
            // if (connected)
            // {
            //     doEncodeClose(traceId, authorization, reasonCode);
            // }
            // else
            // {
            //     doEncodeXXX(traceId, authorization, reasonCode);
            // }
            doNetworkEnd(traceId, authorization);
        }

        private void doNetworkBegin(
            long traceId,
            long authorization)
        {
            doBegin(network, routeId, replyId, traceId, authorization, affinity, EMPTY_OCTETS);
            router.setThrottle(replyId, this::onNetwork);

            assert replyBudgetIndex == NO_CREDITOR_INDEX;
            this.replyBudgetIndex = creditor.acquire(replySharedBudgetId);
        }

        private void doNetworkData(
            long traceId,
            long authorization,
            long budgetId,
            Flyweight payload)
        {
            doNetworkData(traceId, authorization, budgetId, payload.buffer(), payload.offset(), payload.limit());
        }

        private void doNetworkData(
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            int maxLimit = limit;

            if (encodeSlot != NO_SLOT)
            {
                final MutableDirectBuffer encodeBuffer = bufferPool.buffer(encodeSlot);
                encodeBuffer.putBytes(encodeSlotOffset, buffer, offset, limit - offset);
                encodeSlotOffset += limit - offset;
                encodeSlotTraceId = traceId;

                buffer = encodeBuffer;
                offset = 0;
                limit = Math.min(encodeSlotOffset, encodeSlotMaxLimit);
                maxLimit = encodeSlotOffset;
            }

            encodeNetwork(traceId, authorization, budgetId, buffer, offset, limit, maxLimit);
        }

        private void doNetworkEnd(
            long traceId,
            long authorization)
        {
            cleanupBudgetCreditorIfNecessary();
            cleanupEncodeSlotIfNecessary();
            doEnd(network, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doNetworkAbort(
            long traceId,
            long authorization)
        {
            cleanupBudgetCreditorIfNecessary();
            cleanupEncodeSlotIfNecessary();
            doAbort(network, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doNetworkReset(
            long traceId,
            long authorization)
        {
            cleanupDecodeSlotIfNecessary();
            doReset(network, routeId, initialId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doNetworkWindow(
            long traceId,
            long authorization,
            int credit,
            int padding,
            long budgetId)
        {
            assert credit > 0;

            initialBudget += credit;
            doWindow(network, routeId, initialId, traceId, authorization, budgetId, credit, padding);
        }

        private void doNetworkSignal(
            long traceId)
        {
            doSignal(network, routeId, initialId, traceId);
        }

        private void decodeNetwork(
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            AmqpServerDecoder previous = null;
            int progress = offset;
            while (progress <= limit && previous != decoder)
            {
                previous = decoder;
                progress = decoder.decode(this, traceId, authorization, budgetId, buffer, progress, limit);
            }

            if (progress < limit)
            {
                if (decodeSlot == NO_SLOT)
                {
                    decodeSlot = bufferPool.acquire(initialId);
                }

                if (decodeSlot == NO_SLOT)
                {
                    cleanupNetwork(traceId, authorization);
                }
                else
                {
                    final MutableDirectBuffer slotBuffer = bufferPool.buffer(decodeSlot);
                    decodeSlotLimit = limit - progress;
                    slotBuffer.putBytes(0, buffer, progress, decodeSlotLimit);
                }
            }
            else
            {
                cleanupDecodeSlotIfNecessary();
            }
        }

        private void onDecodeHeader(
            long traceId,
            long authorization,
            AmqpProtocolHeaderFW header)
        {
            if (isAmqpHeaderValid(header))
            {
                doEncodeHeader(header.major(), header.minor(), header.revision(), traceId, authorization);
            }

            doNetworkEnd(traceId, authorization);
        }

        private void onDecodeFrameHeader(
            AmqpFrameHeaderFW frameHeader)
        {
            doEncodeFrameHeader(frameHeader.doff(), frameHeader.type(), frameHeader.channel(), frameHeader.performative());
        }

        private void onDecodeOpen(
            long traceId,
            long authorization)
        {
            doEncodeOpen(traceId, authorization);
        }

        private void onDecodeBegin(
            long traceId,
            long authorization)
        {
            doEncodeBegin(traceId, authorization);
        }

        private boolean isAmqpHeaderValid(
            AmqpProtocolHeaderFW header)
        {
            String name = header.name().get((buffer, offset, limit) ->
            {
                byte[] nameInBytes = new byte[4];
                buffer.getBytes(offset, nameInBytes, 0, 4);
                return new String(nameInBytes);
            });
            return "AMQP".equals(name) &&
                header.id() == 0 &&
                header.major() == 1 &&
                header.minor() == 0 &&
                header.revision() == 0;
        }

        private void cleanupNetwork(
            long traceId,
            long authorization)
        {
            doNetworkReset(traceId, authorization);
            doNetworkAbort(traceId, authorization);
            cleanupStreams(traceId, authorization);
        }

        private void cleanupStreams(
            long traceId,
            long authorization)
        {
            // TODO: streams.values().forEach(s -> s.cleanup(traceId, authorization));
        }

        private void cleanupBudgetCreditorIfNecessary()
        {
            if (replyBudgetIndex != NO_CREDITOR_INDEX)
            {
                creditor.release(replyBudgetIndex);
                replyBudgetIndex = NO_CREDITOR_INDEX;
            }
        }

        private void cleanupDecodeSlotIfNecessary()
        {
            if (decodeSlot != NO_SLOT)
            {
                bufferPool.release(decodeSlot);
                decodeSlot = NO_SLOT;
                decodeSlotLimit = 0;
            }
        }

        private void cleanupEncodeSlotIfNecessary()
        {
            if (encodeSlot != NO_SLOT)
            {
                bufferPool.release(encodeSlot);
                encodeSlot = NO_SLOT;
                encodeSlotOffset = 0;
                encodeSlotTraceId = 0;
            }
        }
    }

    // TODO
    private final class AmqpServerSession
    {
        private final Long2ObjectHashMap<AmqpServerStream> links;

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
