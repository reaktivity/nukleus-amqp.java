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
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorType.DECODE_ERROR;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorType.NOT_ALLOWED;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpType.DESCRIBED;
import static org.reaktivity.nukleus.amqp.internal.util.AmqpTypeUtil.amqpReceiverSettleMode;
import static org.reaktivity.nukleus.amqp.internal.util.AmqpTypeUtil.amqpRole;
import static org.reaktivity.nukleus.amqp.internal.util.AmqpTypeUtil.amqpSenderSettleMode;
import static org.reaktivity.nukleus.budget.BudgetCreditor.NO_CREDITOR_INDEX;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.amqp.internal.AmqpConfiguration;
import org.reaktivity.nukleus.amqp.internal.AmqpNukleus;
import org.reaktivity.nukleus.amqp.internal.types.AmqpCapabilities;
import org.reaktivity.nukleus.amqp.internal.types.BoundedOctets16FW;
import org.reaktivity.nukleus.amqp.internal.types.Flyweight;
import org.reaktivity.nukleus.amqp.internal.types.OctetsFW;
import org.reaktivity.nukleus.amqp.internal.types.String32FW;
import org.reaktivity.nukleus.amqp.internal.types.String8FW;
import org.reaktivity.nukleus.amqp.internal.types.StringFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpAttachFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpBeginFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpBinaryFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpCloseFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpEndFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorListFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorType;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpFlowFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpFrameHeaderFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpFrameType;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpOpenFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpProtocolHeaderFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpReceiverSettleMode;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpRole;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpSenderSettleMode;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpSourceListFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpStringFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpTargetListFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpTransferFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpType;
import org.reaktivity.nukleus.amqp.internal.types.codec.MessageFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.PerformativeFW;
import org.reaktivity.nukleus.amqp.internal.types.control.AmqpRouteExFW;
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
import org.reaktivity.nukleus.amqp.internal.util.AmqpTypeUtil;
import org.reaktivity.nukleus.budget.BudgetCreditor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class AmqpServerFactory implements StreamFactory
{
    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(new UnsafeBuffer(), 0, 0);

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
    private final String32FW.Builder stringRW = new String32FW.Builder();
    private final BoundedOctets16FW.Builder boundedOctetsRW = new BoundedOctets16FW.Builder();

    private final AmqpProtocolHeaderFW amqpProtocolHeaderRO = new AmqpProtocolHeaderFW();
    private final AmqpFrameHeaderFW amqpFrameHeaderRO = new AmqpFrameHeaderFW();
    private final AmqpOpenFW amqpOpenRO = new AmqpOpenFW();
    private final AmqpBeginFW amqpBeginRO = new AmqpBeginFW();
    private final AmqpAttachFW amqpAttachRO = new AmqpAttachFW();
    private final AmqpFlowFW amqpFlowRO = new AmqpFlowFW();
    private final AmqpTransferFW amqpTransferRO = new AmqpTransferFW();
    private final AmqpCloseFW amqpCloseRO = new AmqpCloseFW();
    private final AmqpEndFW amqpEndRO = new AmqpEndFW();
    private final AmqpRouteExFW routeExRO = new AmqpRouteExFW();

    private final AmqpProtocolHeaderFW.Builder amqpProtocolHeaderRW = new AmqpProtocolHeaderFW.Builder();
    private final AmqpFrameHeaderFW.Builder amqpFrameHeaderRW = new AmqpFrameHeaderFW.Builder();
    private final AmqpOpenFW.Builder amqpOpenRW = new AmqpOpenFW.Builder();
    private final AmqpBeginFW.Builder amqpBeginRW = new AmqpBeginFW.Builder();
    private final AmqpAttachFW.Builder amqpAttachRW = new AmqpAttachFW.Builder();
    private final AmqpFlowFW.Builder amqpFlowRW = new AmqpFlowFW.Builder();
    private final AmqpTransferFW.Builder amqpTransferRW = new AmqpTransferFW.Builder();
    private final AmqpCloseFW.Builder amqpCloseRW = new AmqpCloseFW.Builder();
    private final AmqpEndFW.Builder amqpEndRW = new AmqpEndFW.Builder();
    private final AmqpErrorListFW.Builder amqpErrorListRW = new AmqpErrorListFW.Builder();
    private final MessageFW.Builder messageRW = new MessageFW.Builder();
    private final AmqpBinaryFW.Builder amqpBinaryRW = new AmqpBinaryFW.Builder();
    private final AmqpStringFW.Builder amqpStringRW = new AmqpStringFW.Builder();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer extBuffer;
    private final MutableDirectBuffer frameBuffer;
    private final MutableDirectBuffer boundedOctetsBuffer;
    private final MutableDirectBuffer terminusListBuffer;
    private final MutableDirectBuffer emptyListBuffer;
    private final MutableDirectBuffer errorListBuffer;
    private final MutableDirectBuffer stringBuffer;
    private final MutableDirectBuffer messageBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final LongSupplier supplyBudgetId;

    private final Long2ObjectHashMap<MessageConsumer> correlations;
    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);
    private final int amqpTypeId;

    private final BufferPool bufferPool;
    private final BudgetCreditor creditor;

    private final AmqpServerDecoder decodeFrameType = this::decodeFrameType;
    private final AmqpServerDecoder decodeHeader = this::decodeHeader;
    private final AmqpServerDecoder decodeOpen = this::decodeOpen;
    private final AmqpServerDecoder decodeBegin = this::decodeBegin;
    private final AmqpServerDecoder decodeAttach = this::decodeAttach;
    private final AmqpServerDecoder decodeFlow = this::decodeFlow;
    private final AmqpServerDecoder decodeClose = this::decodeClose;
    private final AmqpServerDecoder decodeIgnoreAll = this::decodeIgnoreAll;
    private final AmqpServerDecoder decodeUnknownType = this::decodeUnknownType;

    private final int outgoingWindow;
    private final String containerId;
    private final long maxFrameSize;
    private final long initialDeliveryCount;

    private final Map<AmqpFrameType, AmqpServerDecoder> decodersByFrameType;
    {
        final Map<AmqpFrameType, AmqpServerDecoder> decodersByFrameType = new EnumMap<>(AmqpFrameType.class);
        decodersByFrameType.put(AmqpFrameType.OPEN, decodeOpen);
        decodersByFrameType.put(AmqpFrameType.BEGIN, decodeBegin);
        decodersByFrameType.put(AmqpFrameType.CLOSE, decodeClose);
        decodersByFrameType.put(AmqpFrameType.ATTACH, decodeAttach);
        decodersByFrameType.put(AmqpFrameType.FLOW, decodeFlow);
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
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.extBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.frameBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.boundedOctetsBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.terminusListBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.emptyListBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.errorListBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.stringBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.messageBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.bufferPool = bufferPool;
        this.creditor = creditor;
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyBudgetId = requireNonNull(supplyBudgetId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.correlations = new Long2ObjectHashMap<>();
        this.amqpTypeId = supplyTypeId.applyAsInt(AmqpNukleus.NAME);
        this.containerId = config.containerId();
        this.outgoingWindow = config.outgoingWindow();
        this.maxFrameSize = config.maxFrameSize();
        this.initialDeliveryCount = config.initialDeliveryCount();
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

            newStream = new AmqpServer(sender, routeId, initialId, replyId, affinity, budgetId)::onNetwork;
        }
        return newStream;
    }

    private MessageConsumer newReplyStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long replyId = begin.streamId();
        return correlations.remove(replyId);
    }

    private RouteFW resolveTarget(
        long routeId,
        long authorization,
        String targetAddress,
        AmqpCapabilities role)
    {
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, o + l);
            final OctetsFW ext = route.extension();
            if (ext.sizeof() > 0)
            {
                final AmqpRouteExFW routeEx = ext.get(routeExRO::wrap);
                final String targetAddressEx = routeEx.targetAddress().asString();
                return targetAddressEx.equals(targetAddress) && routeEx.capabilities().get().equals(role);
            }
            return true;
        };
        return router.resolve(routeId, authorization, filter, wrapRoute);
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

            server.onDecodeFrameHeader(frameHeader);
            server.decoder = decoder;
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
        AmqpOpenFW open = amqpOpenRO.tryWrap(buffer, offset, limit);

        int progress = offset;

        if (open != null)
        {
            server.onDecodeOpen(traceId, authorization, open);
            server.decoder = decodeFrameType;
            progress = open.limit();
        }
        else
        {
            server.decoder = decodeIgnoreAll;
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
        AmqpBeginFW begin = amqpBeginRO.tryWrap(buffer, offset, limit);

        int progress = offset;

        if (begin != null)
        {
            server.onDecodeBegin(traceId, authorization, begin);
            server.decoder = decodeFrameType;
            progress = begin.limit();
        }
        return progress;
    }

    private int decodeAttach(
        AmqpServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        final AmqpAttachFW attach = amqpAttachRO.tryWrap(buffer, offset, limit);

        int progress = offset;

        if (attach != null)
        {
            server.onDecodeAttach(traceId, authorization, attach, amqpFrameHeaderRO.channel());
            server.decoder = decodeFrameType;
            progress = attach.limit();
        }
        return progress;
    }

    private int decodeFlow(
        AmqpServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        final AmqpFlowFW flow = amqpFlowRO.tryWrap(buffer, offset, limit);

        int progress = offset;

        if (flow != null)
        {
            server.onDecodeFlow(traceId, authorization, flow);
            server.decoder = decodeFrameType;
            progress = flow.limit();
        }
        return progress;
    }

    private int decodeClose(
        AmqpServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        final AmqpCloseFW close = amqpCloseRO.tryWrap(buffer, offset, limit);

        int progress = offset;

        if (close != null)
        {
            server.onDecodeClose(traceId, authorization, close);
            server.decoder = decodeFrameType;
            progress = close.limit();
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
        server.onDecodeError(traceId, authorization, DECODE_ERROR);
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

        private final Int2ObjectHashMap<AmqpSession> sessions;

        private int initialBudget;
        private int initialPadding;
        private int replyBudget;
        private int replyPadding;

        private long replyBudgetIndex = NO_CREDITOR_INDEX;
        private int sharedReplyBudget;

        private int decodeSlot = NO_SLOT;
        private int decodeSlotLimit;

        private int encodeSlot = NO_SLOT;
        private int encodeSlotOffset;
        private long encodeSlotTraceId;
        private int encodeSlotMaxLimit = Integer.MAX_VALUE;

        private int channelId;
        private int initialMaxFrameSize;

        private AmqpServerDecoder decoder;

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
            this.sessions = new Int2ObjectHashMap<>();
        }

        private void doEncodeProtocolHeader(
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
            long authorization,
            AmqpOpenFW amqpOpen)
        {
            int offsetOpenFrame = amqpFrameHeaderRW.limit();

            final String8FW containerIdRO = new String8FW.Builder()
                .wrap(writeBuffer, 0, writeBuffer.capacity())
                .set(containerId, UTF_8)
                .build();

            amqpOpenRW.wrap(frameBuffer, offsetOpenFrame, frameBuffer.capacity())
                .containerId(containerIdRO);

            if (amqpOpen.hasMaxFrameSize())
            {
                amqpOpenRW.maxFrameSize(maxFrameSize);
            }
            this.initialMaxFrameSize = (int) amqpOpen.maxFrameSize();

            final AmqpOpenFW open = amqpOpenRW.build();

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
            long authorization,
            int channel,
            int nextOutgoingId)
        {
            int offsetBeginFrame = amqpFrameHeaderRW.limit();

            final AmqpBeginFW begin = amqpBeginRW
                .wrap(frameBuffer, offsetBeginFrame, frameBuffer.capacity())
                .remoteChannel(channel)
                .nextOutgoingId(nextOutgoingId)
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

            OctetsFW beginWithFrameHeader = payloadRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .put(updatedFrameHeader.buffer(), 0, updatedFrameHeader.sizeof())
                .put(begin.buffer(), offsetBeginFrame, begin.sizeof())
                .build();

            doNetworkData(traceId, authorization, 0L, beginWithFrameHeader);
        }

        private void doEncodeAttach(
            long traceId,
            long authorization,
            String name,
            int channel,
            long handle,
            AmqpRole role,
            AmqpSenderSettleMode senderSettleMode,
            AmqpReceiverSettleMode receiverSettleMode,
            String address)
        {
            amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                .size(0)
                .doff(2)
                .type(0)
                .channel(channel)
                .performative(b -> b.domainId(d -> d.set(DESCRIBED))
                    .descriptorId(d2 -> d2.set(AmqpFrameType.ATTACH.value())));

            AmqpAttachFW.Builder attachRW = amqpAttachRW.wrap(frameBuffer, amqpFrameHeaderRW.limit(), frameBuffer.capacity())
                .name(stringRW.wrap(stringBuffer, 0, stringBuffer.capacity()).set(name, UTF_8).build())
                .handle(handle)
                .role(role)
                .sndSettleMode(senderSettleMode)
                .rcvSettleMode(receiverSettleMode);

            final StringFW addressRO = stringRW.wrap(stringBuffer, 0, stringBuffer.capacity())
                .set(address, UTF_8)
                .build();
            switch (role)
            {
            case SENDER:
                AmqpSourceListFW sourceList = new AmqpSourceListFW.Builder()
                    .wrap(terminusListBuffer, 0, terminusListBuffer.capacity())
                    .address(addressRO)
                    .build();
                AmqpTargetListFW emptyList = new AmqpTargetListFW.Builder()
                    .wrap(emptyListBuffer, 0, emptyListBuffer.capacity())
                    .build();

                attachRW.source(b -> b.sourceList(sourceList))
                    .target(b -> b.targetList(emptyList))
                    .initialDeliveryCount(initialDeliveryCount);
                break;
            case RECEIVER:
                AmqpTargetListFW targetList = new AmqpTargetListFW.Builder()
                    .wrap(terminusListBuffer, 0, terminusListBuffer.capacity())
                    .address(addressRO)
                    .build();
                attachRW.target(b -> b.targetList(targetList));
                break;
            }

            final AmqpAttachFW attach = attachRW.build();

            AmqpFrameHeaderFW originalFrameHeader = amqpFrameHeaderRW.build();

            final AmqpFrameHeaderFW updatedFrameHeader =
                amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                    .size(originalFrameHeader.sizeof() + attach.sizeof())
                    .doff(originalFrameHeader.doff())
                    .type(originalFrameHeader.type())
                    .channel(originalFrameHeader.channel())
                    .performative(b -> b.domainId(d -> d.set(originalFrameHeader.performative().domainId()))
                        .descriptorId(d2 -> d2.set(originalFrameHeader.performative().descriptorId().get())))
                    .build();

            OctetsFW attachWithFrameHeader = payloadRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .put(updatedFrameHeader.buffer(), updatedFrameHeader.offset(), updatedFrameHeader.sizeof())
                .put(attach.buffer(), attach.offset(), attach.sizeof())
                .build();

            doNetworkData(traceId, authorization, 0L, attachWithFrameHeader);
        }

        private void doEncodeTransfer(
            long traceId,
            long authorization,
            int channel,
            long handle,
            int flags,
            OctetsFW extension,
            OctetsFW payload,
            int payloadIndex)
        {
            final AmqpDataExFW dataEx = extension.get(amqpDataExRO::tryWrap);
            byte[] deliveryTagBytes = new byte[1];
            dataEx.deliveryTag().bytes().get((b, o, l) -> deliveryTagBytes[0] = b.getByte(o));
            BoundedOctets16FW deliveryTag = boundedOctetsRW.wrap(boundedOctetsBuffer, 0, boundedOctetsBuffer.capacity())
                .set(deliveryTagBytes)
                .build();
            int flag = dataEx.flags();
            int bitmask = 1;
            int settled = 0;
            if ((flag & bitmask) == bitmask)
            {
                settled = 1;
                bitmask = bitmask << 2;
            }
            // TODO: add cases for resume, aborted, batchable

            AmqpFrameHeaderFW frameHeader =
                amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                    .size(0)
                    .doff(2)
                    .type(0)
                    .channel(channel)
                    .performative(b -> b.domainId(d -> d.set(DESCRIBED))
                                        .descriptorId(d2 -> d2.set(AmqpFrameType.TRANSFER.value())))
                    .build();

            int payloadSize = payload.sizeof();

            final int frameHeaderSize = frameHeader.sizeof();
            final int transferOffset = frameHeader.limit();
            AmqpTransferFW.Builder transferBuilder =  amqpTransferRW.wrap(frameBuffer, transferOffset, frameBuffer.capacity());
            AmqpTransferFW transfer = payloadIndex > 0 ? transferBuilder.handle(handle).build() :
                transferBuilder.handle(handle)
                    .deliveryId(dataEx.deliveryId())
                    .deliveryTag(deliveryTag)
                    .messageFormat(dataEx.messageFormat())
                    .settled(settled)
                    .build();
            final int transferFrameSize = transfer.sizeof() + Byte.BYTES;
            MessageFW messageInfo = messageRW.wrap(messageBuffer, 0, messageBuffer.capacity())
                .sectionType(b -> b.set(AmqpDescribedType.VALUE))
                .messageType(b -> b.set(AmqpType.BINARY4))
                .messageSize(payload.sizeof())
                .build();
            final int messageSize = messageInfo.sizeof();

            payloadRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity());

            boolean hasMore = false;
            if (payloadIndex == 0)
            {
                if (frameHeaderSize + transferFrameSize + messageSize + payloadSize > initialMaxFrameSize)
                {
                    transfer = amqpTransferRW.wrap(frameBuffer, transferOffset, frameBuffer.capacity())
                        .handle(handle)
                        .deliveryId(dataEx.deliveryId())
                        .deliveryTag(deliveryTag)
                        .messageFormat(dataEx.messageFormat())
                        .settled(settled)
                        .more(1)
                        .build();
                    payloadSize = initialMaxFrameSize - frameHeaderSize - transferFrameSize - messageSize;
                    hasMore = true;
                }
                AmqpFrameHeaderFW updatedFrameHeader = amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                    .size(frameHeaderSize + transfer.sizeof() + messageSize + payloadSize)
                    .doff(2)
                    .type(0)
                    .channel(channel)
                    .performative(b -> b.domainId(d -> d.set(DESCRIBED))
                        .descriptorId(d2 -> d2.set(AmqpFrameType.TRANSFER.value())))
                    .build();
                payloadRW.put(updatedFrameHeader.buffer(), updatedFrameHeader.offset(), updatedFrameHeader.sizeof())
                    .put(transfer.buffer(), transfer.offset(), transfer.sizeof())
                    .put(messageInfo.buffer(), messageInfo.offset(), messageInfo.sizeof());
            }
            else
            {
                if (payloadIndex + (initialMaxFrameSize - frameHeaderSize - transferFrameSize) > payload.sizeof())
                {
                    payloadSize = payload.sizeof() - payloadIndex;
                }
                else
                {
                    transfer = amqpTransferRW.wrap(frameBuffer, transferOffset, frameBuffer.capacity())
                        .handle(handle)
                        .deliveryId(dataEx.deliveryId())
                        .deliveryTag(deliveryTag)
                        .messageFormat(dataEx.messageFormat())
                        .settled(settled)
                        .more(1)
                        .build();
                    payloadSize = initialMaxFrameSize - frameHeaderSize - transferFrameSize;
                    hasMore = true;

                }
                AmqpFrameHeaderFW updatedFrameHeader = amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                    .size(frameHeaderSize + transfer.sizeof() + payloadSize)
                    .doff(2)
                    .type(0)
                    .channel(channel)
                    .performative(b -> b.domainId(d -> d.set(DESCRIBED))
                        .descriptorId(d2 -> d2.set(AmqpFrameType.TRANSFER.value())))
                    .build();
                payloadRW.put(updatedFrameHeader.buffer(), updatedFrameHeader.offset(), updatedFrameHeader.sizeof())
                    .put(transfer.buffer(), transfer.offset(), transfer.sizeof());
            }

            payloadRW.put(payload.buffer(), payload.offset() + payloadIndex, payloadSize);
            OctetsFW transferWithFrameHeader = payloadRW.build();
            doNetworkData(traceId, authorization, 0L, transferWithFrameHeader);

            if (hasMore)
            {
                doEncodeTransfer(traceId, authorization, channel, handle, flags, extension, payload,
                    payloadIndex + payloadSize);
            }
        }

        private void doEncodeClose(
            long traceId,
            long authorization,
            AmqpErrorType errorType)
        {
            int offsetCloseFrame = amqpFrameHeaderRW.limit();
            final AmqpCloseFW.Builder closeRW = amqpCloseRW.wrap(frameBuffer, offsetCloseFrame, frameBuffer.capacity());

            AmqpCloseFW closeRO;
            if (errorType != null)
            {
                AmqpErrorListFW errorList = amqpErrorListRW.wrap(errorListBuffer, 0, errorListBuffer.capacity())
                    .condition(errorType)
                    .build();
                closeRO = closeRW.error(e -> e.errorList(errorList)).build();
            }
            else
            {
                closeRO = closeRW.build();
            }

            AmqpFrameHeaderFW originalFrameHeader = amqpFrameHeaderRW.build();

            final AmqpFrameHeaderFW updatedFrameHeader =
                amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                    .size(originalFrameHeader.sizeof() + closeRO.sizeof())
                    .doff(originalFrameHeader.doff())
                    .type(originalFrameHeader.type())
                    .channel(originalFrameHeader.channel())
                    .performative(b -> b.domainId(d -> d.set(originalFrameHeader.performative().domainId()))
                        .descriptorId(d2 -> d2.set(originalFrameHeader.performative().descriptorId().get())))
                    .build();

            OctetsFW openWithFrameHeader = payloadRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .put(updatedFrameHeader.buffer(), 0, updatedFrameHeader.sizeof())
                .put(closeRO.buffer(), offsetCloseFrame, closeRO.sizeof())
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
                if (sessions.isEmpty() && decoder == decodeIgnoreAll)
                {
                    doNetworkEnd(traceId, authorization);
                }
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
            doNetworkWindow(traceId, authorization, bufferPool.slotCapacity(), 0, 0L);
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
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();
            doNetworkAbort(traceId, authorization);
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

            final int slotCapacity = bufferPool.slotCapacity();
            final int sharedReplyCredit = Math.min(slotCapacity, replyBudget - encodeSlotOffset - sharedReplyBudget);
            AtomicInteger minRemoteIncomingWindow = new AtomicInteger();
            sessions.values()
                .forEach(s -> minRemoteIncomingWindow.set(Math.min(s.remoteIncomingWindow, minRemoteIncomingWindow.get())));

            if (sharedReplyCredit > 0)
            {
                final long replySharedBudgetPrevious = creditor.credit(traceId, replyBudgetIndex, sharedReplyCredit);
                sharedReplyBudget = Math.min(minRemoteIncomingWindow.get() * initialMaxFrameSize, replyBudget);

                assert replySharedBudgetPrevious <= slotCapacity
                    : String.format("%d <= %d, replyBudget = %d",
                    replySharedBudgetPrevious, slotCapacity, replyBudget);

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
            long authorization,
            AmqpErrorType errorType)
        {
            cleanupStreams(traceId, authorization);
            // TODO: if open-open has not been exchanged, cannot call doEncodeClose
            doEncodeClose(traceId, authorization, errorType);
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
                doEncodeProtocolHeader(header.major(), header.minor(), header.revision(), traceId, authorization);
            }
            else
            {
                onDecodeError(traceId, authorization, DECODE_ERROR);
            }
        }

        private void onDecodeFrameHeader(
            AmqpFrameHeaderFW frameHeader)
        {
            doEncodeFrameHeader(frameHeader.doff(), frameHeader.type(), frameHeader.channel(), frameHeader.performative());
        }

        private void onDecodeOpen(
            long traceId,
            long authorization,
            AmqpOpenFW open)
        {
            doEncodeOpen(traceId, authorization, open);
        }

        private void onDecodeBegin(
            long traceId,
            long authorization,
            AmqpBeginFW begin)
        {
            if (begin.hasRemoteChannel())
            {
                onDecodeError(traceId, authorization, NOT_ALLOWED);
            }
            else
            {
                this.channelId++;
                AmqpSession session = sessions.computeIfAbsent(channelId, AmqpSession::new);
                session.nextIncomingId((int) begin.nextOutgoingId());
                session.incomingWindow(writeBuffer.capacity());
                session.outgoingWindow(outgoingWindow);
                session.remoteIncomingWindow((int) begin.incomingWindow());
                session.remoteOutgoingWindow((int) begin.outgoingWindow());
                session.onDecodeBegin(traceId, authorization);
            }
        }

        private void onDecodeAttach(
            long traceId,
            long authorization,
            AmqpAttachFW attach,
            int channel)
        {
            AmqpSession session = sessions.get(channel);
            if (session != null)
            {
                session.onDecodeAttach(traceId, authorization, attach);
            }
            else
            {
                onDecodeError(traceId, authorization, NOT_ALLOWED);
            }
        }

        private void onDecodeFlow(
            long traceId,
            long authorization,
            AmqpFlowFW flow)
        {
            AmqpSession session = sessions.get(amqpFrameHeaderRO.channel());
            if (session != null)
            {
                session.onDecodeFlow(traceId, authorization, flow);
            }
            else
            {
                onDecodeError(traceId, authorization, NOT_ALLOWED);
            }
        }

        private void onDecodeClose(
            long traceId,
            long authorization,
            AmqpCloseFW close)
        {
            if (close.fieldCount() == 0)
            {
                sessions.values().forEach(s -> s.cleanup(traceId, authorization));
                doEncodeClose(traceId, authorization, null);
                doNetworkEnd(traceId, authorization);
            }
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
            sessions.values().forEach(s -> s.cleanup(traceId, authorization));
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

        private final class AmqpSession
        {
            private final Long2ObjectHashMap<AmqpServerStream> links;

            private int channelId;
            private int nextIncomingId;
            private int incomingWindow;
            private int nextOutgoingId;
            private int outgoingWindow;
            private int remoteIncomingWindow;
            private int remoteOutgoingWindow;

            private AmqpSession(
                int channelId)
            {
                this.links = new Long2ObjectHashMap<>();
                this.channelId = channelId;
                this.nextOutgoingId++;
            }

            private void nextIncomingId(
                int nextOutgoingId)
            {
                this.nextIncomingId = nextOutgoingId;
            }

            private void incomingWindow(
                int incomingWindow)
            {
                this.incomingWindow = incomingWindow;
            }

            private void outgoingWindow(
                int outgoingWindow)
            {
                this.outgoingWindow = outgoingWindow;
            }

            private void remoteIncomingWindow(
                int incomingWindow)
            {
                this.remoteIncomingWindow = incomingWindow;
            }

            private void remoteOutgoingWindow(
                int outgoingWindow)
            {
                this.remoteOutgoingWindow = outgoingWindow;
            }

            private void onDecodeBegin(
                long traceId,
                long authorization)
            {
                doEncodeBegin(traceId, authorization, channelId, nextOutgoingId);
            }

            private void onDecodeAttach(
                long traceId,
                long authorization,
                AmqpAttachFW attach)
            {
                final long linkKey = attach.handle();
                if (links.containsKey(linkKey))
                {
                    onDecodeError(traceId, authorization, NOT_ALLOWED);
                }
                else
                {
                    AmqpServerStream link = new AmqpServerStream();
                    AmqpServerStream oldLink = links.put(linkKey, link);
                    assert oldLink == null;
                    link.onDecodeAttach(traceId, authorization, attach);
                }
            }

            private void onDecodeFlow(
                long traceId,
                long authorization,
                AmqpFlowFW flow)
            {
                int flowNextIncomingId = (int) flow.nextIncomingId();
                int flowIncomingWindow = (int) flow.incomingWindow();
                int flowNextOutgoingId = (int) flow.nextOutgoingId();
                int flowOutgoingWindow = (int) flow.outgoingWindow();
                long deliveryCount = flow.hasDeliveryCount() ? flow.deliveryCount() : -1;
                int linkCredit = flow.hasLinkCredit() ? (int) flow.linkCredit() : -1;
                this.nextIncomingId = flowNextOutgoingId;
                this.remoteIncomingWindow = flowNextIncomingId + flowIncomingWindow - nextOutgoingId;
                this.remoteOutgoingWindow = flowOutgoingWindow;
                sharedReplyBudget = Math.min(remoteIncomingWindow * initialMaxFrameSize, replyBudget);

                if (flow.hasHandle())
                {
                    links.get(flow.handle()).onDecodeFlow(traceId, authorization, deliveryCount, linkCredit);
                }
                else
                {
                    links.values().forEach(s -> s.onDecodeFlow(traceId, authorization, deliveryCount, linkCredit));
                }
            }

            private void cleanup(
                long traceId,
                long authorization)
            {
                links.values().forEach(l -> l.cleanup(traceId, authorization));
            }

            private class AmqpServerStream
            {
                private MessageConsumer application;
                private long newRouteId;
                private long initialId;
                private long replyId;
                private long budgetId;
                private int replyBudget;
                private long deliveryCount;
                private int linkCredit;

                private int initialSlot = NO_SLOT;
                private int initialSlotOffset;
                private long initialSlotTraceId;

                private int state;
                private Set<AmqpRole> roles;
                private String name;
                private long handle;

                AmqpServerStream()
                {
                    this.roles = EnumSet.noneOf(AmqpRole.class);
                }

                private void onDecodeAttach(
                    long traceId,
                    long authorization,
                    AmqpAttachFW attach)
                {
                    this.name = attach.name().asString();
                    this.handle = attach.handle();
                    AmqpRole role = attach.role();
                    boolean hasSourceAddress = attach.hasSource() && attach.source().sourceList().hasAddress();
                    boolean hasTargetAddress = attach.hasTarget() && attach.target().targetList().hasAddress();

                    final RouteFW route;
                    switch (role)
                    {
                    case RECEIVER:
                        route = resolveTarget(routeId, authorization, hasSourceAddress ?
                            attach.source().sourceList().address().asString() : "", AmqpCapabilities.RECEIVE_ONLY);
                        break;
                    case SENDER:
                        route = resolveTarget(routeId, authorization, hasTargetAddress ?
                            attach.target().targetList().address().asString() : "", AmqpCapabilities.SEND_ONLY);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + role);
                    }

                    if (route != null)
                    {
                        this.newRouteId = route.correlationId();
                        this.initialId = supplyInitialId.applyAsLong(newRouteId);
                        this.replyId = supplyReplyId.applyAsLong(initialId);
                        this.application = router.supplyReceiver(initialId);
                        String address = null;
                        switch (role)
                        {
                        case RECEIVER:
                            if (hasSourceAddress)
                            {
                                address = attach.source().sourceList().address().asString();
                            }
                            break;
                        case SENDER:
                            if (hasTargetAddress)
                            {
                                address = attach.target().targetList().address().asString();
                            }
                            break;
                        }

                        final AmqpSenderSettleMode amqpSenderSettleMode = attach.sndSettleMode();
                        final AmqpReceiverSettleMode amqpReceiverSettleMode = attach.rcvSettleMode();

                        doApplicationBeginIfNecessary(traceId, authorization, affinity, address, role, amqpSenderSettleMode,
                            amqpReceiverSettleMode);
                        doApplicationData(traceId, authorization, role);

                        correlations.put(replyId, this::onApplication);
                    }
                }

                private void onDecodeFlow(
                    long traceId,
                    long authorization,
                    long deliveryCount,
                    int linkCredit)
                {
                    this.linkCredit = (int) (deliveryCount + linkCredit - this.deliveryCount);
                    this.deliveryCount = deliveryCount;
                    this.replyBudget = linkCredit * initialMaxFrameSize;
                    flushReplyWindow(traceId, authorization);
                }

                private void doApplicationBeginIfNecessary(
                    long traceId,
                    long authorization,
                    long affinity,
                    String targetAddress,
                    AmqpRole role,
                    AmqpSenderSettleMode senderSettleMode,
                    AmqpReceiverSettleMode receiverSettleMode)
                {
                    if (!AmqpState.initialOpening(state))
                    {
                        doApplicationBegin(traceId, authorization, affinity, targetAddress, role, senderSettleMode,
                            receiverSettleMode);
                    }
                }

                private void doApplicationBegin(
                    long traceId,
                    long authorization,
                    long affinity,
                    String address,
                    AmqpRole role,
                    AmqpSenderSettleMode senderSettleMode,
                    AmqpReceiverSettleMode receiverSettleMode)
                {
                    assert state == 0;
                    state = AmqpState.openingInitial(state);

                    router.setThrottle(initialId, this::onApplication);

                    final AmqpBeginExFW beginEx = amqpBeginExRW.wrap(extBuffer, 0, extBuffer.capacity())
                        .typeId(amqpTypeId)
                        .address(address)
                        .capabilities(r -> r.set(AmqpTypeUtil.amqpCapabilities(role)))
                        .senderSettleMode(s -> s.set(amqpSenderSettleMode(senderSettleMode)))
                        .receiverSettleMode(r -> r.set(amqpReceiverSettleMode(receiverSettleMode)))
                        .build();

                    doBegin(application, newRouteId, initialId, traceId, authorization, affinity, beginEx);

                    if (initialSlot == NO_SLOT)
                    {
                        initialSlot = bufferPool.acquire(initialId);
                    }

                    if (initialSlot == NO_SLOT)
                    {
                        cleanup(traceId, authorization);
                    }
                }

                private void doApplicationData(
                    long traceId,
                    long authorization,
                    AmqpRole role)
                {
                    assert AmqpState.initialOpening(state);

                    switch (role)
                    {
                    case SENDER:
                        // TODO
                        break;
                    }
                }

                private void doApplicationAbort(
                    long traceId,
                    long authorization,
                    Flyweight extension)
                {
                    setInitialClosed();

                    doAbort(application, newRouteId, initialId, traceId, authorization, extension);
                }

                private void doApplicationAbortIfNecessary(
                    long traceId,
                    long authorization)
                {
                    if (!AmqpState.initialClosed(state))
                    {
                        doApplicationAbort(traceId, authorization, EMPTY_OCTETS);
                    }
                }

                private void setInitialClosed()
                {
                    assert !AmqpState.initialClosed(state);

                    state = AmqpState.closedInitial(state);
                    cleanupInitialSlotIfNecessary();

                    if (AmqpState.closed(state))
                    {
                        sessions.clear();
                        links.clear();
                    }
                }

                private void onApplication(
                    int msgTypeId,
                    DirectBuffer buffer,
                    int index,
                    int length)
                {
                    switch (msgTypeId)
                    {
                    case BeginFW.TYPE_ID:
                        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                        onApplicationBegin(begin);
                        break;
                    case DataFW.TYPE_ID:
                        final DataFW data = dataRO.wrap(buffer, index, index + length);
                        onApplicationData(data);
                        break;
                    case EndFW.TYPE_ID:
                        final EndFW end = endRO.wrap(buffer, index, index + length);
                        onApplicationEnd(end);
                        break;
                    case AbortFW.TYPE_ID:
                        final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                        onApplicationAbort(abort);
                        break;
                    case WindowFW.TYPE_ID:
                        final WindowFW window = windowRO.wrap(buffer, index, index + length);
                        onApplicationWindow(window);
                        break;
                    case ResetFW.TYPE_ID:
                        final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                        onApplicationReset(reset);
                        break;
                    case SignalFW.TYPE_ID:
                        final SignalFW signal = signalRO.wrap(buffer, index, index + length);
                        onApplicationSignal(signal);
                        break;
                    }
                }

                private void onApplicationWindow(
                    WindowFW window)
                {
                    final long traceId = window.traceId();
                    final long authorization = window.authorization();
                    final long budgetId = window.budgetId();
                    final int credit = window.credit();
                    final int padding = window.padding();

                    state = AmqpState.openedInitial(state);

                    this.budgetId = budgetId;

                    initialBudget += credit;
                    initialPadding = padding;

                    if (initialSlot != NO_SLOT)
                    {
                        final MutableDirectBuffer buffer = bufferPool.buffer(initialSlot);
                        final int offset = 0;
                        final int limit = initialSlotOffset;

                        flushApplicationData(initialSlotTraceId, authorization, buffer, offset, limit, EMPTY_OCTETS);
                    }

                    if (initialSlot == NO_SLOT)
                    {
                        if (AmqpState.initialClosing(state) && !AmqpState.initialClosed(state))
                        {
                            flushApplicationEnd(traceId, authorization, EMPTY_OCTETS);
                        }
                    }
                }

                private void onApplicationReset(
                    ResetFW reset)
                {
                    setInitialClosed();

                    final long traceId = reset.traceId();
                    final long authorization = reset.authorization();
                    // TODO

                    cleanup(traceId, authorization);
                }

                private void onApplicationSignal(
                    SignalFW signal)
                {
                    final long signalId = signal.signalId();
                    // TODO
                }

                private boolean isReplyOpen()
                {
                    return AmqpState.replyOpened(state);
                }

                private void onApplicationBegin(
                    BeginFW begin)
                {
                    state = AmqpState.openedReply(state);

                    final long traceId = begin.traceId();
                    final long authorization = begin.authorization();

                    final AmqpBeginExFW amqpBeginEx = begin.extension().get(amqpBeginExRO::tryWrap);
                    if (amqpBeginEx != null)
                    {
                        AmqpRole amqpRole = amqpRole(amqpBeginEx.capabilities().get());
                        AmqpSenderSettleMode amqpSenderSettleMode = amqpSenderSettleMode(amqpBeginEx.senderSettleMode().get());
                        AmqpReceiverSettleMode amqpReceiverSettleMode =
                            amqpReceiverSettleMode(amqpBeginEx.receiverSettleMode().get());
                        final String address = amqpBeginEx.address().asString();

                        deliveryCount = initialDeliveryCount;
                        doEncodeAttach(traceId, authorization, name, channelId, handle, amqpRole, amqpSenderSettleMode,
                            amqpReceiverSettleMode, address);
                    }
                }

                private void onApplicationData(
                    DataFW data)
                {
                    final long traceId = data.traceId();
                    final int reserved = data.reserved();
                    final long authorization = data.authorization();
                    final int flags = data.flags();
                    final OctetsFW extension = data.extension();

                    this.replyBudget -= reserved;
                    sharedReplyBudget -= reserved;

                    if (replyBudget < 0)
                    {
                        doApplicationReset(traceId, authorization);
                        doNetworkAbort(traceId, authorization);
                    }

                    remoteIncomingWindow--;
                    nextOutgoingId++;
                    outgoingWindow--;
                    deliveryCount++;
                    linkCredit--;
                    doEncodeTransfer(traceId, authorization, channelId, handle, flags, extension, data.payload(), 0);
                }

                private void onApplicationEnd(
                    EndFW end)
                {
                    setReplyClosed();
                }

                private void onApplicationAbort(
                    AbortFW abort)
                {
                    setReplyClosed();

                    final long traceId = abort.traceId();
                    final long authorization = abort.authorization();

                    cleanupCorrelationIfNecessary();
                    cleanup(traceId, authorization);
                }

                private void flushApplicationData(
                    long traceId,
                    long authorization,
                    DirectBuffer buffer,
                    int offset,
                    int limit,
                    Flyweight extension)
                {
                    final int maxLength = limit - offset;
                    final int length = Math.max(Math.min(initialBudget - initialPadding, maxLength), 0);

                    if (length > 0)
                    {
                        final int reserved = length + initialPadding;

                        initialBudget -= reserved;

                        assert initialBudget >= 0;

                        doData(application, routeId, initialId, traceId, authorization, budgetId, reserved, buffer, offset,
                            length, extension);
                    }

                    final int remaining = maxLength - length;
                    if (remaining > 0)
                    {
                        if (initialSlot == NO_SLOT)
                        {
                            initialSlot = bufferPool.acquire(initialId);
                        }

                        if (initialSlot == NO_SLOT)
                        {
                            cleanup(traceId, authorization);
                        }
                        else
                        {
                            final MutableDirectBuffer initialBuffer = bufferPool.buffer(initialSlot);
                            initialBuffer.putBytes(0, buffer, offset, remaining);
                            initialSlotOffset = remaining;
                            initialSlotTraceId = traceId;
                        }
                    }
                    else
                    {
                        cleanupInitialSlotIfNecessary();
                    }
                }

                private void flushApplicationEnd(
                    long traceId,
                    long authorization,
                    Flyweight extension)
                {
                    setInitialClosed();
                    roles.clear();

                    doEnd(application, newRouteId, initialId, traceId, authorization, extension);
                }

                private void flushReplyWindow(
                    long traceId,
                    long authorization)
                {
                    if (isReplyOpen())
                    {
                        doWindow(application, newRouteId, replyId, traceId, authorization,
                            replySharedBudgetId, replyBudget, 0);
                    }
                }

                private void doApplicationReset(
                    long traceId,
                    long authorization)
                {
                    setReplyClosed();

                    doReset(application, newRouteId, replyId, traceId, authorization, EMPTY_OCTETS);
                }

                private void doApplicationResetIfNecessary(
                    long traceId,
                    long authorization)
                {
                    correlations.remove(replyId);

                    if (!AmqpState.replyClosed(state))
                    {
                        doApplicationReset(traceId, authorization);
                    }
                }

                private void setReplyClosed()
                {
                    assert !AmqpState.replyClosed(state);

                    state = AmqpState.closedReply(state);

                    if (AmqpState.closed(state))
                    {
                        roles.clear();
                        // TODO
                    }
                }

                private void cleanup(
                    long traceId,
                    long authorization)
                {
                    doApplicationAbortIfNecessary(traceId, authorization);
                    doApplicationResetIfNecessary(traceId, authorization);
                }

                private void cleanupInitialSlotIfNecessary()
                {
                    if (initialSlot != NO_SLOT)
                    {
                        bufferPool.release(initialSlot);
                        initialSlot = NO_SLOT;
                        initialSlotOffset = 0;
                        initialSlotTraceId = 0;
                    }
                }

                private boolean cleanupCorrelationIfNecessary()
                {
                    final MessageConsumer correlated = correlations.remove(replyId);
                    if (correlated != null)
                    {
                        router.clearThrottle(replyId);
                    }

                    return correlated != null;
                }
            }
        }
    }
}
