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
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpTransferFlags.aborted;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpTransferFlags.batchable;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpTransferFlags.isSettled;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpTransferFlags.resume;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpTransferFlags.settled;
import static org.reaktivity.nukleus.amqp.internal.types.AmqpAnnotationKeyFW.KIND_ID;
import static org.reaktivity.nukleus.amqp.internal.types.AmqpAnnotationKeyFW.KIND_NAME;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.APPLICATION_PROPERTIES;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.ATTACH;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.BEGIN;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.CLOSE;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.DETACH;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.END;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.FLOW;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.MESSAGE_ANNOTATIONS;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.OPEN;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.PROPERTIES;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.TRANSFER;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.VALUE;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorType.CONNECTION_FRAMING_ERROR;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorType.DECODE_ERROR;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorType.NOT_ALLOWED;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpOpenFW.DEFAULT_VALUE_MAX_FRAME_SIZE;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpReceiverSettleMode.FIRST;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpRole.RECEIVER;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpRole.SENDER;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpSenderSettleMode.MIXED;
import static org.reaktivity.nukleus.amqp.internal.util.AmqpTypeUtil.amqpCapabilities;
import static org.reaktivity.nukleus.amqp.internal.util.AmqpTypeUtil.amqpReceiverSettleMode;
import static org.reaktivity.nukleus.amqp.internal.util.AmqpTypeUtil.amqpRole;
import static org.reaktivity.nukleus.amqp.internal.util.AmqpTypeUtil.amqpSenderSettleMode;
import static org.reaktivity.nukleus.budget.BudgetCreditor.NO_CREDITOR_INDEX;
import static org.reaktivity.nukleus.budget.BudgetDebitor.NO_DEBITOR_INDEX;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.amqp.internal.AmqpConfiguration;
import org.reaktivity.nukleus.amqp.internal.AmqpNukleus;
import org.reaktivity.nukleus.amqp.internal.types.AmqpAnnotationFW;
import org.reaktivity.nukleus.amqp.internal.types.AmqpApplicationPropertyFW;
import org.reaktivity.nukleus.amqp.internal.types.AmqpCapabilities;
import org.reaktivity.nukleus.amqp.internal.types.AmqpPropertiesFW;
import org.reaktivity.nukleus.amqp.internal.types.Array32FW;
import org.reaktivity.nukleus.amqp.internal.types.ArrayFW;
import org.reaktivity.nukleus.amqp.internal.types.BoundedOctetsFW;
import org.reaktivity.nukleus.amqp.internal.types.Flyweight;
import org.reaktivity.nukleus.amqp.internal.types.OctetsFW;
import org.reaktivity.nukleus.amqp.internal.types.String8FW;
import org.reaktivity.nukleus.amqp.internal.types.StringFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpAttachFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpBeginFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpBinaryFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpByteFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpCloseFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedTypeFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDetachFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpEndFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorListFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorType;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpFlowFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpFrameHeaderFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpMapFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpMessagePropertiesFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpOpenFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpPerformativeFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpProtocolHeaderFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpReceiverSettleMode;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpRole;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpSenderSettleMode;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpSourceListFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpStringFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpSymbolFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpTargetListFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpTransferFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpType;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpULongFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpValueFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpValueHeaderFW;
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
import org.reaktivity.nukleus.budget.BudgetCreditor;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class AmqpServerFactory implements StreamFactory
{
    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(new UnsafeBuffer(), 0, 0);

    private static final int FLAG_FIN = 1;
    private static final int FLAG_INIT = 2;
    private static final int FLAG_INIT_AND_FIN = 3;
    private static final int FRAME_HEADER_SIZE = 11;
    private static final int MIN_MAX_FRAME_SIZE = 512;
    private static final int DESCRIBED_TYPE_SIZE = 3;
    private static final int TRANSFER_HEADER_SIZE = 20;
    private static final int PAYLOAD_HEADER_SIZE = 205;
    private static final long PROTOCOL_HEADER = 0x414D5150_00010000L;

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
    private final AmqpFrameHeaderFW amqpFrameHeaderRO = new AmqpFrameHeaderFW();
    private final AmqpPerformativeFW amqpPerformativeRO = new AmqpPerformativeFW();
    private final AmqpRouteExFW routeExRO = new AmqpRouteExFW();
    private final AmqpValueHeaderFW amqpValueHeaderRO = new AmqpValueHeaderFW();
    private final AmqpDescribedTypeFW amqpDescribedTypeRO = new AmqpDescribedTypeFW();
    private final AmqpMapFW<AmqpValueFW, AmqpValueFW> annotationRO = new AmqpMapFW<>(new AmqpValueFW(), new AmqpValueFW());
    private final OctetsFW deliveryTagRO = new OctetsFW();
    private final AmqpMessagePropertiesFW amqpPropertiesRO = new AmqpMessagePropertiesFW();
    private final AmqpMapFW<AmqpValueFW, AmqpValueFW> applicationPropertyRO =
        new AmqpMapFW<>(new AmqpValueFW(), new AmqpValueFW());

    private final AmqpProtocolHeaderFW.Builder amqpProtocolHeaderRW = new AmqpProtocolHeaderFW.Builder();
    private final AmqpFrameHeaderFW.Builder amqpFrameHeaderRW = new AmqpFrameHeaderFW.Builder();
    private final AmqpOpenFW.Builder amqpOpenRW = new AmqpOpenFW.Builder();
    private final AmqpBeginFW.Builder amqpBeginRW = new AmqpBeginFW.Builder();
    private final AmqpAttachFW.Builder amqpAttachRW = new AmqpAttachFW.Builder();
    private final AmqpFlowFW.Builder amqpFlowRW = new AmqpFlowFW.Builder();
    private final AmqpTransferFW.Builder amqpTransferRW = new AmqpTransferFW.Builder();
    private final AmqpDetachFW.Builder amqpDetachRW = new AmqpDetachFW.Builder();
    private final AmqpEndFW.Builder amqpEndRW = new AmqpEndFW.Builder();
    private final AmqpCloseFW.Builder amqpCloseRW = new AmqpCloseFW.Builder();
    private final AmqpErrorListFW.Builder amqpErrorListRW = new AmqpErrorListFW.Builder();
    private final AmqpValueHeaderFW.Builder amqpValueHeaderRW = new AmqpValueHeaderFW.Builder();
    private final AmqpStringFW.Builder amqpStringRW = new AmqpStringFW.Builder();
    private final AmqpStringFW.Builder amqpValueRW = new AmqpStringFW.Builder();
    private final AmqpSymbolFW.Builder amqpSymbolRW = new AmqpSymbolFW.Builder();
    private final AmqpSourceListFW.Builder amqpSourceListRW = new AmqpSourceListFW.Builder();
    private final AmqpTargetListFW.Builder amqpTargetListRW = new AmqpTargetListFW.Builder();
    private final AmqpBinaryFW.Builder amqpBinaryRW = new AmqpBinaryFW.Builder();
    private final AmqpByteFW.Builder amqpByteRW = new AmqpByteFW.Builder();
    private final AmqpULongFW.Builder amqpULongRW = new AmqpULongFW.Builder();
    private final AmqpDescribedTypeFW.Builder amqpDescribedTypeRW = new AmqpDescribedTypeFW.Builder();
    private final AmqpMessagePropertiesFW.Builder amqpPropertiesRW = new AmqpMessagePropertiesFW.Builder();
    private final Array32FW.Builder<AmqpAnnotationFW.Builder, AmqpAnnotationFW> annotationRW =
        new Array32FW.Builder<>(new AmqpAnnotationFW.Builder(), new AmqpAnnotationFW());
    private final AmqpPropertiesFW.Builder propertyRW = new AmqpPropertiesFW.Builder();
    private final Array32FW.Builder<AmqpApplicationPropertyFW.Builder, AmqpApplicationPropertyFW> applicationPropertyRW =
        new Array32FW.Builder<>(new AmqpApplicationPropertyFW.Builder(), new AmqpApplicationPropertyFW());

    private final MutableInteger minimum = new MutableInteger(Integer.MAX_VALUE);
    private final MutableInteger valueOffset = new MutableInteger();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer frameBuffer;
    private final MutableDirectBuffer extraBuffer;
    private final MutableDirectBuffer valueBuffer;
    private final MutableDirectBuffer stringBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final LongSupplier supplyBudgetId;

    private final Long2ObjectHashMap<MessageConsumer> correlations;
    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);
    private final int amqpTypeId;

    private final BufferPool bufferPool;
    private final BudgetCreditor creditor;
    private final LongFunction<BudgetDebitor> supplyDebitor;

    private final AmqpServerDecoder decodeFrameType = this::decodeFrameType;
    private final AmqpServerDecoder decodeHeader = this::decodeHeader;
    private final AmqpServerDecoder decodeOpen = this::decodeOpen;
    private final AmqpServerDecoder decodeBegin = this::decodeBegin;
    private final AmqpServerDecoder decodeAttach = this::decodeAttach;
    private final AmqpServerDecoder decodeFlow = this::decodeFlow;
    private final AmqpServerDecoder decodeTransfer = this::decodeTransfer;
    private final AmqpServerDecoder decodeDetach = this::decodeDetach;
    private final AmqpServerDecoder decodeEnd = this::decodeEnd;
    private final AmqpServerDecoder decodeClose = this::decodeClose;
    private final AmqpServerDecoder decodeIgnoreAll = this::decodeIgnoreAll;
    private final AmqpServerDecoder decodeUnknownType = this::decodeUnknownType;

    private final int outgoingWindow;
    private final StringFW containerId;
    private final long defaultMaxFrameSize;
    private final long initialDeliveryCount;

    private final Map<AmqpDescribedType, AmqpServerDecoder> decodersByPerformative;
    {
        final Map<AmqpDescribedType, AmqpServerDecoder> decodersByPerformative = new EnumMap<>(AmqpDescribedType.class);
        decodersByPerformative.put(OPEN, decodeOpen);
        decodersByPerformative.put(BEGIN, decodeBegin);
        decodersByPerformative.put(ATTACH, decodeAttach);
        decodersByPerformative.put(FLOW, decodeFlow);
        decodersByPerformative.put(TRANSFER, decodeTransfer);
        // decodersByPerformative.put(AmqpDescribedType.DISPOSITION, decodeDisposition);
        decodersByPerformative.put(DETACH, decodeDetach);
        decodersByPerformative.put(END, decodeEnd);
        decodersByPerformative.put(CLOSE, decodeClose);
        this.decodersByPerformative = decodersByPerformative;
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
        ToIntFunction<String> supplyTypeId,
        LongFunction<BudgetDebitor> supplyDebitor)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.frameBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.extraBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.stringBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.valueBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.bufferPool = bufferPool;
        this.creditor = creditor;
        this.supplyDebitor = supplyDebitor;
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyBudgetId = requireNonNull(supplyBudgetId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.correlations = new Long2ObjectHashMap<>();
        this.amqpTypeId = supplyTypeId.applyAsInt(AmqpNukleus.NAME);
        this.containerId = new String8FW(config.containerId());
        this.outgoingWindow = config.outgoingWindow();
        this.defaultMaxFrameSize = config.maxFrameSize();
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

            newStream = new AmqpServer(sender, routeId, initialId, affinity)::onNetwork;
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
        StringFW targetAddress,
        AmqpCapabilities capabilities)
    {
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, o + l);
            final OctetsFW extension = route.extension();
            final AmqpRouteExFW routeEx = extension.get(routeExRO::tryWrap);
            return routeEx == null || Objects.equals(targetAddress, routeEx.targetAddress()) &&
                    capabilities == routeEx.capabilities().get();
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
        int padding,
        int minimum)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .routeId(routeId)
            .streamId(streamId)
            .traceId(traceId)
            .authorization(authorization)
            .budgetId(budgetId)
            .credit(credit)
            .padding(padding)
            .minimum(minimum)
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

        decode:
        if (frameHeader != null)
        {
            if (frameHeader.size() > server.decodeMaxFrameSize)
            {
                server.onDecodeError(traceId, authorization, CONNECTION_FRAMING_ERROR);
                server.decoder = decodeFrameType;
                progress = limit;
                break decode;
            }

            final AmqpPerformativeFW performative = frameHeader.performative();
            final AmqpDescribedType descriptor = performative.kind();
            final AmqpServerDecoder decoder = decodersByPerformative.getOrDefault(descriptor, decodeUnknownType);
            server.incomingChannel = frameHeader.channel();
            server.decoder = decoder;
            progress = performative.offset();
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
        final AmqpPerformativeFW performative = amqpPerformativeRO.wrap(buffer, offset, limit);
        final AmqpOpenFW open = performative.open();
        assert open != null;
        // TODO: verify decodeChannel == 0
        // TODO: verify not already open
        server.onDecodeOpen(traceId, authorization, open);
        server.decoder = decodeFrameType;
        return open.limit();
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
        final AmqpPerformativeFW performative = amqpPerformativeRO.wrap(buffer, offset, limit);
        final AmqpBeginFW begin = performative.begin();
        assert begin != null;

        server.onDecodeBegin(traceId, authorization, begin);
        server.decoder = decodeFrameType;
        return begin.limit();
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
        final AmqpPerformativeFW performative = amqpPerformativeRO.wrap(buffer, offset, limit);
        final AmqpAttachFW attach = performative.attach();
        assert attach != null;

        server.onDecodeAttach(traceId, authorization, attach);
        server.decoder = decodeFrameType;
        return attach.limit();
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
        final AmqpPerformativeFW performative = amqpPerformativeRO.wrap(buffer, offset, limit);
        final AmqpFlowFW flow = performative.flow();
        assert flow != null;

        server.onDecodeFlow(traceId, authorization, flow);
        server.decoder = decodeFrameType;
        return flow.limit();
    }

    private int decodeTransfer(
        AmqpServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        final AmqpPerformativeFW performative = amqpPerformativeRO.wrap(buffer, offset, limit);
        final AmqpTransferFW transfer = performative.transfer();
        assert transfer != null;

        int progress = offset;
        AmqpServer.AmqpSession.AmqpServerStream sender = server.sessions.get(server.incomingChannel).links.get(transfer.handle());
        int sectionOffset = transfer.limit();
        Array32FW<AmqpAnnotationFW> annotations = null;
        AmqpPropertiesFW properties = null;
        Array32FW<AmqpApplicationPropertyFW> applicationProperties = null;
        int annotationSize = 0;
        int propertySize = 0;
        int applicationPropertySize = 0;
        if (sender.deliveryId == -1)
        {
            AmqpDescribedTypeFW sectionType = amqpDescribedTypeRO.tryWrap(buffer, sectionOffset, limit);
            if (sectionType != null && sectionType.get() == MESSAGE_ANNOTATIONS)
            {
                AmqpMapFW<AmqpValueFW, AmqpValueFW> annotation =
                    annotationRO.tryWrap(buffer, sectionType.limit(), limit);
                if (annotation != null)
                {
                    sectionOffset = annotation.limit();
                    annotations = decodeAnnotations(annotation);
                    annotationSize = sectionType.sizeof() + annotation.sizeof();
                }
                else
                {
                    return decodeError(server, traceId, authorization, limit);
                }
                sectionType = amqpDescribedTypeRO.tryWrap(buffer, sectionOffset, limit);
            }
            if (sectionType != null && sectionType.get() == PROPERTIES)
            {
                AmqpMessagePropertiesFW property = amqpPropertiesRO.tryWrap(buffer, sectionType.limit(), limit);
                if (property != null)
                {
                    sectionOffset = property.limit();
                    properties = decodeProperties(property);
                    propertySize = sectionType.sizeof() + properties.sizeof();
                }
                else
                {
                    return decodeError(server, traceId, authorization, limit);
                }
                sectionType = amqpDescribedTypeRO.tryWrap(buffer, sectionOffset, limit);
            }
            if (sectionType != null && sectionType.get() == APPLICATION_PROPERTIES)
            {
                AmqpMapFW<AmqpValueFW, AmqpValueFW> applicationProperty =
                    applicationPropertyRO.tryWrap(buffer, sectionType.limit(), limit);
                if (applicationProperty != null)
                {
                    sectionOffset = applicationProperty.limit();
                    applicationProperties = decodeApplicationProperties(applicationProperty);
                    applicationPropertySize = sectionType.sizeof() + applicationProperties.sizeof();
                }
                else
                {
                    return decodeError(server, traceId, authorization, limit);
                }
            }
        }
        int reserved;
        boolean canSend = AmqpState.initialOpened(sender.state);
        if (sender.deliveryId != -1)
        {
            assert !transfer.hasDeliveryId() || sender.deliveryId == transfer.deliveryId();
            OctetsFW payload = payloadRO.tryWrap(buffer, sectionOffset, limit);
            reserved = payload.sizeof() + sender.initialPadding;
            canSend &= reserved <= sender.initialBudget;
            if (canSend && sender.debitorIndex != NO_DEBITOR_INDEX)
            {
                reserved = sender.debitor.claim(traceId, sender.debitorIndex, sender.initialId, reserved, reserved, 0);
            }
            if (canSend && reserved != 0)
            {
                server.onDecodeTransfer(traceId, authorization, transfer, annotations, properties, applicationProperties,
                    payload, 0);
                progress = transfer.limit() + payload.sizeof();
                if (!transfer.hasMore() || transfer.more() == 0)
                {
                    sender.deliveryId = -1;
                }
            }
        }
        else
        {
            AmqpValueHeaderFW payloadHeader = amqpValueHeaderRO.wrap(buffer, sectionOffset, limit);
            final long totalPayloadSize = payloadHeader.valueLength();
            OctetsFW payload = payloadRO.tryWrap(buffer, payloadHeader.limit(), limit);
            reserved = transfer.sizeof() + annotationSize + propertySize + applicationPropertySize + payloadHeader.sizeof() +
                payload.sizeof();
            canSend &= reserved <= sender.initialBudget;
            if (canSend && sender.debitorIndex != NO_DEBITOR_INDEX)
            {
                reserved = sender.debitor.claim(traceId, sender.debitorIndex, sender.initialId, payload.sizeof(),
                    payload.sizeof(), 0);
            }
            if (canSend && reserved != 0)
            {
                server.onDecodeTransfer(traceId, authorization, transfer, annotations, properties, applicationProperties,
                    payload, totalPayloadSize);
                progress = transfer.limit() + annotationSize + propertySize + applicationPropertySize +
                    payloadHeader.sizeof() + payload.sizeof();
            }
        }
        server.decoder = decodeFrameType;
        return progress;
    }

    private Array32FW<AmqpAnnotationFW> decodeAnnotations(
        AmqpMapFW<AmqpValueFW, AmqpValueFW> annotation)
    {
        Array32FW.Builder<AmqpAnnotationFW.Builder, AmqpAnnotationFW> annotationBuilder =
            annotationRW.wrap(frameBuffer, 0, frameBuffer.capacity());
        annotation.forEach(kv -> vv ->
        {
            BoundedOctetsFW value = vv.getAsAmqpBinary().get();
            switch (kv.kind())
            {
            case SYMBOL1:
                StringFW symbolKey = kv.getAsAmqpSymbol().get();
                annotationBuilder.item(b -> b.key(k -> k.name(symbolKey))
                    .value(vb -> vb.bytes(value.value(), 0, value.length())));
                break;
            case ULONG0:
            case ULONG1:
            case ULONG8:
                long longKey = kv.getAsAmqpULong().get();
                annotationBuilder.item(b -> b.key(k -> k.id(longKey))
                    .value(vb -> vb.bytes(value.value(), 0, value.length())));
                break;
            }
        });
        return annotationBuilder.build();
    }

    private AmqpPropertiesFW decodeProperties(
        AmqpMessagePropertiesFW property)
    {
        AmqpPropertiesFW.Builder propertyBuilder = propertyRW.wrap(frameBuffer, 0, frameBuffer.capacity());
        if (property.hasMessageId())
        {
            propertyBuilder.messageId(b -> b.stringtype(property.messageId().asString()));
        }
        if (property.hasUserId())
        {
            propertyBuilder.userId(b -> b.bytes(property.userId().value(), 0, property.userId().length()));
        }
        if (property.hasTo())
        {
            propertyBuilder.to((String8FW) property.to());
        }
        if (property.hasSubject())
        {
            propertyBuilder.subject((String8FW) property.subject());
        }
        if (property.hasReplyTo())
        {
            propertyBuilder.replyTo((String8FW) property.replyTo());
        }
        if (property.hasCorrelationId())
        {
            propertyBuilder.correlationId(b -> b.stringtype(property.correlationId().asString()));
        }
        if (property.hasContentType())
        {
            propertyBuilder.contentType((String8FW) property.contentType());
        }
        if (property.hasContentEncoding())
        {
            propertyBuilder.contentEncoding((String8FW) property.contentEncoding());
        }
        if (property.hasAbsoluteExpiryTime())
        {
            propertyBuilder.absoluteExpiryTime(property.absoluteExpiryTime());
        }
        if (property.hasCreationTime())
        {
            propertyBuilder.creationTime(property.creationTime());
        }
        if (property.hasGroupId())
        {
            propertyBuilder.groupId((String8FW) property.groupId());
        }
        if (property.hasGroupSequence())
        {
            propertyBuilder.groupSequence((int) property.groupSequence());
        }
        if (property.hasReplyToGroupId())
        {
            propertyBuilder.replyToGroupId((String8FW) property.replyToGroupId());
        }
        return propertyBuilder.build();
    }

    private Array32FW<AmqpApplicationPropertyFW> decodeApplicationProperties(
        AmqpMapFW<AmqpValueFW, AmqpValueFW> applicationProperty)
    {
        Array32FW.Builder<AmqpApplicationPropertyFW.Builder, AmqpApplicationPropertyFW> applicationPropertyBuilder =
            applicationPropertyRW.wrap(frameBuffer, 0, frameBuffer.capacity());
        applicationProperty.forEach(kv -> vv ->
        {
            String key = kv.getAsAmqpString().asString();
            String value = vv.getAsAmqpString().asString();
            // TODO: handle different type of values
            applicationPropertyBuilder.item(kb -> kb.key(key).value(value));
        });
        return applicationPropertyBuilder.build();
    }

    private int decodeDetach(
        AmqpServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        // final AmqpDetachFW detach = amqpDetachRO.tryWrap(buffer, offset, limit);

        int progress = offset;

        // TODO

        return progress;
    }

    private int decodeError(
        AmqpServer server,
        final long traceId,
        final long authorization,
        final int limit)
    {
        server.onDecodeError(traceId, authorization, DECODE_ERROR);
        server.decoder = decodeIgnoreAll;
        return limit;
    }

    private int decodeEnd(
        AmqpServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        final AmqpPerformativeFW performative = amqpPerformativeRO.wrap(buffer, offset, limit);
        final AmqpEndFW end = performative.end();
        assert end != null;

        server.onDecodeEnd(traceId, authorization, end);
        server.decoder = decodeFrameType;
        return end.limit();
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
        final AmqpPerformativeFW performative = amqpPerformativeRO.wrap(buffer, offset, limit);
        final AmqpCloseFW close = performative.close();
        assert close != null;

        server.onDecodeClose(traceId, authorization, close);
        server.decoder = decodeFrameType;
        return close.limit();
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
        private final AmqpAnnotationsAndProperties amqpAnnotationsAndProperties = new AmqpAnnotationsAndProperties();
        private final MessageConsumer network;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long affinity;
        private final long budgetId;
        private final long replySharedBudgetId;

        private final Int2ObjectHashMap<AmqpSession> sessions;

        private int initialBudget;
        private int initialPadding;
        private int replyBudget;
        private int replyPadding;

        private long replyBudgetIndex = NO_CREDITOR_INDEX;
        private int replySharedBudget;
        private int replyBudgetReserved;

        private int decodeSlot = NO_SLOT;
        private int decodeSlotOffset;
        private int decodeSlotReserved;

        private int encodeSlot = NO_SLOT;
        private int encodeSlotOffset;
        private long encodeSlotTraceId;
        private int encodeSlotMaxLimit = Integer.MAX_VALUE;

        private int incomingChannel;
        private int outgoingChannel;
        private long decodeMaxFrameSize = MIN_MAX_FRAME_SIZE;
        private long encodeMaxFrameSize = MIN_MAX_FRAME_SIZE;

        private AmqpServerDecoder decoder;

        private int state;

        private AmqpServer(
            MessageConsumer network,
            long routeId,
            long initialId,
            long affinity)
        {
            this.network = network;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.budgetId = supplyBudgetId.getAsLong();
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

            replyBudgetReserved += protocolHeader.sizeof() + replyPadding;
            doNetworkData(traceId, authorization, 0L, protocolHeader);
        }

        private void doEncodeOpen(
            long traceId,
            long authorization)
        {
            final AmqpOpenFW.Builder builder = amqpOpenRW.wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity())
                .containerId(containerId);

            if (decodeMaxFrameSize != DEFAULT_VALUE_MAX_FRAME_SIZE)
            {
                builder.maxFrameSize(decodeMaxFrameSize);
            }

            final AmqpOpenFW open = builder.build();

            final AmqpFrameHeaderFW frameHeader = amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                .size(FRAME_HEADER_SIZE + open.sizeof())
                .doff(2)
                .type(0)
                .channel(0)
                .performative(b -> b.open(open))
                .build();

            replyBudgetReserved += frameHeader.sizeof() + replyPadding;
            doNetworkData(traceId, authorization, 0L, frameHeader);
        }

        private void doEncodeBegin(
            long traceId,
            long authorization,
            int nextOutgoingId)
        {
            final AmqpBeginFW begin = amqpBeginRW.wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity())
                .remoteChannel(incomingChannel)
                .nextOutgoingId(nextOutgoingId)
                .incomingWindow(bufferPool.slotCapacity())
                .outgoingWindow(outgoingWindow)
                .build();

            final AmqpFrameHeaderFW frameHeader = amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                .size(FRAME_HEADER_SIZE + begin.sizeof())
                .doff(2)
                .type(0)
                .channel(outgoingChannel)
                .performative(b -> b.begin(begin))
                .build();

            replyBudgetReserved += frameHeader.sizeof() + replyPadding;
            doNetworkData(traceId, authorization, 0L, frameHeader);
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
            StringFW address,
            StringFW otherAddress)
        {
            AmqpAttachFW.Builder builder = amqpAttachRW.wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity())
                .name(amqpStringRW.wrap(stringBuffer, 0, stringBuffer.capacity()).set(name, UTF_8).build().get())
                .handle(handle)
                .role(role)
                .sndSettleMode(senderSettleMode)
                .rcvSettleMode(receiverSettleMode);

            switch (role)
            {
            case SENDER:
                AmqpSourceListFW sourceList = amqpSourceListRW
                    .wrap(extraBuffer, 0, extraBuffer.capacity())
                    .address(address)
                    .build();
                AmqpTargetListFW emptyList = amqpTargetListRW
                    .wrap(extraBuffer, sourceList.limit(), extraBuffer.capacity())
                    .build();

                builder.source(b -> b.sourceList(sourceList))
                    .target(b -> b.targetList(emptyList))
                    .initialDeliveryCount(initialDeliveryCount);
                break;
            case RECEIVER:
                int listOffset = 0;
                if (otherAddress != null)
                {
                    final AmqpSourceListFW source = amqpSourceListRW
                        .wrap(extraBuffer, 0, extraBuffer.capacity())
                        .address(otherAddress)
                        .build();
                    listOffset = source.limit();
                    builder.source(b -> b.sourceList(source));
                }

                AmqpTargetListFW targetList = amqpTargetListRW
                    .wrap(extraBuffer, listOffset, extraBuffer.capacity())
                    .address(address)
                    .build();
                builder.target(b -> b.targetList(targetList));
                break;
            }

            final AmqpAttachFW attach = builder.build();

            final AmqpFrameHeaderFW frameHeader = amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                .size(FRAME_HEADER_SIZE + attach.sizeof())
                .doff(2)
                .type(0)
                .channel(channel)
                .performative(b -> b.attach(attach))
                .build();

            replyBudgetReserved += frameHeader.sizeof() + replyPadding;
            doNetworkData(traceId, authorization, 0L, frameHeader);
        }

        private void doEncodeFlow(
            long traceId,
            long authorization,
            int channel,
            int nextOutgoingId,
            int nextIncomingId,
            long incomingWindow,
            long handle,
            long deliveryCount,
            int linkCredit)
        {
            final AmqpFlowFW flow = amqpFlowRW.wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity())
                .nextIncomingId(nextIncomingId)
                .incomingWindow(incomingWindow)
                .nextOutgoingId(nextOutgoingId)
                .outgoingWindow(outgoingWindow)
                .handle(handle)
                .deliveryCount(deliveryCount)
                .linkCredit(linkCredit)
                .build();

            final AmqpFrameHeaderFW frameHeader = amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                .size(FRAME_HEADER_SIZE + flow.sizeof())
                .doff(2)
                .type(0)
                .channel(channel)
                .performative(b -> b.flow(flow))
                .build();

            replyBudgetReserved += frameHeader.sizeof() + replyPadding;
            doNetworkData(traceId, authorization, 0L, frameHeader);
        }

        private int doEncodeTransfer(
            long traceId,
            long authorization,
            int channel,
            long handle,
            int flags,
            OctetsFW extension,
            OctetsFW payload)
        {
            final AmqpDataExFW dataEx = extension.get(amqpDataExRO::tryWrap);
            int transferFrames = 0;
            if (dataEx != null)
            {
                assert (flags & FLAG_INIT) != 0;
                final int deferred = dataEx.deferred();
                final int transferFlags = dataEx.flags();
                int extraBufferOffset = 0;
                final AmqpMapFW<AmqpValueFW, AmqpValueFW> annotations =
                    amqpAnnotationsAndProperties.annotations(dataEx.annotations(), extraBufferOffset);
                extraBufferOffset = annotations == null ? extraBufferOffset : annotations.limit();
                final AmqpMessagePropertiesFW properties = properties(dataEx, extraBufferOffset);
                extraBufferOffset = properties == null ? extraBufferOffset : properties.limit();
                final AmqpMapFW<AmqpValueFW, AmqpValueFW> applicationProperties =
                    amqpAnnotationsAndProperties.properties(dataEx, extraBufferOffset);
                extraBufferOffset = applicationProperties == null ? extraBufferOffset : applicationProperties.limit();
                int payloadIndex = 0;
                int payloadSize = payload.sizeof();
                int originalPayloadSize = payloadSize;
                AmqpValueHeaderFW valueHeader = amqpValueHeaderRW.wrap(extraBuffer, extraBufferOffset, extraBuffer.capacity())
                    .sectionType(b -> b.set(VALUE))
                    .valueType(b -> b.set(AmqpType.BINARY4))
                    .valueLength(deferred == 0 ? originalPayloadSize : originalPayloadSize + deferred)
                    .build();
                extraBufferOffset = valueHeader.limit();
                final BoundedOctetsFW deliveryTag =
                    amqpBinaryRW.wrap(extraBuffer, extraBufferOffset, extraBuffer.capacity())
                        .set(dataEx.deliveryTag().bytes().value(), 0, dataEx.deliveryTag().length())
                        .build()
                        .get();
                final int valueHeaderSize = valueHeader.sizeof();
                final int annotationsSize = annotations == null ? 0 : DESCRIBED_TYPE_SIZE + annotations.sizeof();
                final int propertiesSize = properties == null ? 0 : DESCRIBED_TYPE_SIZE + properties.sizeof();
                final int applicationPropertiesSize = applicationProperties == null ? 0 :
                    DESCRIBED_TYPE_SIZE + applicationProperties.sizeof();
                while (payloadIndex < originalPayloadSize)
                {
                    AmqpTransferFW.Builder builder = amqpTransferRW.wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity());
                    AmqpTransferFW transfer = payloadIndex > 0 ? builder.handle(handle).build() : builder.handle(handle)
                        .deliveryId(dataEx.deliveryId())
                        .deliveryTag(deliveryTag)
                        .messageFormat(dataEx.messageFormat())
                        .settled(isSettled(transferFlags) ? 1 : 0)
                        .build();
                    final int transferFrameSize = transfer.sizeof() + Byte.BYTES;
                    payloadRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity());
                    if (payloadIndex == 0)
                    {
                        if (FRAME_HEADER_SIZE + transferFrameSize + annotationsSize + propertiesSize +
                            applicationPropertiesSize + valueHeaderSize + payloadSize > encodeMaxFrameSize || flags == FLAG_INIT)
                        {
                            transfer = amqpTransferRW.wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity())
                                .handle(handle)
                                .deliveryId(dataEx.deliveryId())
                                .deliveryTag(deliveryTag)
                                .messageFormat(dataEx.messageFormat())
                                .settled(isSettled(transferFlags) ? 1 : 0)
                                .more(1)
                                .build();
                            if (flags == FLAG_INIT_AND_FIN)
                            {
                                payloadSize = (int) (encodeMaxFrameSize - FRAME_HEADER_SIZE - transferFrameSize -
                                    annotationsSize - propertiesSize - applicationPropertiesSize - valueHeaderSize);
                            }
                        }
                        AmqpFrameHeaderFW transferFrame = setTransferFrame(transfer, FRAME_HEADER_SIZE + transfer.sizeof() +
                            annotationsSize + propertiesSize + applicationPropertiesSize + valueHeaderSize + payloadSize,
                            channel);
                        int sectionOffset = transferFrame.limit();
                        amqpSection(annotations, properties, applicationProperties, sectionOffset);
                        payloadRW.put(valueHeader.buffer(), valueHeader.offset(), valueHeader.sizeof());
                    }
                    else
                    {
                        if (payloadIndex + (encodeMaxFrameSize - FRAME_HEADER_SIZE - transferFrameSize) > originalPayloadSize)
                        {
                            payloadSize = originalPayloadSize - payloadIndex;
                        }
                        else
                        {
                            transfer = amqpTransferRW.wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity())
                                .handle(handle)
                                .deliveryId(dataEx.deliveryId())
                                .deliveryTag(deliveryTag)
                                .messageFormat(dataEx.messageFormat())
                                .settled(isSettled(transferFlags) ? 1 : 0)
                                .more(1)
                                .build();
                            payloadSize = (int) (encodeMaxFrameSize - FRAME_HEADER_SIZE - transferFrameSize);
                        }
                        setTransferFrame(transfer, FRAME_HEADER_SIZE + transfer.sizeof() + payloadSize, channel);
                    }
                    payloadRW.put(payload.buffer(), payload.offset() + payloadIndex, payloadSize);
                    OctetsFW transferWithFrameHeader = payloadRW.build();
                    doNetworkData(traceId, authorization, 0L, transferWithFrameHeader);
                    payloadIndex += payloadSize;
                    transferFrames++;
                }
            }
            else
            {
                AmqpTransferFW.Builder transferBuilder = amqpTransferRW.wrap(frameBuffer, FRAME_HEADER_SIZE,
                    frameBuffer.capacity())
                    .handle(handle);
                AmqpTransferFW transfer = flags == FLAG_FIN ? transferBuilder.build() : transferBuilder.more(1).build();
                payloadRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity());
                setTransferFrame(transfer, FRAME_HEADER_SIZE + transfer.sizeof() + payload.sizeof(), channel);
                payloadRW.put(payload.buffer(), payload.offset(), payload.sizeof());
                OctetsFW transferWithFrameHeader = payloadRW.build();
                doNetworkData(traceId, authorization, 0L, transferWithFrameHeader);
                transferFrames++;
            }
            return transferFrames;
        }

        private void amqpSection(
            AmqpMapFW<AmqpValueFW, AmqpValueFW> annotations,
            AmqpMessagePropertiesFW properties,
            AmqpMapFW<AmqpValueFW, AmqpValueFW> applicationProperties,
            int sectionOffset)
        {
            if (annotations != null)
            {
                AmqpDescribedTypeFW messageAnnotationsType = amqpDescribedTypeRW.wrap(frameBuffer, sectionOffset,
                    frameBuffer.capacity())
                    .set(MESSAGE_ANNOTATIONS)
                    .build();
                payloadRW.put(messageAnnotationsType.buffer(), messageAnnotationsType.offset(),
                    messageAnnotationsType.sizeof())
                    .put(annotations.buffer(), annotations.offset(), annotations.sizeof());
                sectionOffset = messageAnnotationsType.limit();
            }
            if (properties != null)
            {
                AmqpDescribedTypeFW propertiesType = amqpDescribedTypeRW.wrap(frameBuffer, sectionOffset,
                    frameBuffer.capacity())
                    .set(PROPERTIES)
                    .build();
                payloadRW.put(propertiesType.buffer(), propertiesType.offset(), propertiesType.sizeof())
                    .put(properties.buffer(), properties.offset(), properties.sizeof());
                sectionOffset = propertiesType.limit();
            }
            if (applicationProperties != null)
            {
                AmqpDescribedTypeFW applicationPropertiesType = amqpDescribedTypeRW.wrap(frameBuffer, sectionOffset,
                    frameBuffer.capacity())
                    .set(APPLICATION_PROPERTIES)
                    .build();
                payloadRW.put(applicationPropertiesType.buffer(), applicationPropertiesType.offset(),
                    applicationPropertiesType.sizeof())
                    .put(applicationProperties.buffer(), applicationProperties.offset(), applicationProperties.sizeof());
            }
        }

        private AmqpFrameHeaderFW setTransferFrame(
            AmqpTransferFW transfer,
            long size,
            int channel)
        {
            AmqpFrameHeaderFW frameHeader = amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                .size(size)
                .doff(2)
                .type(0)
                .channel(channel)
                .performative(b -> b.transfer(transfer))
                .build();
            payloadRW.put(frameHeader.buffer(), frameHeader.offset(), frameHeader.sizeof());
            return frameHeader;
        }

        private AmqpMessagePropertiesFW properties(
            AmqpDataExFW dataEx,
            int bufferOffset)
        {
            AmqpMessagePropertiesFW properties = null;
            AmqpPropertiesFW propertiesEx = dataEx.properties();
            if (propertiesEx.fieldCount() > 0)
            {
                AmqpMessagePropertiesFW.Builder amqpProperties = amqpPropertiesRW.wrap(extraBuffer, bufferOffset,
                    extraBuffer.capacity());
                if (propertiesEx.hasMessageId())
                {
                    amqpProperties.messageId(propertiesEx.messageId().stringtype());
                }
                if (propertiesEx.hasUserId())
                {
                    final BoundedOctetsFW userId = amqpBinaryRW.wrap(stringBuffer, 0, stringBuffer.capacity())
                        .set(propertiesEx.userId().bytes().value(), 0, propertiesEx.userId().length())
                        .build()
                        .get();
                    amqpProperties.userId(userId);
                }
                if (propertiesEx.hasTo())
                {
                    amqpProperties.to(propertiesEx.to());
                }
                if (propertiesEx.hasSubject())
                {
                    amqpProperties.subject(propertiesEx.subject());
                }
                if (propertiesEx.hasReplyTo())
                {
                    amqpProperties.replyTo(propertiesEx.replyTo());
                }
                if (propertiesEx.hasCorrelationId())
                {
                    amqpProperties.correlationId(propertiesEx.correlationId().stringtype());
                }
                if (propertiesEx.hasContentType())
                {
                    amqpProperties.contentType(propertiesEx.contentType());
                }
                if (propertiesEx.hasContentEncoding())
                {
                    amqpProperties.contentEncoding(propertiesEx.contentEncoding());
                }
                if (propertiesEx.hasAbsoluteExpiryTime())
                {
                    amqpProperties.absoluteExpiryTime(propertiesEx.absoluteExpiryTime());
                }
                if (propertiesEx.hasCreationTime())
                {
                    amqpProperties.creationTime(propertiesEx.creationTime());
                }
                if (propertiesEx.hasGroupId())
                {
                    amqpProperties.groupId(propertiesEx.groupId());
                }
                if (propertiesEx.hasGroupSequence())
                {
                    amqpProperties.groupSequence(propertiesEx.groupSequence());
                }
                if (propertiesEx.hasReplyToGroupId())
                {
                    amqpProperties.replyToGroupId(propertiesEx.replyToGroupId());
                }
                properties = amqpProperties.build();
            }
            return properties;
        }

        private void doEncodeDetach(
            long traceId,
            long authorization,
            AmqpErrorType errorType,
            int channel,
            long handle)
        {
            final AmqpDetachFW.Builder detachRW = amqpDetachRW.wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity())
                .handle(handle)
                .closed(1);

            AmqpDetachFW detach;
            if (errorType != null)
            {
                AmqpErrorListFW errorList = amqpErrorListRW.wrap(extraBuffer, 0, extraBuffer.capacity())
                    .condition(errorType)
                    .build();
                detach = detachRW.error(e -> e.errorList(errorList)).build();
            }
            else
            {
                detach = detachRW.build();
            }

            final AmqpFrameHeaderFW frameHeader = amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                .size(FRAME_HEADER_SIZE + detach.sizeof())
                .doff(2)
                .type(0)
                .channel(channel)
                .performative(b -> b.detach(detach))
                .build();

            OctetsFW detachWithFrameHeader = payloadRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .put(frameHeader.buffer(), 0, frameHeader.sizeof())
                .build();

            doNetworkData(traceId, authorization, 0L, detachWithFrameHeader);
        }

        private void doEncodeEnd(
            long traceId,
            long authorization,
            int channel,
            AmqpErrorType errorType)
        {
            final AmqpEndFW.Builder builder = amqpEndRW.wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity());

            AmqpEndFW end;
            if (errorType != null)
            {
                AmqpErrorListFW errorList = amqpErrorListRW.wrap(extraBuffer, 0, extraBuffer.capacity())
                    .condition(errorType)
                    .build();

                end = builder.error(e -> e.errorList(errorList)).build();
            }
            else
            {
                end = builder.build();
            }

            final AmqpFrameHeaderFW frameHeader = amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                .size(FRAME_HEADER_SIZE + end.sizeof())
                .doff(2)
                .type(0)
                .channel(channel)
                .performative(b -> b.end(end))
                .build();

            replyBudgetReserved += frameHeader.sizeof() + replyPadding;
            doNetworkData(traceId, authorization, 0L, frameHeader);
        }

        private void doEncodeClose(
            long traceId,
            long authorization,
            AmqpErrorType errorType)
        {
            final AmqpCloseFW.Builder builder = amqpCloseRW.wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity());

            AmqpCloseFW close;
            if (errorType != null)
            {
                AmqpErrorListFW errorList = amqpErrorListRW.wrap(extraBuffer, 0, extraBuffer.capacity())
                    .condition(errorType)
                    .build();
                close = builder.error(e -> e.errorList(errorList)).build();
            }
            else
            {
                close = builder.build();
            }

            final AmqpFrameHeaderFW frameHeader = amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                .size(FRAME_HEADER_SIZE + close.sizeof())
                .doff(2)
                .type(0)
                .channel(0)
                .performative(b -> b.close(close))
                .build();

            replyBudgetReserved += frameHeader.sizeof() + replyPadding;
            doNetworkData(traceId, authorization, 0L, frameHeader);
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

            state = AmqpState.openingInitial(state);

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
                final long budgetId = data.budgetId();
                final OctetsFW payload = data.payload();

                DirectBuffer buffer = payload.buffer();
                int offset = payload.offset();
                int limit = payload.limit();
                int reserved = data.reserved();

                if (decodeSlot != NO_SLOT)
                {
                    final MutableDirectBuffer slotBuffer = bufferPool.buffer(decodeSlot);
                    slotBuffer.putBytes(decodeSlotOffset, buffer, offset, limit - offset);
                    decodeSlotOffset += limit - offset;
                    decodeSlotReserved += reserved;

                    buffer = slotBuffer;
                    offset = 0;
                    limit = decodeSlotOffset;
                    reserved = decodeSlotReserved;
                }

                decodeNetwork(traceId, authorization, budgetId, reserved, buffer, offset, limit);
            }
        }

        private void releaseBufferSlotIfNecessary()
        {
            if (decodeSlot != NO_SLOT)
            {
                bufferPool.release(decodeSlot);
                decodeSlot = NO_SLOT;
                decodeSlotOffset = 0;
            }
        }

        private void onNetworkEnd(
            EndFW end)
        {
            final long authorization = end.authorization();

            state = AmqpState.closeInitial(state);

            if (decodeSlot == NO_SLOT)
            {
                final long traceId = end.traceId();

                cleanupStreams(traceId, authorization);

                doNetworkEndIfNecessary(traceId, authorization);
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
            int credit = window.credit();
            final int padding = window.padding();

            state = AmqpState.openReply(state);

            replyBudget += credit;
            replyPadding += padding;

            if (replyBudgetReserved > 0)
            {
                final int reservedCredit = Math.min(credit, replyBudgetReserved);
                replyBudgetReserved -= reservedCredit;
            }

            if (encodeSlot != NO_SLOT)
            {
                final MutableDirectBuffer buffer = bufferPool.buffer(encodeSlot);
                final int limit = Math.min(encodeSlotOffset, encodeSlotMaxLimit);
                final int maxLimit = encodeSlotOffset;

                encodeNetwork(encodeSlotTraceId, authorization, budgetId, buffer, 0, limit, maxLimit);
            }

            flushReplySharedBudget(traceId);
        }

        private void flushReplySharedBudget(
            long traceId)
        {
            final int slotCapacity = bufferPool.slotCapacity();
            minimum.value = Integer.MAX_VALUE;
            sessions.values().forEach(s -> minimum.value = Math.min(s.remoteIncomingWindow, minimum.value));

            final int replySharedBudgetMax = sessions.values().size() > 0 ?
                (int) Math.min(minimum.value * encodeMaxFrameSize, replyBudget) : replyBudget;
            final int replySharedCredit = replySharedBudgetMax - Math.max(this.replySharedBudget, 0)
                - Math.max(encodeSlotOffset, 0);

            if (replySharedCredit > 0 && replyBudgetReserved == 0)
            {
                final long replySharedBudgetPrevious = creditor.credit(traceId, replyBudgetIndex, replySharedCredit);

                this.replySharedBudget += replySharedCredit;
                assert replySharedBudgetPrevious <= slotCapacity
                    : String.format("%d <= %d, replyBudget = %d",
                    replySharedBudgetPrevious, slotCapacity, replyBudget);

                assert replySharedBudget <= slotCapacity
                    : String.format("%d <= %d", replySharedBudget, slotCapacity);
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
            doEncodeClose(traceId, authorization, errorType);
            doNetworkEnd(traceId, authorization);
        }

        private void doNetworkBegin(
            long traceId,
            long authorization)
        {
            state = AmqpState.openingReply(state);

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
            state = AmqpState.closeReply(state);

            cleanupBudgetCreditorIfNecessary();
            cleanupEncodeSlotIfNecessary();

            doEnd(network, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doNetworkAbort(
            long traceId,
            long authorization)
        {
            state = AmqpState.closeReply(state);

            cleanupBudgetCreditorIfNecessary();
            cleanupEncodeSlotIfNecessary();

            doAbort(network, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doNetworkReset(
            long traceId,
            long authorization)
        {
            state = AmqpState.closeInitial(state);

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

            state = AmqpState.openInitial(state);

            initialBudget += credit;
            doWindow(network, routeId, initialId, traceId, authorization, budgetId, credit, padding, 0);
        }

        private void doNetworkSignal(
            long traceId)
        {
            doSignal(network, routeId, initialId, traceId);
        }

        private void decodeNetworkIfNecessary(
            long traceId)
        {
            if (decodeSlot != NO_SLOT)
            {
                final long authorization = 0L; // TODO
                final long budgetId = 0L; // TODO

                final DirectBuffer buffer = bufferPool.buffer(decodeSlot);
                final int offset = 0;
                final int limit = decodeSlotOffset;
                final int reserved = decodeSlotReserved;

                decodeNetwork(traceId, authorization, budgetId, reserved, buffer, offset, limit);

                final int initialCredit = reserved - decodeSlotReserved;
                if (initialCredit > 0)
                {
                    doNetworkWindow(traceId, authorization, initialCredit, 0, 0);
                }
            }
        }

        private void decodeNetwork(
            long traceId,
            long authorization,
            long budgetId,
            int reserved,
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
                    decodeSlotOffset = limit - progress;
                    decodeSlotReserved = (int)((long) reserved * (limit - progress) / (limit - offset));
                    slotBuffer.putBytes(0, buffer, progress, decodeSlotOffset);
                }
            }
            else
            {
                cleanupDecodeSlotIfNecessary();

                if (AmqpState.initialClosed(state))
                {
                    cleanupStreams(traceId, authorization);
                    doNetworkEndIfNecessary(traceId, authorization);
                }
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

        private void onDecodeOpen(
            long traceId,
            long authorization,
            AmqpOpenFW open)
        {
            // TODO: use buffer slot capacity instead
            this.encodeMaxFrameSize = Math.min(replySharedBudget, open.maxFrameSize());
            this.decodeMaxFrameSize = defaultMaxFrameSize;
            doEncodeOpen(traceId, authorization);
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
                AmqpSession session = sessions.computeIfAbsent(incomingChannel, AmqpSession::new);
                session.outgoingChannel(outgoingChannel);
                session.nextIncomingId((int) begin.nextOutgoingId());
                session.incomingWindow(writeBuffer.capacity());
                session.outgoingWindow(outgoingWindow);
                session.remoteIncomingWindow((int) begin.incomingWindow());
                session.remoteOutgoingWindow((int) begin.outgoingWindow());
                session.onDecodeBegin(traceId, authorization);
                this.outgoingChannel++;
            }
        }

        private void onDecodeAttach(
            long traceId,
            long authorization,
            AmqpAttachFW attach)
        {
            AmqpSession session = sessions.get(incomingChannel);
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
            AmqpSession session = sessions.get(incomingChannel);
            if (session != null)
            {
                session.onDecodeFlow(traceId, authorization, flow);
            }
            else
            {
                onDecodeError(traceId, authorization, NOT_ALLOWED);
            }
        }

        private void onDecodeTransfer(
            long traceId,
            long authorization,
            AmqpTransferFW transfer,
            Array32FW<AmqpAnnotationFW> annotations,
            AmqpPropertiesFW properties,
            Array32FW<AmqpApplicationPropertyFW> applicationProperties,
            OctetsFW payload,
            long totalPayloadSize)
        {
            AmqpSession session = sessions.get(incomingChannel);
            if (session != null)
            {
                // TODO: if incoming-window exceeded, we may need to remove the session from sessions map
                session.onDecodeTransfer(traceId, authorization, transfer, annotations, properties, applicationProperties,
                    payload, totalPayloadSize);
            }
            else
            {
                onDecodeError(traceId, authorization, NOT_ALLOWED);
            }
        }

        private void onDecodeEnd(
            long traceId,
            long authorization,
            AmqpEndFW end)
        {
            if (end.fieldCount() == 0)
            {
                AmqpSession session = sessions.get(incomingChannel);
                int channel = 0;
                if (session != null)
                {
                    session.cleanup(traceId, authorization);
                    sessions.remove(incomingChannel);
                    channel = session.outgoingChannel;
                }
                flushReplySharedBudget(traceId);
                doEncodeEnd(traceId, authorization, channel, null);
            }
            else
            {
                // TODO: detach with error
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
                doNetworkEndIfNecessary(traceId, authorization);
            }
        }

        private boolean isAmqpHeaderValid(
            AmqpProtocolHeaderFW header)
        {
            return PROTOCOL_HEADER == header.buffer().getLong(header.offset(), ByteOrder.BIG_ENDIAN);
        }

        private void cleanupNetwork(
            long traceId,
            long authorization)
        {
            cleanupStreams(traceId, authorization);

            doNetworkResetIfNecessary(traceId, authorization);
            doNetworkAbortIfNecessary(traceId, authorization);
        }

        private void cleanupStreams(
            long traceId,
            long authorization)
        {
            sessions.values().forEach(s -> s.cleanup(traceId, authorization));
        }

        private void doNetworkEndIfNecessary(
                long traceId,
                long authorization)
        {
            if (!AmqpState.replyClosed(state))
            {
                doNetworkEnd(traceId, authorization);
            }
        }

        private void doNetworkResetIfNecessary(
                long traceId,
                long authorization)
        {
            if (!AmqpState.initialClosed(state))
            {
                doNetworkReset(traceId, authorization);
            }
        }

        private void doNetworkAbortIfNecessary(
                long traceId,
                long authorization)
        {
            if (!AmqpState.replyClosed(state))
            {
                doNetworkAbort(traceId, authorization);
            }
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
                decodeSlotOffset = 0;
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

        private final class AmqpAnnotationsAndProperties
        {
            private final AmqpMapFW.Builder<AmqpValueFW, AmqpValueFW, AmqpValueFW.Builder, AmqpValueFW.Builder> annotationsRW =
                new AmqpMapFW.Builder<>(new AmqpValueFW(), new AmqpValueFW(), new AmqpValueFW.Builder(),
                    new AmqpValueFW.Builder());
            private final AmqpMapFW.Builder<AmqpValueFW, AmqpValueFW, AmqpValueFW.Builder, AmqpValueFW.Builder>
                applicationPropertyRW = new AmqpMapFW.Builder<>(new AmqpValueFW(), new AmqpValueFW(), new AmqpValueFW.Builder(),
                new AmqpValueFW.Builder());

            private int valueOffset;

            private AmqpMapFW<AmqpValueFW, AmqpValueFW> annotations(
                ArrayFW<AmqpAnnotationFW> annotations,
                int bufferOffset)
            {
                if (annotations.fieldCount() > 0)
                {
                    annotationsRW.wrap(extraBuffer, bufferOffset, extraBuffer.capacity());
                    annotations.forEach(this::annotation);
                    return annotationsRW.build();
                }
                return null;
            }

            private AmqpMapFW<AmqpValueFW, AmqpValueFW> properties(
                AmqpDataExFW dataEx,
                int bufferOffset)
            {
                ArrayFW<AmqpApplicationPropertyFW> applicationProperties = dataEx.applicationProperties();
                if (applicationProperties.fieldCount() > 0)
                {
                    applicationPropertyRW.wrap(extraBuffer, bufferOffset, extraBuffer.capacity());
                    applicationProperties.forEach(this::property);
                    return applicationPropertyRW.build();
                }
                return null;
            }

            private void annotation(
                AmqpAnnotationFW item)
            {
                switch (item.key().kind())
                {
                case KIND_ID:
                    AmqpULongFW id = amqpULongRW.wrap(valueBuffer, valueOffset, valueBuffer.capacity())
                        .set(item.key().id()).build();
                    valueOffset += id.sizeof();
                    AmqpByteFW value1 = amqpByteRW.wrap(valueBuffer, valueOffset, valueBuffer.capacity())
                        .set(item.value().bytes().value().getByte(0)).build();
                    valueOffset += value1.sizeof();
                    annotationsRW.entry(k -> k.setAsAmqpULong(id), v -> v.setAsAmqpByte(value1));
                    break;
                case KIND_NAME:
                    AmqpSymbolFW name = amqpSymbolRW.wrap(valueBuffer, valueOffset, valueBuffer.capacity())
                        .set(item.key().name()).build();
                    valueOffset += name.sizeof();
                    AmqpByteFW value2 = amqpByteRW.wrap(valueBuffer, valueOffset, valueBuffer.capacity())
                        .set(item.value().bytes().value().getByte(0)).build();
                    valueOffset += value2.sizeof();
                    annotationsRW.entry(k -> k.setAsAmqpSymbol(name), v -> v.setAsAmqpByte(value2));
                    break;
                }
            }

            private void property(
                AmqpApplicationPropertyFW item)
            {
                AmqpStringFW key = amqpStringRW.wrap(valueBuffer, valueOffset, valueBuffer.capacity())
                    .set(item.key()).build();
                valueOffset += key.sizeof();
                AmqpStringFW value = amqpValueRW.wrap(valueBuffer, valueOffset, valueBuffer.capacity())
                    .set(item.value()).build();
                valueOffset += value.sizeof();
                applicationPropertyRW.entry(k -> k.setAsAmqpString(key), v -> v.setAsAmqpString(value));
            }
        }

        private final class AmqpSession
        {
            private final Long2ObjectHashMap<AmqpServerStream> links;
            private final int incomingChannel;

            private int outgoingChannel;
            private int nextIncomingId;
            private int incomingWindow;
            private int nextOutgoingId;
            private int outgoingWindow;
            private int remoteIncomingWindow;
            private int remoteOutgoingWindow;

            private AmqpSession(
                int incomingChannel)
            {
                this.links = new Long2ObjectHashMap<>();
                this.incomingChannel = incomingChannel;
                this.nextOutgoingId++;
            }

            private void outgoingChannel(
                int outgoingChannel)
            {
                this.outgoingChannel = outgoingChannel;
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
                doEncodeBegin(traceId, authorization, nextOutgoingId);
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
                    AmqpRole role = attach.role();
                    boolean hasSourceAddress = attach.hasSource() && attach.source().sourceList().hasAddress();
                    boolean hasTargetAddress = attach.hasTarget() && attach.target().targetList().hasAddress();
                    AmqpSourceListFW source = attach.hasSource() ? attach.source().sourceList() : null;
                    AmqpTargetListFW target = attach.hasTarget() ? attach.target().targetList() : null;

                    final RouteFW route;
                    switch (role)
                    {
                    case RECEIVER:
                        route = resolveTarget(routeId, authorization, hasSourceAddress ? source.address() : null,
                            AmqpCapabilities.RECEIVE_ONLY);
                        break;
                    case SENDER:
                        route = resolveTarget(routeId, authorization, hasTargetAddress ? target.address() : null,
                            AmqpCapabilities.SEND_ONLY);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + role);
                    }
                    if (route != null)
                    {
                        StringFW addressFrom = null;
                        StringFW addressTo = null;
                        if (source != null)
                        {
                            addressFrom = source.address();
                        }
                        if (target != null)
                        {
                            addressTo = target.address();
                        }

                        AmqpServerStream link = new AmqpServerStream(addressFrom, addressTo, role, route);
                        AmqpServerStream oldLink = links.put(linkKey, link);
                        assert oldLink == null;
                        link.onDecodeAttach(traceId, authorization, attach);
                    }
                    else
                    {
                        // TODO: reject
                    }
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
                assert flow.hasHandle() == flow.hasDeliveryCount();
                assert flow.hasHandle() == flow.hasLinkCredit();

                this.nextIncomingId = flowNextOutgoingId;
                this.remoteIncomingWindow = flowNextIncomingId + flowIncomingWindow - nextOutgoingId;
                this.remoteOutgoingWindow = flowOutgoingWindow;

                flushReplySharedBudget(traceId);

                if (flow.hasHandle())
                {
                    AmqpServerStream attachedLink = links.get(flow.handle());
                    attachedLink.onDecodeFlow(traceId, authorization, flow.deliveryCount(), (int) flow.linkCredit());
                }
            }

            private void onDecodeTransfer(
                long traceId,
                long authorization,
                AmqpTransferFW transfer,
                Array32FW<AmqpAnnotationFW> annotations,
                AmqpPropertiesFW properties,
                Array32FW<AmqpApplicationPropertyFW> applicationProperties,
                OctetsFW payload,
                long totalPayloadSize)
            {
                this.nextIncomingId++;
                this.remoteOutgoingWindow--;
                this.incomingWindow--;

                final long deliveryId = transfer.hasDeliveryId() ? transfer.deliveryId() : 0;
                final BoundedOctetsFW deliveryTag = transfer.hasDeliveryTag() ? transfer.deliveryTag() : null;
                final long messageFormat = transfer.hasMessageFormat() ? transfer.messageFormat() : 0;
                int flags = 0;
                if (transfer.hasSettled() && transfer.settled() == 1)
                {
                    flags = settled(flags);
                }
                if (transfer.hasResume() && transfer.resume() == 1)
                {
                    flags = resume(flags);
                }
                if (transfer.hasAborted() && transfer.aborted() == 1)
                {
                    flags = aborted(flags);
                }
                if (transfer.hasBatchable() && transfer.batchable() == 1)
                {
                    flags = batchable(flags);
                }
                AmqpServerStream link = links.get(transfer.handle());
                if (link.deliveryId == -1)
                {
                    link.linkCredit--;
                    // TODO: case where linkCredit become negative
                }
                link.onDecodeTransfer(traceId, authorization, deliveryId, deliveryTag, messageFormat, flags,
                    annotations, properties, applicationProperties, payload.sizeof(), payload, totalPayloadSize);
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
                private long deliveryId = -1;
                private long deliveryCount;
                private int linkCredit;

                private BudgetDebitor debitor;
                private long debitorIndex = NO_DEBITOR_INDEX;

                private int initialBudget;
                private int initialPadding;

                private String name;
                private long handle;
                private AmqpRole role;
                private StringFW addressFrom;
                private StringFW addressTo;

                private int state;
                private int capabilities;

                AmqpServerStream(
                    StringFW addressFrom,
                    StringFW addressTo,
                    AmqpRole role,
                    RouteFW route)
                {
                    this.addressFrom = addressFrom;
                    this.addressTo = addressTo;
                    this.role = role;
                    this.capabilities = 0;
                    this.newRouteId = route.correlationId();
                    this.initialId = supplyInitialId.applyAsLong(newRouteId);
                    this.replyId = supplyReplyId.applyAsLong(initialId);
                    this.application = router.supplyReceiver(initialId);
                }

                private void onDecodeAttach(
                    long traceId,
                    long authorization,
                    AmqpAttachFW attach)
                {
                    this.name = attach.name().asString();
                    this.handle = attach.handle();

                    final AmqpCapabilities capability = amqpCapabilities(role);
                    final AmqpSenderSettleMode amqpSenderSettleMode = attach.sndSettleMode();
                    final AmqpReceiverSettleMode amqpReceiverSettleMode = attach.rcvSettleMode();

                    doApplicationBeginIfNecessary(traceId, authorization, affinity, capability, amqpSenderSettleMode,
                        amqpReceiverSettleMode);

                    correlations.put(replyId, this::onApplication);
                }

                private void onDecodeFlow(
                    long traceId,
                    long authorization,
                    long deliveryCount,
                    int linkCredit)
                {
                    this.linkCredit = (int) (deliveryCount + linkCredit - this.deliveryCount);
                    this.deliveryCount = deliveryCount;
                    flushReplyWindow(traceId, authorization);
                }

                private void onDecodeTransfer(
                    long traceId,
                    long authorization,
                    long deliveryId,
                    BoundedOctetsFW deliveryTag,
                    long messageFormat,
                    int flags,
                    Array32FW<AmqpAnnotationFW> annotations,
                    AmqpPropertiesFW properties,
                    Array32FW<AmqpApplicationPropertyFW> applicationProperties,
                    int reserved,
                    OctetsFW payload,
                    long totalPayloadSize)
                {
                    if (totalPayloadSize > 0)
                    {
                        final AmqpDataExFW.Builder amqpDataEx = amqpDataExRW.wrap(extraBuffer, 0, extraBuffer.capacity())
                            .typeId(amqpTypeId)
                            .deferred(reserved < totalPayloadSize ? (int) (totalPayloadSize - reserved) : 0)
                            .deliveryId(deliveryId)
                            .deliveryTag(b -> b.bytes(deliveryTag.get(deliveryTagRO::tryWrap)))
                            .messageFormat(messageFormat)
                            .flags(flags);
                        if (annotations != null)
                        {
                            amqpDataEx.annotations(annotations);
                        }
                        if (properties != null)
                        {
                            amqpDataEx.properties(properties);
                        }
                        if (applicationProperties != null)
                        {
                            amqpDataEx.applicationProperties(applicationProperties);
                        }
                        final AmqpDataExFW dataEx = amqpDataEx.build();
                        doApplicationData(traceId, authorization, reserved, payload, dataEx);
                    }
                    else
                    {
                        doApplicationData(traceId, authorization, reserved, payload, EMPTY_OCTETS);
                    }
                    this.deliveryId = deliveryId;
                }

                private void onDecodeError(
                    long traceId,
                    long authorization,
                    AmqpErrorType errorType)
                {
                    doEncodeDetach(traceId, authorization, errorType, outgoingChannel, handle);
                }

                private void doApplicationBeginIfNecessary(
                    long traceId,
                    long authorization,
                    long affinity,
                    AmqpCapabilities capability,
                    AmqpSenderSettleMode senderSettleMode,
                    AmqpReceiverSettleMode receiverSettleMode)
                {
                    final int newCapabilities = capabilities | capability.value();
                    if (!AmqpState.initialOpening(state))
                    {
                        this.capabilities = newCapabilities;
                        doApplicationBegin(traceId, authorization, affinity, senderSettleMode,
                            receiverSettleMode);
                    }
                }

                private void doApplicationBegin(
                    long traceId,
                    long authorization,
                    long affinity,
                    AmqpSenderSettleMode senderSettleMode,
                    AmqpReceiverSettleMode receiverSettleMode)
                {
                    assert state == 0;
                    state = AmqpState.openingInitial(state);

                    router.setThrottle(initialId, this::onApplication);

                    StringFW address = null;
                    switch (role)
                    {
                    case RECEIVER:
                        address = addressFrom;
                        break;
                    case SENDER:
                        address = addressTo;
                    }
                    final AmqpBeginExFW beginEx = amqpBeginExRW.wrap(extraBuffer, 0, extraBuffer.capacity())
                        .typeId(amqpTypeId)
                        .address((String8FW) address)
                        .capabilities(r -> r.set(AmqpCapabilities.valueOf(capabilities)))
                        .senderSettleMode(s -> s.set(amqpSenderSettleMode(senderSettleMode)))
                        .receiverSettleMode(r -> r.set(amqpReceiverSettleMode(receiverSettleMode)))
                        .build();

                    doBegin(application, newRouteId, initialId, traceId, authorization, affinity, beginEx);
                }

                private void doApplicationData(
                    long traceId,
                    long authorization,
                    int reserved,
                    OctetsFW payload,
                    Flyweight extension)
                {
                    assert AmqpState.initialOpening(state);

                    final DirectBuffer buffer = payload.buffer();
                    final int offset = payload.offset();
                    final int limit = payload.limit();
                    final int length = limit - offset;
                    assert reserved >= length + initialPadding;

                    this.initialBudget -= reserved;

                    assert initialBudget >= 0;

                    doData(application, newRouteId, initialId, traceId, authorization, replySharedBudgetId, reserved, buffer,
                        offset, length, extension);
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

                    state = AmqpState.closeInitial(state);

                    if (debitorIndex != NO_DEBITOR_INDEX)
                    {
                        debitor.release(debitorIndex, initialId);
                        debitorIndex = NO_DEBITOR_INDEX;
                    }

                    if (AmqpState.closed(state))
                    {
                        capabilities = 0;
                        links.remove(handle);
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

                    this.state = AmqpState.openInitial(state);
                    this.budgetId = budgetId;
                    this.initialBudget += credit;
                    this.initialPadding = padding;

                    if (budgetId != 0L && debitorIndex == NO_DEBITOR_INDEX)
                    {
                        debitor = supplyDebitor.apply(budgetId);
                        debitorIndex = debitor.acquire(budgetId, initialId, AmqpServer.this::decodeNetworkIfNecessary);
                    }

                    if (isReplyOpen())
                    {
                        this.linkCredit = (int) (Math.min(bufferPool.slotCapacity(), initialBudget) /
                            Math.min(bufferPool.slotCapacity(), decodeMaxFrameSize));
                        minimum.value = Integer.MAX_VALUE;
                        links.values().forEach(l -> minimum.value = l.linkCredit == 0 ? minimum.value :
                            Math.min(l.linkCredit, minimum.value));
                        incomingWindow = minimum.value == Integer.MAX_VALUE ? 0 : minimum.value;

                        doEncodeFlow(traceId, authorization, outgoingChannel, nextOutgoingId, nextIncomingId, incomingWindow,
                            handle, deliveryCount, linkCredit);
                    }

                    if (AmqpState.initialClosing(state) && !AmqpState.initialClosed(state))
                    {
                        doApplicationEnd(traceId, authorization, EMPTY_OCTETS);
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
                    state = AmqpState.openReply(state);

                    final long traceId = begin.traceId();
                    final long authorization = begin.authorization();

                    final AmqpBeginExFW amqpBeginEx = begin.extension().get(amqpBeginExRO::tryWrap);
                    if (amqpBeginEx != null)
                    {
                        AmqpRole amqpRole = amqpRole(amqpBeginEx.capabilities().get());
                        AmqpSenderSettleMode amqpSenderSettleMode = amqpSenderSettleMode(amqpBeginEx.senderSettleMode().get());
                        AmqpReceiverSettleMode amqpReceiverSettleMode =
                            amqpReceiverSettleMode(amqpBeginEx.receiverSettleMode().get());
                        final StringFW address = amqpBeginEx.address();
                        StringFW otherAddress = null;
                        switch (amqpRole)
                        {
                        case SENDER:
                            otherAddress = addressTo;
                            break;
                        case RECEIVER:
                            otherAddress = addressFrom;
                            break;
                        }

                        deliveryCount = initialDeliveryCount;
                        doEncodeAttach(traceId, authorization, name, outgoingChannel, handle, amqpRole, amqpSenderSettleMode,
                            amqpReceiverSettleMode, address, otherAddress);

                        if (amqpRole == RECEIVER)
                        {
                            this.linkCredit = (int) (Math.min(bufferPool.slotCapacity(), initialBudget) /
                                Math.min(bufferPool.slotCapacity(), decodeMaxFrameSize));
                            minimum.value = Integer.MAX_VALUE;
                            links.values().forEach(l -> minimum.value = l.linkCredit == 0 ? minimum.value :
                                Math.min(l.linkCredit, minimum.value));
                            incomingWindow = minimum.value == Integer.MAX_VALUE ? 0 : minimum.value;

                            sessions.values().forEach(s -> minimum.value = Math.min(s.remoteIncomingWindow, minimum.value));
                            doEncodeFlow(traceId, authorization, outgoingChannel, nextOutgoingId, nextIncomingId, incomingWindow,
                                handle, deliveryCount, linkCredit);
                        }
                    }
                    else
                    {
                        AmqpRole amqpRole = role == RECEIVER ? SENDER : RECEIVER;
                        doEncodeAttach(traceId, authorization, name, outgoingChannel, handle, amqpRole, MIXED, FIRST, addressFrom,
                            addressTo);
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
                    replySharedBudget -= reserved;

                    if (replyBudget < 0)
                    {
                        doApplicationReset(traceId, authorization);
                        doNetworkAbort(traceId, authorization);
                    }

                    nextOutgoingId++;
                    outgoingWindow--;
                    if ((flags & FLAG_FIN) == 1)
                    {
                        deliveryCount++;
                        linkCredit--;
                    }
                    remoteIncomingWindow -= doEncodeTransfer(traceId, authorization, outgoingChannel, handle, flags, extension,
                        data.payload());
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

                private void doApplicationEnd(
                    long traceId,
                    long authorization,
                    Flyweight extension)
                {
                    setInitialClosed();
                    capabilities = 0;
                    links.remove(handle);

                    doEnd(application, newRouteId, initialId, traceId, authorization, extension);
                }

                private void flushReplyWindow(
                    long traceId,
                    long authorization)
                {
                    if (isReplyOpen())
                    {
                        final int maxFrameSize = (int) encodeMaxFrameSize;
                        final int slotCapacity = bufferPool.slotCapacity();
                        final int maxFrameCount = (slotCapacity + maxFrameSize - 1) / maxFrameSize;
                        final int padding = PAYLOAD_HEADER_SIZE + (TRANSFER_HEADER_SIZE * maxFrameCount);
                        final int newReplyBudget = (int) (linkCredit * encodeMaxFrameSize);
                        final int credit = newReplyBudget - replyBudget;
                        if (credit > 0)
                        {
                            replyBudget += credit;
                            doWindow(application, newRouteId, replyId, traceId, authorization, replySharedBudgetId, credit,
                                padding, maxFrameSize);
                        }
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

                    state = AmqpState.closeReply(state);

                    if (AmqpState.closed(state))
                    {
                        capabilities = 0;
                        links.remove(handle);
                    }
                }

                private void cleanup(
                    long traceId,
                    long authorization)
                {
                    doApplicationAbortIfNecessary(traceId, authorization);
                    doApplicationResetIfNecessary(traceId, authorization);
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
