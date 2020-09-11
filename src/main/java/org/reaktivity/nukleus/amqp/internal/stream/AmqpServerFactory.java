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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpTransferFlags.aborted;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpTransferFlags.batchable;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpTransferFlags.isSettled;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpTransferFlags.resume;
import static org.reaktivity.nukleus.amqp.internal.stream.AmqpTransferFlags.settled;
import static org.reaktivity.nukleus.amqp.internal.types.AmqpAnnotationKeyFW.KIND_ID;
import static org.reaktivity.nukleus.amqp.internal.types.AmqpAnnotationKeyFW.KIND_NAME;
import static org.reaktivity.nukleus.amqp.internal.types.AmqpCapabilities.RECEIVE_ONLY;
import static org.reaktivity.nukleus.amqp.internal.types.AmqpCapabilities.SEND_AND_RECEIVE;
import static org.reaktivity.nukleus.amqp.internal.types.AmqpCapabilities.SEND_ONLY;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.APPLICATION_PROPERTIES;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.ATTACH;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.BEGIN;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.CLOSE;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.DATA;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.DETACH;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.END;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.FLOW;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.MESSAGE_ANNOTATIONS;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.OPEN;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.PROPERTIES;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.SEQUENCE;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.TRANSFER;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType.VALUE;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorType.CONNECTION_FRAMING_ERROR;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorType.DECODE_ERROR;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorType.LINK_DETACH_FORCED;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorType.LINK_TRANSFER_LIMIT_EXCEEDED;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorType.NOT_ALLOWED;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorType.RESOURCE_LIMIT_EXCEEDED;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorType.SESSION_WINDOW_VIOLATION;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpOpenFW.DEFAULT_VALUE_MAX_FRAME_SIZE;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpReceiverSettleMode.FIRST;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpRole.RECEIVER;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpRole.SENDER;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpSenderSettleMode.MIXED;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpType.BINARY1;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpType.BINARY4;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpType.STRING1;
import static org.reaktivity.nukleus.amqp.internal.types.codec.AmqpType.STRING4;
import static org.reaktivity.nukleus.amqp.internal.util.AmqpTypeUtil.amqpCapabilities;
import static org.reaktivity.nukleus.amqp.internal.util.AmqpTypeUtil.amqpReceiverSettleMode;
import static org.reaktivity.nukleus.amqp.internal.util.AmqpTypeUtil.amqpSenderSettleMode;
import static org.reaktivity.nukleus.budget.BudgetCreditor.NO_CREDITOR_INDEX;
import static org.reaktivity.nukleus.budget.BudgetDebitor.NO_DEBITOR_INDEX;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.concurrent.Signaler.NO_CANCEL_ID;

import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.Map;
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
import org.reaktivity.nukleus.amqp.internal.types.AmqpAnnotationKeyFW;
import org.reaktivity.nukleus.amqp.internal.types.AmqpApplicationPropertyFW;
import org.reaktivity.nukleus.amqp.internal.types.AmqpBodyKind;
import org.reaktivity.nukleus.amqp.internal.types.AmqpCapabilities;
import org.reaktivity.nukleus.amqp.internal.types.AmqpPropertiesFW;
import org.reaktivity.nukleus.amqp.internal.types.Array32FW;
import org.reaktivity.nukleus.amqp.internal.types.BoundedOctetsFW;
import org.reaktivity.nukleus.amqp.internal.types.Flyweight;
import org.reaktivity.nukleus.amqp.internal.types.OctetsFW;
import org.reaktivity.nukleus.amqp.internal.types.String8FW;
import org.reaktivity.nukleus.amqp.internal.types.StringFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpAttachFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpBeginFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpBinaryFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpCloseFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedType;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDescribedTypeFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpDetachFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpEndFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorListFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpErrorType;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpFlowFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpFrameHeaderFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpHeaderFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpMapFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpMessagePropertiesFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpOpenFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpPerformativeFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpProtocolHeaderFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpReceiverSettleMode;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpRole;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpSectionType;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpSectionTypeFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpSenderSettleMode;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpSourceFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpSourceListFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpStringFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpSymbolFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpTargetFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpTargetListFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpTransferFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpType;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpULongFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpValueFW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpVariableLength32FW;
import org.reaktivity.nukleus.amqp.internal.types.codec.AmqpVariableLength8FW;
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
import org.reaktivity.nukleus.concurrent.Signaler;
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
    private static final int TRANSFER_HEADER_SIZE = 20;
    private static final int PAYLOAD_HEADER_SIZE = 205;
    private static final int NO_DELIVERY_ID = -1;
    private static final long PROTOCOL_HEADER = 0x414D5150_00010000L;
    private static final long DEFAULT_IDLE_TIMEOUT = 0;
    private static final int DECODE_IDLE_TIMEOUT_ID = 0;
    private static final int ENCODE_IDLE_TIMEOUT_ID = 1;

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

    private final OctetsFW.Builder payloadRW = new OctetsFW.Builder();
    private final OctetsFW.Builder messageFragmentRW = new OctetsFW.Builder();

    private final OctetsFW payloadRO = new OctetsFW();

    private final AmqpProtocolHeaderFW amqpProtocolHeaderRO = new AmqpProtocolHeaderFW();
    private final AmqpFrameHeaderFW amqpFrameHeaderRO = new AmqpFrameHeaderFW();
    private final AmqpPerformativeFW amqpPerformativeRO = new AmqpPerformativeFW();
    private final AmqpRouteExFW routeExRO = new AmqpRouteExFW();
    private final AmqpHeaderFW headersRO = new AmqpHeaderFW();
    private final AmqpMapFW<AmqpValueFW, AmqpValueFW> deliveryAnnotationsRO =
        new AmqpMapFW<>(new AmqpValueFW(), new AmqpValueFW());
    private final AmqpMapFW<AmqpValueFW, AmqpValueFW> annotationsRO = new AmqpMapFW<>(new AmqpValueFW(), new AmqpValueFW());
    private final OctetsFW deliveryTagRO = new OctetsFW();
    private final AmqpMessagePropertiesFW amqpPropertiesRO = new AmqpMessagePropertiesFW();
    private final AmqpMapFW<AmqpValueFW, AmqpValueFW> applicationPropertyRO =
        new AmqpMapFW<>(new AmqpValueFW(), new AmqpValueFW());
    private final AmqpMapFW<AmqpValueFW, AmqpValueFW> footerRO = new AmqpMapFW<>(new AmqpValueFW(), new AmqpValueFW());
    private final AmqpSectionTypeFW amqpSectionTypeRO = new AmqpSectionTypeFW();
    private final AmqpValueFW amqpValueRO = new AmqpValueFW();

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
    private final AmqpStringFW.Builder amqpStringRW = new AmqpStringFW.Builder();
    private final AmqpStringFW.Builder amqpValueRW = new AmqpStringFW.Builder();
    private final AmqpSymbolFW.Builder amqpSymbolRW = new AmqpSymbolFW.Builder();
    private final AmqpSourceListFW.Builder amqpSourceListRW = new AmqpSourceListFW.Builder();
    private final AmqpTargetListFW.Builder amqpTargetListRW = new AmqpTargetListFW.Builder();
    private final AmqpBinaryFW.Builder amqpBinaryRW = new AmqpBinaryFW.Builder();
    private final AmqpULongFW.Builder amqpULongRW = new AmqpULongFW.Builder();
    private final AmqpVariableLength8FW.Builder amqpVariableLength8RW = new AmqpVariableLength8FW.Builder();
    private final AmqpVariableLength32FW.Builder amqpVariableLength32RW = new AmqpVariableLength32FW.Builder();
    private final AmqpDescribedTypeFW.Builder amqpDescribedTypeRW = new AmqpDescribedTypeFW.Builder();
    private final AmqpMessagePropertiesFW.Builder amqpPropertiesRW = new AmqpMessagePropertiesFW.Builder();
    private final Array32FW.Builder<AmqpAnnotationFW.Builder, AmqpAnnotationFW> annotationRW =
        new Array32FW.Builder<>(new AmqpAnnotationFW.Builder(), new AmqpAnnotationFW());
    private final AmqpPropertiesFW.Builder propertyRW = new AmqpPropertiesFW.Builder();
    private final Array32FW.Builder<AmqpApplicationPropertyFW.Builder, AmqpApplicationPropertyFW> applicationPropertyRW =
        new Array32FW.Builder<>(new AmqpApplicationPropertyFW.Builder(), new AmqpApplicationPropertyFW());

    private final AmqpDescribedTypeFW applicationPropertiesSectionType = new AmqpDescribedTypeFW.Builder()
        .wrap(new UnsafeBuffer(new byte[3]), 0, 3)
        .set(APPLICATION_PROPERTIES)
        .build();

    private final AmqpDescribedTypeFW messagePropertiesSectionType = new AmqpDescribedTypeFW.Builder()
        .wrap(new UnsafeBuffer(new byte[3]), 0, 3)
        .set(PROPERTIES)
        .build();

    private final AmqpDescribedTypeFW messageAnnotationsSectionType = new AmqpDescribedTypeFW.Builder()
        .wrap(new UnsafeBuffer(new byte[3]), 0, 3)
        .set(MESSAGE_ANNOTATIONS)
        .build();

    private final AmqpDescribedTypeFW dataSectionType = new AmqpDescribedTypeFW.Builder()
        .wrap(new UnsafeBuffer(new byte[3]), 0, 3)
        .set(DATA)
        .build();

    private final AmqpDescribedTypeFW sequenceSectionType = new AmqpDescribedTypeFW.Builder()
        .wrap(new UnsafeBuffer(new byte[3]), 0, 3)
        .set(SEQUENCE)
        .build();

    private final AmqpDescribedTypeFW valueSectionType = new AmqpDescribedTypeFW.Builder()
        .wrap(new UnsafeBuffer(new byte[3]), 0, 3)
        .set(VALUE)
        .build();

    private final OctetsFW nullConstructor = new OctetsFW()
        .wrap(new UnsafeBuffer(new byte[] {0x40}), 0, 1);


    private final OctetsFW emptyFrameHeader = new OctetsFW()
        .wrap(new UnsafeBuffer(new byte[] {0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00}), 0, 8);

    private final StringFW timeoutDescription = new String8FW("idle-timeout expired");

    private final AmqpMessageEncoder amqpMessageHelper = new AmqpMessageEncoder();
    private final AmqpMessageDecoder amqpMessageDecodeHelper = new AmqpMessageDecoder();

    private final MutableInteger minimum = new MutableInteger(Integer.MAX_VALUE);
    private final MutableInteger maximum = new MutableInteger(0);

    private final Signaler signaler;

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
    private final AmqpServerDecoder decodeProtocolHeader = this::decodeProtocolHeader;
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
    private final long defaultIdleTimeout;

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
        LongFunction<BudgetDebitor> supplyDebitor,
        Signaler signaler)
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
        this.defaultIdleTimeout = config.idleTimeout();
        this.initialDeliveryCount = config.initialDeliveryCount();
        this.signaler = signaler;
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

    private RouteFW resolveRoute(
        long routeId,
        long authorization,
        StringFW address,
        AmqpCapabilities capabilities)
    {
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, o + l);
            final OctetsFW extension = route.extension();
            final AmqpRouteExFW routeEx = extension.get(routeExRO::tryWrap);

            if (routeEx != null && address != null)
            {
                final String addressEx = routeEx.address().asString();
                final AmqpCapabilities routeCapabilities = routeEx.capabilities().get();

                return address.asString().equals(addressEx) &&
                        (capabilities == SEND_AND_RECEIVE ?
                                (capabilities.value() & routeCapabilities.value()) == SEND_AND_RECEIVE.value() :
                                (capabilities.value() & routeCapabilities.value()) != 0);

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
        int flags,
        long budgetId,
        int reserved,
        Flyweight payload,
        Flyweight extension)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .routeId(routeId)
            .streamId(replyId)
            .traceId(traceId)
            .authorization(authorization)
            .flags(flags)
            .budgetId(budgetId)
            .reserved(reserved)
            .payload(payload != null ? payloadRO.wrap(payload.buffer(), payload.offset(), payload.limit()) : null)
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

    @FunctionalInterface
    private interface AmqpSectionDecoder
    {
        int decode(
            AmqpServer.AmqpSession.AmqpServerStream stream,
            DirectBuffer buffer,
            int offset,
            int limit);
    }

    @FunctionalInterface
    private interface AmqpSectionEncoder
    {
        int encode(
            int deferred,
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
        final int length = limit - offset;

        int progress = offset;

        decode:
        if (length != 0)
        {
            final AmqpFrameHeaderFW frameHeader = amqpFrameHeaderRO.tryWrap(buffer, offset, limit);
            if (frameHeader == null)
            {
                progress = server.onDecodeEmptyFrame(buffer, offset, limit);
                break decode;
            }

            final long frameSize = frameHeader.size();

            if (frameSize > server.decodeMaxFrameSize)
            {
                server.onDecodeError(traceId, authorization, CONNECTION_FRAMING_ERROR, null);
                server.decoder = decodeFrameType;
                progress = limit;
                break decode;
            }

            if (length < frameSize)
            {
                break decode;
            }

            final AmqpPerformativeFW performative = frameHeader.performative();
            final AmqpDescribedType descriptor = performative.kind();
            final AmqpServerDecoder decoder = decodersByPerformative.getOrDefault(descriptor, decodeUnknownType);
            server.decodeChannel = frameHeader.channel();
            server.decodableBodyBytes = frameSize - frameHeader.doff() * 4;
            server.decoder = decoder;
            progress = performative.offset();
        }

        return progress;
    }

    private int decodeProtocolHeader(
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
        assert performative.kind() == AmqpPerformativeFW.KIND_TRANSFER;

        final AmqpTransferFW transfer = performative.transfer();
        final long deliveryId = transfer.hasDeliveryId() ? transfer.deliveryId() : NO_DELIVERY_ID;
        final long handle = transfer.handle();

        int progress = offset;
        decode:
        {
            AmqpServer.AmqpSession session = server.sessions.get(server.decodeChannel);
            assert session != null; // TODO error if null

            AmqpServer.AmqpSession.AmqpServerStream sender = session.links.get(handle);
            assert sender != null; // TODO error if null

            server.decodableBodyBytes -= transfer.offset() - performative.offset();
            if (!sender.fragmented)
            {
                assert deliveryId != NO_DELIVERY_ID; // TODO: error
                assert deliveryId == session.remoteDeliveryId + 1; // TODO: error
                session.remoteDeliveryId = deliveryId;
            }

            if (deliveryId != NO_DELIVERY_ID && deliveryId != session.remoteDeliveryId) // TODO: error
            {
                break decode;
            }
            server.decodableBodyBytes -= transfer.sizeof();
            final int fragmentOffset = transfer.limit();
            final int fragmentSize = (int) server.decodableBodyBytes;
            final int fragmentLimit = fragmentOffset + fragmentSize;

            assert fragmentLimit <= limit;

            int reserved = fragmentSize + sender.initialPadding;
            boolean canSend = reserved <= sender.initialBudget;

            if (canSend && sender.debitorIndex != NO_DEBITOR_INDEX)
            {
                reserved = sender.debitor.claim(traceId, sender.debitorIndex, sender.initialId, reserved, reserved, 0);
            }

            if (canSend && reserved != 0)
            {
                server.onDecodeTransfer(traceId, authorization, transfer, reserved, buffer, fragmentOffset, fragmentLimit);

                server.decoder = decodeFrameType;
                progress = fragmentLimit;
            }
        }

        return progress;
    }

    private void decodeError(
        AmqpServer server,
        final long traceId,
        final long authorization,
        final int limit)
    {
        server.onDecodeError(traceId, authorization, DECODE_ERROR, null);
        server.decoder = decodeIgnoreAll;
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
        final AmqpPerformativeFW performative = amqpPerformativeRO.wrap(buffer, offset, limit);
        final AmqpDetachFW detach = performative.detach();
        assert detach != null;

        server.onDecodeDetach(traceId, authorization, detach);
        server.decoder = decodeFrameType;
        return detach.limit();
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
        server.onDecodeError(traceId, authorization, DECODE_ERROR, null);
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
        private final long budgetId;
        private final long replySharedBudgetId;

        private final Int2ObjectHashMap<AmqpSession> sessions;

        private int initialBudget;
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

        private int decodeChannel;
        private int outgoingChannel;
        private long decodableBodyBytes;
        private long decodeMaxFrameSize = MIN_MAX_FRAME_SIZE;
        private long encodeMaxFrameSize = MIN_MAX_FRAME_SIZE;
        private long encodeIdleTimeout = 0;
        private long decodeIdleTimeout = 0;

        private long decodeIdleTimeoutId = NO_CANCEL_ID;
        private long decodeIdleTimeoutAt;

        private long encodeIdleTimeoutId = NO_CANCEL_ID;
        private long encodeIdleTimeoutAt;

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
            this.decoder = decodeProtocolHeader;
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

        private void doEncodeEmptyFrame(
            long traceId,
            long authorization)
        {
            doNetworkData(traceId, authorization, 0L, emptyFrameHeader);
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

            if (defaultIdleTimeout != DEFAULT_IDLE_TIMEOUT)
            {
                builder.idleTimeOut(defaultIdleTimeout);
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
            int remoteChannel,
            int nextOutgoingId)
        {
            final AmqpBeginFW begin = amqpBeginRW.wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity())
                .remoteChannel(remoteChannel)
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
            StringFW addressFrom,
            StringFW addressTo,
            long deliveryCount)
        {
            AmqpAttachFW.Builder builder = amqpAttachRW.wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity())
                .name(amqpStringRW.wrap(stringBuffer, 0, stringBuffer.capacity()).set(name, UTF_8).build().get())
                .handle(handle)
                .role(role)
                .sndSettleMode(senderSettleMode)
                .rcvSettleMode(receiverSettleMode);

            int extraOffset = 0;
            if (addressFrom != null && addressFrom.length() != -1)
            {
                AmqpSourceListFW sourceList = amqpSourceListRW
                    .wrap(extraBuffer, extraOffset, extraBuffer.capacity())
                    .address(addressFrom)
                    .build();
                builder.source(b -> b.sourceList(sourceList));
                extraOffset = sourceList.limit();
            }

            if (addressTo != null)
            {
                if (addressTo.length() != -1)
                {
                    AmqpTargetListFW targetList = amqpTargetListRW
                        .wrap(extraBuffer, extraOffset, extraBuffer.capacity())
                        .address(addressTo)
                        .build();
                    builder.target(b -> b.targetList(targetList));
                }
                else
                {
                    AmqpTargetListFW targetList = amqpTargetListRW
                        .wrap(extraBuffer, extraOffset, extraBuffer.capacity())
                        .build();
                    builder.target(b -> b.targetList(targetList));
                }
            }

            if (role == AmqpRole.SENDER)
            {
                builder.initialDeliveryCount(deliveryCount);
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

        private void doEncodeTransfer(
            long traceId,
            long authorization,
            int channel,
            AmqpTransferFW transfer,
            DirectBuffer buffer,
            int offset,
            int length)
        {
            final int frameSize = FRAME_HEADER_SIZE + transfer.sizeof() + length;
            assert frameSize <= encodeMaxFrameSize;

            final AmqpFrameHeaderFW frameHeader = amqpFrameHeaderRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                .size(frameSize)
                .doff(2)
                .type(0)
                .channel(channel)
                .performative(b -> b.transfer(transfer))
                .build();

            frameBuffer.putBytes(transfer.limit(), buffer, offset, length);
            replyBudgetReserved += frameSize + replyPadding;
            OctetsFW payload = payloadRO.wrap(frameBuffer, 0, frameSize);
            doNetworkData(traceId, authorization, 0L, payload);
        }

        private void doEncodeTransferFragments(
            long traceId,
            long authorization,
            int channel,
            long handle,
            boolean more,
            DirectBuffer fragmentBuffer,
            int fragmentProgress,
            int fragmentLimit)
        {
            int fragmentRemaining = fragmentLimit - fragmentProgress;

            AmqpTransferFW transferCont = amqpTransferRW
                    .wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity())
                    .handle(handle)
                    .more(1)
                    .build();
            int fragmentSizeCont = (int) encodeMaxFrameSize - FRAME_HEADER_SIZE - transferCont.sizeof();
            while (fragmentRemaining > fragmentSizeCont)
            {
                doEncodeTransfer(traceId, authorization, outgoingChannel, transferCont,
                        fragmentBuffer, fragmentProgress, fragmentSizeCont);
                fragmentProgress += fragmentSizeCont;
                fragmentRemaining -= fragmentSizeCont;
                doSignalEncodeIdleTimeoutIfNecessary();
            }

            AmqpTransferFW.Builder transferFinBuilder = amqpTransferRW
                    .wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity())
                    .handle(handle);

            if (more)
            {
                transferFinBuilder.more(1);
            }

            AmqpTransferFW transferFin = transferFinBuilder
                    .build();

            int fragmentSizeFin = (int) encodeMaxFrameSize - FRAME_HEADER_SIZE - transferFin.sizeof();
            assert fragmentRemaining <= fragmentSizeFin;

            doEncodeTransfer(traceId, authorization, channel, transferFin,
                    fragmentBuffer, fragmentProgress, fragmentRemaining);
            doSignalEncodeIdleTimeoutIfNecessary();
            fragmentProgress += fragmentRemaining;
            assert fragmentProgress == fragmentLimit;
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
            AmqpErrorType errorType,
            StringFW errorDescription)
        {
            final AmqpCloseFW.Builder builder = amqpCloseRW.wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity());

            AmqpCloseFW close;
            if (errorType != null)
            {
                AmqpErrorListFW.Builder errorBuilder = amqpErrorListRW.wrap(extraBuffer, 0, extraBuffer.capacity())
                    .condition(errorType);
                if (errorDescription != null)
                {
                    errorBuilder.description(errorDescription);
                }
                AmqpErrorListFW errorList = errorBuilder.build();
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

                OctetsFW payload = payloadRO.wrap(buffer, offset, limit);
                doData(network, routeId, replyId, traceId, authorization, FLAG_INIT_AND_FIN, budgetId, reserved,
                    payload, EMPTY_OCTETS);
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

            cleanupStreams(traceId, authorization);
            cleanupBudgetCreditorIfNecessary();
            cleanupEncodeSlotIfNecessary();

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
            replyPadding = padding;

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

            cleanupStreams(traceId, authorization);
            cleanupBudgetCreditorIfNecessary();
            cleanupEncodeSlotIfNecessary();

            doNetworkReset(traceId, authorization);
        }

        private void onNetworkSignal(
            SignalFW signal)
        {
            final int signalId = signal.signalId();

            switch (signalId)
            {
            case DECODE_IDLE_TIMEOUT_ID:
                onDecodeIdleTimeoutSignal(signal);
                break;
            case ENCODE_IDLE_TIMEOUT_ID:
                onEncodeIdleTimeoutSignal(signal);
                break;
            default:
                break;
            }
        }

        private void onDecodeIdleTimeoutSignal(
            SignalFW signal)
        {
            final long traceId = signal.traceId();
            final long authorization = signal.authorization();

            final long now = System.currentTimeMillis();
            if (now >= decodeIdleTimeoutAt)
            {
                onDecodeError(traceId, authorization, RESOURCE_LIMIT_EXCEEDED, timeoutDescription);
                decoder = decodeIgnoreAll;
            }
            else
            {
                decodeIdleTimeoutId = signaler.signalAt(decodeIdleTimeoutAt, routeId, replyId, DECODE_IDLE_TIMEOUT_ID);
            }
        }

        private void onEncodeIdleTimeoutSignal(
            SignalFW signal)
        {
            final long traceId = signal.traceId();
            final long authorization = signal.authorization();

            doEncodeEmptyFrame(traceId, authorization);
            doSignalEncodeIdleTimeoutIfNecessary();
        }

        private int onDecodeEmptyFrame(
            final DirectBuffer buffer,
            final int offset,
            final int limit)
        {
            int progress = offset;
            OctetsFW frame = payloadRO.wrap(buffer, offset, limit);
            if (frame.value().equals(emptyFrameHeader.value()))
            {
                doSignalDecodeIdleTimeoutIfNecessary();
                progress = limit;
            }
            return progress;
        }

        private void onDecodeError(
            long traceId,
            long authorization,
            AmqpErrorType errorType,
            StringFW errorDescription)
        {
            cleanupStreams(traceId, authorization);
            doEncodeClose(traceId, authorization, errorType, errorDescription);
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
            DirectBuffer buffer = payload.buffer();
            int offset = payload.offset();
            int limit = payload.limit();
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
                onDecodeError(traceId, authorization, DECODE_ERROR, null);
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
            this.encodeIdleTimeout = open.hasIdleTimeOut() ? open.idleTimeOut() : 0;
            this.decodeIdleTimeout = defaultIdleTimeout;
            doEncodeOpen(traceId, authorization);
            doSignalDecodeIdleTimeoutIfNecessary();
            doSignalEncodeIdleTimeoutIfNecessary();
        }

        private void onDecodeBegin(
            long traceId,
            long authorization,
            AmqpBeginFW begin)
        {
            if (begin.hasRemoteChannel())
            {
                onDecodeError(traceId, authorization, NOT_ALLOWED, null);
            }
            else
            {
                AmqpSession session = sessions.computeIfAbsent(decodeChannel, AmqpSession::new);
                session.outgoingChannel(outgoingChannel);
                session.nextIncomingId((int) begin.nextOutgoingId());
                session.incomingWindow(writeBuffer.capacity());
                session.outgoingWindow(outgoingWindow);
                session.remoteIncomingWindow((int) begin.incomingWindow());
                session.remoteOutgoingWindow((int) begin.outgoingWindow());
                session.onDecodeBegin(traceId, authorization);
                this.outgoingChannel++;
                doSignalDecodeIdleTimeoutIfNecessary();
                doSignalEncodeIdleTimeoutIfNecessary();
            }
        }

        private void onDecodeAttach(
            long traceId,
            long authorization,
            AmqpAttachFW attach)
        {
            AmqpSession session = sessions.get(decodeChannel);
            if (session != null)
            {
                session.onDecodeAttach(traceId, authorization, attach);
                doSignalDecodeIdleTimeoutIfNecessary();
            }
            else
            {
                onDecodeError(traceId, authorization, NOT_ALLOWED, null);
            }
        }

        private void onDecodeFlow(
            long traceId,
            long authorization,
            AmqpFlowFW flow)
        {
            AmqpSession session = sessions.get(decodeChannel);
            if (session != null)
            {
                session.onDecodeFlow(traceId, authorization, flow);
                doSignalDecodeIdleTimeoutIfNecessary();
            }
            else
            {
                onDecodeError(traceId, authorization, NOT_ALLOWED, null);
            }
        }

        private void onDecodeTransfer(
            long traceId,
            long authorization,
            AmqpTransferFW transfer,
            int reserved,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            AmqpSession session = sessions.get(decodeChannel);
            if (session != null)
            {
                session.onDecodeTransfer(traceId, authorization, transfer, reserved, buffer, offset, limit);
                doSignalDecodeIdleTimeoutIfNecessary();
            }
            else
            {
                onDecodeError(traceId, authorization, NOT_ALLOWED, null);
            }
        }

        private void onDecodeDetach(
            long traceId,
            long authorization,
            AmqpDetachFW detach)
        {
            AmqpErrorType error = null;
            if (detach.hasError())
            {
                error = detach.error().errorList().condition();
            }
            AmqpSession session = sessions.get(decodeChannel);
            session.onDecodeDetach(traceId, authorization, error, detach.handle());
            doSignalDecodeIdleTimeoutIfNecessary();
        }

        private void onDecodeEnd(
            long traceId,
            long authorization,
            AmqpEndFW end)
        {
            AmqpErrorType errorType = null;
            if (end.fieldCount() > 0)
            {
                errorType = end.error().errorList().condition();
            }
            AmqpSession session = sessions.get(decodeChannel);
            if (session != null)
            {
                session.cleanup(traceId, authorization);
                sessions.remove(decodeChannel);
                flushReplySharedBudget(traceId);
                doEncodeEnd(traceId, authorization, session.outgoingChannel, errorType);
                doSignalDecodeIdleTimeoutIfNecessary();
                doSignalEncodeIdleTimeoutIfNecessary();
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
                doEncodeClose(traceId, authorization, null, null);
                doNetworkEndIfNecessary(traceId, authorization);
            }
        }

        private boolean isAmqpHeaderValid(
            AmqpProtocolHeaderFW header)
        {
            return PROTOCOL_HEADER == header.buffer().getLong(header.offset(), BIG_ENDIAN);
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

        private void doSignalDecodeIdleTimeoutIfNecessary()
        {
            if (decodeIdleTimeout > 0)
            {
                decodeIdleTimeoutAt = System.currentTimeMillis() + decodeIdleTimeout;

                if (decodeIdleTimeoutId == NO_CANCEL_ID && decodeIdleTimeout > 0)
                {
                    decodeIdleTimeoutId = signaler.signalAt(decodeIdleTimeoutAt, routeId, replyId, DECODE_IDLE_TIMEOUT_ID);
                }
            }
        }

        private void doSignalEncodeIdleTimeoutIfNecessary()
        {
            if (encodeIdleTimeout > 0)
            {
                encodeIdleTimeoutAt = System.currentTimeMillis() + encodeIdleTimeout - (encodeIdleTimeout / 2);

                if (encodeIdleTimeoutId == NO_CANCEL_ID)
                {
                    encodeIdleTimeoutId = signaler.signalAt(encodeIdleTimeoutAt, routeId, replyId, ENCODE_IDLE_TIMEOUT_ID);
                }
            }
        }

        private final class AmqpSession
        {
            private final Long2ObjectHashMap<AmqpServerStream> links;
            private final int incomingChannel;

            private long deliveryId = NO_DELIVERY_ID;
            private long remoteDeliveryId = NO_DELIVERY_ID;
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
                doEncodeBegin(traceId, authorization, incomingChannel, nextOutgoingId);
            }

            private void onDecodeAttach(
                long traceId,
                long authorization,
                AmqpAttachFW attach)
            {
                final long linkKey = attach.handle();
                if (links.containsKey(linkKey))
                {
                    onDecodeError(traceId, authorization, NOT_ALLOWED, null);
                }
                else
                {
                    AmqpRole role = attach.role();

                    AmqpSourceFW source = attach.hasSource() ? attach.source() : null;
                    AmqpSourceListFW sourceList = source != null ? source.sourceList() : null;
                    StringFW sourceAddress = sourceList != null && sourceList.hasAddress() ? sourceList.address() : null;

                    AmqpTargetFW target = attach.hasTarget() ? attach.target() : null;
                    AmqpTargetListFW targetList = target != null ? target.targetList() : null;
                    StringFW targetAddress = targetList != null && targetList.hasAddress() ? targetList.address() : null;

                    final RouteFW route;
                    switch (role)
                    {
                    case RECEIVER:
                        route = resolveRoute(routeId, authorization, sourceAddress, RECEIVE_ONLY);
                        break;
                    case SENDER:
                        route = resolveRoute(routeId, authorization, targetAddress, SEND_ONLY);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + role);
                    }

                    if (route != null)
                    {
                        String addressFrom = sourceAddress != null ? sourceAddress.asString() : null;
                        String addressTo = targetAddress != null ? targetAddress.asString() : null;

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
                int reserved,
                DirectBuffer buffer,
                int offset,
                int limit)
            {
                this.nextIncomingId++;
                this.remoteOutgoingWindow--;
                this.incomingWindow--;
                if (incomingWindow < 0)
                {
                    cleanup(traceId, authorization);
                    sessions.remove(incomingChannel);
                    flushReplySharedBudget(traceId);
                    doEncodeEnd(traceId, authorization, outgoingChannel, SESSION_WINDOW_VIOLATION);
                    doSignalEncodeIdleTimeoutIfNecessary();
                }
                else
                {
                    final BoundedOctetsFW deliveryTag = transfer.hasDeliveryTag() ? transfer.deliveryTag() : null;
                    final long messageFormat = transfer.hasMessageFormat() ? transfer.messageFormat() : 0;
                    boolean settled = transfer.hasSettled() && transfer.settled() == 1;
                    boolean resume = transfer.hasResume() && transfer.resume() == 1;
                    boolean aborted = transfer.hasAborted() && transfer.aborted() == 1;
                    boolean batchable = transfer.hasBatchable() && transfer.batchable() == 1;
                    boolean more = transfer.hasMore() && transfer.more() == 1;
                    AmqpServerStream link = links.get(transfer.handle());
                    link.onDecodeTransfer(traceId, authorization, reserved, deliveryTag, messageFormat, settled,
                        resume, aborted, batchable, more, buffer, offset, limit);
                }
            }

            private void onDecodeDetach(
                long traceId,
                long authorization,
                AmqpErrorType errorType,
                long handle)
            {
                AmqpServerStream link = links.get(handle);
                if (link != null)
                {
                    link.onDecodeDetach(traceId, authorization, errorType);
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

                private int state;
                private int capabilities;

                private boolean fragmented;
                private long remoteDeliveryCount;
                private long deliveryCount;
                private int remoteLinkCredit;
                private int linkCredit;

                private BudgetDebitor debitor;
                private long debitorIndex = NO_DEBITOR_INDEX;

                private long initialBudgetId;
                private int initialBudget;
                private int initialPadding;

                private int replyBudget;

                private String name;
                private long handle;
                private AmqpRole role;
                private StringFW addressFrom;
                private StringFW addressTo;

                private AmqpBodyKind encodeBodyKind;
                private AmqpBodyKind decodeBodyKind;

                private AmqpSectionDecoder decoder;
                private int decodableBytes;

                AmqpServerStream(
                    String addressFrom,
                    String addressTo,
                    AmqpRole role,
                    RouteFW route)
                {
                    this.addressFrom = new String8FW(addressFrom);
                    this.addressTo = new String8FW(addressTo);
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

                    remoteDeliveryCount = attach.hasInitialDeliveryCount() ? attach.initialDeliveryCount() : 0;

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
                    this.linkCredit = (int) (deliveryCount + linkCredit - remoteDeliveryCount);
                    this.remoteDeliveryCount = deliveryCount;
                    flushReplyWindow(traceId, authorization);
                }

                private void onDecodeTransfer(
                    long traceId,
                    long authorization,
                    int reserved,
                    BoundedOctetsFW deliveryTag,
                    long messageFormat,
                    boolean settled,
                    boolean resume,
                    boolean aborted,
                    boolean batchable,
                    boolean more,
                    DirectBuffer buffer,
                    int offset,
                    int limit)
                {
                    int flags = 0;
                    if (!fragmented)
                    {
                        flags |= FLAG_INIT;
                    }
                    if (!more)
                    {
                        flags |= FLAG_FIN;
                        deliveryCount++;
                    }

                    int transferFlags = 0;
                    transferFlags = settled ? settled(transferFlags) : transferFlags;
                    transferFlags = resume ? resume(transferFlags) : transferFlags;
                    transferFlags = aborted ? aborted(transferFlags) : transferFlags;
                    transferFlags = batchable ? batchable(transferFlags) : transferFlags;

                    OctetsFW payload = null;
                    Flyweight extension = EMPTY_OCTETS;
                    decode:
                    if (!fragmented)
                    {
                        this.remoteLinkCredit--;
                        if (remoteLinkCredit < 0)
                        {
                            onDecodeError(traceId, authorization, LINK_TRANSFER_LIMIT_EXCEEDED);
                            break decode;
                        }

                        final AmqpDataExFW.Builder amqpDataEx = amqpDataExRW.wrap(extraBuffer, 0, extraBuffer.capacity())
                            .typeId(amqpTypeId)
                            .deliveryTag(b -> b.bytes(deliveryTag.get(deliveryTagRO::tryWrap)))
                            .messageFormat(messageFormat)
                            .flags(transferFlags);

                        final OctetsFW messageFragment = amqpMessageDecodeHelper.decodeFragmentInit(this, buffer, offset, limit,
                            amqpDataEx);
                        if (messageFragment.sizeof() > 0)
                        {
                            payload = messageFragment;
                        }

                        extension = amqpDataEx
                            .bodyKind(b -> b.set(decodeBodyKind))
                            .deferred(decodableBytes)
                            .build();
                    }
                    else
                    {
                        OctetsFW messageFragment =  amqpMessageDecodeHelper.decodeFragment(this, buffer, offset, limit);
                        if (messageFragment.sizeof() > 0)
                        {
                            payload = messageFragment;
                        }
                    }
                    doApplicationData(traceId, authorization, flags, reserved, payload, extension);

                    this.fragmented = more;
                }

                private void onDecodeDetach(
                    long traceId,
                    long authorization,
                    AmqpErrorType errorType)
                {
                    if (errorType == null)
                    {
                        doApplicationEnd(traceId, authorization, EMPTY_OCTETS);
                    }
                    else
                    {
                        cleanup(traceId, authorization);
                        // TODO: support abortEx
                    }
                }

                private void onDecodeError(
                    long traceId,
                    long authorization,
                    AmqpErrorType errorType)
                {
                    doEncodeDetach(traceId, authorization, errorType, outgoingChannel, handle);
                    doSignalEncodeIdleTimeoutIfNecessary();
                    cleanup(traceId, authorization);
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
                    int flags,
                    int reserved,
                    OctetsFW payload,
                    Flyweight extension)
                {
                    assert AmqpState.initialOpening(state);

                    final int length = payload != null ? payload.sizeof() : 0;
                    assert reserved >= length + initialPadding;

                    this.initialBudget -= reserved;

                    assert initialBudget >= 0;

                    doData(application, newRouteId, initialId, traceId, authorization, flags, initialBudgetId, reserved, payload,
                        extension);
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
                    this.initialBudgetId = budgetId;
                    this.initialBudget += credit;
                    this.initialPadding = padding;

                    if (budgetId != 0L && debitorIndex == NO_DEBITOR_INDEX)
                    {
                        debitor = supplyDebitor.apply(budgetId);
                        debitorIndex = debitor.acquire(budgetId, initialId, AmqpServer.this::decodeNetworkIfNecessary);
                    }

                    flushInitialWindow(traceId, authorization);

                    if (AmqpState.initialClosing(state) && !AmqpState.initialClosed(state))
                    {
                        doApplicationEnd(traceId, authorization, EMPTY_OCTETS);
                    }
                }

                private void flushInitialWindow(
                    long traceId,
                    long authorization)
                {
                    if (AmqpState.replyOpened(state) && role == SENDER)
                    {
                        this.remoteLinkCredit = (int) (Math.min(bufferPool.slotCapacity(), initialBudget) /
                                                       Math.min(bufferPool.slotCapacity(), decodeMaxFrameSize));
                        maximum.value = 0;
                        links.values().forEach(l -> maximum.value += l.remoteLinkCredit);
                        incomingWindow = maximum.value;

                        doEncodeFlow(traceId, authorization, outgoingChannel, nextOutgoingId, nextIncomingId, incomingWindow,
                            handle, deliveryCount, remoteLinkCredit);
                    }
                }

                private void onApplicationReset(
                    ResetFW reset)
                {
                    final long traceId = reset.traceId();
                    final long authorization = reset.authorization();

                    if (!AmqpState.replyOpened(state))
                    {
                        AmqpRole amqpRole = role == RECEIVER ? SENDER : RECEIVER;
                        if (amqpRole == RECEIVER)
                        {
                            doEncodeAttach(traceId, authorization, name, outgoingChannel, handle, amqpRole, MIXED, FIRST,
                                addressFrom, null, deliveryCount);
                        }
                        else
                        {
                            doEncodeAttach(traceId, authorization, name, outgoingChannel, handle, amqpRole, MIXED, FIRST,
                                null, addressTo, deliveryCount);
                        }
                    }

                    setInitialClosed();

                    onDecodeError(traceId, authorization, LINK_DETACH_FORCED);
                }

                private void onApplicationSignal(
                    SignalFW signal)
                {
                    final long signalId = signal.signalId();
                    // TODO
                }

                private void onApplicationBegin(
                    BeginFW begin)
                {
                    state = AmqpState.openReply(state);

                    final long traceId = begin.traceId();
                    final long authorization = begin.authorization();

                    AmqpRole amqpRole = role == RECEIVER ? SENDER : RECEIVER;
                    AmqpSenderSettleMode amqpSenderSettleMode = MIXED;
                    AmqpReceiverSettleMode amqpReceiverSettleMode = FIRST;
                    deliveryCount = initialDeliveryCount;

                    final AmqpBeginExFW amqpBeginEx = begin.extension().get(amqpBeginExRO::tryWrap);
                    if (amqpBeginEx != null)
                    {
                        amqpSenderSettleMode = amqpSenderSettleMode(amqpBeginEx.senderSettleMode().get());
                        amqpReceiverSettleMode = amqpReceiverSettleMode(amqpBeginEx.receiverSettleMode().get());
                    }

                    doEncodeAttach(traceId, authorization, name, outgoingChannel, handle, amqpRole, amqpSenderSettleMode,
                        amqpReceiverSettleMode, addressFrom, addressTo, deliveryCount);

                    doSignalEncodeIdleTimeoutIfNecessary();

                    flushInitialWindow(traceId, authorization);
                }

                private void onApplicationData(
                    DataFW data)
                {
                    final long traceId = data.traceId();
                    final int reserved = data.reserved();
                    final long authorization = data.authorization();
                    final int flags = data.flags();
                    final OctetsFW extension = data.extension();
                    final OctetsFW payload = data.payload();

                    this.replyBudget -= reserved;
                    replySharedBudget -= reserved;

                    if (replyBudget < 0)
                    {
                        doApplicationReset(traceId, authorization);
                        doNetworkAbort(traceId, authorization);
                    }

                    nextOutgoingId++;
                    outgoingWindow--;

                    if ((flags & FLAG_INIT) == FLAG_INIT)
                    {
                        deliveryId++;
                        onApplicationDataInit(traceId, reserved, authorization, flags, extension, payload);
                    }
                    else
                    {
                        onApplicationDataContOrFin(traceId, reserved, authorization, flags, payload);
                    }
                }

                private void onApplicationDataInit(
                    long traceId,
                    int reserved,
                    long authorization,
                    int flags,
                    OctetsFW extension,
                    OctetsFW payload)
                {
                    final AmqpDataExFW dataEx = extension.get(amqpDataExRO::tryWrap);
                    assert dataEx != null;

                    final int deferred = dataEx.deferred();
                    final boolean more = (flags & FLAG_FIN) == 0;

                    final AmqpBodyKind bodyKind = dataEx.bodyKind().get();
                    final OctetsFW deliveryTagBytes = dataEx.deliveryTag().bytes();
                    final BoundedOctetsFW deliveryTag =
                            amqpBinaryRW.wrap(stringBuffer, 0, stringBuffer.capacity())
                                .set(deliveryTagBytes.value(), 0, deliveryTagBytes.sizeof())
                                .build()
                                .get();
                    final long messageFormat = dataEx.messageFormat();
                    final boolean settled = isSettled(dataEx.flags());

                    final OctetsFW messageFragment = amqpMessageHelper.encodeFragmentInit(
                            deferred, extension, payload);

                    this.encodeBodyKind = bodyKind;

                    final AmqpTransferFW.Builder transferBuilder = amqpTransferRW
                            .wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity())
                            .handle(handle)
                            .deliveryId(deliveryId)
                            .deliveryTag(deliveryTag)
                            .messageFormat(messageFormat)
                            .settled(settled ? 1 : 0);

                    if (more)
                    {
                        transferBuilder.more(1);
                    }

                    final AmqpTransferFW transfer = transferBuilder.build();
                    final DirectBuffer fragmentBuffer = messageFragment.buffer();
                    final int fragmentOffset = messageFragment.offset();
                    final int fragmentLimit = messageFragment.limit();
                    final int fragmentSize = fragmentLimit - fragmentOffset;
                    final int frameSize = FRAME_HEADER_SIZE + transfer.sizeof() + fragmentSize;

                    if (frameSize <= encodeMaxFrameSize)
                    {
                        doEncodeTransfer(traceId, authorization, outgoingChannel, transfer,
                                fragmentBuffer, fragmentOffset, fragmentSize);
                    }
                    else
                    {
                        AmqpTransferFW transferInit = amqpTransferRW
                                .wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity())
                                .handle(handle)
                                .deliveryId(deliveryId)
                                .deliveryTag(deliveryTag)
                                .messageFormat(messageFormat)
                                .settled(settled ? 1 : 0)
                                .more(1)
                                .build();

                        int fragmentSizeInit = (int) encodeMaxFrameSize - FRAME_HEADER_SIZE - transferInit.sizeof();
                        int fragmentProgress = fragmentOffset;

                        doEncodeTransfer(traceId, authorization, outgoingChannel,
                                transferInit, fragmentBuffer, fragmentProgress, fragmentSizeInit);
                        fragmentProgress += fragmentSizeInit;

                        doEncodeTransferFragments(
                            traceId, authorization, outgoingChannel, handle, more,
                            fragmentBuffer, fragmentProgress, fragmentLimit);
                    }
                    doSignalEncodeIdleTimeoutIfNecessary();
                }

                private void onApplicationDataContOrFin(
                    long traceId,
                    int reserved,
                    long authorization,
                    int flags,
                    OctetsFW payload)
                {
                    final boolean more = (flags & FLAG_FIN) == 0;

                    OctetsFW messageFragment = amqpMessageHelper.encodeFragment(encodeBodyKind, payload);

                    final AmqpTransferFW.Builder transferBuilder = amqpTransferRW
                            .wrap(frameBuffer, FRAME_HEADER_SIZE, frameBuffer.capacity())
                            .handle(handle);

                    if (more)
                    {
                        transferBuilder.more(1);
                    }

                    final AmqpTransferFW transfer = transferBuilder.build();
                    final DirectBuffer fragmentBuffer = messageFragment.buffer();
                    final int fragmentOffset = messageFragment.offset();
                    final int fragmentLimit = messageFragment.limit();
                    final int fragmentSize = fragmentLimit - fragmentOffset;
                    final int frameSize = FRAME_HEADER_SIZE + transfer.sizeof() + fragmentSize;

                    if (frameSize <= encodeMaxFrameSize)
                    {
                        doEncodeTransfer(traceId, authorization, outgoingChannel, transfer,
                                fragmentBuffer, fragmentOffset, fragmentSize);
                    }
                    else
                    {
                        doEncodeTransferFragments(
                            traceId, authorization, outgoingChannel, handle, more,
                            fragmentBuffer, fragmentOffset, fragmentLimit);
                    }
                    doSignalEncodeIdleTimeoutIfNecessary();
                }

                private void onApplicationEnd(
                    EndFW end)
                {
                    setReplyClosed();

                    final long traceId = end.traceId();
                    final long authorization = end.authorization();

                    doEncodeDetach(traceId, authorization, null, decodeChannel, handle);
                    doSignalEncodeIdleTimeoutIfNecessary();
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
                    if (AmqpState.replyOpened(state))
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
                    doCancelIdleTimeoutIfNecessary();
                }

                private void doCancelIdleTimeoutIfNecessary()
                {
                    if (decodeIdleTimeoutId != NO_CANCEL_ID)
                    {
                        signaler.cancel(decodeIdleTimeoutId);
                        decodeIdleTimeoutId = NO_CANCEL_ID;
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

    private final class AmqpMessageEncoder
    {
        private final AmqpMapFW.Builder<AmqpValueFW, AmqpValueFW, AmqpValueFW.Builder, AmqpValueFW.Builder> annotationsRW =
            new AmqpMapFW.Builder<>(new AmqpValueFW(), new AmqpValueFW(), new AmqpValueFW.Builder(),
                new AmqpValueFW.Builder());
        private final AmqpMapFW.Builder<AmqpValueFW, AmqpValueFW, AmqpValueFW.Builder, AmqpValueFW.Builder>
            applicationPropertiesRW = new AmqpMapFW.Builder<>(new AmqpValueFW(), new AmqpValueFW(), new AmqpValueFW.Builder(),
            new AmqpValueFW.Builder());

        private AmqpSectionEncoder sectionEncoder;
        private int encodableBytes;

        private OctetsFW encodeFragmentInit(
            int deferred,
            OctetsFW extension,
            OctetsFW payload)
        {
            messageFragmentRW.wrap(extraBuffer, 0, extraBuffer.capacity());

            final AmqpDataExFW dataEx = extension.get(amqpDataExRO::tryWrap);
            assert dataEx != null;
            final AmqpBodyKind bodyKind = dataEx.bodyKind().get();

            encodeMessageProperties(dataEx.properties());
            encodeMessageAnnotations(dataEx.annotations());
            encodeApplicationProperties(dataEx.applicationProperties());

            if (payload == null)
            {
                return encodeSectionValueNull();
            }
            else
            {
                this.sectionEncoder = lookupBodyEncoder(bodyKind);
                return encodeSections(deferred, payload);
            }
        }

        private OctetsFW encodeFragment(
            AmqpBodyKind bodyKind,
            OctetsFW payload)
        {
            assert bodyKind != null;

            messageFragmentRW.wrap(extraBuffer, 0, extraBuffer.capacity());

            this.sectionEncoder = lookupBodyBytesEncoder(bodyKind);

            return encodeSections(0, payload);
        }

        private OctetsFW encodeSections(
            int deferred,
            OctetsFW payload)
        {
            AmqpSectionEncoder previous = null;
            final DirectBuffer buffer = payload.buffer();
            int progress = payload.offset();
            final int limit = payload.limit();

            while (progress <= limit && previous != sectionEncoder)
            {
                previous = sectionEncoder;
                progress = sectionEncoder.encode(deferred, buffer, progress, limit);
            }

            assert progress == limit; // more

            return messageFragmentRW.build();
        }

        private AmqpSectionEncoder lookupBodyBytesEncoder(
            AmqpBodyKind bodyKind)
        {
            AmqpSectionEncoder encoder;
            switch (bodyKind)
            {
            case DATA:
                encoder = this::encodeSectionDataBytes;
                break;
            case SEQUENCE:
                encoder = this::encodeSectionSequenceBytes;
                break;
            case VALUE:
            case VALUE_STRING8:
            case VALUE_STRING32:
            case VALUE_BINARY8:
            case VALUE_BINARY32:
            case VALUE_SYMBOL8:
            case VALUE_SYMBOL32:
                encoder = this::encodeSectionValueBytes;
                break;
            default:
                throw new IllegalArgumentException("Unexpected body kind: " + bodyKind);
            }
            return encoder;
        }

        private AmqpSectionEncoder lookupBodyEncoder(
            final AmqpBodyKind bodyKind)
        {
            AmqpSectionEncoder encoder;

            switch (bodyKind)
            {
            case DATA:
                encoder = this::encodeSectionData;
                break;
            case SEQUENCE:
                encoder = this::encodeSectionSequence;
                break;
            case VALUE:
                encoder = this::encodeSectionValue;
                break;
            case VALUE_STRING8:
                encoder = this::encodeSectionValueString8;
                break;
            case VALUE_STRING32:
                encoder = this::encodeSectionValueString32;
                break;
            case VALUE_BINARY8:
                encoder = this::encodeSectionValueBinary8;
                break;
            case VALUE_BINARY32:
                encoder = this::encodeSectionValueBinary32;
                break;
            case VALUE_SYMBOL8:
                encoder = this::encodeSectionValueSymbol8;
                break;
            case VALUE_SYMBOL32:
                encoder = this::encodeSectionValueSymbol32;
                break;
            default:
                throw new IllegalArgumentException("Unexpected body kind: " + bodyKind);
            }

            return encoder;
        }

        private void encodeMessageProperties(
            AmqpPropertiesFW properties)
        {
            if (properties.fieldCount() > 0)
            {
                AmqpDescribedTypeFW type = messagePropertiesSectionType;
                messageFragmentRW.put(type.buffer(), type.offset(), type.sizeof());
                messageFragmentRW.put((b, o, l) ->
                {
                    AmqpMessagePropertiesFW.Builder amqpProperties = amqpPropertiesRW.wrap(b, o, l);
                    if (properties.hasMessageId())
                    {
                        amqpProperties.messageId(properties.messageId().stringtype());
                    }
                    if (properties.hasUserId())
                    {
                        final BoundedOctetsFW userId = amqpBinaryRW.wrap(stringBuffer, 0, stringBuffer.capacity())
                            .set(properties.userId().bytes().value(), 0, properties.userId().length())
                            .build()
                            .get();
                        amqpProperties.userId(userId);
                    }
                    if (properties.hasTo())
                    {
                        amqpProperties.to(properties.to());
                    }
                    if (properties.hasSubject())
                    {
                        amqpProperties.subject(properties.subject());
                    }
                    if (properties.hasReplyTo())
                    {
                        amqpProperties.replyTo(properties.replyTo());
                    }
                    if (properties.hasCorrelationId())
                    {
                        amqpProperties.correlationId(properties.correlationId().stringtype());
                    }
                    if (properties.hasContentType())
                    {
                        amqpProperties.contentType(properties.contentType());
                    }
                    if (properties.hasContentEncoding())
                    {
                        amqpProperties.contentEncoding(properties.contentEncoding());
                    }
                    if (properties.hasAbsoluteExpiryTime())
                    {
                        amqpProperties.absoluteExpiryTime(properties.absoluteExpiryTime());
                    }
                    if (properties.hasCreationTime())
                    {
                        amqpProperties.creationTime(properties.creationTime());
                    }
                    if (properties.hasGroupId())
                    {
                        amqpProperties.groupId(properties.groupId());
                    }
                    if (properties.hasGroupSequence())
                    {
                        amqpProperties.groupSequence(properties.groupSequence());
                    }
                    if (properties.hasReplyToGroupId())
                    {
                        amqpProperties.replyToGroupId(properties.replyToGroupId());
                    }
                    return amqpProperties.build().sizeof();
                });
            }
        }

        private void encodeMessageAnnotations(
            Array32FW<AmqpAnnotationFW> value)
        {
            if (value.fieldCount() > 0)
            {
                AmqpDescribedTypeFW type = messageAnnotationsSectionType;
                messageFragmentRW.put(type.buffer(), type.offset(), type.sizeof());
                messageFragmentRW.put((b, o, l) ->
                {
                    annotationsRW.wrap(b, o, l);
                    value.forEach(this::encodeMessageAnnotation);
                    return annotationsRW.build().sizeof();
                });
            }
        }

        private void encodeMessageAnnotation(
            AmqpAnnotationFW item)
        {
            final AmqpAnnotationKeyFW key = item.key();
            final OctetsFW valueBytes = item.value().bytes();
            final AmqpValueFW value = valueBytes.get(amqpValueRO::wrap);

            switch (key.kind())
            {
            case KIND_ID:
                AmqpULongFW id = amqpULongRW.wrap(valueBuffer, 0, valueBuffer.capacity())
                    .set(key.id())
                    .build();
                annotationsRW.entry(
                    k -> k.setAsAmqpULong(id),
                    v -> v.set(value));
                break;
            case KIND_NAME:
                AmqpSymbolFW name = amqpSymbolRW.wrap(valueBuffer, 0, valueBuffer.capacity())
                    .set(key.name())
                    .build();
                annotationsRW.entry(
                    k -> k.setAsAmqpSymbol(name),
                    v -> v.set(value));
                break;
            }
        }

        private void encodeApplicationProperties(
            Array32FW<AmqpApplicationPropertyFW> value)
        {
            if (value.fieldCount() > 0)
            {
                AmqpDescribedTypeFW type = applicationPropertiesSectionType;
                messageFragmentRW.put(type.buffer(), type.offset(), type.sizeof());
                messageFragmentRW.put((b, o, l) ->
                {
                    applicationPropertiesRW.wrap(b, o, l);
                    value.forEach(this::encodeApplicationProperty);
                    return applicationPropertiesRW.build().sizeof();
                });
            }
        }

        private void encodeApplicationProperty(
            AmqpApplicationPropertyFW item)
        {
            int  valueOffset = 0;
            AmqpStringFW key = amqpStringRW.wrap(valueBuffer, valueOffset, valueBuffer.capacity())
                .set(item.key())
                .build();
            valueOffset += key.sizeof();

            AmqpStringFW value = amqpValueRW.wrap(valueBuffer, valueOffset, valueBuffer.capacity())
                .set(item.value())
                .build();

            applicationPropertiesRW.entry(k -> k.setAsAmqpString(key), v -> v.setAsAmqpString(value));
        }

        private int encodeSectionData(
            int deferred,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            int progress = offset;
            AmqpType constructorByte = AmqpType.valueOf(buffer.getByte(offset) & 0xFF);
            if (constructorByte != null)
            {
                messageFragmentRW
                    .put(dataSectionType.buffer(), dataSectionType.offset(), dataSectionType.sizeof())
                    .put(buffer, progress, Byte.BYTES);

                progress++;
                switch (constructorByte)
                {
                case BINARY1:
                    this.encodableBytes = buffer.getByte(progress);
                    messageFragmentRW.put(buffer, progress, Byte.BYTES);
                    progress += Byte.BYTES;
                    break;
                case BINARY4:
                    this.encodableBytes = buffer.getInt(progress, BIG_ENDIAN);
                    messageFragmentRW.put(buffer, progress, Integer.BYTES);
                    progress += Integer.BYTES;
                    break;
                }
                this.sectionEncoder = this::encodeSectionDataBytes;
            }
            return progress;
        }

        private int encodeSectionDataBytes(
            int deferred,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            int progress = offset;
            int totalSize = limit - offset;
            int size = Math.min(encodableBytes, totalSize);
            messageFragmentRW.put(buffer, offset, size);
            this.encodableBytes = totalSize - size;
            progress += size;
            if (encodableBytes > 0)
            {
                this.sectionEncoder = this::encodeSectionData;
            }
            return progress;
        }

        private int encodeSectionSequence(
            int deferred,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            int progress = offset;
            AmqpType constructorByte = AmqpType.valueOf(buffer.getByte(offset) & 0xFF);
            if (constructorByte != null)
            {
                messageFragmentRW.put(sequenceSectionType.buffer(), sequenceSectionType.offset(), sequenceSectionType.sizeof())
                    .put(buffer, progress, Byte.BYTES);
                progress++;
                switch (constructorByte)
                {
                case LIST1:
                    this.encodableBytes = buffer.getByte(progress);
                    messageFragmentRW.put(buffer, progress, Byte.BYTES);
                    progress += Byte.BYTES;
                    break;
                case LIST4:
                    this.encodableBytes = buffer.getInt(progress, BIG_ENDIAN);
                    messageFragmentRW.put(buffer, progress, Integer.BYTES);
                    progress += Integer.BYTES;
                    break;
                }
                this.sectionEncoder = this::encodeSectionSequenceBytes;
            }
            return progress;
        }

        private int encodeSectionSequenceBytes(
            int deferred,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            int progress = offset;
            int totalSize = limit - offset;
            int size = Math.min(encodableBytes, totalSize);
            messageFragmentRW.put(buffer, offset, size);
            this.encodableBytes = totalSize - size;
            progress += size;
            if (encodableBytes > 0)
            {
                this.sectionEncoder = this::encodeSectionSequence;
            }
            return progress;
        }

        private int encodeSectionValue(
            int deferred,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            int progress = offset;
            int constructor = buffer.getByte(offset) & 0xF0;

            messageFragmentRW.put(valueSectionType.buffer(), valueSectionType.offset(), valueSectionType.sizeof())
                .put(buffer, progress, Byte.BYTES);
            progress++;

            switch (constructor)
            {
            case 0x40:
                this.encodableBytes = 0;
                break;
            case 0x50:
                this.encodableBytes = Byte.BYTES;
                break;
            case 0x60:
                this.encodableBytes = Short.BYTES;
                break;
            case 0x70:
                this.encodableBytes = Integer.BYTES;
                break;
            case 0x80:
                this.encodableBytes = Long.BYTES;
                break;
            case 0x90:
                this.encodableBytes = Long.BYTES + Long.BYTES;
                break;
            case 0xc0:
            case 0xe0:
                this.encodableBytes = buffer.getByte(progress);
                messageFragmentRW.put(buffer, progress, Byte.BYTES);
                progress += Byte.BYTES;
                break;
            case 0xd0:
            case 0xf0:
                this.encodableBytes = buffer.getInt(progress, BIG_ENDIAN);
                messageFragmentRW.put(buffer, progress, Integer.BYTES);
                progress += Integer.BYTES;
                break;
            }

            this.sectionEncoder = this::encodeSectionValueBytes;

            return progress;
        }

        private OctetsFW encodeSectionValueNull()
        {
            return messageFragmentRW.put(valueSectionType.buffer(), valueSectionType.offset(), valueSectionType.sizeof())
                .put(nullConstructor)
                .build();
        }

        private int encodeSectionValueString8(
            int deferred,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            this.encodableBytes = limit - offset;
            messageFragmentRW.put(valueSectionType.buffer(), valueSectionType.offset(), valueSectionType.sizeof());

            int length = deferred == 0 ? encodableBytes : encodableBytes + deferred;
            AmqpVariableLength8FW bodyHeader =
                amqpVariableLength8RW.wrap(valueBuffer, 0, valueBuffer.capacity())
                    .constructor(c -> c.set(STRING1))
                    .length(length)
                    .build();
            int bodyHeaderSize = bodyHeader.sizeof();
            messageFragmentRW.put(bodyHeader.buffer(), bodyHeader.offset(), bodyHeaderSize);

            this.sectionEncoder = this::encodeSectionValueBytes;
            return offset;
        }

        private int encodeSectionValueString32(
            int deferred,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            this.encodableBytes = limit - offset;
            messageFragmentRW.put(valueSectionType.buffer(), valueSectionType.offset(), valueSectionType.sizeof());

            int length = deferred == 0 ? encodableBytes : encodableBytes + deferred;
            AmqpVariableLength32FW bodyHeader =
                amqpVariableLength32RW.wrap(valueBuffer, 0, valueBuffer.capacity())
                    .constructor(c -> c.set(STRING4))
                    .length(length)
                    .build();
            int bodyHeaderSize = bodyHeader.sizeof();
            messageFragmentRW.put(bodyHeader.buffer(), bodyHeader.offset(), bodyHeaderSize);

            this.sectionEncoder = this::encodeSectionValueBytes;
            return offset;
        }

        private int encodeSectionValueBinary8(
            int deferred,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            this.encodableBytes = limit - offset;
            messageFragmentRW.put(valueSectionType.buffer(), valueSectionType.offset(), valueSectionType.sizeof());

            int length = deferred == 0 ? encodableBytes : encodableBytes + deferred;
            AmqpVariableLength8FW bodyHeader =
                amqpVariableLength8RW.wrap(valueBuffer, 0, valueBuffer.capacity())
                    .constructor(c -> c.set(BINARY1))
                    .length(length)
                    .build();
            int bodyHeaderSize = bodyHeader.sizeof();
            messageFragmentRW.put(bodyHeader.buffer(), bodyHeader.offset(), bodyHeaderSize);

            this.sectionEncoder = this::encodeSectionValueBytes;
            return offset;
        }

        private int encodeSectionValueBinary32(
            int deferred,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            this.encodableBytes = limit - offset;
            messageFragmentRW.put(valueSectionType.buffer(), valueSectionType.offset(), valueSectionType.sizeof());

            int length = deferred == 0 ? encodableBytes : encodableBytes + deferred;
            AmqpVariableLength32FW bodyHeader =
                amqpVariableLength32RW.wrap(valueBuffer, 0, valueBuffer.capacity())
                .constructor(c -> c.set(BINARY4))
                .length(length)
                .build();
            int bodyHeaderSize = bodyHeader.sizeof();
            messageFragmentRW.put(bodyHeader.buffer(), bodyHeader.offset(), bodyHeaderSize);

            this.sectionEncoder = this::encodeSectionValueBytes;
            return offset;
        }

        private int encodeSectionValueSymbol8(
            int deferred,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            this.encodableBytes = limit - offset;
            messageFragmentRW.put(valueSectionType.buffer(), valueSectionType.offset(), valueSectionType.sizeof());

            int length = deferred == 0 ? encodableBytes : encodableBytes + deferred;
            AmqpVariableLength8FW bodyHeader =
                amqpVariableLength8RW.wrap(valueBuffer, 0, valueBuffer.capacity())
                    .constructor(c -> c.set(AmqpType.SYMBOL1))
                    .length(length)
                    .build();
            int bodyHeaderSize = bodyHeader.sizeof();
            messageFragmentRW.put(bodyHeader.buffer(), bodyHeader.offset(), bodyHeaderSize);

            this.sectionEncoder = this::encodeSectionValueBytes;
            return offset;
        }

        private int encodeSectionValueSymbol32(
            int deferred,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            this.encodableBytes = limit - offset;
            messageFragmentRW.put(valueSectionType.buffer(), valueSectionType.offset(), valueSectionType.sizeof());

            int length = deferred == 0 ? encodableBytes : encodableBytes + deferred;
            AmqpVariableLength32FW bodyHeader =
                amqpVariableLength32RW.wrap(valueBuffer, 0, valueBuffer.capacity())
                    .constructor(c -> c.set(AmqpType.SYMBOL4))
                    .length(length)
                    .build();
            int bodyHeaderSize = bodyHeader.sizeof();
            messageFragmentRW.put(bodyHeader.buffer(), bodyHeader.offset(), bodyHeaderSize);

            this.sectionEncoder = this::encodeSectionValueBytes;
            return offset;
        }

        private int encodeSectionValueBytes(
            int deferred,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            int progress = offset;
            int size = limit - offset;
            messageFragmentRW.put(buffer, offset, size);
            progress += size;
            return progress;
        }
    }

    private final class AmqpMessageDecoder
    {
        private int decodeOffset;

        private OctetsFW decodeFragmentInit(
            AmqpServer.AmqpSession.AmqpServerStream stream,
            DirectBuffer buffer,
            int offset,
            int limit,
            AmqpDataExFW.Builder amqpDataEx)
        {
            stream.decodeBodyKind = null;

            skipHeaders(buffer, offset, limit);
            skipDeliveryAnnotations(buffer, decodeOffset, limit);
            final Array32FW<AmqpAnnotationFW> annotations = decodeAnnotations(buffer, decodeOffset, limit);
            amqpDataEx.annotations(annotations);
            final AmqpPropertiesFW properties = decodeProperties(buffer, decodeOffset, limit);
            amqpDataEx.properties(properties);
            final Array32FW<AmqpApplicationPropertyFW> applicationProperties =
                decodeApplicationProperties(buffer, decodeOffset, limit);
            amqpDataEx.applicationProperties(applicationProperties);

            messageFragmentRW.wrap(valueBuffer, 0, valueBuffer.capacity());

            final AmqpSectionTypeFW sectionType = amqpSectionTypeRO.tryWrap(buffer, decodeOffset, limit);
            stream.decoder = lookupSectionDecoder(sectionType.get());

            return decodeMessageFragment(stream, buffer, sectionType.limit(), limit);
        }

        private OctetsFW decodeFragment(
            AmqpServer.AmqpSession.AmqpServerStream stream,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            messageFragmentRW.wrap(valueBuffer, 0, valueBuffer.capacity());
            return decodeMessageFragment(stream, buffer, offset, limit);
        }

        private AmqpSectionDecoder lookupSectionDecoder(
            AmqpSectionType sectionType)
        {
            AmqpSectionDecoder decoder;
            switch (sectionType)
            {
            case DATA:
                decoder = this::decodeSectionData;
                break;
            case SEQUENCE:
                decoder = this::decodeSectionSequence;
                break;
            case VALUE:
                decoder = this::decodeSectionValue;
                break;
            case FOOTER:
                decoder = this::skipFooter;
                break;
            default:
                throw new IllegalArgumentException("Unexpected section type: " + sectionType);
            }

            return decoder;
        }

        private OctetsFW decodeMessageFragment(
            AmqpServer.AmqpSession.AmqpServerStream stream,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            AmqpSectionDecoder previous = null;
            int progress = offset;

            while (progress <= limit && previous != stream.decoder)
            {
                previous = stream.decoder;
                progress = stream.decoder.decode(stream, buffer, progress, limit);
            }

            assert progress == limit;

            return messageFragmentRW.build();
        }

        private int decodeSection(
            AmqpServer.AmqpSession.AmqpServerStream stream,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            int progress = offset;
            if (progress < limit)
            {
                final AmqpSectionTypeFW sectionType = amqpSectionTypeRO.tryWrap(buffer, progress, limit);
                stream.decoder = lookupSectionDecoder(sectionType.get());
                progress = sectionType.limit();
            }
            return progress;
        }

        private int decodeSectionData(
            AmqpServer.AmqpSession.AmqpServerStream stream,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            stream.decodeBodyKind = AmqpBodyKind.DATA;
            int constructor = buffer.getByte(offset) & 0xff;
            int progress = offset;
            stream.decodableBytes = 0;
            messageFragmentRW.put(buffer, progress, Byte.BYTES);
            progress++;

            switch (constructor)
            {
            case 0xa0:
                stream.decodableBytes = buffer.getByte(progress);
                messageFragmentRW.put(buffer, progress, Byte.BYTES);
                progress += Byte.BYTES;
                break;
            case 0xb0:
                stream.decodableBytes = buffer.getInt(progress, BIG_ENDIAN);
                messageFragmentRW.put(buffer, progress, Integer.BYTES);
                progress += Integer.BYTES;
                break;
            }

            int available = Math.min(stream.decodableBytes, limit - progress);
            messageFragmentRW.put(buffer, progress, available);
            progress += available;
            stream.decodableBytes -= available;
            assert stream.decodableBytes >= 0;
            if (stream.decodableBytes == 0)
            {
                stream.decoder = this::decodeSection;
            }
            else
            {
                stream.decoder = this::decodeSectionBytes;
            }

            return progress;
        }

        private int decodeSectionSequence(
            AmqpServer.AmqpSession.AmqpServerStream stream,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            stream.decodeBodyKind = AmqpBodyKind.SEQUENCE;
            int constructor = buffer.getByte(offset) & 0xff;
            int progress = offset;
            stream.decodableBytes = 0;
            messageFragmentRW.put(buffer, progress, Byte.BYTES);
            progress++;

            switch (constructor)
            {
            case 0x45:
                break;
            case 0xc0:
                stream.decodableBytes = buffer.getByte(progress);
                messageFragmentRW.put(buffer, progress, Byte.BYTES);
                progress += Byte.BYTES;
                break;
            case 0xd0:
                stream.decodableBytes = buffer.getInt(progress, BIG_ENDIAN);
                messageFragmentRW.put(buffer, progress, Integer.BYTES);
                progress += Integer.BYTES;
                break;
            }

            int available = Math.min(stream.decodableBytes, limit - progress);
            messageFragmentRW.put(buffer, progress, available);
            progress += available;
            stream.decodableBytes -= available;
            assert stream.decodableBytes >= 0;
            if (stream.decodableBytes == 0)
            {
                stream.decoder = this::decodeSection;
            }
            else
            {
                stream.decoder = this::decodeSectionBytes;
            }

            return progress;
        }

        private int decodeSectionValue(
            AmqpServer.AmqpSession.AmqpServerStream stream,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            int constructor = buffer.getByte(offset) & 0xff;
            int constructorMask = constructor & 0xf0;
            int progress = offset;
            stream.decodableBytes = 0;
            stream.decodeBodyKind = AmqpBodyKind.VALUE;
            if (constructorMask != 0xa0 && constructorMask != 0xb0 && constructor != 0x40)
            {
                messageFragmentRW.put(buffer, progress, Byte.BYTES);
            }
            progress++;

            switch (constructorMask)
            {
            case 0x40:
                stream.decodableBytes = 0;
                if (constructor == 0x40)
                {
                    stream.decodeBodyKind = AmqpBodyKind.VALUE_NULL;
                }
                break;
            case 0x50:
                stream.decodableBytes = Byte.BYTES;
                break;
            case 0x60:
                stream.decodableBytes = Short.BYTES;
                break;
            case 0x70:
                stream.decodableBytes = Integer.BYTES;
                break;
            case 0x80:
                stream.decodableBytes = Long.BYTES;
                break;
            case 0x90:
                stream.decodableBytes = Long.BYTES + Long.BYTES;
                break;
            case 0xa0:
                stream.decodableBytes = buffer.getByte(progress);
                switch (constructor)
                {
                case 0xa0:
                    stream.decodeBodyKind = AmqpBodyKind.VALUE_BINARY8;
                    break;
                case 0xa1:
                    stream.decodeBodyKind = AmqpBodyKind.VALUE_STRING8;
                    break;
                case 0xa3:
                    stream.decodeBodyKind = AmqpBodyKind.VALUE_SYMBOL8;
                    break;
                }
                progress++;
                break;
            case 0xb0:
                stream.decodableBytes = buffer.getInt(progress, BIG_ENDIAN);
                switch (constructor)
                {
                case 0xb0:
                    stream.decodeBodyKind = AmqpBodyKind.VALUE_BINARY32;
                    break;
                case 0xb1:
                    stream.decodeBodyKind = AmqpBodyKind.VALUE_STRING32;
                    break;
                case 0xb3:
                    stream.decodeBodyKind = AmqpBodyKind.VALUE_SYMBOL32;
                    break;
                }
                progress += Integer.BYTES;
                break;
            case 0xc0:
            case 0xe0:
                stream.decodableBytes = buffer.getByte(progress);
                messageFragmentRW.put(buffer, progress, Byte.BYTES);
                progress += Byte.BYTES;
                break;
            case 0xd0:
            case 0xf0:
                stream.decodableBytes = buffer.getInt(progress, BIG_ENDIAN);
                messageFragmentRW.put(buffer, progress, Integer.BYTES);
                progress += Integer.BYTES;
                break;
            }

            int available = Math.min(stream.decodableBytes, limit - progress);
            messageFragmentRW.put(buffer, progress, available);
            progress += available;
            stream.decodableBytes -= available;
            assert stream.decodableBytes >= 0;
            if (stream.decodableBytes == 0)
            {
                stream.decoder = this::decodeSection;
            }
            else
            {
                stream.decoder = this::decodeSectionBytes;
            }

            return progress;
        }

        private int decodeSectionBytes(
            AmqpServer.AmqpSession.AmqpServerStream stream,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            int progress = offset;
            final int length = limit - offset;
            if (length > 0)
            {
                int available = Math.min(stream.decodableBytes, limit - progress);
                messageFragmentRW.put(buffer, progress, available);
                progress += available;
                stream.decodableBytes -= available;
                assert stream.decodableBytes >= 0;
                if (stream.decodableBytes == 0)
                {
                    stream.decoder = this::decodeSection;
                }
            }
            return progress;
        }

        private void skipHeaders(
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            this.decodeOffset = offset;
            final AmqpSectionTypeFW sectionType = amqpSectionTypeRO.tryWrap(buffer, offset, limit);

            if (sectionType != null && sectionType.get() == AmqpSectionType.HEADER)
            {
                AmqpHeaderFW headers = headersRO.tryWrap(buffer, sectionType.limit(), limit);
                this.decodeOffset = headers.limit();
            }
        }

        private void skipDeliveryAnnotations(
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            final AmqpSectionTypeFW sectionType = amqpSectionTypeRO.tryWrap(buffer, offset, limit);

            if (sectionType != null && sectionType.get() == AmqpSectionType.DELIVERY_ANNOTATIONS)
            {
                AmqpMapFW<AmqpValueFW, AmqpValueFW> deliveryAnnotations =
                    deliveryAnnotationsRO.tryWrap(buffer, sectionType.limit(), limit);

                this.decodeOffset = deliveryAnnotations.limit();
            }
        }

        private Array32FW<AmqpAnnotationFW> decodeAnnotations(
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            AmqpMapFW<AmqpValueFW, AmqpValueFW> annotations = null;
            Array32FW.Builder<AmqpAnnotationFW.Builder, AmqpAnnotationFW> annotationBuilder =
                annotationRW.wrap(frameBuffer, 0, frameBuffer.capacity());
            final AmqpSectionTypeFW sectionType = amqpSectionTypeRO.tryWrap(buffer, offset, limit);

            if (sectionType != null && sectionType.get() == AmqpSectionType.MESSAGE_ANNOTATIONS)
            {
                annotations = annotationsRO.tryWrap(buffer, sectionType.limit(), limit);
                assert annotations != null;
                this.decodeOffset = annotations.limit();

                annotations.forEach(kv -> vv ->
                {
                    switch (kv.kind())
                    {
                    case SYMBOL1:
                        StringFW symbolKey = kv.getAsAmqpSymbol().get();
                        annotationBuilder.item(b -> b.key(k -> k.name(symbolKey))
                                                     .value(vb -> vb.bytes(vv.buffer(), vv.offset(), vv.sizeof())));
                        break;
                    case ULONG0:
                    case ULONG1:
                    case ULONG8:
                        long longKey = kv.getAsAmqpULong().get();
                        annotationBuilder.item(b -> b.key(k -> k.id(longKey))
                                                     .value(vb -> vb.bytes(vv.buffer(), vv.offset(), vv.sizeof())));
                        break;
                    default:
                        break;
                    }
                });
            }

            return annotationBuilder.build();
        }

        private AmqpPropertiesFW decodeProperties(
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            AmqpPropertiesFW.Builder propertyBuilder = propertyRW.wrap(frameBuffer, 0, frameBuffer.capacity());
            final AmqpSectionTypeFW sectionType = amqpSectionTypeRO.tryWrap(buffer, offset, limit);

            if (sectionType != null && sectionType.get() == AmqpSectionType.PROPERTIES)
            {
                AmqpMessagePropertiesFW property = amqpPropertiesRO.tryWrap(buffer, sectionType.limit(), limit);
                assert property != null;
                this.decodeOffset = property.limit();

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
            }

            return propertyBuilder.build();
        }

        private Array32FW<AmqpApplicationPropertyFW> decodeApplicationProperties(
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            Array32FW.Builder<AmqpApplicationPropertyFW.Builder, AmqpApplicationPropertyFW> applicationPropertyBuilder =
                applicationPropertyRW.wrap(frameBuffer, 0, frameBuffer.capacity());
            final AmqpSectionTypeFW sectionType = amqpSectionTypeRO.tryWrap(buffer, offset, limit);

            if (sectionType != null && sectionType.get() == AmqpSectionType.APPLICATION_PROPERTIES)
            {
                AmqpMapFW<AmqpValueFW, AmqpValueFW> applicationProperty = applicationPropertyRO.tryWrap(buffer,
                    sectionType.limit(), limit);
                applicationProperty.forEach(kv -> vv ->
                {
                    String key = kv.getAsAmqpString().asString();
                    String value = vv.getAsAmqpString().asString();
                    // TODO: handle different type of values
                    applicationPropertyBuilder.item(kb -> kb.key(key).value(value));
                });
                this.decodeOffset = applicationProperty.limit();
            }

            return applicationPropertyBuilder.build();
        }

        private int skipFooter(
            AmqpServer.AmqpSession.AmqpServerStream stream,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            int progress = offset;

            final int length = limit - progress;
            if (length > 0)
            {
                AmqpMapFW<AmqpValueFW, AmqpValueFW> footer = footerRO.tryWrap(buffer, progress, limit);
                progress = footer.limit();
            }
            return progress;
        }
    }
}
