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

//import static java.nio.ByteOrder.BIG_ENDIAN;
//import static java.nio.ByteOrder.nativeOrder;
//import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

//import java.nio.ByteOrder;
//import java.security.MessageDigest;
//import java.util.Base64;
//import java.util.Base64.Encoder;
//import java.util.LinkedHashMap;
//import java.util.Map;
//import java.util.Objects;
//import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import org.agrona.DirectBuffer;
//import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
//import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.amqp.internal.types.stream.*;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.amqp.internal.AmqpConfiguration;
//import org.reaktivity.nukleus.amqp.internal.types.Flyweight;
//import org.reaktivity.nukleus.amqp.internal.types.ListFW;
import org.reaktivity.nukleus.amqp.internal.types.OctetsFW;
import org.reaktivity.nukleus.amqp.internal.types.control.RouteFW;

public final class AmqpServerFactory implements StreamFactory
{
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

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;

//    private final Long2ObjectHashMap<AmqpServerConnect> correlations;
    private final MessageFunction<RouteFW> wrapRoute;

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
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
//        this.correlations = new Long2ObjectHashMap<>();
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

        final long acceptRouteId = begin.routeId();

        final MessagePredicate filter = (t, b, o, l) -> true;
        final RouteFW route = router.resolve(acceptRouteId, begin.authorization(), filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long acceptInitialId = begin.streamId();
            newStream = new AmqpServerConnection(
                throttle,
                acceptRouteId,
                acceptInitialId)::onNetwork;

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

        private DecoderState decodeState;

        private AmqpServerConnection(
            MessageConsumer acceptReply,
            long acceptRouteId,
            long acceptId)
        {

            this.decodeState = this::decodeHeader;
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
            // TODO
        }

        private void onData(
            DataFW data)
        {
            // TODO
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
            // TODO
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

        private int decodeHeader(
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            return 0; // TODO
        }
    }

    @FunctionalInterface
    private interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int length);
    }
}
