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
package org.reaktivity.nukleus.amqp.internal.types.codec;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.amqp.internal.types.Flyweight;

public class AmqpHeaderFW extends Flyweight
{
    public static final int FIELD_OFFSET_PROTOCOL_HEADER_NAME = 0;

    private static final int FIELD_SIZE_PROTOCOL_HEADER_NAME = BitUtil.SIZE_OF_INT;

    public static final int FIELD_OFFSET_PROTOCOL_ID = FIELD_OFFSET_PROTOCOL_HEADER_NAME + FIELD_SIZE_PROTOCOL_HEADER_NAME;

    public static final int FIELD_SIZE_PROTOCOL_ID = BitUtil.SIZE_OF_BYTE;

    public static final int FIELD_OFFSET_PROTOCOL_VERSION_MAJOR = FIELD_OFFSET_PROTOCOL_ID + FIELD_SIZE_PROTOCOL_ID;

    public static final int FIELD_SIZE_PROTOCOL_VERSION_MAJOR = BitUtil.SIZE_OF_BYTE;

    public static final int FIELD_OFFSET_PROTOCOL_VERSION_MINOR =
        FIELD_OFFSET_PROTOCOL_VERSION_MAJOR + FIELD_SIZE_PROTOCOL_VERSION_MAJOR;

    public static final int FIELD_SIZE_PROTOCOL_VERSION_MINOR = BitUtil.SIZE_OF_BYTE;

    public static final int FIELD_OFFSET_PROTOCOL_VERSION_REVISION =
        FIELD_OFFSET_PROTOCOL_VERSION_MINOR + FIELD_SIZE_PROTOCOL_VERSION_MINOR;

    public static final int FIELD_SIZE_PROTOCOL_VERSION_REVISION = BitUtil.SIZE_OF_BYTE;

    @Override
    public int limit()
    {
        return maxLimit();
    }

    public String protocolHeaderName()
    {
        return buffer().getStringAscii(offset() + FIELD_OFFSET_PROTOCOL_HEADER_NAME);
    }

    public byte protocolId()
    {
        return buffer().getByte(offset() + FIELD_OFFSET_PROTOCOL_ID);
    }

    public byte protocolVersionMajor()
    {
        return buffer().getByte(offset() + FIELD_OFFSET_PROTOCOL_VERSION_MAJOR);
    }

    public byte protocolVersionMinor()
    {
        return buffer().getByte(offset() + FIELD_OFFSET_PROTOCOL_VERSION_MINOR);
    }

    public byte protocolVersionRevision()
    {
        return buffer().getByte(offset() + FIELD_OFFSET_PROTOCOL_VERSION_REVISION);
    }

    @Override
    public AmqpHeaderFW tryWrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.tryWrap(buffer, offset, maxLimit);

        return this;
    }

    @Override
    public AmqpHeaderFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        checkLimit(limit(), maxLimit);

        return this;
    }

    public static final class Builder extends Flyweight.Builder<AmqpHeaderFW>
    {
        public Builder()
        {
            super(new AmqpHeaderFW());
        }

        @Override
        public AmqpHeaderFW.Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);
            return this;
        }
    }
}
