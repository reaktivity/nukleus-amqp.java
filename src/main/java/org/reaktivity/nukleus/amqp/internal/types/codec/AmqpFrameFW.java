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
package org.reaktivity.nukleus.amqp.internal.types.codec;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.amqp.internal.types.Flyweight;

import org.agrona.BitUtil;

// TODO: Replace with generated class and remove this class
public class AmqpFrameFW extends Flyweight
{
    public static final int FIELD_OFFSET_LENGTH = 0;

    private static final int FIELD_SIZE_LENGTH = BitUtil.SIZE_OF_INT;

    public static final int FIELD_OFFSET_DOFF = FIELD_OFFSET_LENGTH + FIELD_SIZE_LENGTH;

    private static final int FIELD_SIZE_DOFF = BitUtil.SIZE_OF_BYTE;

    public static final int FIELD_OFFSET_TYPE = FIELD_OFFSET_DOFF + FIELD_SIZE_DOFF;

    private static final int FIELD_SIZE_TYPE = BitUtil.SIZE_OF_BYTE;

    public static final int FIELD_OFFSET_CHANNEL = FIELD_OFFSET_TYPE + FIELD_SIZE_TYPE;

    private static final int FIELD_SIZE_CHANNEL = BitUtil.SIZE_OF_SHORT;

    public static final int FIELD_OFFSET_PERFORMATIVE = FIELD_OFFSET_CHANNEL + FIELD_SIZE_CHANNEL;

    private static final int FIELD_SIZE_PERFORMATIVE = BitUtil.SIZE_OF_SHORT + BitUtil.SIZE_OF_BYTE;

    public static final int FIELD_OFFSET_PAYLOAD = FIELD_OFFSET_PERFORMATIVE + FIELD_SIZE_PERFORMATIVE;

    public int performative()
    {
        return buffer().getByte(offset() + FIELD_OFFSET_PERFORMATIVE + FIELD_SIZE_PERFORMATIVE) & 0x0f;
    }

    @Override
    public int limit()
    {
        return 0;
    }

    @Override
    public AmqpFrameFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);
        checkLimit(limit(), maxLimit);
        return this;
    }

    public static final class Builder extends Flyweight.Builder<AmqpFrameFW>
    {
        public Builder()
        {
            super(new AmqpFrameFW());
        }

        @Override
        public Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);
            return this;
        }
    }

    private static void validateLength(
        long length8bytes)
    {
        if (length8bytes >> 17L != 0L)
        {
            throw new IllegalStateException(String.format("frame payload=%d too long", length8bytes));
        }
    }

    private static int lengthSize0(byte b)
    {
        switch (b & 0x7f)
        {
            case 0x7e:
                return 3;

            case 0x7f:
                return 9;

            default:
                return 1;
        }
    }
}
