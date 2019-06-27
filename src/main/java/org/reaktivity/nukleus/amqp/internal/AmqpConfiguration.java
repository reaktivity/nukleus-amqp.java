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
package org.reaktivity.nukleus.amqp.internal;

import org.reaktivity.nukleus.Configuration;

public class AmqpConfiguration extends Configuration
{
    public static final PropertyDef<String> AMQP_CONTAINER_ID;
    public static final IntPropertyDef AMQP_CHANNEL_MAX;
    public static final LongPropertyDef AMQP_MAX_FRAME_SIZE;

    private static final ConfigurationDef AMQP_CONFIG;

    static
    {
        final ConfigurationDef config = new ConfigurationDef("nukleus.amqp");
        AMQP_CONTAINER_ID = config.property("container.id", "container-id:1");
        AMQP_CHANNEL_MAX = config.property("channel.max", 65535);
        AMQP_MAX_FRAME_SIZE = config.property("max.frame.size", 4294967295L);
        AMQP_CONFIG = config;
    }

    public AmqpConfiguration(
        Configuration config)
    {
        super(AMQP_CONFIG, config);
    }

    public String containerId()
    {
        return AMQP_CONTAINER_ID.get(this);
    }

    public int channelMax()
    {
        return AMQP_CHANNEL_MAX.getAsInt(this);
    }

    public long maxFrameSize()
    {
        return AMQP_MAX_FRAME_SIZE.getAsLong(this);
    }
}
