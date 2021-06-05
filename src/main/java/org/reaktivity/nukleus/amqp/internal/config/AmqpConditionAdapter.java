/**
 * Copyright 2016-2021 The Reaktivity Project
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
package org.reaktivity.nukleus.amqp.internal.config;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.bind.adapter.JsonbAdapter;

import org.reaktivity.nukleus.amqp.internal.AmqpNukleus;
import org.reaktivity.nukleus.amqp.internal.types.AmqpCapabilities;
import org.reaktivity.reaktor.config.Condition;
import org.reaktivity.reaktor.config.ConditionAdapterSpi;

public final class AmqpConditionAdapter implements ConditionAdapterSpi, JsonbAdapter<Condition, JsonObject>
{
    private static final String ADDRESS_NAME = "address";
    private static final String CAPABILITIES_NAME = "capabilities";

    @Override
    public String type()
    {
        return AmqpNukleus.NAME;
    }

    @Override
    public JsonObject adaptToJson(
        Condition condition)
    {
        AmqpCondition mqttCondition = (AmqpCondition) condition;

        JsonObjectBuilder object = Json.createObjectBuilder();

        if (mqttCondition.address != null)
        {
            object.add(ADDRESS_NAME, mqttCondition.address);
        }

        if (mqttCondition.capabilities != null)
        {
            object.add(CAPABILITIES_NAME, mqttCondition.capabilities.toString().toLowerCase());
        }

        return object.build();
    }

    @Override
    public Condition adaptFromJson(
        JsonObject object)
    {
        String address = object.containsKey(ADDRESS_NAME)
                ? object.getString(ADDRESS_NAME)
                : null;

        AmqpCapabilities capabilities = object.containsKey(CAPABILITIES_NAME)
                ? AmqpCapabilities.valueOf(object.getString(CAPABILITIES_NAME).toUpperCase())
                : null;

        return new AmqpCondition(address, capabilities);
    }
}
