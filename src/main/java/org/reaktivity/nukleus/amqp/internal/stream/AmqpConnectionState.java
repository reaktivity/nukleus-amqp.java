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

public enum AmqpConnectionState
{
    START,
    HDR_RCVD,
    HDR_SENT,
    HDR_EXCH,
    OPEN_PIPE,
    OC_PIPE,
    OPEN_RCVD,
    OPEN_SENT,
    CLOSE_PIPE,
    OPENED,
    CLOSE_RCVD,
    CLOSE_SENT,
    DISCARDING,
    END,
    ERROR;

    public static AmqpConnectionState receivedHeader(
        AmqpConnectionState state)
    {
        AmqpConnectionState newState = ERROR;

        switch (state)
        {
        case START:
            newState = HDR_RCVD;
            break;
        case HDR_SENT:
            newState = HDR_EXCH;
            break;
        case OPEN_PIPE:
            newState = OPEN_SENT;
            break;
        case OC_PIPE:
            newState = CLOSE_PIPE;
            break;
        }

        return newState;
    }

    public static AmqpConnectionState sentHeader(
        AmqpConnectionState state)
    {
        AmqpConnectionState newState = ERROR;

        switch (state)
        {
        case START:
            newState = HDR_SENT;
            break;
        case HDR_RCVD:
            newState = HDR_EXCH;
            break;
        }

        return newState;
    }

    public static AmqpConnectionState receivedOpen(
        AmqpConnectionState state)
    {
        AmqpConnectionState newState = ERROR;

        switch (state)
        {
        case HDR_EXCH:
            newState = OPEN_RCVD;
            break;
        case OPEN_SENT:
            newState = OPENED;
            break;
        case CLOSE_PIPE:
            newState = CLOSE_SENT;
            break;
        }

        return newState;
    }

    public static AmqpConnectionState sentOpen(
        AmqpConnectionState state)
    {
        AmqpConnectionState newState = ERROR;

        switch (state)
        {
        case HDR_EXCH:
            newState = OPEN_SENT;
            break;
        case OPEN_RCVD:
            newState = OPENED;
            break;
        case HDR_SENT:
            newState = OPEN_PIPE;
            break;
        }

        return newState;
    }

    public static AmqpConnectionState receivedClose(
        AmqpConnectionState state)
    {
        AmqpConnectionState newState = ERROR;

        switch (state)
        {
        case OPENED:
            newState = CLOSE_RCVD;
            break;
        case CLOSE_SENT:
            newState = END;
            break;
        }

        return newState;
    }

    public static AmqpConnectionState sentClose(
        AmqpConnectionState state)
    {
        AmqpConnectionState newState = ERROR;

        switch (state)
        {
        case CLOSE_RCVD:
            newState = END;
            break;
        case OPENED:
            newState = CLOSE_SENT;
            break;
        case OPEN_PIPE:
            newState = OC_PIPE;
            break;
        case OPEN_SENT:
            newState = CLOSE_PIPE;
            break;
        }

        return newState;
    }
}
