import os
from enum import Enum
from typing import Optional, Union, List, Set, Callable, Awaitable, Any
import logging
import json
import asyncio
import ssl
import certifi
from .models import *
from picows import WSFrame, WSListener, WSMsgType, WSCloseCode, WSTransport, ws_connect
from ..logging import get_logger
import logging
from ..exceptions import AuthError
import traceback

env_key = "POLYGON_API_KEY"
logger = get_logger("WebSocketClient")


class PolygonWSListener(WSListener):
    def __init__(self, client):
        self.client = client
        self.transport = None
        self.processor = None
        self.reconnects = 0
        self._full_msg = bytearray()
        self._full_msg_type = None
        
    async def _delay_subscription(self):
        # Wait for 3 seconds after authentication before subscribing
        await asyncio.sleep(3)
        if self.client.schedule_resub:
            self.client._handle_subscriptions()
        
    def on_ws_connected(self, transport: WSTransport):
        """Called when WebSocket connection is established"""
        self.transport = transport
        logger.info("WebSocket connected")
        # Send auth message
        transport.send(WSMsgType.TEXT, self.client.json.dumps({"action": "auth", "params": self.client.api_key}))
    
    def on_ws_frame(self, transport: WSTransport, frame: WSFrame):
        """Called when a WebSocket frame is received"""
        # Handle control frames
        if frame.msg_type == WSMsgType.CLOSE:
            close_code = frame.get_close_code()
            close_message = frame.get_close_message()
            logger.info(f"WebSocket connection closed: code={close_code}, reason={close_message}")
            transport.send_close(close_code)
            return
        elif frame.msg_type == WSMsgType.PING:
            transport.send(WSMsgType.PONG, frame.get_payload_as_bytes())
            return
        elif frame.msg_type == WSMsgType.PONG:
            # Just ignore pong frames
            return
            
        # Handle data frames (with fragmentation support)
        if frame.fin:
            if self._full_msg:
                # Last fragment of a fragmented message
                if frame.msg_type == 0:  # Continuation frame
                    self._full_msg.extend(frame.get_payload_as_memoryview())
                    # Process the message directly without creating a copy
                    message = self._full_msg
                    msg_type = self._full_msg_type
                    # We'll clear the buffer after processing
                else:
                    # This shouldn't happen - fin=True but not a continuation frame
                    logger.warning(f"Unexpected frame type {frame.msg_type} with fin=True when processing fragmented message")
                    self._full_msg.clear()
                    self._full_msg_type = None
                    return
            else:
                # Single-frame message
                if frame.msg_type == WSMsgType.TEXT:
                    message = frame.get_payload_as_memoryview()
                    msg_type = WSMsgType.TEXT
                else:
                    logger.info(f"Ignoring unexpected frame type: {frame.msg_type}")
                    return
        else:
            # Fragment (not final)
            if not self._full_msg:
                # First fragment
                self._full_msg_type = frame.msg_type
            self._full_msg.extend(frame.get_payload_as_memoryview())
            return  # Wait for more fragments
            
        # Process the complete message
        try:
            # Handle potential JSON parsing errors more gracefully
            try:
                msgJson = self.client.json.loads(message)
            except json.JSONDecodeError as json_err:
                error_pos = json_err.pos
                logger.error(f"JSON decode error at position {error_pos}: {json_err}")
                
                # Try to recover by trimming the message if it appears to be truncated
                if "unexpected end of data" in str(json_err):
                    # Find the last complete JSON object by looking for the last '}]' sequence
                    last_complete = message.rfind(b'}]' if isinstance(message, (bytes, bytearray, memoryview)) else '}]')
                    if last_complete > 0:
                        try:
                            # Try parsing up to the last complete object
                            fixed_msg = message[:last_complete+2]
                            msgJson = self.client.json.loads(fixed_msg)
                            logger.info(f"Recovered from truncated JSON by trimming to length {len(fixed_msg)}")
                        except json.JSONDecodeError:
                            raise json_err
                    else:
                        raise json_err
                else:
                    raise json_err
            
            # Handle auth response
            if msgJson and isinstance(msgJson, list) and len(msgJson) > 0 and "status" in msgJson[0]:
                if msgJson[0]["status"] == "auth_failed":
                    logger.error(f"Authentication failed: {msgJson[0]['message']}")
                    transport.send_close(WSCloseCode.PROTOCOL_ERROR)
                    return
                elif msgJson[0]["status"] == "connected":
                    logger.info("Authentication successful")
                    # Add a delay before subscribing to ensure auth is fully processed
                    asyncio.create_task(self._delay_subscription())
                    return
            
            # Handle regular messages
            if not self.client.raw:
                has_status_messages = False  # Initialize the variable
                for m in msgJson:
                    # Check for subscription status messages
                    if "ev" in m and m["ev"] == "status":
                        has_status_messages = True
                        status_msg = m.get('message', '')
                        logger.info(f"Status message: {status_msg}")
                
                # Only parse if we have messages to process
                if msgJson and (not has_status_messages or len(msgJson) > 1):
                    cmsg = parse(msgJson, logger)
                else:
                    cmsg = []
            else:
                cmsg = message
                
            if cmsg and len(cmsg) > 0 and self.client.processor:
                # Create a task for async processing
                asyncio.create_task(self.client.processor(cmsg))
                
        except Exception as e:
            # Log the error with minimal message details
            logger.error(f"Error processing message: {e}")
            traceback.print_exc()
        finally:
            # Clear the buffer if we were using a fragmented message
            if self._full_msg and msg_type == self._full_msg_type:
                self._full_msg.clear()
                self._full_msg_type = None
            
    def on_ws_disconnected(self, transport):
        """Called when WebSocket connection is closed"""
        logger.info("WebSocket connection closed")
        self.reconnects += 1
        self.client.scheduled_subs = set(self.client.subs)
        self.client.subs = set()
        self.client.schedule_resub = True


class WebSocketClient:
    def __init__(
        self,
        api_key: Optional[str] = os.getenv(env_key),
        feed: Union[str, Feed] = Feed.RealTime,
        market: Union[str, Market] = Market.Stocks,
        raw: bool = False,
        verbose: bool = False,
        subscriptions: Optional[List[str]] = None,
        max_reconnects: Optional[int] = 5,
        secure: bool = True,
        custom_json: Optional[Any] = None,
        **kwargs,
    ):
        """
        Initialize a Polygon WebSocketClient.

        :param api_key: Your API key.
        :param feed: The feed to subscribe to.
        :param raw: Whether to pass raw Union[str, bytes] to user callback.
        :param verbose: Whether to log client and server status messages.
        :param subscriptions: List of subscription parameters.
        :param max_reconnects: How many times to reconnect on network outage before ending .connect event loop.
        :param custom_json: Optional module exposing loads/dumps functions (similar to Python's json module) to be used for JSON conversions.
        :return: A client.
        """
        if api_key is None:
            raise AuthError(
                f"Must specify env var {env_key} or pass api_key in constructor"
            )
        self.api_key = api_key
        self.feed = feed
        self.market = market
        self.raw = raw
        if verbose:
            logger.setLevel(logging.DEBUG)
        self.websocket_cfg = kwargs
        if isinstance(feed, Enum):
            feed = feed.value
        if isinstance(market, Enum):
            market = market.value
        self.url = f"ws{'s' if secure else ''}://{feed}/{market}"
        self.subscribed = False
        self.subs: Set[str] = set()
        self.max_reconnects = max_reconnects
        self.transport = None
        self.listener = PolygonWSListener(self)
        self.processor = None
        if subscriptions is None:
            subscriptions = []
        self.scheduled_subs: Set[str] = set(subscriptions)
        self.schedule_resub = True
        if custom_json:
            self.json = custom_json
        else:
            self.json = json


    
    async def connect(
        self,
        processor: Union[
            Callable[[List[WebSocketMessage]], Awaitable],
            Callable[[Union[str, bytes]], Awaitable],
        ],
        close_timeout: int = 1,
        **kwargs,
    ):
        """
        Connect to websocket server and run `processor(msg)` on every new `msg`.

        :param processor: The callback to process messages.
        :param close_timeout: How long to wait for handshake when calling .close.
        :raises AuthError: If invalid API key is supplied.
        """
        logger.info(f"Connecting to: {self.url}")
        self.processor = processor
        
        # For picows, we don't need to explicitly pass SSL context
        # The library handles secure connections based on the URL scheme
        

        
        # Always resubscribe on reconnect
        self.schedule_resub = True
            
        while True:
            try:
                # Connect using picows
                _, client = await ws_connect(
                    lambda: self.listener, 
                    self.url, 
#                    max_frame_size=1024*1024,  # from 10K to 1MB
                    **kwargs
                )
                self.transport = client.transport
                
                # Wait for disconnection
                await self.transport.wait_disconnected()
                
                # Check if we should reconnect
                if (self.max_reconnects is not None and 
                    self.listener.reconnects > self.max_reconnects):
                    logger.info(f"Max reconnects ({self.max_reconnects}) reached")
                    break
                    
                # Wait before reconnecting
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Connection error: {e}")
                traceback.print_exc()
                await asyncio.sleep(1)
                
                # Check if we should reconnect
                self.listener.reconnects += 1
                if (self.max_reconnects is not None and 
                    self.listener.reconnects > self.max_reconnects):
                    logger.info(f"Max reconnects ({self.max_reconnects}) reached")
                    break
    

    
    def _handle_subscriptions(self):
        """Handle subscription reconciliation"""
        if not self.transport:
            return
            
        logger.info("Reconciling subscriptions")
        
        # Always send all subscriptions to ensure they're properly registered
        # This is more reliable than just sending the difference
        if self.scheduled_subs:
            subs = ",".join(self.scheduled_subs)
            logger.info(f"Subscribing to all: {subs}")
            self.transport.send(WSMsgType.TEXT, 
                self.json.dumps({"action": "subscribe", "params": subs})
            )

            
        # Handle unsubscriptions
        old_subs = self.subs.difference(self.scheduled_subs)
        if old_subs:
            subs = ",".join(old_subs)
            logger.info(f"Unsubscribing from: {subs}")
            self.transport.send(WSMsgType.TEXT, 
                self.json.dumps({"action": "unsubscribe", "params": subs})
            )
            
        self.subs = set(self.scheduled_subs)
        self.schedule_resub = False
        


    def run(
        self,
        handle_msg: Union[
            Callable[[List[WebSocketMessage]], None],
            Callable[[Union[str, bytes]], None],
        ],
        close_timeout: int = 1,
        **kwargs,
    ):
        """
        Connect to websocket server and run `processor(msg)` on every new `msg`. Synchronous version of `.connect`.

        :param processor: The callback to process messages.
        :param close_timeout: How long to wait for handshake when calling .close.
        :raises AuthError: If invalid API key is supplied.
        """

        async def handle_msg_wrapper(msgs):
            handle_msg(msgs)

        asyncio.run(self.connect(handle_msg_wrapper, close_timeout, **kwargs))

    async def _subscribe(self, topics: Union[List[str], Set[str]]):
        if self.transport is None or len(topics) == 0:
            return
        subs = ",".join(topics)
        logger.info(f"Subscribing to: {subs}")
        self.transport.send(WSMsgType.TEXT,
            self.json.dumps({"action": "subscribe", "params": subs})
        )

    async def _unsubscribe(self, topics: Union[List[str], Set[str]]):
        if self.transport is None or len(topics) == 0:
            return
        subs = ",".join(topics)
        logger.info(f"Unsubscribing from: {subs}")
        self.transport.send(WSMsgType.TEXT,
            self.json.dumps({"action": "unsubscribe", "params": subs})
        )

    @staticmethod
    def _parse_subscription(s: str):
        s = s.strip()
        split = s.split(".", 1)  # Split at the first period
        if len(split) != 2:
            logger.warning("invalid subscription:", s)
            return [None, None]

        return split

    def subscribe(self, *subscriptions: str):
        """
        Subscribe to given subscriptions.

        :param subscriptions: Subscriptions (args)
        """
        for s in subscriptions:
            topic, sym = self._parse_subscription(s)
            if topic == None:
                continue
            logger.info("Adding subscription: %s", s)
            self.scheduled_subs.add(s)
            # If user subs to X.*, remove other X.\w+
            if sym == "*":
                for t in list(self.subs):
                    if t.startswith(topic):
                        self.scheduled_subs.discard(t)

        self.schedule_resub = True

    def unsubscribe(self, *subscriptions: str):
        """
        Unsubscribe from given subscriptions.

        :param subscriptions: Subscriptions (args)
        """
        for s in subscriptions:
            topic, sym = self._parse_subscription(s)
            if topic == None:
                continue
            logger.info("Removing subscription: %s", s)
            self.scheduled_subs.discard(s)

            # If user unsubs to X.*, remove other X.\w+
            if sym == "*":
                for t in list(self.subs):
                    if t.startswith(topic):
                        self.scheduled_subs.discard(t)

        self.schedule_resub = True

    def unsubscribe_all(self):
        """
        Unsubscribe from all subscriptions.
        """
        self.scheduled_subs = set()
        self.schedule_resub = True

    async def close(self):
        """
        Close the websocket connection.
        """
        logger.debug("closing")

        if self.transport:
            self.transport.send_close(WSCloseCode.GOING_AWAY)
            self.transport = None
        else:
            logger.warning("no websocket connection open to close")
