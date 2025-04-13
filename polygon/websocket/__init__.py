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

env_key = "POLYGON_API_KEY"
logger = get_logger("WebSocketClient")


class PolygonWSListener(WSListener):
    def __init__(self, client):
        self.client = client
        self.transport = None
        self.processor = None
        self.reconnects = 0
        
    def on_ws_connected(self, transport: WSTransport):
        """Called when WebSocket connection is established"""
        self.transport = transport
        logger.debug("connected")
        # Send auth message
        transport.send(WSMsgType.TEXT, self.client.json.dumps({"action": "auth", "params": self.client.api_key}))
    
    def on_ws_frame(self, transport: WSTransport, frame: WSFrame):
        """Called when a WebSocket frame is received"""
        if frame.msg_type == WSMsgType.CLOSE:
            close_code = frame.get_close_code()
            close_reason = frame.get_close_reason()
            logger.debug(f"connection closed: code={close_code}, reason={close_reason}")
            transport.send_close(close_code)
            return
            
        if frame.msg_type != WSMsgType.TEXT:
            logger.debug(f"Received unexpected frame type: {frame.msg_type}")
            return
            
        message = frame.get_payload_as_ascii_text()
        
        # Process the message
        try:
            msgJson = self.client.json.loads(message)
            
            # Handle auth response
            if len(msgJson) > 0 and "status" in msgJson[0]:
                if msgJson[0]["status"] == "auth_failed":
                    logger.error(f"Authentication failed: {msgJson[0]['message']}")
                    transport.send_close(WSCloseCode.PROTOCOL_ERROR)
                    return
                elif msgJson[0]["status"] == "connected":
                    logger.debug(f"authed: {message}")
                    # Handle subscriptions after successful auth
                    if self.client.schedule_resub:
                        self.client._handle_subscriptions()
                    return
            
            # Handle regular messages
            if not self.client.raw:
                for m in msgJson:
                    if "ev" in m and m["ev"] == "status":
                        logger.debug(f"status: {m.get('message', '')}")
                        continue
                        
                cmsg = parse(msgJson, logger)
            else:
                cmsg = message
                
            if len(cmsg) > 0 and self.client.processor:
                asyncio.create_task(self.client.processor(cmsg))
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            
    def on_ws_disconnected(self, transport):
        """Called when WebSocket connection is closed"""
        logger.debug("WebSocket connection closed")
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
        logger.debug(f"connect: {self.url}")
        self.processor = processor
        
        # For picows, we don't need to explicitly pass SSL context
        # The library handles secure connections based on the URL scheme
            
        while True:
            try:
                # Connect using picows
                _, client = await ws_connect(
                    lambda: self.listener, 
                    self.url, 
                    **kwargs
                )
                self.transport = client.transport
                
                # Wait for disconnection
                await self.transport.wait_disconnected()
                
                # Check if we should reconnect
                if (self.max_reconnects is not None and 
                    self.listener.reconnects > self.max_reconnects):
                    logger.debug(f"Max reconnects ({self.max_reconnects}) reached")
                    break
                    
                # Wait before reconnecting
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Connection error: {e}")
                await asyncio.sleep(1)
                
                # Check if we should reconnect
                self.listener.reconnects += 1
                if (self.max_reconnects is not None and 
                    self.listener.reconnects > self.max_reconnects):
                    logger.debug(f"Max reconnects ({self.max_reconnects}) reached")
                    break
    
    def _handle_subscriptions(self):
        """Handle subscription reconciliation"""
        if not self.transport:
            return
            
        logger.debug(f"reconciling: {self.subs} {self.scheduled_subs}")
        
        # Handle new subscriptions
        new_subs = self.scheduled_subs.difference(self.subs)
        if new_subs:
            subs = ",".join(new_subs)
            logger.debug(f"subbing: {subs}")
            self.transport.send(WSMsgType.TEXT, 
                self.json.dumps({"action": "subscribe", "params": subs})
            )
            
        # Handle unsubscriptions
        old_subs = self.subs.difference(self.scheduled_subs)
        if old_subs:
            subs = ",".join(old_subs)
            logger.debug(f"unsubbing: {subs}")
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
        logger.debug(f"subbing: {subs}")
        self.transport.send(WSMsgType.TEXT,
            self.json.dumps({"action": "subscribe", "params": subs})
        )

    async def _unsubscribe(self, topics: Union[List[str], Set[str]]):
        if self.transport is None or len(topics) == 0:
            return
        subs = ",".join(topics)
        logger.debug(f"unsubbing: {subs}")
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
            logger.debug("sub desired: %s", s)
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
            logger.debug("sub undesired: %s", s)
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
