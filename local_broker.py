import asyncio
import logging
from asyncio import CancelledError

from amqtt.broker import Broker
from amqtt.plugins.logging import EventLoggerPlugin, PacketLoggerPlugin
from amqtt.plugins.authentication import AnonymousAuthPlugin, FileAuthPlugin
from amqtt.plugins.manager import PluginManager
from amqtt.plugins.topic_checking import TopicTabooPlugin, TopicAccessControlListPlugin

print("start")
async def start_broker():
    config = {
        'listeners': {
            'default': {
                'type': 'tcp',
                'bind': 'localhost:1883'  # Replace with the desired host and port
            }
        },
        'sys_interval': 10,
        'auth': {
            'allow-anonymous': True  # Set to False to require authentication
        },
        'topic-check':{
        'enabled': True,  # Enable topic checking
        'plugins': [
            EventLoggerPlugin,
            PacketLoggerPlugin,
            AnonymousAuthPlugin,
            FileAuthPlugin,
            PluginManager,
            TopicTabooPlugin,
            TopicAccessControlListPlugin
        ]  # List of plugins to perform topic filtering
    }
    }

    broker = Broker(config)
    await broker.start()
    print("Broker started and listening for connections...")

    # Run the broker until a KeyboardInterrupt (Ctrl+C) is received
    try:
        while True:

            try:
                await asyncio.sleep(2)
            except CancelledError:
                print("--------Broker stopped--------")

    except KeyboardInterrupt:
        await broker.shutdown()


# Run the event loop to start the broker
if __name__ == "__main__":
    formatter = "[%(asctime)s] :: %(levelname)s :: %(name)s :: %(message)s"
    # formatter = "%(asctime)s :: %(levelname)s :: %(message)s"
    # logging.basicConfig(level=logging.DEBUG, format=formatter)

    # logging.basicConfig(level=logging.DEBUG, format=formatter)
    # logging.basicConfig(level=logging.WARNING, format=formatter)
    # logging.basicConfig(level=logging.ERROR, format=formatter)
    # logging.basicConfig(level=logging.CRITICAL, format=formatter)
    # logging.debug('This is a debug message')
    # asyncio.get_event_loop().run_until_complete(test_coro())
    # asyncio.get_event_loop().run_forever()

    asyncio.run(start_broker())
