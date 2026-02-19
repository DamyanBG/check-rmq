import asyncio
import aio_pika
import aio_pika.abc


async def main():
    # Connect to RabbitMQ
    connection: aio_pika.abc.AbstractRobustConnection = await aio_pika.connect_robust(
        "amqp://guest:guest@192.168.68.126:5672/"
    )

    async with connection:
        queue_name = "postages"

        # Creating channel
        channel: aio_pika.abc.AbstractChannel = await connection.channel()

        # Declare queue passively (just check it, don't create if missing)
        queue: aio_pika.abc.AbstractQueue = await channel.declare_queue(
            queue_name,
            passive=True  # This won't create the queue if it doesn't exist
        )

        # Get message count from the declaration result
        result = queue.declaration_result

        print(f"\n{'='*50}")
        print(f"Queue: {queue_name}")
        print(f"{'='*50}")
        print(f"Messages in queue: {result.message_count}")
        print(f"Active consumers: {result.consumer_count}")
        print(f"{'='*50}\n")


if __name__ == "__main__":
    asyncio.run(main())
