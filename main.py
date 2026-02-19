import asyncio
import aio_pika
import aio_pika.abc


async def main(loop):
    # Connecting with the given parameters is also possible.
    # aio_pika.connect_robust(host="host", login="login", password="password")
    # You can only choose one option to create a connection, url or kw-based params.
    connection: aio_pika.abc.AbstractRobustConnection = await aio_pika.connect_robust(
        "amqp://guest:guest@192.168.68.126:5672/", loop=loop
    )

    async with connection:
        queue_name = "postages"

        # Creating channel
        channel: aio_pika.abc.AbstractChannel = await connection.channel()
        
        # Set QoS: process one message at a time
        await channel.set_qos(prefetch_count=1)

        # Declaring queue with manual acknowledgment (auto_ack=False)
        queue: aio_pika.abc.AbstractQueue = await channel.declare_queue(
            queue_name,
            auto_delete=False,
            durable=True
        )

        message_count = 0
        
        async with queue.iterator() as queue_iter:
            # Cancel consuming after __aexit__
            async for message in queue_iter:
                message_count += 1
                job_data = message.body.decode()
                print(f"\n[Message #{message_count}] Received: {job_data}")
                
                try:
                    # Simulate job processing with delay
                    print(f"⏳ Processing message #{message_count}...")
                    await asyncio.sleep(5)  # Simulate 2 second processing time
                    
                    # ACK every even message number, NACK every odd one
                    if message_count % 2 == 0:
                        print(f"✓ Processing successful - ACKing message #{message_count}")
                        await message.ack()
                    else:
                        print(f"✗ Processing failed - NACKing message #{message_count} (will requeue)")
                        # requeue=True: message goes back to queue for retry
                        # requeue=False: message goes to dead-letter queue
                        await message.nack(requeue=True)
                    
                except Exception as e:
                    print(f"✗ Error processing message: {e}")
                    await message.nack(requeue=True)
                
                # Exit after 10 messages for testing
                if message_count >= 10:
                    print("\n[Test Complete] Processed 10 messages, exiting...")
                    break


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()