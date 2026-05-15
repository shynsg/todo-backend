import amqp from "amqplib";

const rabbitmqUrl = process.env.RABBITMQ_URL;
const exchangeName = process.env.TODO_EVENTS_EXCHANGE || "todo.events";

let channelPromise;

async function getChannel() {
  if (!rabbitmqUrl) {
    return null;
  }

  if (!channelPromise) {
    channelPromise = amqp.connect(rabbitmqUrl)
      .then(async (connection) => {
        connection.on("error", () => {
          channelPromise = null;
        });
        connection.on("close", () => {
          channelPromise = null;
        });

        const channel = await connection.createChannel();
        await channel.assertExchange(exchangeName, "topic", {
          durable: true
        });
        return channel;
      })
      .catch((error) => {
        channelPromise = null;
        throw error;
      });
  }

  return channelPromise;
}

export async function publishTodoCreated(todo) {
  const channel = await getChannel();

  if (!channel) {
    return {
      published: false,
      reason: "rabbitmq_disabled"
    };
  }

  const event = {
    eventId: `todo-created-${todo.id}-${Date.now()}`,
    eventType: "todo.created",
    version: 1,
    occurredAt: new Date().toISOString(),
    source: "todo-backend",
    data: {
      todoId: todo.id,
      title: todo.title,
      completed: todo.completed
    }
  };

  const published = channel.publish(
    exchangeName,
    "todo.created",
    Buffer.from(JSON.stringify(event)),
    {
      contentType: "application/json",
      deliveryMode: 2,
      messageId: event.eventId,
      timestamp: Math.floor(Date.now() / 1000),
      type: event.eventType
    }
  );

  return {
    published,
    eventId: event.eventId,
    exchange: exchangeName,
    routingKey: "todo.created"
  };
}
