import { client } from "./redis";

async function main() {
  // Create 3 consumer groups In, US, and EU
  //   await client.xgroup("CREATE", "betterstack:website", "us", "0", "MKSTREAM");
  //   await client.xgroup("CREATE", "betterstack:website", "eu", "0", "MKSTREAM");
  //   await client.xgroup("CREATE", "betterstack:website", "in", "0", "MKSTREAM");

  //   console.log("Consumer groups created: us, eu, in");

  // See the consumer groups info
  //   const groups = await client.xinfo("GROUPS", "betterstack:website");
  //   console.log("Consumer groups info:", groups);

  // Add data to the stream
  await client.xadd(
    "betterstack:website",
    "*",
    "url",
    "https://A.com",
    "id",
    "1"
  );
  await client.xadd(
    "betterstack:website",
    "*",
    "url",
    "https://B.com",
    "id",
    "1"
  );
  await client.xadd(
    "betterstack:website",
    "*",
    "url",
    "https://C.com",
    "id",
    "1"
  );
  console.log("Data added to stream 'betterstack:website'");

  // Read all data from the stream
  //   const streamData = await client.xread("STREAMS", "betterstack:website", "0");
  //   console.log("Stream data:", streamData);

  // Read messages from the 'us' consumer group with a consumer named 'us-consumer-1'
  const messagesUS = await client.xreadgroup(
    "GROUP",
    "us", // Consumer group name
    "us-consumer-1", // Consumer name
    "COUNT",
    1, // Read only 1 message
    "STREAMS",
    "betterstack:website", // Stream name
    ">"
  );

  console.log("Messages read from 'us' consumer group:", messagesUS);

  // Read messages from the 'us' consumer group with a different consumer named 'us-consumer-2'
  const messagesUS2 = await client.xreadgroup(
    "GROUP",
    "us", // Consumer group name
    "us-consumer-2", // Different consumer name
    "COUNT",
    1, // Read only 1 message
    "STREAMS",
    "betterstack:website", // Stream name
    ">"
  );
  console.log(
    "Messages read from 'us' consumer group (second consumer):",
    messagesUS2
  );

  const messagesUS3 = await client.xreadgroup(
    "GROUP",
    "us", // Consumer group name
    "us-consumer-3", // Another consumer name
    "COUNT",
    1, // Read only 1 message
    "STREAMS",
    "betterstack:website", // Stream name
    ">"
  );
  console.log(
    "Messages read from 'us' consumer group (third consumer):",
    messagesUS3
  );

  // Read messages from the 'eu' consumer group with a consumer named 'eu-consumer-1'
  const messagesEU = await client.xreadgroup(
    "GROUP",
    "eu", // Consumer group name
    "eu-consumer-1", // Consumer name
    "COUNT",
    1, // Number of messages to read
    "STREAMS",
    "betterstack:website", // Stream name
    ">"
  );

  console.log("Messages read from 'eu' consumer group:", messagesEU);

  // One more read from 'eu' consumer group with a different consumer
  const messagesEU2 = await client.xreadgroup(
    "GROUP",
    "eu", // Consumer group name
    "eu-consumer-2", // Different consumer name
    "COUNT",
    2, // Number of messages to read
    "STREAMS",
    "betterstack:website", // Stream name
    ">"
  );

  console.log(
    "Messages read from 'eu' consumer group (second consumer):",
    messagesEU2
  );

  // Read messages from the 'in' consumer group with a consumer named 'in-consumer-1'
  const messagesIN = await client.xreadgroup(
    "GROUP",
    "in", // Consumer group name
    "in-consumer-1", // Consumer name
    "COUNT",
    2, // Number of messages to read
    "STREAMS",
    "betterstack:website", // Stream name
    ">"
  );

  console.log("Messages read from 'in' consumer group:", messagesIN);

  // One more read from 'in' consumer group with a different consumer
  const messagesIN2 = await client.xreadgroup(
    "GROUP",
    "in", // Consumer group name
    "in-consumer-2", // Different consumer name
    "COUNT",
    1, // Number of messages to read
    "STREAMS",
    "betterstack:website", // Stream name
    ">"
  );

  console.log(
    "Messages read from 'in' consumer group (second consumer):",
    messagesIN2
  );

  // Close the Redis client connection
  await client.quit();
  console.log("Redis client connection closed.");
}

main().catch(console.error);
