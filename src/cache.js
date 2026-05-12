import Redis from "ioredis";

const redisUrl = process.env.REDIS_URL;

export const redis = redisUrl ? new Redis(redisUrl, { lazyConnect: true }) : null;

export async function getRedisStatus() {
  if (!redis) {
    return "disabled";
  }

  try {
    if (redis.status === "wait") {
      await redis.connect();
    }

    await redis.ping();
    return "ok";
  } catch {
    return "error";
  }
}
