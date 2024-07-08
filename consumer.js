const { kafka } = require("./client");
const group = process.argv[2];

if (!group) {
    console.error("Please provide a consumer group ID as an argument.");
    process.exit(1);
}

async function init() {
    const consumer = kafka.consumer({ groupId: group });

    try {
        console.log(`Connecting consumer with group ID: ${group}`);
        await consumer.connect();
        console.log("Consumer connected!");

        await consumer.subscribe({
            topic: "rider-updates",
            fromBeginning: true,
        });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                console.log(
                    `${group}: [${topic}]: PART:${partition}:`,
                    message.value.toString()
                );
            },
        });

        // Graceful shutdown
        process.on("SIGINT", async () => {
            try {
                console.log("Disconnecting the consumer...");
                await consumer.disconnect();
                console.log("Consumer disconnected. Exiting...");
                process.exit(0);
            } catch (err) {
                console.error("Failed to disconnect the consumer:", err);
                process.exit(1);
            }
        });
    } catch (err) {
        console.error("Error in consumer:", err);
        try {
            await consumer.disconnect();
        } catch (disconnectErr) {
            console.error(
                "Failed to disconnect consumer after error:",
                disconnectErr
            );
        }
        process.exit(1);
    }
}

init().catch((err) => {
    console.error("Failed to initialize:", err);
    process.exit(1);
});
