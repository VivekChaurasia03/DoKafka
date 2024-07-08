const { kafka } = require("./client");

async function init() {
    const admin = kafka.admin();
    try {
        console.log("Admin Connecting...");
        await admin.connect();
        console.log("Admin Connected");

        const topicsToCreate = [
            {
                topic: "rider-updates",
                numPartitions: 2,
            },
        ];

        console.log(
            `Creating Topics: ${topicsToCreate.map((t) => t.topic).join(", ")}`
        );
        const created = await admin.createTopics({
            topics: topicsToCreate,
        });

        if (created) {
            console.log(
                `Topics Created: ${topicsToCreate
                    .map((t) => t.topic)
                    .join(", ")}`
            );
        } else {
            console.log(
                "No new topics were created, they might already exist."
            );
        }

        console.log("Disconnecting admin...");
        await admin.disconnect();
        console.log("Admin disconnected");
    } catch (err) {
        console.error("Error in admin operations:", err);
        try {
            await admin.disconnect();
        } catch (disconnectErr) {
            console.error(
                "Failed to disconnect admin after error:",
                disconnectErr
            );
        }
        process.exit(1);
    }
}

init().catch((err) => {
    console.error("Failed to initialize admin:", err);
    process.exit(1);
});
