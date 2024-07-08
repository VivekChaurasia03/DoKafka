const { kafka } = require("./client");
const readline = require("readline");

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

async function init() {
    const producer = kafka.producer();

    console.log("Connecting producer!");
    await producer.connect();
    console.log("Connected producer!");

    rl.setPrompt("> ");
    rl.prompt();

    rl.on("line", async function (line) {
        const [riderName, location] = line.split(" ");
        if (!riderName || !location) {
            console.log(
                "Invalid input. Please provide both rider name and location."
            );
            rl.prompt();
            return;
        }

        const partition =
            location.toLowerCase() === "north"
                ? 0
                : location.toLowerCase() === "south"
                ? 1
                : null;
        if (partition === null) {
            console.log("Invalid location. Please specify 'north' or 'south'.");
            rl.prompt();
            return;
        }

        try {
            await producer.send({
                topic: "rider-updates",
                messages: [
                    {
                        partition: partition,
                        key: "location-update",
                        value: JSON.stringify({
                            name: riderName,
                            loc: location,
                        }),
                    },
                ],
            });
            console.log(`Sent update for ${riderName} in ${location}.`);
        } catch (err) {
            console.error("Failed to send message:", err);
        }

        rl.prompt();
    }).on("close", async () => {
        console.log("Disconnecting the producer!");
        await producer.disconnect();
        console.log("Producer disconnected. Exiting...");
        process.exit(0);
    });
}

init().catch((err) => {
    console.error("Failed to initialize:", err);
    process.exit(1);
});
