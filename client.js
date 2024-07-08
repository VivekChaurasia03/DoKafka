const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["192.168.1.4:9092"],
});

exports.kafka = kafka;
