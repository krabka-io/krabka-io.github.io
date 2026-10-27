// npm install kafkajs@2.2.4
// Save as app.cjs and run: node app.cjs
const { Kafka } = require('kafkajs');

async function main() {
    const kafka = new Kafka({ clientId: 'quickstart', brokers: ['localhost:9092'] });
    const producer = kafka.producer();
    await producer.connect();
    try {
        await producer.send({
            topic: 'quickstart-events',
            messages: [{ key: 'user-1', value: 'Hello Krabka from KafkaJS!' }],
        });
        console.log('Record delivered to quickstart-events.');
    } finally {
        await producer.disconnect();
    }
}

main().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
