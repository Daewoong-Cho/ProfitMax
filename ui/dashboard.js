import React, { useEffect } from 'react';
import { Kafka, logLevel } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  logLevel: logLevel.ERROR, // 로그 레벨 설정 (옵션)
});

const consumer = kafka.consumer({ groupId: 'my-group' });

const KafkaConsumer = () => {
  useEffect(() => {
    const runConsumer = async () => {
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic', fromBeginning: true });

      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          console.log({
            topic,
            partition,
            offset: message.offset,
            value: message.value.toString(),
          });
        },
      });
    };

    runConsumer();

    // 컴포넌트 언마운트 시 Kafka 연결 해제
    return () => {
      consumer.disconnect();
    };
  }, []);

  return (
    <div>
      <h1>Kafka Consumer</h1>
      {/* 여기에 메시지를 표시하거나 처리하는 로직을 추가하세요 */}
    </div>
  );
};

export default KafkaConsumer;
