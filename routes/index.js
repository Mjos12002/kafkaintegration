var express = require('express');
const fs = require('fs');
var path = require('path');
const { Kafka } = require('kafkajs')
var router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
  
  const kafka = new Kafka({
    clientId: 'test-kafka-cluster-2',
    brokers: ['uatcons.crvs.nida.gov.rw:49092'],
    groupId: 'moh-crvs',
    ssl: true,
    // ssl: {
    //   rejectUnauthorized: true,
    //   ca: [fs.readFileSync('accesskey/crvs-cert.pem', 'utf-8')]
    // },
    sasl: {
      mechanism: 'SCRAM-SHA-256',
      username: 'moh-broker',
      password: 'moh-broker-91823'
    }
  })
  const consumer = kafka.consumer({ groupId: 'moh-crvs' })
  checkconnectivity(consumer);
  res.send('How is it');
});
router.get('/welcome', function(req, res, next) {
  res.send("I m returning now or not");
})

async function checkconnectivity(consumer){
  await consumer.connect()
  await consumer.subscribe({ topic: 'death', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      })
    },
  })
}

module.exports = router;


