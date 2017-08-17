const IncomingWebhook = require('@slack/client').IncomingWebhook;
const SLACK_WEBHOOK_URL = "";

const webhook = new IncomingWebhook(SLACK_WEBHOOK_URL);

module.exports.subscribe_alert = (event, callback) => {
  console.log("subscribe_alert is invoked.")
  console.log(event);
  const pubsubMessage = event.data;
  const data = JSON.parse(new Buffer(pubsubMessage.data, 'base64').toString());
  const message = {
    text: data.message
  }
  console.log(message);
  
  webhook.send(message, (err, res) => {
    console.log("Get a response. " + res)
    if (err) {
      console.log('Error:', err);
    }
    callback(err);
  });
};
