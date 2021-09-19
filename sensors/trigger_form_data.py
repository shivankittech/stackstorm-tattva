from st2reactor.sensor.base import Sensor
import pika, sys, os

class triggerFormData(Sensor):
    
            
    def setup(self):
        connection = pika.BlockingConnection(pika.URLParameters('amqps://phrcvsqp:VPGdSRQc-pMIBkv3jwGlHbTp5wnKLghm@vulture.rmq.cloudamqp.com/phrcvsqp'))
        channel = connection.channel()
        channel.queue_declare(queue='forms_data', durable=True)
        
    
    def run(self,channel):
        def callback(ch, method, properties, body):
            self._on_message(body)
                    
        
        self.channel.basic_consume(queue='forms_data', on_message_callback=callback, auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()
        
        
    def cleanup(self):
        pass
        

    def add_trigger(self, trigger):
        pass

        

    def update_trigger(self, trigger):
        pass

    def remove_trigger(self, trigger):
        pass

    def _on_connect(self, client, userdata, flags, rc):
        self._logger.debug('[MQTTSensor]: Connected with code {}' + str(rc))
        self.isMqttConnected = True
        for topic in self._topicTriggers:        
            self._logger.debug('[MQTTSensor]: Sub to ' + str(topic))
            self._client.subscribe(topic)

    def _on_message(self, msg):
        message = msg.payload.decode("utf-8")
        payload = {
            
            'message': str(message),
            
        }
        self._sensor_service.dispatch(trigger=self._topicTriggers[msg.topic], payload=payload)
