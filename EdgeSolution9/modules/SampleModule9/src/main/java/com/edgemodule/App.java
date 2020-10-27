package com.edgemodule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Subscription;

public class App {
    private static MessageCallbackMqtt msgCallback = new MessageCallbackMqtt();
    private static EventCallback eventCallback = new EventCallback();
    //private static final String INPUT_NAME = "input1";
    private static final String OUTPUT_NAME = "output1";
    public static String msgNats;
    public static Message msgMS;
    public static IotHubClientProtocol protocol;
    public static DeviceClient dclient;
    public static ModuleClient client;
	public static String connString = "HostName=NatsIothub123.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=oI52BbMlDj9I2JsFQcUVjYatHNySPO82lqJXM64d9fQ=";

    protected static class EventCallback implements IotHubEventCallback {
        @Override
        public void execute(IotHubStatusCode status, Object context) {
            if (context instanceof Message) {
                System.out.println("Send message with status: " + status.name());
            } else {
                System.out.println("Invalid context passed");
            }
        }
    }

    protected static class MessageCallbackMqtt implements MessageCallback {
        private int counter = 0;

        @Override
        public IotHubMessageResult execute(Message msg, Object context) {
            this.counter += 1;

            System.out.println(String.format("Received message %d: %s", this.counter,
                    new String(msgMS.getBytes(), Message.DEFAULT_IOTHUB_MESSAGE_CHARSET)));
            if (context instanceof ModuleClient) {
                ModuleClient client = (ModuleClient) context;
                client.sendEventAsync(msgMS, eventCallback, msgMS, App.OUTPUT_NAME);
            }
            return IotHubMessageResult.COMPLETE;
        }
    }

    protected static class ConnectionStatusChangeCallback implements IotHubConnectionStatusChangeCallback {

        @Override
        public void execute(IotHubConnectionStatus status, IotHubConnectionStatusChangeReason statusChangeReason,
                Throwable throwable, Object callbackContext) {
            String statusStr = "Connection Status: %s";
            switch (status) {
                case CONNECTED:
                    System.out.println(String.format(statusStr, "Connected"));
                    break;
                case DISCONNECTED:
                    System.out.println(String.format(statusStr, "Disconnected"));
                    if (throwable != null) {
                        throwable.printStackTrace();
                    }
                    System.exit(1);
                    break;
                case DISCONNECTED_RETRYING:
                    System.out.println(String.format(statusStr, "Retrying"));
                    break;
                default:
                    break;
            }
        }
    }

    protected static class NatsSubscriptionRunnable implements Runnable {
        
        public void run() {
            String server = "nats://demo.nats.io:4222";
            Connection nc;

            try {

                nc = Nats.connect(server);
                Subscription sub = nc.subscribe("Schaeffler");
                nc.publish("yuri", "Subscribed to Nats99".getBytes(StandardCharsets.UTF_8));

                try {
                    client.setMessageCallback(msgCallback, client);
                    client.registerConnectionStatusChangeCallback(new ConnectionStatusChangeCallback(), null);
                    client.open();
                    // dclient = new DeviceClient(connString, protocol);
                    // dclient.open();
                    nc.publish("yuri", "Connected to IoT Hub".getBytes(StandardCharsets.UTF_8));
                } catch (IllegalArgumentException | IOException e) {
                    nc.publish("yuri", "Connection error".getBytes(StandardCharsets.UTF_8));
                    e.printStackTrace();
                }
                
                

                while (true){
                    nc.publish("yuri", "reading msg".getBytes(StandardCharsets.UTF_8));
                    io.nats.client.Message msg = sub.nextMessage(Duration.ZERO);
                    msgNats = new String(msg.getData(), StandardCharsets.UTF_8);
                    System.out.println("msg received by NATS : "+msgNats);
                    nc.publish("yuri", msgNats.getBytes(StandardCharsets.UTF_8));
                    msgMS = new Message(msgNats);
                    
                    client.sendEventAsync(msgMS, eventCallback, msgMS, App.OUTPUT_NAME);
                    nc.publish("yuri", "SENT".getBytes(StandardCharsets.UTF_8));
                    Thread mthread = new Thread(new MessageSender());
                    mthread.start();
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    protected static class MessageSender implements Runnable {
		public void run() {
			try {
                
                

				Object lockobj = new Object();

				// Send the message.
				// EventCallback callback = new EventCallback();
				// dclient.sendEventAsync(msgMS, eventCallback, lockobj);

				synchronized (lockobj) {
					lockobj.wait();
				}
			} catch (InterruptedException e) {
				System.out.println("Finished.");
			}
	    }
	}

    public static void main(String[] args) {
        try {
            
            protocol = IotHubClientProtocol.MQTT;
            System.out.println("Start to create client with MQTT protocol");
            client = ModuleClient.createFromEnvironment(protocol);

            

            Thread thread = new Thread(new NatsSubscriptionRunnable());
            thread.start();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
