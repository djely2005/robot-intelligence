use rumqttc::{MqttOptions, AsyncClient, Event, EventLoop, Incoming, QoS};
use tokio::{task, time};
use std::time::Duration;
use serde::{Serialize, Deserialize};
use reqwest;

#[derive(Serialize)]
struct MqttCommand {
    tag_id: String,
    floor: i32,
}

#[derive(Deserialize)]
struct DbCommand {
    id: String,
    tag_id: String,
    floor: i32,
}

#[derive(Deserialize, Serialize, Debug)]
struct RobotFeedback {
    tag_id: String,
    floor: i32,
}

#[tokio::main]
async fn main() {
    // MQTT setup
    let mut mqttoptions = MqttOptions::new("command-handler", "127.0.0.1", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    // Subscribe to robot feedback topic
    client.subscribe("robot/feedback", QoS::AtLeastOnce).await.unwrap();
    client.subscribe("robot/put", QoS::AtLeastOnce).await.unwrap();

    // Clone for publishing
    let publisher_client = client.clone();

    // Task to handle incoming MQTT messages
    task::spawn(async move {
        while let Ok(event) = eventloop.poll().await {
            if let Event::Incoming(Incoming::Publish(publish)) = event {
                println!("Received message on topic {}: {:?}", publish.topic, publish.payload);

                // Parse feedback message
                if let Ok(feedback) = serde_json::from_slice::<RobotFeedback>(&publish.payload) {
                    println!("Parsed feedback: {:?}", feedback);
                    if(publish.topic == "robot/put"){
                        let client = reqwest::Client::new();
                        let res = client
                            .post(&format!("http://localhost:3000/commands/{}/complete", feedback.tag_id))
                            .send()
                            .await;

                        match res {
                            Ok(resp) => println!("Successfully posted feedback: {}", resp.status()),
                            Err(err) => eprintln!("Error sending feedback: {:?}", err),
                        }
                    }
                    if (publish.topic == "robot/feedback"){
                        // Send POST request to backend
                        let client = reqwest::Client::new();
                        let res = client
                            .post("http://localhost:3000/robot/feedback")
                            .json(&feedback)
                            .send()
                            .await;

                        match res {
                            Ok(resp) => println!("Successfully posted feedback: {}", resp.status()),
                            Err(err) => eprintln!("Error sending feedback: {:?}", err),
                        }
                    }

                } else {
                    eprintln!("Failed to parse robot feedback payload.");
                }
            }
        }
    });

    // Main loop: fetch and publish commands
    loop {
        // Get pending commands from API
        let pending_commands = reqwest::get("http://localhost:3000/commands/pending")
            .await
            .unwrap()
            .json::<Vec<DbCommand>>()
            .await
            .unwrap();

        for cmd in pending_commands {
            let mqtt_cmd = MqttCommand {
                tag_id: cmd.tag_id.clone(),
                floor: cmd.floor,
            };

            let json_payload = serde_json::to_string(&mqtt_cmd).unwrap();

            println!("Publishing command: {}", json_payload);
            publisher_client
                .publish("robot/commands", QoS::AtLeastOnce, false, json_payload.into_bytes())
                .await
                .unwrap();

            // Mark command complete in backend
            reqwest::Client::new()
                .post(&format!("http://localhost:3000/commands/{}/complete", cmd.id))
                .send()
                .await
                .unwrap();

            time::sleep(Duration::from_millis(500)).await;
        }

        time::sleep(Duration::from_secs(5)).await;
    }
}

