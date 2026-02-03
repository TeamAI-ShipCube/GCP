import os
import json
from google.cloud import pubsub_v1, storage
from flask import Flask, request

app = Flask(__name__)

publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
TOPIC_ID = "central-event-bus"
BUCKET_NAME = f"raw-shiphero-events-{PROJECT_ID}"

@app.route('/webhook', methods=['POST'])
def receive_webhook():
    data = request.get_json()
    if not data:
        return "Invalid JSON", 400

    event_id = data.get('id', 'unknown_id')

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"events/{event_id}.json")
    blob.upload_from_string(json.dumps(data), content_type='application/json')

    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    publisher.publish(topic_path, json.dumps(data).encode("utf-8"))

    return f"Event {event_id} ingested", 200

# if __name__ == "__main__":
#     app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))


#run below in terminal
# gcloud run deploy shiphero-ingester \
#     --source . \
#     --region us-central1 \
#     --service-account sa-ingestion-svc@gemini-enterprise-481717.iam.gserviceaccount.com \
#     --allow-unauthenticated \
#     --set-env-vars GOOGLE_CLOUD_PROJECT=gemini-enterprise-481717

# Dataflow
# gcloud dataflow jobs run pubsub-to-gcs-backup \
#     --gcs-location gs://dataflow-templates-us-central1/latest/PubSub_to_GCS_Text \
#     --region us-central1 \
#     --parameters \
# inputTopic=projects/[PROJECT_ID]/topics/central-event-bus,\
# outputDirectory=gs://raw-shiphero-events-[PROJECT_ID]/dataflow_backup/,\
# outputFilenamePrefix=backup-,\
# outputFilenameSuffix=.json

# Cloud Scheduler
# Create a scheduler job to hit your ingestion service every hour
# gcloud scheduler jobs create http inventory-sync-job \
#     --schedule "0 * * * *" \
#     --uri "https://[YOUR-CLOUD-RUN-URL]/webhook" \
#     --http-method POST \
#     --message-body '{"type": "scheduled_sync", "target": "inventory"}' \
#     --headers "Content-Type=application/json"


