from flask import Flask, request, jsonify
import logging
import os
import base64
import json
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# Configure for Cloud Run
PORT =8080

DB_HOST = os.getenv("DB_HOST", "host.docker.internal")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "mydb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")

def get_db_conn():
    try:
        logger.info(f"Attempting to connect to database at {DB_HOST}:{DB_PORT}")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        logger.info("Database connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        raise

# Configure loggingg
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    logger.info("Health check endpoint accessed")
    return jsonify({"status": "healthy", "service": "ingester-crash"}), 200
@app.route('/pubsub-ingest', methods=['POST'])
def user():
    logger.info("Received request for user endpoint")
    envelope = request.get_json()
    logger.info(f"Received envelope: {envelope}")
 
 
    try: 
        logger.info("Processing incoming data")
        
        # Check if this is a Pub/Sub message or direct API call
        if envelope and "message" in envelope and "data" in envelope["message"]:
            # Pub/Sub format - decode base64 data
            logger.info("Processing Pub/Sub format")
            message_data = envelope["message"]["data"]
            decoded_data = base64.b64decode(message_data).decode('utf-8')
            data = json.loads(decoded_data)
        else:
            # Direct API call format
            logger.info("Processing direct API call format")
            data = envelope if envelope else {}

        user_name = data.get("user_name")
        user_id = data.get("user_id")
        phone_number = data.get("phone_number")

        logger.info("Attempting to insert data into database")
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "INSERT INTO users(user_id,user_name,phone_number) VALUES (%s, %s, %s)",
                    (user_id, user_name, phone_number)
                )
                conn.commit()
                logger.info(f"Successfully inserted user data for user_id={user_id}, user_name={user_name}, phone_number={phone_number}")

        return jsonify({"status": "success"})
    except Exception as e:
        logger.error(f"Error processing user data: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 400


if __name__ == '__main__':
    logger.info("Starting Flask application on host 0.0.0.0, port 8080")
    app.run(host="0.0.0.0", port=8080, debug=False)
