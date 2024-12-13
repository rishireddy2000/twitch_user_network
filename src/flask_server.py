from flask import Flask, jsonify
from pyspark.sql import SparkSession
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

class SparkConnector:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("TwitchFriendsAPI") \
            .enableHiveSupport() \
            .getOrCreate()

        # Print tables for debugging
        print("\nAvailable tables:")
        self.spark.sql("SHOW TABLES").show()

    def get_connections(self, user_id):
        """Get all users connected to the given user_id"""
        query = """
        SELECT DISTINCT 
            CASE 
                WHEN source_user = {0} THEN friend
                ELSE source_user 
            END as connected_to,
            connection_strength
        FROM twitch_first_level_connections
        WHERE source_user = {0} OR friend = {0}
        ORDER BY connection_strength DESC
        """.format(user_id)

        print(f"Executing query: {query}")
        result = self.spark.sql(query)
        return result.toPandas()

spark_connector = SparkConnector()

@app.route('/api/connections/<int:user_id>')
def get_connections(user_id):
    try:
        print(f"Received request for user_id: {user_id}")
        connections_df = spark_connector.get_connections(user_id)

        connections = connections_df.to_dict('records')

        return jsonify({
            'status': 'success',
            'user_id': user_id,
            'connections': connections,
            'total_connections': len(connections)
        })

    except Exception as e:
        print("Error processing request:", str(e))
        import traceback
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    print("Starting Flask server...")
    app.run(host='0.0.0.0', port=5555, debug=True)