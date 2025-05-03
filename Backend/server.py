# import asyncio
# import websockets
# from websocket import WebSocketApp
# import json
# import boto3
# import decimal
#
# # Initialize DynamoDB resource
# dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
# table = dynamodb.Table("new_table")
#
# # Store the last processed payload
# last_processed_payload = None
#
# # Custom JSON encoder to handle Decimal objects from DynamoDB
# class DecimalEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o, decimal.Decimal):
#             return float(o)
#         return super().default(o)
#
# # Fetch data from DynamoDB and modify the data structure
# async def fetch_data_from_dynamodb():
#     global last_processed_payload
#     try:
#         response = table.scan()
#         items = response["Items"]
#
#         # Initialize data structure with default values
#         data = {
#             "SensorData": {
#                 "heartRate": 0,
#                 "temperature": 0.0,
#                 "ecg": 0
#             }
#         }
#
#         if items:
#             # Get the latest item based on timestamp
#             latest_item = max(items, key=lambda x: int(x.get("timestamp", 0)))
#             payload = latest_item.get("payload", {})
#
#             # Check if the payload has been updated
#             if payload != last_processed_payload:
#                 last_processed_payload = payload
#
#                 # Update data fields from the payload
#                 data["SensorData"]["heartRate"] = int(payload.get("heartRate", {}).get("N", 0))
#                 data["SensorData"]["temperature"] = float(payload.get("temperature", {}).get("N", 0.0))
#                 data["SensorData"]["ecg"] = int(payload.get("ecg", {}).get("N", 0))
#
#         return data
#     except Exception as e:
#         print(f"Error fetching data from DynamoDB: {e}")
#         return {}
#
# # Handle incoming client connections
# async def handle_client(websocket):
#     while True:
#         try:
#             message = await websocket.recv()
#             received_data = json.loads(message)
#             print("Received from client:", json.dumps(received_data, indent=4))
#
#         except websockets.exceptions.ConnectionClosed:
#             break
#
#         except Exception as e:
#             print(f"Error receiving data from client: {e}")
#             break
#
# # Send data from DynamoDB to WebSocket clients
# async def send_data_from_dynamodb(websocket, path):
#     receive_task = asyncio.create_task(handle_client(websocket))
#
#     try:
#         while True:
#             data = await fetch_data_from_dynamodb()
#             print("Fetched data from DynamoDB:", json.dumps(data, indent=4))
#
#             if data:
#                 await websocket.send(json.dumps(data))
#             else:
#                 print("No data to send.")
#
#             await asyncio.sleep(1)
#
#     except Exception as e:
#         print(f"Error sending data to client: {e}")
#
#     finally:
#         receive_task.cancel()
#         try:
#             await receive_task
#         except asyncio.CancelledError:
#             pass
#
# # Start the WebSocket server
# start_server = websockets.serve(send_data_from_dynamodb, "127.0.0.1", 5050)
#
# asyncio.get_event_loop().run_until_complete(start_server)
# asyncio.get_event_loop().run_forever()


import asyncio
import websockets
import json
import boto3
import decimal
from boto3.dynamodb.conditions import Key

# Initialize DynamoDB resource
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("project1_db")


# Custom JSON encoder to handle Decimal objects from DynamoDB
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super().default(o)


# Fetch the latest data from DynamoDB
async def fetch_latest_data():
    try:
        # Query DynamoDB for the most recent item (assuming "timestamp" is the sorting key)
        response = table.query(
            KeyConditionExpression=Key("your_partition_key").eq("your_partition_value"),
            ScanIndexForward=False,  # Get latest data first
            Limit=1  # Only fetch the most recent record
        )

        items = response.get("Items", [])

        # Initialize default data structure
        data = {
            "SensorData": {
                "heartRate": 0,
                "temperature": 0.0
            }
        }

        if items:
            latest_item = items[0]  # Get the latest item
            payload = latest_item.get("payload", {})

            # Extract sensor data
            data["SensorData"]["heartRate"] = int(payload.get("heartRate", {}).get("N", 0))
            data["SensorData"]["temperature"] = float(payload.get("temperature", {}).get("N", 0.0))

        print(f"✅ Latest Data Fetched: {json.dumps(data, indent=4)}")
        return data
    except Exception as e:
        print(f" Error fetching data from DynamoDB: {e}")
        return {}


# Handle incoming client messages
async def handle_client(websocket):
    while True:
        try:
            message = await websocket.recv()
            received_data = json.loads(message)
            print(" Received from client:", json.dumps(received_data, indent=4))

        except websockets.exceptions.ConnectionClosed:
            print(" Client disconnected.")
            break

        except Exception as e:
            print(f" Error receiving data from client: {e}")
            break


# Send data from DynamoDB to WebSocket clients
async def send_data_to_clients(websocket, path):
    receive_task = asyncio.create_task(handle_client(websocket))

    try:
        while True:
            data = await fetch_latest_data()

            if data:
                await websocket.send(json.dumps(data))
                print(f" Sent to client: {json.dumps(data, indent=4)}")
            else:
                print(" No new data to send.")

            await asyncio.sleep(1)  # Fetch every second

    except Exception as e:
        print(f" Error sending data to client: {e}")

    finally:
        receive_task.cancel()
        try:
            await receive_task
        except asyncio.CancelledError:
            pass


# Start the WebSocket server
start_server = websockets.serve(send_data_to_clients, "127.0.0.1", 5050)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
