from confluent_kafka import Producer
import os
import cv2
from dotenv import load_dotenv
import time

load_dotenv()

# KAFKA_BROKER=os.getenv('KAFKA_BROKER')
KAFKA_TOPIC=os.getenv('KAFKA_TOPIC')
DIR_IN=os.getenv('DIR_IN')
SUPPORTED_IMG_TYPE=os.getenv('SUPPORTED_IMG_TYPE')
SUPPORTED_VIDEO_TYPE=os.getenv('SUPPORTED_VIDEO_TYPE')

# Convert the frame/image to byte 
def convert_to_byte(img, format='.jpg'):
    if img is None or img.size == 0:
        raise ValueError("Image is empty or not loaded correctly")
    success, buffer_arr = cv2.imencode(format, img)
    if not success:
        raise RuntimeError("Failed to encode image")
    return buffer_arr.tobytes()

# Report the status of the recent sent msg
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Process image files
def process_img(file_path):
    img = cv2.imread(file_path)
    msg = convert_to_byte(img, os.path.splitext(file_path)[1])

    height, width = img.shape[:2] 
    headers = {
        "type": 'image',
        "media_name" : os.path.basename(file_path),
        "original_height": str(height),
        "original_width": str(width)
    }
    return headers,msg

# Process video files
def process_video(file_path):
    video = cv2.VideoCapture(file_path)
    video_title = os.path.basename(file_path)

    width = int(video.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(video.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = int(video.get(cv2.CAP_PROP_FPS))

    frame_id = 0
    headers_list = []
    msg_list = []
    while video.isOpened():
        print(frame_id)
        ret, frame = video.read()
        if not ret: # the video ended
            break
        msg = convert_to_byte(frame)
        current_headers = {
            "type": "video",
            "media_name" : video_title,
            "original_height": str(height),
            "original_width": str(width),
            "fps": str(fps),
            "frame_id": str(frame_id),
            "last_frame": 'no'
        }
        msg_list.append(msg)
        headers_list.append(current_headers)
        frame_id += 1
    video.release()
    headers_list[-1]['last_frame'] = 'yes'
    return headers_list, msg_list


if __name__ == '__main__':
    conf = {
        'bootstrap.servers': "localhost:9092",
        'client.id': 'img-sender'
    }
    producer = Producer(conf)

    topic = KAFKA_TOPIC
    dir = DIR_IN

    files = [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]
    for file in files:
        file_path = os.path.join(dir,file)
        file_format = os.path.splitext(file)[1].lower()
        if file_format in SUPPORTED_IMG_TYPE:
            headers, msg = process_img(file_path)
            producer.produce(topic, value=msg, on_delivery=delivery_report, headers=headers)
            producer.poll(0)  

        if file_format in SUPPORTED_VIDEO_TYPE:
            headers_list, msg_list = process_video(file_path)

            for headers, msg in zip(headers_list, msg_list):
                producer.produce(topic, value=msg, on_delivery=delivery_report, headers=headers)
                producer.poll(0)  

    producer.flush()


