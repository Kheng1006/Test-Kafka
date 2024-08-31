from confluent_kafka import Consumer, KafkaException, KafkaError
import os
import cv2
from dotenv import load_dotenv
import numpy as np

load_dotenv()

KAFKA_BROKER=os.getenv('KAFKA_BROKER')
KAFKA_TOPIC=os.getenv('KAFKA_TOPIC')
DIR_OUT=os.getenv('DIR_OUT')
SUPPORTED_IMG_TYPE=os.getenv('SUPPORTED_IMG_TYPE')
SUPPORTED_VIDEO_TYPE=os.getenv('SUPPORTED_VIDEO_TYPE')

# Convert the byte array to image
def process_img(msg, original_height=224, original_width=224):
    nparr = np.frombuffer(msg.value(), np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    img = cv2.resize(img, (original_width, original_height))
    return img

if __name__ == '__main__':
    config = {
        'bootstrap.servers': KAFKA_BROKER,  
        'group.id': 'my_consumer_group',         
        'auto.offset.reset': 'latest',       
        'enable.auto.commit': False   
    }           

    consumer = Consumer(config)

    consumer.subscribe([KAFKA_TOPIC])  

    print("Consumer is running...")
    video_list = {}
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            headers = msg.headers()
            media_type = dict(headers).get('type', 'unknown').decode("utf-8")
            media_name = dict(headers).get('media_name', 'unknown').decode("utf-8")
            original_height = int(dict(headers).get('original_height', 0).decode("utf-8"))
            original_width = int(dict(headers).get('original_width', 0).decode("utf-8"))
            if media_type == 'image':
                img = process_img(msg, original_height, original_width)
                out_path = os.path.join(DIR_OUT, media_name)
                cv2.imwrite(out_path, img)
            else: # is a video
                frame_id = int(dict(headers).get('frame_id', 0).decode("utf-8"))
                last_frame = dict(headers).get('last_frame', 'unknown').decode("utf-8")

                # process each image; 
                # If that video has not appeared before, create a new dictionary item for it;
                #       Else append to an existing one
                msg = process_img(msg, original_height, original_width)
                if video_list.get(media_name) is None:
                    video_list[media_name] = [(frame_id, msg)]
                else: video_list[media_name].append((frame_id, msg))

                # Process the video if this is the last frame
                if last_frame == 'yes':
                    fps = int(dict(headers).get('fps', 0).decode("utf-8"))

                    temp_video = video_list[media_name]
                    sorted_tuples = sorted(temp_video, key=lambda x: x[0])
                    images = [t[1] for t in sorted_tuples]

                    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
                    output_dir = os.path.join(DIR_OUT, media_name)
                    video_writer = cv2.VideoWriter(output_dir, fourcc, fps, (original_width, original_height))
                    for image in images:
                        video_writer.write(image)
                    video_writer.release()

                    video_list.pop(media_name)

            consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        print("Interrupted by user")

    finally:
        consumer.close()
        print("Consumer closed")

