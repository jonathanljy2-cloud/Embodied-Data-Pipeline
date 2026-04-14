from importlib.resources import path

import cv2
import os
import pandas as pd

def extract_frames(video_path, output_dir, video_id, fps=1):
    os.makedirs(output_dir, exist_ok=True)
    
    # 解码视频 → 逐帧读取
    cap = cv2.VideoCapture(video_path)
    video_fps = cap.get(cv2.CAP_PROP_FPS)

    if video_fps == 0:
        raise ValueError("Invalid video or cannot read FPS")
    
    # 原视频：30 FPS = 每 30 帧取 1 帧
    frame_interval = max(int(video_fps / fps), 1)

    frames = []
    frame_id = 0
    saved_id = 0

    while True:
        # frame = 一张图片（numpy array）
        ret, frame = cap.read()
        if not ret:
            break

        if frame_id % frame_interval == 0:
            filename = f"frame_{saved_id:05d}.jpg"
            filepath = os.path.join(output_dir, filename)

            cv2.imwrite(filepath, frame)

            timestamp = frame_id / video_fps
            frames.append({
                "frame_id": saved_id,
                "timestamp": timestamp,
                "path": filepath,
                "video_id": video_id
            })

            saved_id += 1

        frame_id += 1

    cap.release()

    return pd.DataFrame(frames)

# frames -> metadata.parquet:
# frame_id | timestamp | path
# --------------------------------
# 0        | 0.0       | frame_00000.jpg
# 1        | 1.0       | frame_00001.jpg
# ...