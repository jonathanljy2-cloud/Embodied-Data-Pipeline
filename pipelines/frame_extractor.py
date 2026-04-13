import cv2
import os
import pandas as pd

def extract_frames(video_path, output_dir, fps=1):
    os.makedirs(output_dir, exist_ok=True)

    cap = cv2.VideoCapture(video_path)
    video_fps = cap.get(cv2.CAP_PROP_FPS)

    if video_fps == 0:
        raise ValueError("Invalid video or cannot read FPS")

    frame_interval = max(int(video_fps / fps), 1)

    frames = []
    frame_id = 0
    saved_id = 0

    while True:
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
                "path": filepath
            })

            saved_id += 1

        frame_id += 1

    cap.release()

    return pd.DataFrame(frames)