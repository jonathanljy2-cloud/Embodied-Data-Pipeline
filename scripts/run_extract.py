import argparse
import yaml
import os
from pipelines.frame_extractor import extract_frames

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)

    for file in os.listdir(cfg["input_dir"]):
        if not file.endswith(".mp4"):
            continue

        video_path = os.path.join(cfg["input_dir"], file)
        video_id = file.replace(".mp4", "")

        output_dir = os.path.join(
            cfg["output"], f"video_id={video_id}"
        )

        df = extract_frames(
            video_path,
            output_dir,
            video_id,
            cfg.get("fps", 1)
        )

        df.to_parquet(os.path.join(output_dir, "metadata.parquet"))

        print(f"Processed {video_id}")

if __name__ == "__main__":
    main()