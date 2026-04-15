import argparse
import yaml
import os
import pandas as pd
from pipelines.frame_extractor import extract_frames
import logging
import json

logging.basicConfig(level=logging.INFO)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    all_dfs = []
    success = 0
    failed = 0
    total_frames = 0

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
        try:
            df = extract_frames(
                video_path,
                output_dir,
                video_id,
                cfg.get("fps", 1)
            )
            df.to_parquet(os.path.join(output_dir, "metadata.parquet"))
            success += 1
            total_frames += len(df)
        except Exception as e:
            logger.error(f"{video_id} failed: {e}")
            failed += 1
            continue
        
        all_dfs.append(df)

        logger = logging.getLogger(__name__)
        logger.info(f"Processing {video_id}")
        if len(df) == 0:
            logger.warning(f"{video_id} has no frames")
        else:
            logger.info(f"Finished {video_id}, {len(df)} frames")

    final_df = pd.concat(all_dfs)
    final_df.to_parquet("data/output/metadata_all.parquet")

    manifest = {
    "total_videos": success + failed,
    "success": success,
    "failed": failed,
    "total_frames": total_frames
    }

    manifest_path = os.path.join(cfg["output"], "manifest.json")

    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"Manifest saved to {manifest_path}")

if __name__ == "__main__":
    main()