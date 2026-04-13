import argparse
import yaml
from pipelines.frame_extractor import extract_frames

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)
    
    if cfg is None:
        raise ValueError("Config file is empty or invalid")
    
    df = extract_frames(
        cfg["input"],
        cfg["output"],
        cfg.get("fps", 1)
    )

    df.to_parquet(f"{cfg['output']}/metadata.parquet")

    print("Done. Frames + metadata saved.")

if __name__ == "__main__":
    main()