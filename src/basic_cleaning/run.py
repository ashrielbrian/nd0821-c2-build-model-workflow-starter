#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import os
import argparse
import logging
import tempfile
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    min_price, max_price = args.min_price, args.max_price

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info(f'Downloaded input artifact to {artifact_local_path}')

    with tempfile.TemporaryDirectory() as tmp_dir:

        temp_path = os.path.join(tmp_dir, 'clean_data.csv')
        df = pd.read_csv(artifact_local_path)

        logger.info(f'Input data has {len(df)} rows. Cleaning...')

        # data cleaning
        idx  = df['price'].between(min_price, max_price)
        df = df[idx].copy()
        df['last_review'] = pd.to_datetime(df['last_review'])

        idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
        df = df[idx].copy()
        
        logger.info(f'Clean dataset has {len(df)} rows. Uploading...')

        df.to_csv(temp_path, index=False)
        artifact = wandb.Artifact(
            args.output_artifact,
            type=args.output_type,
            description=args.output_description
        )

        artifact.add_file(temp_path)
        run.log_artifact(artifact)
        artifact.wait()

        logger.info(f'{args.output_artifact} successfully uploaded to W&B.')

    




if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Data to be downloaded and cleaned",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Artifact name of the clean dataset to be uploaded to W&B",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Artifact type of the clean dataset",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Artifact description of the clean dataset",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum Airbnb rental price accepted",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum Airbnb rental price accepted",
        required=True
    )


    args = parser.parse_args()

    go(args)
