#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    # Starts a W&B run to track the job (job_type="basic_cleaning") 
    run = wandb.init(job_type="basic_cleaning")
    # Saves the parameters you passed dduring run process as run config.
    run.config.update(args)

    # Download input artifact from W&B. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()
    logger.info("Downloading the input artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    ######################
    # YOUR CODE HERE     #
    ######################


    # 1) Load data
    logger.info("Reading dataset")
    df = pd.read_csv(artifact_local_path)

    # 2) Drop price outliers 
    logger.info("Filtering price")
    idx = df["price"].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # 3) Convert last_review to datetime
    logger.info("Converting the last_review column to datetime")
    df["last_review"] = pd.to_datetime(df["last_review"])

    # 4) Save cleaned data
    output_filename = "clean_sample.csv"
    logger.info("Saving the cleaned data")
    df.to_csv(output_filename, index=False)

    # 4) Log cleaned artifact to W&B
    logger.info("Upload the cleaned artifact to W&B")
    # Creates a new W&B artifact object with a name, type, and description
    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    # Attaches the cleaned CSV file to the artifact
    artifact.add_file(output_filename)
    # Uploads the artifact to W&B, making it available for later steps
    run.log_artifact(artifact)

    # Ensure the artifact is uploaded before finishing
    # Waits until the artifact is fully uploaded before continuing, to avoid incomplete uploads.
    artifact.wait()
    # Ends the W&B run 
    run.finish()
    logger.info("Basic cleaning completed and artifact logged.")



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type= str,
        help= "The input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help= "The name for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="The type for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help= "A description for the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="The minimum price to consider",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help= "The maximum price to consider",
        required=True
    )


    args = parser.parse_args()

    go(args)
