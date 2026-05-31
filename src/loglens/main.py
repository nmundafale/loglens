import logging
import argparse
import asyncio
import sys
import os
from loglens.processors.apache_spark.error_diagnosis import process_error_diagnosis
from loglens.processors.apache_spark.cluster_utilization import process_cluster_utilization
from loglens.processors.apache_spark.config import settings

logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(
        description="Loglens: Distill and analyze massive Spark logs using AI."
    )
    
    parser.add_argument(
        "log_path", 
        type=str, 
        help="Path to the Spark .log file you want to analyze."
    )
    
    parser.add_argument(
        "--system", 
        type=str, 
        choices=["spark"],
        default="spark",
        help="The target system of the log file (e.g., 'spark')."
    )
    
    parser.add_argument(
        "--mode", 
        type=str, 
        choices=["diagnosis", "utilization"],
        default="diagnosis",
        help="The mode of the AI agent: diagnose errors or analyze cluster utilization."
    )
    
    parser.add_argument(
        "--intent", 
        type=str, 
        default="",
        help="Specific instructions for the AI (e.g., 'Optimize JVM garbage collection')."
    )

    args = parser.parse_args()
    
    # Set default intents based on the mode if none is provided
    if not args.intent:
        if args.mode == "diagnosis":
            args.intent = "Identify the root cause of the failure and provide the PySpark code to fix it."
        else:
            args.intent = "Determine if the cluster is under-utilized or if tasks are skewed, and provide PySpark configuration settings."

    logger.info("Starting Loglens...")

    if not os.path.exists(args.log_path):
        print(f"Error: The file '{args.log_path}' does not exist.")
        sys.exit(1)

    print(f"Reading {args.log_path}...")
    try:
        with open(args.log_path, "r", encoding="utf-8") as f:
            log_text = f.read()
    except Exception as e:
        print(f"Failed to read file: {e}")
        sys.exit(1)

    try:
        if args.system == "spark":
            if args.mode == "diagnosis":
                final_diagnosis = asyncio.run(process_error_diagnosis(log_text, args.intent))
            else:
                final_diagnosis = asyncio.run(process_cluster_utilization(log_text, args.intent))
        else:
            print(f"Error: Unsupported system '{args.system}'")
            sys.exit(1)
            
        print("\n" + "="*50)
        print("FINAL AI DIAGNOSIS")
        print("="*50)
        # Configure output encoding to prevent Windows PowerShell charmap UnicodeEncodeError
        sys.stdout.reconfigure(encoding='utf-8')
        print(final_diagnosis)
        print("="*50 + "\n")
    except KeyboardInterrupt:
        print("\nAnalysis cancelled by user.")
        sys.exit(0)

if __name__ == "__main__":
    main()