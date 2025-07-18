import logging
import os
from dotenv import load_dotenv
from utils.pdf.pdf_processor import PDFTableProcessor
import asyncio

# Load environment variables from .env file
if not os.path.exists('.env'):
    raise FileNotFoundError("Missing .env file in project root")
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    exam_id = "00eb6d05-6173-11f0-b610-80b655697afc"
    files = [
        r"C:\Users\droge\OneDrive\Documents\NASCO F4 PRINT OUT.pdf",
        r"C:\Users\droge\OneDrive\Documents\FAMGI F4 PRINT OUT.pdf",
        "doc (28).pdf",
        r"C:\Users\droge\OneDrive\Documents\doc - 2025-07-11T105318.471 (2).pdf",
        r"C:\Users\droge\OneDrive\Documents\doc (26).pdf"
    ]

    for pdf_path in files:
        try:
            logger.info(f"Processing PDF file: {pdf_path}")
            print(f"Processing PDF file: {pdf_path}")
            if not os.path.exists(pdf_path):
                logger.error(f"PDF file does not exist: {pdf_path}")
                print(f"Error: PDF file does not exist: {pdf_path}")
                continue    

            # Initialize PDF processor
            processor = PDFTableProcessor()
            logger.info(f"Generating insert statements for PDF: {pdf_path}")
            statements = processor.prepare_insert_statements(pdf_path, exam_id=exam_id)

            if not statements:
                logger.error(f"No insert statements generated for {pdf_path}")
                print(f"Error: No insert statements generated for {pdf_path}")
                continue

            logger.info(f"Generated {len(statements)} insert statements for {pdf_path}")
            # Print third statement for debugging
            print(f"Third statement: {statements[2] if len(statements) > 2 else 'Not enough statements'}")

            # Execute insert statements using static method
            result = await PDFTableProcessor.execute_insert_statements(statements)
            if result is None:
                logger.error(f"execute_insert_statements returned None for {pdf_path}")
                print(f"Error: execute_insert_statements returned None for {pdf_path}")
                continue

            print(f"Execution result for {pdf_path}: {result}")

        except FileNotFoundError:
            logger.error(f"PDF file not found: {pdf_path}")
            print(f"Error: PDF file not found: {pdf_path}")
            continue
        except Exception as e:
            logger.error(f"Error processing PDF {pdf_path}: {str(e)}")
            print(f"Error processing PDF {pdf_path}: {str(e)}")
            continue

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Event loop error: {str(e)}")
        print(f"Error: {str(e)}")