# test2.py
# This script is used to test the DivisionProcessor functionality.

from utils.processor.division import DivisionProcessor
import asyncio
import json

async def main():
    processor = DivisionProcessor(exam_id='1f0656e3-8756-680b-ac24-8d5b3e217521')
    result = await processor.process_exam()
    print(json.dumps(result, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())