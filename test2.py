# test2.py
# This script is used to test the DivisionProcessor functionality.

from utils.processor.division import DivisionProcessor
from utils.processor.subjects import SubjectProcessor
import asyncio
import json
from app.core.config import Settings

async def main():
    # Initialize the SubjectProcessor
    # subject_processor = SubjectProcessor(exam_id='1f0656e3-8756-680b-ac24-8d5b3e217521',settings=Settings())
    # processed=await subject_processor.process_all()
    # print(json.dumps(processed, indent=4, ensure_ascii=False))

    processor = DivisionProcessor(exam_id='1f0656e3-8756-680b-ac24-8d5b3e217521')
    result = await processor.process_exam()
    print(json.dumps(result, indent=4, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())