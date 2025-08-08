import asyncio
import time
import logging
import pandas as pd
from app.core.config import Settings
from .subjects import SubjectProcessor


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def test_subject_processor(exam_id: str, settings: Settings):
    """
    Test function to run and time each method of the SubjectProcessor class.
    Logs the execution time and results for each method.
    """
    processor = SubjectProcessor(exam_id=exam_id, settings=settings)
    logging.info(f"Starting tests for SubjectProcessor with exam_id: {exam_id}")

    async def time_method(method_name, method, *args, is_async=True):
        start_time = time.time()
        try:
            if is_async:
                result = await method(*args)
            else:
                result = method(*args)
            elapsed_time = time.time() - start_time
            if isinstance(result, pd.DataFrame):
                logging.info(f"{method_name} completed in {elapsed_time:.2f} seconds, returned DataFrame with {len(result)} rows")
            elif isinstance(result, dict):
                logging.info(f"{method_name} completed in {elapsed_time:.2f} seconds, returned dict with keys: {list(result.keys())[:5]}...")
            elif isinstance(result, list):
                logging.info(f"{method_name} completed in {elapsed_time:.2f} seconds, returned list with {len(result)} items")
            elif isinstance(result, int):
                logging.info(f"{method_name} completed in {elapsed_time:.2f} seconds, returned integer: {result}")
            else:
                logging.info(f"{method_name} completed in {elapsed_time:.2f} seconds, returned: {result}")
            return result
        except Exception as e:
            elapsed_time = time.time() - start_time
            logging.error(f"{method_name} failed after {elapsed_time:.2f} seconds with error: {str(e)}")
            return None

    # Test load_grades
    grades_data = await time_method("load_grades", processor.load_grades)

    # Test load_divisions
    divisions_data = await time_method("load_divisions", processor.load_divisions)

    # Test load_student_subjects
    student_subjects_df = await time_method("load_student_subjects", processor.load_student_subjects)

    # Test load_schools
    schools_df = await time_method("load_schools", processor.load_schools)

    # Test load_exam_subjects
    exam_subjects_df = await time_method("load_exam_subjects", processor.load_exam_subjects)

    # Test lookup_grade_and_division
    processor.GRADES_DATA = grades_data
    processor.DIVISIONS_DATA = divisions_data
    test_marks = 85.0 if grades_data else None
    result = await time_method(
        "lookup_grade_and_division",
        processor.lookup_grade_and_division,
        marks=test_marks,
        return_type="grade",
        is_async=False
    )

    # Test calculate_grades_and_marks
    await time_method("calculate_grades_and_marks", processor.calculate_grades_and_marks)

    # Test calculate_rankings
    await time_method("calculate_rankings", processor.calculate_rankings, is_async=False)

    # Test update_student_subjects_rankings
    updated_count = await time_method(
        "update_student_subjects_rankings",
        processor.update_student_subjects_rankings,
        batch_size=5000
    )

    # Test export_subject_data
    await time_method(
        "export_subject_data",
        processor.export_subject_data,
        subject_code="011",
        filename="test_subject_011.csv"
    )

    # Test get_first_row
    first_row = await time_method("get_first_row", processor.get_first_row, is_async=False)

    logging.info("All tests completed")

if __name__ == "__main__":
    settings = Settings()
    exam_id = "1f0656e3-8756-680b-ac24-8d5b3e217521"
    asyncio.run(test_subject_processor(exam_id, settings))