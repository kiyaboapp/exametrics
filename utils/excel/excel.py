import os
import shutil
from datetime import datetime
import logging
import aiomysql
from dotenv import load_dotenv
import openpyxl
from openpyxl.worksheet.datavalidation import DataValidation
# from app.schemas.school import SchoolType
from app.db.models.school import SchoolType
from sqlalchemy import select, and_, func
from app.db.models import School, Ward, Council, Region, StudentSubject, ExamSubject, Student
from app.db.database import get_db
from sqlalchemy.ext.asyncio import AsyncSession


# Load environment variables
if not os.path.exists('.env'):
    raise FileNotFoundError("Missing .env file in project root")
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration with connection pool
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'db': os.getenv('DB_NAME', 'exametrics'),
    'maxsize': int(os.getenv('DB_POOL_SIZE', 5))
}

async def get_excel_workbook_name(
    ward_id: int = 0,
    council_name: str = "",
    region_name: str = "",
    school_type: str = "",
    practical_mode: int = 0
) -> str:
    """Generate a unique Excel workbook filename based on parameters and timestamp."""
    separator = "_"
    result = []
    
    # Query ward_name from wards table if ward_id is provided
    ward_name = ""
    if ward_id:
        pool = None
        conn = None
        cursor = None
        try:
            pool = await aiomysql.create_pool(**DB_CONFIG)
            conn = await pool.acquire()
            cursor = await conn.cursor(aiomysql.DictCursor)
            await cursor.execute("SELECT ward_name FROM wards WHERE ward_id = %s", (ward_id,))
            result = await cursor.fetchone()
            ward_name = result['ward_name'] if result and result['ward_name'] else ""
            logger.debug(f"Queried ward_name='{ward_name}' for ward_id={ward_id}")
        except aiomysql.Error as e:
            logger.error(f"Error querying ward_name for ward_id={ward_id}: {e}")
            ward_name = ""
        finally:
            if cursor:
                await cursor.close()
            if conn and pool:
                pool.release(conn)
            if pool:
                pool.close()
                await pool.wait_closed()
    
    # Build base name from non-empty parameters
    if ward_name:
        result.append(ward_name)
    if council_name:
        result.append(council_name)
    if region_name:
        result.append(region_name)
    if school_type:
        result.append(school_type)
    
    # Handle practical mode
    if practical_mode != 0:
        mode_name = {
            2: "Practical",
            1: "Theory",
            0: "Theory_Practical"
        }.get(practical_mode, "CustomMode")
        result.append(mode_name)
    
    # Add timestamp for uniqueness
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Final assembly
    if result:
        return f"ENTRY_{separator.join(result)}_{timestamp}".upper()
    return f"ENTRY_SHEET_JOINT_EXAM_{timestamp}".upper()

async def export_to_excel(
    exam_id: str,
    ward_id: int = 0,
    council_name: str = "",
    region_name: str = "",
    school_type: str = "",
    practical_mode: int = 0
) -> str:
    """Export student data to an Excel file and return the file path."""
    pool = None
    conn = None
    cursor = None
    
    # Initialize connection pool outside try
    pool = await aiomysql.create_pool(**DB_CONFIG)
    
    try:
        conn = await pool.acquire()
        cursor = await conn.cursor(aiomysql.DictCursor)
        
        # Debug: Check data existence
        await cursor.execute(
            "SELECT COUNT(*) AS count FROM exam_subjects WHERE exam_id = %s AND is_present = TRUE",
            (exam_id,)
        )
        exam_subjects_count = (await cursor.fetchone())['count']
        logger.info(f"Found {exam_subjects_count} subjects for exam_id={exam_id}")
        
        await cursor.execute(
            "SELECT COUNT(*) AS count FROM student_subjects WHERE exam_id = %s",
            (exam_id,)
        )
        student_subjects_count = (await cursor.fetchone())['count']
        logger.info(f"Found {student_subjects_count} student-subject mappings for exam_id={exam_id}")
        
        # Build WHERE clause with parameterized query
        where_clause = "WHERE exam_subjects.is_present = TRUE AND exam_subjects.exam_id = %s"
        params = [exam_id]
        location_filter = []
        if ward_id:
            location_filter.append("schools.ward_id = %s")
            params.append(ward_id)
        if council_name:
            location_filter.append("schools.council_name = %s")
            params.append(council_name)
        if region_name:
            location_filter.append("schools.region_name = %s")
            params.append(region_name)
        if school_type:
            location_filter.append("schools.school_type = %s")
            params.append(school_type)
        
        if location_filter:
            where_clause += " AND " + " AND ".join(location_filter)
        
        if practical_mode == 2:  # ExportOnlyPracticalVersions
            where_clause += " AND exam_subjects.has_practical = TRUE"
        
        # SQL query with LEFT JOIN for wards
        sql = f"""
        SELECT students.student_id, students.full_name, students.sex,
               exam_subjects.subject_code, exam_subjects.subject_name, exam_subjects.subject_short,
               school_info.centre_number, schools.school_name,
               wards.ward_name, schools.council_name, schools.region_name,
               schools.school_type, exam_subjects.has_practical
        FROM schools
        LEFT JOIN wards ON schools.ward_id = wards.ward_id
        INNER JOIN students ON schools.centre_number = students.centre_number
        INNER JOIN (exam_subjects INNER JOIN student_subjects
                    ON exam_subjects.subject_code = student_subjects.subject_code
                    AND exam_subjects.exam_id = student_subjects.exam_id)
        ON students.student_id = student_subjects.student_id
        {where_clause}
        ORDER BY students.student_id ASC, exam_subjects.subject_code ASC
        """
        
        await cursor.execute(sql, params)
        records = await cursor.fetchall()
        
        # Error message for empty result
        msg = "NO STUDENT REGISTERED FOR THIS CONDITION!"
        if ward_id:
            msg += f" WARD_ID={ward_id}"
        if council_name:
            msg += f" COUNCIL={council_name}"
        if region_name:
            msg += f" REGION={region_name}"
        if school_type:
            msg += f" SCHOOL TYPE={school_type}"
        if practical_mode != 0:
            msg += f" PRACTICAL MODE={practical_mode}"
        if exam_id:
            msg += f" EXAM_ID={exam_id}"
        
        if not records:
            logger.error(f"Error 1002: {msg}")
            raise ValueError(msg)
        
        # Initialize dictionaries for subjects and schools
        subject_dict = {}
        school_dict = {}
        data_array = []
        
        # Process records
        for record in records:
            school_key = (
                f"{record['centre_number'] or ''}|{record['school_name'] or ''}|"
                f"{record['ward_name'] or ''}|{record['council_name'] or ''}|"
                f"{record['region_name'] or ''}|{record['school_type'] or ''}"
            )
            
            if practical_mode != 2:  # Not ExportOnlyPracticalVersions
                # Add theory row
                data_array.append([
                    record['student_id'] or '',
                    record['full_name'] or '',
                    record['sex'] or '',
                    '',  # Empty marks column
                    str(record['subject_code'] or ''),
                    record['subject_name'] or '',
                    record['subject_short'] or '',
                    record['centre_number'] or '',
                    record['school_name'] or '',
                    record['ward_name'] or '',
                    record['council_name'] or '',
                    record['region_name'] or '',
                    exam_id  # New exam_id column
                ])
                
                subj_key = (
                    f"{record['subject_code'] or ''}|{record['subject_name'] or ''}|"
                    f"{record['subject_short'] or ''}"
                )
                if subj_key not in subject_dict:
                    subject_dict[subj_key] = [
                        str(record['subject_code'] or ''),
                        record['subject_name'] or '',
                        record['subject_short'] or '',
                        0, 0
                    ]
                subject_dict[subj_key][3] += 1
                
                if school_key not in school_dict:
                    school_dict[school_key] = [
                        record['centre_number'] or '',
                        record['school_name'] or '',
                        record['ward_name'] or '',
                        record['council_name'] or '',
                        record['region_name'] or '',
                        0, 0
                    ]
                school_dict[school_key][5] += 1
            
            if record['has_practical'] and practical_mode != 1:  # Not ExportWithoutPractical
                # Add practical row
                data_array.append([
                    record['student_id'] or '',
                    record['full_name'] or '',
                    record['sex'] or '',
                    '',  # Empty marks column
                    f"{record['subject_code'] or ''}-P",
                    f"{record['subject_name'] or ''}-Practical",
                    f"{record['subject_short'] or ''}-P",
                    record['centre_number'] or '',
                    record['school_name'] or '',
                    record['ward_name'] or '',
                    record['council_name'] or '',
                    record['region_name'] or '',
                    exam_id  # New exam_id column
                ])
                
                prac_subj_key = (
                    f"{record['subject_code'] or ''}-P|"
                    f"{record['subject_name'] or ''}-Practical|"
                    f"{record['subject_short'] or ''}-P"
                )
                if prac_subj_key not in subject_dict:
                    subject_dict[prac_subj_key] = [
                        f"{record['subject_code'] or ''}-P",
                        f"{record['subject_name'] or ''}-Practical",
                        f"{record['subject_short'] or ''}-P",
                        0, 0
                    ]
                subject_dict[prac_subj_key][3] += 1
                
                if school_key not in school_dict:
                    school_dict[school_key] = [
                        record['centre_number'] or '',
                        record['school_name'] or '',
                        record['ward_name'] or '',
                        record['council_name'] or '',
                        record['region_name'] or '',
                        0, 0
                    ]
                school_dict[school_key][5] += 1
        
        # Copy master.xlsm to output
        original_file = os.path.join(os.path.dirname(__file__), "master.xlsm")
        if not os.path.exists(original_file):
            logger.error("Error 1001: master.xlsm not found!")
            raise FileNotFoundError("master.xlsm not found!")
        
        os.makedirs("./output", exist_ok=True)
        save_path = os.path.join(
            "./output",
            f"{await get_excel_workbook_name(ward_id, council_name, region_name, school_type, practical_mode)}.xlsm"
        )
        
        # Ensure exclusive file access
        shutil.copyfile(original_file, save_path)
        
        # Open Excel workbook
        workbook = openpyxl.load_workbook(save_path, keep_vba=True)
        students_sheet = workbook["Students"]
        subjects_sheet = workbook["Subjects"]
        schools_sheet = workbook["Schools"]
        interface_sheet = workbook["Interface"]
        
        # Clear Interface!A500
        interface_sheet["A500"] = ""
        
        # Skip clearing Table1, Subjects, and Schools (assume template is empty)
        table1 = students_sheet.tables.get("Table1")
        
        # Set number formats
        students_sheet.column_dimensions["E"].number_format = "@"
        subjects_sheet.column_dimensions["A"].number_format = "@"
        
        # Write student data
        if data_array:
            for i, row in enumerate(data_array, start=2):
                for j, value in enumerate(row, start=1):
                    students_sheet.cell(row=i, column=j).value = value
            if table1:
                table1.ref = f"A1:M{len(data_array) + 1}"
        else:
            students_sheet["A2:M2"] = [""] * 13
            if table1:
                table1.ref = "A1:M2"
        
        # Write subjects data
        row_index = 2
        for subj_data in subject_dict.values():
            subjects_sheet[f"A{row_index}"] = subj_data[0]
            subjects_sheet[f"B{row_index}"] = subj_data[2]
            subjects_sheet[f"C{row_index}"] = subj_data[1]
            subjects_sheet[f"D{row_index}"] = f"=COUNTIFS(Students!E:E,A{row_index})"
            subjects_sheet[f"E{row_index}"] = f"=COUNTIFS(Students!E:E,A{row_index},Students!D:D,\"<>\")"
            subjects_sheet[f"F{row_index}"] = f"=D{row_index}-E{row_index}"
            subjects_sheet[f"G{row_index}"] = f"=IF(D{row_index}=0,\"N/A\",E{row_index}/D{row_index})"
            row_index += 1
        
        if row_index > 2:
            subjects_sheet[f"C{row_index}"] = "Total"
            subjects_sheet[f"D{row_index}"] = f"=SUM(D2:D{row_index-1})"
            subjects_sheet[f"E{row_index}"] = f"=SUM(E2:E{row_index-1})"
            subjects_sheet[f"F{row_index}"] = f"=SUM(F2:F{row_index-1})"
            subjects_sheet[f"G{row_index}"] = f"=IF(D{row_index}=0,\"N/A\",E{row_index}/D{row_index})"
        
        subjects_sheet.column_dimensions["G"].number_format = "0.0%"
        
        # Write schools data
        row_index = 2
        for school_data in school_dict.values():
            schools_sheet[f"A{row_index}"] = school_data[0]
            schools_sheet[f"B{row_index}"] = school_data[1]
            schools_sheet[f"C{row_index}"] = school_data[2]  # ward_name
            schools_sheet[f"D{row_index}"] = school_data[3]  # council_name
            schools_sheet[f"E{row_index}"] = school_data[4]  # region_name
            schools_sheet[f"F{row_index}"] = f"=COUNTIFS(Students!H:H,A{row_index})"
            schools_sheet[f"G{row_index}"] = f"=COUNTIFS(Students!H:H,A{row_index},Students!D:D,\"<>\")"
            schools_sheet[f"H{row_index}"] = f"=F{row_index}-G{row_index}"
            schools_sheet[f"I{row_index}"] = f"=IF(F{row_index}=0,\"N/A\",G{row_index}/F{row_index})"
            row_index += 1
        
        if row_index > 2:
            schools_sheet[f"E{row_index}"] = "Total"
            schools_sheet[f"F{row_index}"] = f"=SUM(F2:F{row_index-1})"
            schools_sheet[f"G{row_index}"] = f"=SUM(G2:G{row_index-1})"
            schools_sheet[f"H{row_index}"] = f"=SUM(H2:H{row_index-1})"
            schools_sheet[f"I{row_index}"] = f"=IF(F{row_index}=0,\"N/A\",G{row_index}/F{row_index})"
        
        schools_sheet.column_dimensions["I"].number_format = "0.0%"
        
        # Create dropdowns in Interface sheet
        # Dropdown for C5: centre_number from Schools (A2:A<last_row>)
        centre_numbers = [str(school_data[0]) for school_data in school_dict.values() if school_data[0]]
        if centre_numbers:
            dv_centre = DataValidation(type="list", formula1=f'"{",".join(centre_numbers)}"', allow_blank=True)
            dv_centre.add("C5")
            interface_sheet.add_data_validation(dv_centre)
            logger.debug(f"Added dropdown to Interface!C5 with {len(centre_numbers)} centre numbers")
        
        # Dropdown for C6: subject_code from Subjects (A2:A<last_row>)
        subject_codes = [str(subj_data[0]) for subj_data in subject_dict.values() if subj_data[0]]
        if subject_codes:
            interface_sheet["C6"].number_format = "@"  # Ensure C6 is text format
            dv_subject = DataValidation(type="list", formula1=f'"{",".join(subject_codes)}"', allow_blank=True)
            dv_subject.add("C6")
            interface_sheet.add_data_validation(dv_subject)
            logger.debug(f"Added dropdown to Interface!C6 with {len(subject_codes)} subject codes")
        
        # Save and close
        workbook.save(save_path)
        workbook.close()
        
        logger.info(f"Export completed successfully to {save_path}")
        return save_path
    
    except aiomysql.Error as e:
        logger.error(f"Error 1004: Database error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error 1003: Excel operation failed: {e}")
        raise
    finally:
        if cursor:
            await cursor.close()
        if conn and pool:
            pool.release(conn)
        if pool:
            pool.close()  
            await pool.wait_closed()



async def import_marks_from_excel(file_path: str) -> int:
    """Read marks from an Excel file and update student_subjects table. Return number of updated records."""
    pool = None
    conn = None
    cursor = None
    
    try:
        if not os.path.exists(file_path):
            logger.error(f"Error 2001: Excel file not found at {file_path}")
            raise FileNotFoundError(f"Excel file not found: {file_path}")
        
        workbook = openpyxl.load_workbook(file_path, read_only=True, keep_vba=True)
        students_sheet = workbook["Students"]
        interface_sheet = workbook["Interface"]
        
        submitted_by = interface_sheet["A500"].value or ""
        if not submitted_by:
            logger.warning("No submitted_by value found in Interface!A500")
        
        pool = await aiomysql.create_pool(**DB_CONFIG)
        conn = await pool.acquire()
        cursor = await conn.cursor()
        
        # Verify existing records
        await cursor.execute(
            "SELECT DISTINCT exam_id, student_id, subject_code FROM student_subjects WHERE exam_id = %s",
            (students_sheet["M2"].value or "",)
        )
        existing_records = {(row[0], row[1], row[2]) for row in await cursor.fetchall()}
        
        # Group marks by (exam_id, student_id, clean_subject_code)
        marks_dict = {}
        for row in students_sheet.iter_rows(min_row=2, max_col=13, values_only=True):
            student_id = str(row[0] or "")
            marks = row[3]
            subject_code = str(row[4] or "")
            exam_id = str(row[12] or "")
            
            if not all([exam_id, student_id, subject_code]):
                logger.warning(f"Skipping row with missing data: exam_id={exam_id}, student_id={student_id}, subject_code={subject_code}")
                continue
            
            try:
                marks_value = float(marks) if marks is not None else None
            except (ValueError, TypeError):
                logger.warning(f"Invalid marks value '{marks}' for student_id={student_id}, subject_code={subject_code}")
                continue
            
            clean_subject_code = subject_code[:-2] if subject_code.endswith("-P") else subject_code
            key = (exam_id, student_id, clean_subject_code)
            
            if key not in existing_records:
                logger.warning(f"No record found in database: exam_id={exam_id}, student_id={student_id}, subject_code={clean_subject_code}")
                continue
            
            if key not in marks_dict:
                marks_dict[key] = {"theory_marks": None, "practical_marks": None}
            
            if subject_code.endswith("-P"):
                marks_dict[key]["practical_marks"] = marks_value
            else:
                marks_dict[key]["theory_marks"] = marks_value
        
        await conn.begin()
        
        updated_records = 0
        for (exam_id, student_id, clean_subject_code), marks in marks_dict.items():
            query = """
            UPDATE student_subjects
            SET theory_marks = %s, practical_marks = %s, submitted_by = %s
            WHERE exam_id = %s AND student_id = %s AND subject_code = %s
            """
            params = (
                marks["theory_marks"],
                marks["practical_marks"],
                submitted_by,
                exam_id,
                student_id,
                clean_subject_code
            )
            
            await cursor.execute(query, params)
            if cursor.rowcount > 0:
                updated_records += 1
                logger.debug(f"Updated record: exam_id={exam_id}, student_id={student_id}, subject_code={clean_subject_code}, "
                            f"theory_marks={marks['theory_marks']}, practical_marks={marks['practical_marks']}, submitted_by={submitted_by}")
        
        await conn.commit()
        logger.info(f"Successfully updated {updated_records} records from {file_path}")
        
        workbook.close()
        
        return updated_records
    
    except aiomysql.Error as e:
        logger.error(f"Error 2002: Database error: {e}")
        if conn:
            await conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Error 2003: Import operation failed: {e}")
        if conn:
            await conn.rollback()
        raise
    finally:
        if cursor:
            await cursor.close()
        if conn and pool:
            pool.release(conn)
        if pool:
            pool.close()  # <-- This is NOT awaitable
            await pool.wait_closed()


async def calculate_marks_and_ranks(exam_id: str) -> int:
    """Calculate overall_marks and integer rankings for student_subjects table, excluding invalid locations. Return number of updated records."""
    pool = None
    conn = None
    cursor = None
    
    try:
        pool = await aiomysql.create_pool(**DB_CONFIG)
        conn = await pool.acquire()
        cursor = await conn.cursor(aiomysql.DictCursor)
        
        # Fetch student_subjects with has_practical and location info
        query = """
        SELECT ss.exam_id, ss.student_id, ss.subject_code, 
               ss.theory_marks, ss.practical_marks,
               es.has_practical,
               si.ward_id, si.council_name, si.region_name, si.school_type
        FROM student_subjects ss
        INNER JOIN exam_subjects es ON ss.exam_id = es.exam_id AND ss.subject_code = es.subject_code
        INNER JOIN students s ON ss.student_id = s.student_id
        INNER JOIN schools si ON s.centre_number = si.centre_number
        WHERE ss.exam_id = %s
        """
        await cursor.execute(query, (exam_id,))
        records = await cursor.fetchall()
        
        if not records:
            logger.warning(f"No records found for exam_id={exam_id}")
            return 0
        
        # Calculate overall_marks and prepare for ranking
        records_with_marks = []
        for record in records:
            overall_marks = None
            if record['has_practical']:
                if record['theory_marks'] is not None and record['practical_marks'] is not None:
                    overall_marks = (record['theory_marks'] + record['practical_marks']) / 2
                elif record['theory_marks'] is not None:
                    overall_marks = record['theory_marks'] / 2
                elif record['practical_marks'] is not None:
                    overall_marks = record['practical_marks'] / 2
            else:
                overall_marks = record['theory_marks']
            
            records_with_marks.append({
                'exam_id': record['exam_id'],
                'student_id': record['student_id'],
                'subject_code': record['subject_code'],
                'overall_marks': overall_marks,
                'ward_id': record['ward_id'],
                'council_name': record['council_name'],
                'region_name': record['region_name'],
                'school_type': record['school_type'],
                'subj_pos': None,
                'subj_ward_pos': None,
                'subj_council_pos': None,
                'subj_region_pos': None,
                'subj_ward_pos_gvt': None,
                'subj_ward_pos_pvt': None,
                'subj_ward_pos_unknown': None,
                'subj_council_pos_gvt': None,
                'subj_council_pos_pvt': None,
                'subj_council_pos_unknown': None,
                'subj_region_pos_gvt': None,
                'subj_region_pos_pvt': None,
                'subj_region_pos_unknown': None,
                'subj_pos_out_of': None,
                'subj_ward_pos_out_of': None,
                'subj_council_pos_out_of': None,
                'subj_region_pos_out_of': None,
                'subj_ward_pos_gvt_out_of': None,
                'subj_ward_pos_pvt_out_of': None,
                'subj_ward_pos_unknown_out_of': None,
                'subj_council_pos_gvt_out_of': None,
                'subj_council_pos_pvt_out_of': None,
                'subj_council_pos_unknown_out_of': None,
                'subj_region_pos_gvt_out_of': None,
                'subj_region_pos_pvt_out_of': None,
                'subj_region_pos_unknown_out_of': None
            })
        
        # Compute dense rankings and out_of counts
        def compute_ranks(records, key_func, pos_field, out_of_field, valid_check=lambda r: True):
            grouped = {}
            for rec in records:
                if rec['overall_marks'] is None or not valid_check(rec):
                    rec[pos_field] = None
                    rec[out_of_field] = None
                    continue
                key = key_func(rec)
                if key not in grouped:
                    grouped[key] = []
                grouped[key].append(rec)
            
            for key, group in grouped.items():
                sorted_group = sorted(group, key=lambda x: x['overall_marks'] or -float('inf'), reverse=True)
                total_ranked = len([r for r in group if r['overall_marks'] is not None])
                current_pos = 1
                current_marks = None
                for i, rec in enumerate(sorted_group):
                    if rec['overall_marks'] is None:
                        rec[pos_field] = None
                        rec[out_of_field] = None
                        continue
                    if rec['overall_marks'] != current_marks:
                        current_pos = i + 1
                        current_marks = rec['overall_marks']
                    rec[pos_field] = current_pos
                    rec[out_of_field] = total_ranked
        
        # Overall position (subj_pos, no location check)
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'],), 
                      'subj_pos', 
                      'subj_pos_out_of')
        
        # Ward positions (only if ward_id and school_type are not null)
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['ward_id']), 
                      'subj_ward_pos', 
                      'subj_ward_pos_out_of', 
                      lambda r: r['ward_id'] is not None and r['school_type'] is not None)
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['ward_id'], r['school_type'] if r['school_type'] == 'gvt' else None), 
                      'subj_ward_pos_gvt', 
                      'subj_ward_pos_gvt_out_of', 
                      lambda r: r['ward_id'] is not None and r['school_type'] == 'gvt')
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['ward_id'], r['school_type'] if r['school_type'] == 'pvt' else None), 
                      'subj_ward_pos_pvt', 
                      'subj_ward_pos_pvt_out_of', 
                      lambda r: r['ward_id'] is not None and r['school_type'] == 'pvt')
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['ward_id'], r['school_type'] if r['school_type'] == 'unknown' else None), 
                      'subj_ward_pos_unknown', 
                      'subj_ward_pos_unknown_out_of', 
                      lambda r: r['ward_id'] is not None and r['school_type'] == 'unknown')
        
        # Council positions (only if council_name and school_type are not null)
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['council_name']), 
                      'subj_council_pos', 
                      'subj_council_pos_out_of', 
                      lambda r: r['council_name'] is not None and r['school_type'] is not None)
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['council_name'], r['school_type'] if r['school_type'] == 'gvt' else None), 
                      'subj_council_pos_gvt', 
                      'subj_council_pos_gvt_out_of', 
                      lambda r: r['council_name'] is not None and r['school_type'] == 'gvt')
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['council_name'], r['school_type'] if r['school_type'] == 'pvt' else None), 
                      'subj_council_pos_pvt', 
                      'subj_council_pos_pvt_out_of', 
                      lambda r: r['council_name'] is not None and r['school_type'] == 'pvt')
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['council_name'], r['school_type'] if r['school_type'] == 'unknown' else None), 
                      'subj_council_pos_unknown', 
                      'subj_council_pos_unknown_out_of', 
                      lambda r: r['council_name'] is not None and r['school_type'] == 'unknown')
        
        # Region positions (only if region_name and school_type are not null)
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['region_name']), 
                      'subj_region_pos', 
                      'subj_region_pos_out_of', 
                      lambda r: r['region_name'] is not None and r['school_type'] is not None)
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['region_name'], r['school_type'] if r['school_type'] == 'gvt' else None), 
                      'subj_region_pos_gvt', 
                      'subj_region_pos_gvt_out_of', 
                      lambda r: r['region_name'] is not None and r['school_type'] == 'gvt')
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['region_name'], r['school_type'] if r['school_type'] == 'pvt' else None), 
                      'subj_region_pos_pvt', 
                      'subj_region_pos_pvt_out_of', 
                      lambda r: r['region_name'] is not None and r['school_type'] == 'pvt')
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['region_name'], r['school_type'] if r['school_type'] == 'unknown' else None), 
                      'subj_region_pos_unknown', 
                      'subj_region_pos_unknown_out_of', 
                      lambda r: r['region_name'] is not None and r['school_type'] == 'unknown')
        
        # Update student_subjects table
        await conn.begin()
        updated_records = 0
        query = """
        UPDATE student_subjects
        SET overall_marks = %s,
            subj_pos = %s,
            subj_ward_pos = %s,
            subj_council_pos = %s,
            subj_region_pos = %s,
            subj_ward_pos_gvt = %s,
            subj_ward_pos_pvt = %s,
            subj_ward_pos_unknown = %s,
            subj_council_pos_gvt = %s,
            subj_council_pos_pvt = %s,
            subj_council_pos_unknown = %s,
            subj_region_pos_gvt = %s,
            subj_region_pos_pvt = %s,
            subj_region_pos_unknown = %s,
            subj_pos_out_of = %s,
            subj_ward_pos_out_of = %s,
            subj_council_pos_out_of = %s,
            subj_region_pos_out_of = %s,
            subj_ward_pos_gvt_out_of = %s,
            subj_ward_pos_pvt_out_of = %s,
            subj_ward_pos_unknown_out_of = %s,
            subj_council_pos_gvt_out_of = %s,
            subj_council_pos_pvt_out_of = %s,
            subj_council_pos_unknown_out_of = %s,
            subj_region_pos_gvt_out_of = %s,
            subj_region_pos_pvt_out_of = %s,
            subj_region_pos_unknown_out_of = %s
        WHERE exam_id = %s AND student_id = %s AND subject_code = %s
        """
        for record in records_with_marks:
            params = (
                record['overall_marks'],
                record['subj_pos'],
                record['subj_ward_pos'],
                record['subj_council_pos'],
                record['subj_region_pos'],
                record['subj_ward_pos_gvt'],
                record['subj_ward_pos_pvt'],
                record['subj_ward_pos_unknown'],
                record['subj_council_pos_gvt'],
                record['subj_council_pos_pvt'],
                record['subj_council_pos_unknown'],
                record['subj_region_pos_gvt'],
                record['subj_region_pos_pvt'],
                record['subj_region_pos_unknown'],
                record['subj_pos_out_of'],
                record['subj_ward_pos_out_of'],
                record['subj_council_pos_out_of'],
                record['subj_region_pos_out_of'],
                record['subj_ward_pos_gvt_out_of'],
                record['subj_ward_pos_pvt_out_of'],
                record['subj_ward_pos_unknown_out_of'],
                record['subj_council_pos_gvt_out_of'],
                record['subj_council_pos_pvt_out_of'],
                record['subj_council_pos_unknown_out_of'],
                record['subj_region_pos_gvt_out_of'],
                record['subj_region_pos_pvt_out_of'],
                record['subj_region_pos_unknown_out_of'],
                record['exam_id'],
                record['student_id'],
                record['subject_code']
            )
            await cursor.execute(query, params)
            updated_records += cursor.rowcount
        
        await conn.commit()
        logger.info(f"Successfully updated {updated_records} records for exam_id={exam_id}")
        
        return updated_records
    
    except aiomysql.Error as e:
        logger.error(f"Error 3001: Database error: {e}")
        if conn:
            await conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Error 3002: Operation failed: {e}")
        if conn:
            await conn.rollback()
        raise
    finally:
        if cursor:
            await cursor.close()
        if conn and pool:
            pool.release(conn)
        if pool:
            pool.close()  # <-- This is NOT awaitable
            await pool.wait_closed()

            
async def main():
    try:
        exam_id="00eb6d05-6173-11f0-b610-80b655697afc"
        updates=await calculate_marks_and_ranks(exam_id)
        print(f"Updated {updates} records for exam_id={exam_id}")
    except Exception as e:
        logger.error(f"Main function failed: {e}")
        raise


# if __name__ == "__main__":
#     async def main():
#         try:
#             file_path = await export_to_excel(
#                 exam_id="1f063d43-67bc-6c3a-a294-69a24a3c35ac",
#                 ward_id=0,
#                 council_name="",
#                 region_name="",
#                 school_type="",
#                 practical_mode=0
#             )
#             print(f"Generated Excel file: {file_path}")
#         except Exception as e:
#             logger.error(f"Main function failed: {e}")
#             raise
    
#     asyncio.run(main())



# if __name__ == "__main__":
#     asyncio.run(main())