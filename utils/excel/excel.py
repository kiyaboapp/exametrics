import os
import shutil
from datetime import datetime
import logging
import aiomysql
from dotenv import load_dotenv
import openpyxl
from openpyxl.worksheet.datavalidation import DataValidation

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
    ward_name: str = "",
    council_name: str = "",
    region_name: str = "",
    school_type: str = "",
    practical_mode: int = 0
) -> str:
    """Generate a unique Excel workbook filename based on parameters and timestamp."""
    separator = "_"
    result = []
    
    # Build base name from non-empty parameters
    if ward_name:
        result.append(ward_name.replace(" ", "_"))
    if council_name:
        result.append(council_name.replace(" ", "_"))
    if region_name:
        result.append(region_name.replace(" ", "_"))
    if school_type:
        result.append(school_type.replace(" ", "_"))
    
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
    ward_name: str = "",
    council_name: str = "",
    region_name: str = "",
    school_type: str = "",
    practical_mode: int = 0
) -> str:
    """Export student data to an Excel file and return the file path."""
    pool = None
    conn = None
    cursor = None
    
    try:
        pool = await aiomysql.create_pool(**DB_CONFIG)
        conn = await pool.acquire()
        cursor = await conn.cursor(aiomysql.DictCursor)
        
        # Debug: Check data existence
        await cursor.execute(
            "SELECT COUNT(*) AS count FROM exam_subjects WHERE exam_id = %s",
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
        where_clause = "WHERE es.exam_id = %s"
        params = [exam_id]
        location_filter = []
        if ward_name:
            location_filter.append("s.ward_name = %s")
            params.append(ward_name)
        if council_name:
            location_filter.append("s.council_name = %s")
            params.append(council_name)
        if region_name:
            location_filter.append("s.region_name = %s")
            params.append(region_name)
        if school_type:
            location_filter.append("s.school_type = %s")
            params.append(school_type)
        
        if location_filter:
            where_clause += " AND " + " AND ".join(location_filter)
        
        if practical_mode == 2:  # ExportOnlyPracticalVersions
            where_clause += " AND es.has_practical = TRUE"
        
        # SQL query with INNER JOINs
        sql = f"""
        SELECT st.student_id, st.student_global_id, st.full_name, st.sex,
               es.subject_code, es.subject_name, es.subject_short,
               s.centre_number, s.school_name,
               s.ward_name, s.council_name, s.region_name,
               s.school_type, es.has_practical
        FROM schools s
        INNER JOIN students st ON s.centre_number = st.centre_number
        INNER JOIN student_subjects ss ON st.student_global_id = ss.student_global_id AND st.exam_id = ss.exam_id
        INNER JOIN exam_subjects es ON ss.exam_id = es.exam_id AND ss.subject_code = es.subject_code
        {where_clause}
        ORDER BY st.student_id ASC, es.subject_code ASC
        """
        
        await cursor.execute(sql, params)
        records = await cursor.fetchall()
        
        # Error message for empty result
        msg = "NO STUDENT REGISTERED FOR THIS CONDITION!"
        if ward_name:
            msg += f" WARD_NAME={ward_name}"
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
            logger.info(school_key)
            
            if practical_mode != 2:  # Not ExportOnlyPracticalVersions
                # Add theory row
                data_array.append([
                    record['student_id'] or '',  # Use student_id for readability
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
                    exam_id,
                    record['student_global_id'] or ''  # Add student_global_id in column N
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
                    record['student_id'] or '',  # Use student_id for readability
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
                    exam_id,
                    record['student_global_id'] or ''  # Add student_global_id in column N
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
            f"{await get_excel_workbook_name(ward_name, council_name, region_name, school_type, practical_mode)}.xlsm"
        )
        
        shutil.copyfile(original_file, save_path)
        
        # Open Excel workbook
        workbook = openpyxl.load_workbook(save_path, keep_vba=True)
        students_sheet = workbook["Students"]
        subjects_sheet = workbook["Subjects"]
        schools_sheet = workbook["Schools"]
        interface_sheet = workbook["Interface"]
        
        # Clear Interface!A500
        interface_sheet["A500"] = ""
        
        # Set number formats
        students_sheet.column_dimensions["E"].number_format = "@"
        subjects_sheet.column_dimensions["A"].number_format = "@"
        
        # Write student data
        if data_array:
            for i, row in enumerate(data_array, start=2):
                for j, value in enumerate(row, start=1):
                    students_sheet.cell(row=i, column=j).value = value
            table1 = students_sheet.tables.get("Table1")
            if table1:
                table1.ref = f"A1:N{len(data_array) + 1}"  # Updated to include column N
        else:
            students_sheet["A2:N2"] = [""] * 14  # Updated to include column N
            table1 = students_sheet.tables.get("Table1")
            if table1:
                table1.ref = "A1:N2"
        
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
        centre_numbers = [str(school_data[0]) for school_data in school_dict.values() if school_data[0]]
        if centre_numbers:
            dv_centre = DataValidation(type="list", formula1=f'"{",".join(centre_numbers)}"', allow_blank=True)
            dv_centre.add("C5")
            interface_sheet.add_data_validation(dv_centre)
            logger.debug(f"Added dropdown to Interface!C5 with {len(centre_numbers)} centre numbers")
        
        subject_codes = [str(subj_data[0]) for subj_data in subject_dict.values() if subj_data[0]]
        if subject_codes:
            interface_sheet["C6"].number_format = "@"
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
            "SELECT DISTINCT exam_id, student_global_id, subject_code FROM student_subjects WHERE exam_id = %s",
            (students_sheet["M2"].value or "",)
        )
        existing_records = {(row[0], row[1], row[2]) for row in await cursor.fetchall()}
        
        # Group marks by (exam_id, student_global_id, clean_subject_code)
        marks_dict = {}
        for row in students_sheet.iter_rows(min_row=2, max_col=14, values_only=True):  # Updated to include column N
            student_global_id = str(row[13] or "")  # Use student_global_id from column N
            marks = row[3]
            subject_code = str(row[4] or "")
            exam_id = str(row[12] or "")
            logger.info(f"{student_global_id} -{subject_code}")
            if not all([exam_id, student_global_id, subject_code]):
                logger.warning(f"Skipping row with missing data: exam_id={exam_id}, student_global_id={student_global_id}, subject_code={subject_code}")
                continue
            
            try:
                marks_value = float(marks) if marks is not None else None
            except (ValueError, TypeError):
                logger.warning(f"Invalid marks value '{marks}' for student_global_id={student_global_id}, subject_code={subject_code}")
                continue
            
            clean_subject_code = subject_code[:-2] if subject_code.endswith("-P") else subject_code
            key = (exam_id, student_global_id, clean_subject_code)
            
            if key not in existing_records:
                logger.warning(f"No record found in database: exam_id={exam_id}, student_global_id={student_global_id}, subject_code={clean_subject_code}")
                continue
            
            if key not in marks_dict:
                marks_dict[key] = {"theory_marks": None, "practical_marks": None}
            
            if subject_code.endswith("-P"):
                marks_dict[key]["practical_marks"] = marks_value
            else:
                marks_dict[key]["theory_marks"] = marks_value
        
        await conn.begin()
        
        updated_records = 0
        for (exam_id, student_global_id, clean_subject_code), marks in marks_dict.items():
            query = """
            UPDATE student_subjects
            SET theory_marks = %s, practical_marks = %s, submitted_by = %s
            WHERE exam_id = %s AND student_global_id = %s AND subject_code = %s
            """
            params = (
                marks["theory_marks"],
                marks["practical_marks"],
                submitted_by,
                exam_id,
                student_global_id,
                clean_subject_code
            )
            
            await cursor.execute(query, params)
            if cursor.rowcount > 0:
                updated_records += 1
                logger.debug(f"Updated record: exam_id={exam_id}, student_global_id={student_global_id}, subject_code={clean_subject_code}, "
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
            pool.close()
            await pool.wait_closed()

async def calculate_marks_and_ranks(exam_id: str) -> int:
    """Calculate overall_marks and integer rankings for student_subjects table. Return number of updated records."""
    pool = None
    conn = None
    cursor = None
    
    try:
        pool = await aiomysql.create_pool(**DB_CONFIG)
        conn = await pool.acquire()
        cursor = await conn.cursor(aiomysql.DictCursor)
        
        # Fetch student_subjects with has_practical and location info
        query = """
        SELECT ss.exam_id, ss.student_global_id, ss.subject_code, 
               ss.theory_marks, ss.practical_marks,
               es.has_practical,
               s.ward_name, s.council_name, s.region_name, s.school_type
        FROM student_subjects ss
        INNER JOIN exam_subjects es ON ss.exam_id = es.exam_id AND ss.subject_code = es.subject_code
        INNER JOIN students st ON ss.student_global_id = st.student_global_id AND ss.exam_id = st.exam_id
        INNER JOIN schools s ON ss.centre_number = s.centre_number
        WHERE ss.exam_id = %s AND (ss.theory_marks>=0 OR ss.practical_marks>=0)
        """
        await cursor.execute(query, (exam_id,))
        records = await cursor.fetchall()
        
        if not records:
            logger.warning(f"No records found for exam_id={exam_id}")
            return 0
        logger.info(f"Found {len(records)} Entries in exam {exam_id}")
        # Calculate overall_marks and prepare for ranking
        records_with_marks = []
        i=0
        my_records=len(records)
        for record in records:
            i=i+1
            overall_marks = None
            if record['has_practical']:
                if record['theory_marks'] is not None and record['practical_marks'] is not None:
                    overall_marks = (record['theory_marks'] + record['practical_marks']) *2/ 3
                elif record['theory_marks'] is not None:
                    overall_marks = record['theory_marks'] *2/ 3
                elif record['practical_marks'] is not None:
                    overall_marks = record['practical_marks'] *2/ 3
            else:
                overall_marks = record['theory_marks']
            
            records_with_marks.append({
                'exam_id': record['exam_id'],
                'student_global_id': record['student_global_id'],
                'subject_code': record['subject_code'],
                'overall_marks': overall_marks,
                'ward_name': record['ward_name'],
                'council_name': record['council_name'],
                'region_name': record['region_name'],
                'school_type': record['school_type'],
                'subject_pos': None,
                'ward_subject_pos': None,
                'council_subject_pos': None,
                'region_subject_pos': None,
                'ward_subject_pos_gvt': None,
                'ward_subject_pos_pvt': None,
                'council_subject_pos_gvt': None,
                'council_subject_pos_pvt': None,
                'region_subject_pos_gvt': None,
                'region_subject_pos_pvt': None,
                'subject_out_of': None,
                'ward_subject_out_of': None,
                'council_subject_out_of': None,
                'region_subject_out_of': None
            })
            logger.info(f"  Inserting {record['student_global_id']} - {record['subject_code']} - {record['theory_marks']} - {record['practical_marks']} | {i} out of {my_records}")
        
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
        
        # Overall position
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'],), 
                      'subject_pos', 
                      'subject_out_of')
        
        # Ward positions
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['ward_name']), 
                      'ward_subject_pos', 
                      'ward_subject_out_of', 
                      lambda r: r['ward_name'] is not None and r['school_type'] is not None)
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['ward_name'], r['school_type'] if r['school_type'] == 'gvt' else None), 
                      'ward_subject_pos_gvt', 
                      'ward_subject_out_of', 
                      lambda r: r['ward_name'] is not None and r['school_type'] == 'gvt')
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['ward_name'], r['school_type'] if r['school_type'] == 'pvt' else None), 
                      'ward_subject_pos_pvt', 
                      'ward_subject_out_of', 
                      lambda r: r['ward_name'] is not None and r['school_type'] == 'pvt')
        
        # Council positions
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['council_name']), 
                      'council_subject_pos', 
                      'council_subject_out_of', 
                      lambda r: r['council_name'] is not None and r['school_type'] is not None)
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['council_name'], r['school_type'] if r['school_type'] == 'gvt' else None), 
                      'council_subject_pos_gvt', 
                      'council_subject_out_of', 
                      lambda r: r['council_name'] is not None and r['school_type'] == 'gvt')
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['council_name'], r['school_type'] if r['school_type'] == 'pvt' else None), 
                      'council_subject_pos_pvt', 
                      'council_subject_out_of', 
                      lambda r: r['council_name'] is not None and r['school_type'] == 'pvt')
        
        # Region positions
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['region_name']), 
                      'region_subject_pos', 
                      'region_subject_out_of', 
                      lambda r: r['region_name'] is not None and r['school_type'] is not None)
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['region_name'], r['school_type'] if r['school_type'] == 'gvt' else None), 
                      'region_subject_pos_gvt', 
                      'region_subject_out_of', 
                      lambda r: r['region_name'] is not None and r['school_type'] == 'gvt')
        compute_ranks(records_with_marks, 
                      lambda r: (r['subject_code'], r['region_name'], r['school_type'] if r['school_type'] == 'pvt' else None), 
                      'region_subject_pos_pvt', 
                      'region_subject_out_of', 
                      lambda r: r['region_name'] is not None and r['school_type'] == 'pvt')
        
        # Update student_subjects table
        logger.info(f"Starting Execution Preparation==========")
        await conn.begin()
        updated_records = 0
        query = """
        UPDATE student_subjects
        SET overall_marks = %s,
            subject_pos = %s,
            ward_subject_pos = %s,
            council_subject_pos = %s,
            region_subject_pos = %s,
            ward_subject_pos_gvt = %s,
            ward_subject_pos_pvt = %s,
            council_subject_pos_gvt = %s,
            council_subject_pos_pvt = %s,
            region_subject_pos_gvt = %s,
            region_subject_pos_pvt = %s,
            subject_out_of = %s,
            ward_subject_out_of = %s,
            council_subject_out_of = %s,
            region_subject_out_of = %s
        WHERE exam_id = %s AND student_global_id = %s AND subject_code = %s
        """
        i=0
        totals=len(records_with_marks)
        for record in records_with_marks:
            logger.info(f"      {i+1} / {totals} : {record['student_global_id']}")
            i=i+1
            params = (
                record['overall_marks'],
                record['subject_pos'],
                record['ward_subject_pos'],
                record['council_subject_pos'],
                record['region_subject_pos'],
                record['ward_subject_pos_gvt'],
                record['ward_subject_pos_pvt'],
                record['council_subject_pos_gvt'],
                record['council_subject_pos_pvt'],
                record['region_subject_pos_gvt'],
                record['region_subject_pos_pvt'],
                record['subject_out_of'],
                record['ward_subject_out_of'],
                record['council_subject_out_of'],
                record['region_subject_out_of'],
                record['exam_id'],
                record['student_global_id'],
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
            pool.close()
            await pool.wait_closed()

async def main():
    try:
        excel_path=r"C:\Users\droge\OneDrive\Desktop\HANDLER\DJANGO\exametrics\output\ENTRY_BUSOKELO_20250722_175856.xlsm"
        exam_id = "1f0656e3-8756-680b-ac24-8d5b3e217521"
        council_name = "Busokelo"
        # file_path = await export_to_excel(
        #     exam_id=exam_id,
        #     council_name=council_name
        # )
        # print(f"Generated Excel file: {file_path}") =>=TESTED AND WORKING
        
        # uploaded=await import_marks_from_excel(excel_path)    =>WROKED PERFECTLY
        updates = await calculate_marks_and_ranks(exam_id)
        print(f"Updated {updates} records for exam_id={exam_id}")
    except Exception as e:
        logger.error(f"Main function failed: {e}")
        raise

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())