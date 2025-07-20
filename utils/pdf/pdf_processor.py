import pdfplumber
import pandas as pd
import numpy as np
from typing import Tuple, Optional, Dict,List
import re
from datetime import datetime
import aiomysql
import threading
import os
import traceback
import logging
from uuid6 import uuid7


import aiomysql
import logging

from dotenv import load_dotenv


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


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PDFTableProcessor:
    # Database configuration
    DB_CONFIG = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': int(os.getenv('DB_PORT', 3306)),
                'user': os.getenv('DB_USER', 'root'),
                'password': os.getenv('DB_PASSWORD', ''),
                'db': os.getenv('DB_NAME', 'exametrics'),
                'pool_size': int(os.getenv('DB_POOL_SIZE', 5)),
                'max_queries': int(os.getenv('DB_MAX_QUERIES', 5000))
            }


    async def _save_to_database(school_info: Dict[str, str], filename: str):
        """
        Saves processing information to database asynchronously
        """
        try:
            # Create connection pool
            pool = await aiomysql.create_pool(**PDFTableProcessor.DB_CONFIG)
            async with pool.acquire() as connection:
                async with connection.cursor() as cursor:
                    # Create table if not exists
                    await cursor.execute("""
                        CREATE TABLE IF NOT EXISTS school_records (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            school_name VARCHAR(255) NOT NULL,
                            centre_number VARCHAR(50),
                            exam_type VARCHAR(100),
                            exam_year VARCHAR(20),
                            file_name VARCHAR(255),
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # Insert record
                    query = """
                        INSERT INTO school_records 
                        (school_name, centre_number, exam_type, exam_year, file_name)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    values = (
                        school_info.get('SCHOOL_NAME', ''),
                        school_info.get('CENTRE_NUMBER', ''),
                        school_info.get('EXAM_TYPE', ''),
                        school_info.get('EXAM_YEAR', ''),
                        filename
                    )
                    
                    await cursor.execute(query, values)
                    await connection.commit()
                    
        except aiomysql.Error as e:
            print(f"Database error: {e}")
            traceback.print_exc()
        finally:
            if 'pool' in locals():
                pool.close()
                await pool.wait_closed()

    @staticmethod
    def extract_school_info(pdf_path: str, first_candidate: Optional[str] = None) -> Dict[str, str]:
        """Extract school information from PDF text"""
        school_info = {
            'EXAM_TYPE': '',
            'CENTRE_NUMBER': '',
            'EXAM_YEAR': '',
            'SCHOOL_NAME': '',
            'SCHOOL_TYPE':''
        }
        
        prefix_mapping = {
            'P': 'PSLE',
            'S': 'CSEE',
            'F': 'FTNA',
            'D': 'DSEE',
            'A': 'ACSEE',
            'T': 'STNA',
            'N': 'SFNA'
        }
        
        # if first_candidate:
        #     match = re.search(r'([A-Za-z]+)\d+', first_candidate.upper())
        #     if match and match.group(1) and match.group(1)[0] in prefix_mapping:
        #         school_info['EXAM_TYPE'] = prefix_mapping[match.group(1)[0]]

        with pdfplumber.open(pdf_path) as pdf:
            text_lines = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    text_lines.extend(text.split('\n'))
            
            pattern = re.compile(
                r'(?P<exam_type>STNA|SFNA|PSLE|FTNA|CSEE|ACSEE|DSEE)\s*(?P<year>\d{4})\s*[:;]\s*(?P<centre>[A-Z]\d+)\s*[-–]\s*(?P<school>.+)',
                re.IGNORECASE
            )
            
            alt_pattern = re.compile(
                r'(?P<centre>[A-Z]\d+)\s*[-–]\s*(?P<school>.+)',
                re.IGNORECASE
            )
            
            for line in text_lines:
                line = line.strip()
                if not line:
                    continue
                
                match = pattern.search(line)
                if match:
                    groups = match.groupdict()
                    if groups.get('exam_type'):
                        school_info['EXAM_TYPE'] = groups['exam_type'].upper()
                    if groups.get('year'):
                        school_info['EXAM_YEAR'] = groups['year']
                    if groups.get('centre'):
                        school_info['CENTRE_NUMBER'] = groups['centre'].upper()
                    if groups.get('school'):
                        school_info['SCHOOL_NAME'] = groups['school'].strip()
                    break
                
                match = alt_pattern.search(line)
                if match:
                    groups = match.groupdict()
                    if groups.get('centre'):
                        school_info['CENTRE_NUMBER'] = groups['centre'].upper()
                    if groups.get('school'):
                        school_info['SCHOOL_NAME'] = groups['school'].strip()
                    break
            
            if school_info['SCHOOL_NAME']:
                school_info['SCHOOL_NAME'] = re.sub(r'[^\w\s-]', '', school_info['SCHOOL_NAME']).strip()
                school_info['SCHOOL_NAME'] = re.sub(r'[A-Za-z]?\d{4,}', '', school_info['SCHOOL_NAME']).strip()
            
            school_info = {k: v for k, v in school_info.items() if v}
            
            return school_info

    @staticmethod
    def _is_valid_necta(table: pd.DataFrame, table_index: int, is_fee_table: bool) -> Tuple[bool, str]:
        """Validate NECTA document structure and determine school type"""
        required = {'CANDIDATE', 'SEX', 'FULL NAME'}
        actual = set(str(c).upper() for c in table.columns)
        is_valid = len(required.intersection(actual)) >= 2
        
        school_type = ''
        if is_valid:
            school_type = "PRIVATE" if is_fee_table else "GOVERNMENT"
        
        # logger.info(f"Validating table {table_index + 1}: Columns={list(table.columns)}, IsFeeTable={is_fee_table}, IsValidNECTA={is_valid}, SchoolType={school_type}")
        return is_valid, school_type

    @staticmethod
    def extract_tables(pdf_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, str]]:
        """Extract and process tables from PDF"""
        with pdfplumber.open(pdf_path) as pdf:
            tables = []
            for page in pdf.pages:
                page_tables = page.extract_tables()
                for table in page_tables:
                    if table and len(table) > 1:
                        clean_table = [
                            [str(c).strip() if c is not None else '' 
                            for c in row] 
                            for row in table if any(c and str(c).strip() for c in row)
                        ]
                        if len(clean_table) > 1:
                            df = pd.DataFrame(clean_table[1:], columns=clean_table[0])
                            tables.append(df)
            
            if not tables:
                raise ValueError("No tables found in PDF")
            
            # Check if first table is a fee table
            fee_columns = {'DEPOSITOR NAME', 'DATE', 'CONTROL NO.', 'AMOUNT', 'CAND NO.'}
            start_index = 0
            is_fee_table = False
            if tables and set(str(c).upper() for c in tables[0].columns).issuperset(fee_columns):
                start_index = 1
                is_fee_table = True
                logger.info("Detected fee table as first table, skipping to second table")
            else:
                logger.info("No fee table detected, starting with first table")
            
            # Validate NECTA format using the appropriate table
            if start_index >= len(tables):
                logger.error("No tables available after skipping fee table")
                raise ValueError("No valid tables found after fee table")
            
            is_valid, school_type = PDFTableProcessor._is_valid_necta(tables[start_index], start_index, is_fee_table)
            if not is_valid:
                logger.error(f"Table {start_index + 1} is not a valid NECTA document")
                raise ValueError("Invalid NECTA document format")
            
            # Log columns of all tables for debugging
            for i, table in enumerate(tables):
                # logger.info(f"Table {i + 1} columns: {list(table.columns)}")
                pass
            
            first_candidate = tables[start_index].iloc[0, 0] if len(tables[start_index]) > 0 else None
            school_info = PDFTableProcessor.extract_school_info(pdf_path, first_candidate)
            school_info['SCHOOL_TYPE'] = school_type
            
            main_data = PDFTableProcessor._process_main_tables(tables[start_index:-1])
            subjects_df = PDFTableProcessor._process_subjects_table(tables[-1])
            
            return main_data, subjects_df, school_info

    @staticmethod
    def _process_main_tables(tables: list) -> pd.DataFrame:
        """Process student data tables"""
        processed_tables = []
        columns_removed = set()

        for i, table in enumerate(tables):
            # Step 1: Clean and validate column names
            cleaned_columns = []
            for col in table.columns:
                col_str = str(col).strip().upper() if col is not None else ''
                cleaned_columns.append(col_str if col_str != '' else None)  # Mark empty/invalid as None

            # Step 2: Filter out None (invalid) columns and corresponding data
            valid_column_indices = [i for i, col in enumerate(cleaned_columns) if col is not None]
            valid_columns = [cleaned_columns[i] for i in valid_column_indices]
            table = table.iloc[:, valid_column_indices]
            table.columns = valid_columns

            if not table.columns.any():
                logger.warning(f"Table {i + 1} has no valid columns after stripping. Skipping.")
                continue

            # Step 3: Handle duplicate columns
            if len(table.columns) != len(set(table.columns)):
                duplicates = [col for col in table.columns if list(table.columns).count(col) > 1]
                logger.warning(f"Duplicate columns found in table {i + 1}: {duplicates}")
                new_columns = []
                col_count = {}
                for col in table.columns:
                    if col in col_count:
                        col_count[col] += 1
                        new_columns.append(f"{col}_{col_count[col]}")
                    else:
                        col_count[col] = 0
                        new_columns.append(col)
                table.columns = new_columns
                logger.info(f"Renamed columns to resolve duplicates in table {i + 1}: {list(table.columns)}")

            # Step 4: Ensure core columns exist
            core_columns = ['CANDIDATE', 'SEX', 'FULL NAME']
            for col in core_columns:
                if col not in table.columns:
                    table[col] = np.nan
                    logger.info(f"Added missing core column '{col}' to table {i + 1}")

            # Step 5: Process subject columns with numeric 3-digit codes
            for col in table.columns:
                if str(col).isdigit() and len(col) == 3:
                    table.loc[:, col] = table[col].apply(  # ✅ Use .loc to avoid SettingWithCopyWarning
                        lambda x: 1 if str(x).strip().lower() in ['✓', '✔', 'x', 'tick'] else np.nan
                    )

            # Step 6: Optionally remove last 3 columns (footer columns)
            original_columns = list(table.columns)
            if len(original_columns) >= 4:
                columns_to_keep = original_columns[:-3]
                removed_cols = original_columns[-3:]
                columns_removed.update(removed_cols)
                table = table[columns_to_keep].copy()  # ✅ Use .copy() to ensure we're working on a fresh object

            processed_tables.append(table)

        # Step 7: Merge and clean the final DataFrame
        if not processed_tables:
            logger.error("No valid tables found to process.")
            raise ValueError("No valid student data tables available.")

        try:
            merged = pd.concat(processed_tables, ignore_index=True)
            # logger.info(f"Successfully concatenated tables into DataFrame with columns: {list(merged.columns)}")
        except Exception as e:
            # logger.error(f"Failed to concatenate tables: {str(e)}")
            raise

        return merged.replace({np.nan: None})






    @staticmethod
    def _process_subjects_table(table: pd.DataFrame) -> pd.DataFrame:
        """Process subjects table"""
        data_start = 0
        for i, row in table.iterrows():
            if str(row.iloc[0]).strip().isdigit():
                data_start = i
                break
        
        subjects = []
        for i in range(data_start, len(table)):
            row = table.iloc[i]
            if len(row) < 3:
                continue
                
            serial = str(row.iloc[0]).strip()
            if not serial.isdigit():
                continue
                
            subject_str = str(row.iloc[1]).strip()
            count_str = str(row.iloc[2]).strip() if len(row) > 2 else "0"
            
            if '-' in subject_str:
                code, name = subject_str.split('-', 1)
                code = code.strip()
                name = name.strip()
                
                try:
                    count = int(count_str)
                except ValueError:
                    count = 0
                
                subjects.append({
                    'CODE': code,
                    'SUBJECT': name,
                    'REGISTERED': count
                })
        
        subjects_df = pd.DataFrame(subjects)
        if not subjects_df.empty:
            subjects_df = subjects_df.sort_values('CODE')
        
        return subjects_df

    @staticmethod
    def _split_full_name(full_name: str) -> Tuple[str, str, str]:
        """Split full name into components"""
        if not full_name or pd.isna(full_name):
            return '', '', ''
        
        parts = str(full_name).strip().split()
        
        if len(parts) == 0:
            return '', '', ''
        elif len(parts) == 1:
            return parts[0], '', ''
        elif len(parts) == 2:
            return parts[0], '', parts[1]
        else:
            return parts[0], ' '.join(parts[1:-1]), parts[-1]

    @staticmethod
    def _generate_output_filename(data: pd.DataFrame, school_info: Dict[str, str]) -> str:
        """Generate output filename"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if not school_info:
            return f"NECTA_Results_{timestamp}.xlsx"
        
        centre = school_info.get('CENTRE_NUMBER', '').strip()
        exam_type = school_info.get('EXAM_TYPE', '').strip()
        year = school_info.get('EXAM_YEAR', '').strip()
        school = school_info.get('SCHOOL_NAME', '').strip()
        
        if school:
            school = re.sub(r'\bsecondary school\b', 'SS', school, flags=re.IGNORECASE)
            school = re.sub(r'[^\w\s-]', '', school).strip()
            school = re.sub(r'\s+', ' ', school)
        
        parts = []
        if centre: parts.append(centre)
        if exam_type: parts.append(exam_type)
        if year: parts.append(year)
        if school: parts.append(school)
        
        if parts:
            filename = " - ".join(parts) + f" - {timestamp}.xlsx"
        else:
            filename = f"NECTA_Results_{timestamp}.xlsx"
        
        filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
        return filename

    @staticmethod
    def convert_to_excel(pdf_path: str, excel_path: str, split_names_option: bool = False) -> str:
        """Convert PDF to Excel and save to database"""
        student_data, subject_data, school_info = PDFTableProcessor.extract_tables(pdf_path)
        student_data = student_data.drop(columns=[col for col in student_data.columns if col in ['REPEATER', 'RETAEPER'] or col is None or str(col).strip() == ''], errors='ignore')
                
        if split_names_option and 'FULL NAME' in student_data.columns:
            name_parts = student_data['FULL NAME'].apply(
                PDFTableProcessor._split_full_name
            ).apply(pd.Series)
            name_parts.columns = ['FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME']
            
            full_name_idx = student_data.columns.get_loc('FULL NAME')
            for i, col in enumerate(['FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME']):
                student_data.insert(full_name_idx + i, col, name_parts[col])
            
            student_data = student_data.drop(columns=['FULL NAME'], errors='ignore')
        
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            border_format = workbook.add_format({'border': 1, 'valign': 'top'})
            header_format = workbook.add_format({
                'bold': True, 'border': 1, 'valign': 'top',
                'align': 'center', 'fg_color': '#4472C4', 'font_color': 'white'
            })
            center_format = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'top'})
            number_format = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'top', 'num_format': '0'})
            
            school_df = pd.DataFrame([school_info])
            school_df.to_excel(writer, sheet_name='School Info', index=False, startrow=1, header=False)
            school_sheet = writer.sheets['School Info']
            
            for col_num, value in enumerate(school_df.columns.values):
                school_sheet.write(0, col_num, value, header_format)
            
            for col in range(0, len(school_df.columns)):
                school_sheet.write(1, col, school_df.iloc[0, col], border_format)
            
            school_sheet.set_column('A:A', 15)
            school_sheet.set_column('B:B', 15)
            school_sheet.set_column('C:C', 12)
            school_sheet.set_column('D:D', 50)
            
            student_data.to_excel(writer, sheet_name='Students', index=False, startrow=1, header=False)
            student_sheet = writer.sheets['Students']
            
            for col_num, value in enumerate(student_data.columns.values):
                student_sheet.write(0, col_num, value, header_format)
            
            max_row = len(student_data)
            max_col = len(student_data.columns) - 1
            
            for row in range(1, max_row + 1):
                for col in range(0, max_col + 1):
                    if str(student_data.columns[col]).isdigit() and len(student_data.columns[col]) == 3:
                        student_sheet.write(row, col, student_data.iloc[row-1, col], center_format)
                    elif student_data.columns[col] in ['CANDIDATE', 'SEX']:
                        student_sheet.write(row, col, student_data.iloc[row-1, col], center_format)
                    else:
                        student_sheet.write(row, col, student_data.iloc[row-1, col], border_format)
            
            for col_num, col_name in enumerate(student_data.columns):
                if str(col_name).isdigit() and len(col_name) == 3:
                    student_sheet.set_column(col_num, col_num, 8)
                elif col_name in ['CANDIDATE', 'SEX']:
                    student_sheet.set_column(col_num, col_num, 12)
                elif col_name in ['FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME']:
                    student_sheet.set_column(col_num, col_num, 20)
                else:
                    max_len = max(student_data[col_name].astype(str).map(len).max(), len(col_name)) + 2
                    student_sheet.set_column(col_num, col_num, max_len)
            
            subject_data.to_excel(writer, sheet_name='Subjects', index=False, startrow=1, header=False)
            subject_sheet = writer.sheets['Subjects']
            
            for col_num, value in enumerate(subject_data.columns.values):
                subject_sheet.write(0, col_num, value, header_format)
            
            max_row = len(subject_data)
            max_col = len(subject_data.columns) - 1
            
            for row in range(1, max_row + 1):
                for col in range(0, max_col + 1):
                    if subject_data.columns[col] == 'REGISTERED':
                        subject_sheet.write(row, col, subject_data.iloc[row-1, col], number_format)
                    else:
                        subject_sheet.write(row, col, subject_data.iloc[row-1, col], border_format)
            
            subject_sheet.set_column('A:A', 10)
            subject_sheet.set_column('B:B', 40)
            subject_sheet.set_column('C:C', 12)
            
            for sheet in [school_sheet, student_sheet, subject_sheet]:
                sheet.freeze_panes(1, 0)
                sheet.set_landscape()
                sheet.fit_to_pages(1, 0)
                sheet.set_margins(left=0.5, right=0.5, top=0.5, bottom=0.5)
                sheet.repeat_rows(0)
                sheet.set_footer('&RPage &P of &N')
            
            student_sheet.set_header('&C&"Calibri,Bold"&14Student Registration Data')
            subject_sheet.set_header('&C&"Calibri,Bold"&14Subject Information')
            school_sheet.set_header('&C&"Calibri,Bold"&14School Information')
            
            student_sheet.autofilter(0, 0, 0, max_col)
        
        # Save to database after successful Excel creation
        PDFTableProcessor._save_to_database(school_info, os.path.basename(excel_path))
        
        return excel_path


    @staticmethod
    def parse_pdf_to_data(pdf_path: str, split_names_option: bool = True) -> tuple:
        """Convert PDF to Excel and return extracted data"""
        student_data, subject_data, school_info = PDFTableProcessor.extract_tables(pdf_path)
        student_data = student_data.drop(columns=[col for col in student_data.columns if col in ['REPEATER', 'RETAEPER'] or col is None or str(col).strip() == ''], errors='ignore')
                
        if split_names_option and 'FULL NAME' in student_data.columns:
            name_parts = student_data['FULL NAME'].apply(
                PDFTableProcessor._split_full_name
            ).apply(pd.Series)
            name_parts.columns = ['FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME']
            
            full_name_idx = student_data.columns.get_loc('FULL NAME')
            for i, col in enumerate(['FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME']):
                student_data.insert(full_name_idx + i, col, name_parts[col])
            
            student_data = student_data.drop(columns=['FULL NAME'], errors='ignore')
        
        result={ 
            'student_data': student_data, 
            'subject_data': subject_data, 
            'school_info': school_info 
        }
        return result


    @staticmethod
    def prepare_insert_statements(pdf_path: str, exam_id: str) -> List[str]:
        """Prepare SQL insert statements for the extracted data"""
        data = PDFTableProcessor.parse_pdf_to_data(pdf_path)
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary containing student_data, subject_data, and school_info")    

        if 'student_data' not in data or 'subject_data' not in data or 'school_info' not in data:
            raise ValueError("Data dictionary must contain 'student_data', 'subject_data', and 'school_info' keys") 
    
        if not isinstance(data['student_data'], pd.DataFrame) or not isinstance(data['subject_data'], pd.DataFrame):
            raise ValueError("student_data and subject_data must be pandas DataFrames")
    
        if not isinstance(data['school_info'], dict):
            raise ValueError("school_info must be a dictionary")
        
        insert_statements = []
        
        # 1. Register school_info
        school_data = data['school_info']
        insert_school = f"""
        INSERT IGNORE INTO schools (centre_number, school_name, school_type)
        VALUES ('{school_data['CENTRE_NUMBER'].replace("'", "''")}', '{school_data['SCHOOL_NAME'].replace("'", "''")}', 'unknown');
        """
        insert_statements.append(insert_school)

        # 2. Register exam_subjects
        subject_data = data['subject_data']
        subject_inserts = []
        for _, row in subject_data.iterrows():
            subject_code = row['CODE'].replace("'", "''")
            subject_name = row['SUBJECT'].replace("'", "''")
            subject_short = subject_name[:20].replace("'", "''")  # Truncate and escape
            # Insert into exam_subjects, fetching details from subjects table if available
            subject_inserts.append(f"""
            INSERT IGNORE INTO exam_subjects (
                subject_code, exam_id, subject_name, subject_short, is_present, has_practical, exclude_from_gpa
            )
            SELECT 
                '{subject_code}', 
                '{exam_id}', 
                COALESCE((SELECT subject_name FROM subjects WHERE subject_code = '{subject_code}'), '{subject_name}'),
                COALESCE((SELECT subject_short FROM subjects WHERE subject_code = '{subject_code}'), '{subject_short}'),
                TRUE,
                COALESCE((SELECT has_practical FROM subjects WHERE subject_code = '{subject_code}'), 0),
                COALESCE((SELECT exclude_from_gpa FROM subjects WHERE subject_code = '{subject_code}'), 0);
            """)
        insert_statements.extend(subject_inserts)

        # 3. Register students
        student_data = data['student_data']
        student_inserts = []
        for _, row in student_data.iterrows():
            student_id = row['CANDIDATE'].replace("'", "''")
            first_name = row['FIRST_NAME'].replace("'", "''")
            middle_name = row['MIDDLE_NAME'].replace("'", "''") if pd.notnull(row['MIDDLE_NAME']) else None
            last_name = row['LAST_NAME'].replace("'", "''")
            sex = row['SEX'].replace("'", "''")
            centre_number = school_data['CENTRE_NUMBER'].replace("'", "''")
            student_insert = f"""
            INSERT IGNORE INTO students (student_global_id, student_id, first_name, middle_name, surname, sex, centre_number)
            VALUES ('{str(uuid7())}', '{student_id}', '{first_name}', 
                    {'NULL' if middle_name is None else f"'{middle_name}'"}, 
                    '{last_name}', '{sex}', '{centre_number}');
            """
            student_inserts.append(student_insert)
        insert_statements.extend(student_inserts)

        # 4. Register student subjects
        student_subject_inserts = []
        subject_columns = [col for col in student_data.columns if col not in ['CANDIDATE', 'SEX', 'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME']]
        for _, row in student_data.iterrows():
            student_id = row['CANDIDATE'].replace("'", "''")
            for subject_code in subject_columns:
                registration_status = row[subject_code]
                if pd.notnull(registration_status) and registration_status == 1:
                    student_subject_inserts.append(f"""
                    INSERT IGNORE INTO student_subjects (exam_id, student_id, subject_code)
                    VALUES ('{exam_id}', '{student_id}', '{subject_code.replace("'", "''")}');
                    """)
        insert_statements.extend(student_subject_inserts)

        return insert_statements

    @staticmethod
    async def execute_insert_statements(insert_statements: List[str]) -> Dict[str, Dict[str, int]]:
        """
        Execute a list of SQL insert statements using aiomysql, tracking metrics for affected tables.

        Args:
            insert_statements (List[str]): List of SQL INSERT statements to execute.

        Returns:
            Dict[str, Dict[str, int]]: Metrics for each affected table (inserted, duplicates, failures) and errors.

        Raises:
            ValueError: If required database configuration is missing (excluding empty password).
            RuntimeError: If database connection or execution fails.
        """
        # Validate configuration (allow empty password)
        required_fields = ['host', 'user', 'db']
        missing_fields = [field for field in required_fields if not PDFTableProcessor.DB_CONFIG[field]]
        if missing_fields:
            logger.error(f"Missing required DB configuration: {missing_fields}")
            raise ValueError(f"Missing DB configuration: {missing_fields}")
        
        if not PDFTableProcessor.DB_CONFIG['password']:
            logger.warning("DB_PASSWORD is empty. This is insecure for production environments.")

        pool = None
        conn = None
        cursor = None
        # Initialize results for tables that might be affected
        possible_tables = ['examination_boards', 'exams', 'schools', 'exam_subjects', 'students', 'student_subjects']
        results = {table: {'inserted': 0, 'duplicates': 0, 'failures': 0} for table in possible_tables}
        results['errors'] = []

        try:
            # Create connection pool
            logger.info("Creating MySQL connection pool")
            pool = await aiomysql.create_pool(
                host=PDFTableProcessor.DB_CONFIG['host'],
                port=PDFTableProcessor.DB_CONFIG['port'],
                user=PDFTableProcessor.DB_CONFIG['user'],
                password=PDFTableProcessor.DB_CONFIG['password'],
                db=PDFTableProcessor.DB_CONFIG['db'],
                maxsize=PDFTableProcessor.DB_CONFIG['pool_size'],
                charset='utf8mb4'
            )

            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    for idx, statement in enumerate(insert_statements):
                        # Identify table type
                        statement_lower = statement.lower()
                        table = None
                        for possible_table in possible_tables:
                            if possible_table in statement_lower:
                                table = possible_table
                                break
                        if not table:
                            logger.warning(f"Unknown table in statement {idx + 1}: {statement.strip()}")
                            table = 'unknown'

                        # Use INSERT IGNORE for exam_subjects, students, and student_subjects
                        if table in ['exam_subjects', 'students', 'student_subjects']:
                            statement = statement.replace('INSERT INTO', 'INSERT IGNORE INTO')

                        try:
                            logger.debug(f"Executing statement {idx + 1} for {table}: {statement.strip()}")
                            await cursor.execute(statement)
                            affected_rows = cursor.rowcount
                            if affected_rows == 0 and 'INSERT IGNORE' in statement:
                                results[table]['duplicates'] += 1
                            else:
                                results[table]['inserted'] += 1
                        except aiomysql.Error as e:
                            error_code, error_msg = e.args
                            error_detail = f"Error executing statement {idx + 1} for {table}: ({error_code}, '{error_msg}') - Statement: {statement.strip()}"
                            logger.error(error_detail)
                            if error_code == 1062:  # Duplicate entry
                                results[table]['duplicates'] += 1
                            else:
                                results[table]['failures'] += 1
                                results['errors'].append(error_detail)

                    # Commit the transaction
                    await conn.commit()

                    # Filter results to only include affected tables
                    affected_results = {
                        table: metrics for table, metrics in results.items()
                        if table != 'errors' and (metrics['inserted'] > 0 or metrics['duplicates'] > 0 or metrics['failures'] > 0)
                    }
                    affected_results['errors'] = results['errors']
                    logger.info(f"Execution summary: {affected_results}")

        except aiomysql.Error as e:
            logger.error(f"Database error: {str(e)}")
            raise RuntimeError(f"Database error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise RuntimeError(f"Unexpected error: {str(e)}")
        finally:
            if cursor:
                await cursor.close()
            if conn:
                await conn.close()
            if pool:
                pool.close()
                await pool.wait_closed()
                logger.info("Database connection pool closed")

        logger.debug(f"Returning results: {affected_results}")
        return affected_results




