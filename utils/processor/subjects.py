import aiomysql
import pandas as pd
import numpy as np
import time
import logging
import nest_asyncio
try:
    from app.core.config import Settings
except ImportError:
    logging.error("Failed to import Settings from app.core.config. Please verify the module path.")
    raise

# Apply nest_asyncio to allow running async code in environments like Jupyter
nest_asyncio.apply()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SubjectProcessor:
    def __init__(self, exam_id: str, settings: Settings):
        self.exam_id = exam_id
        self.settings = settings
        self.RANKING_COLUMNS = [
            'subject_pos', 'subject_out_of',
            'ward_subject_pos', 'ward_subject_out_of',
            'council_subject_pos', 'council_subject_out_of',
            'region_subject_pos', 'region_subject_out_of',
            'ward_subject_pos_gvt', 'ward_subject_out_of_gvt',
            'ward_subject_pos_pvt', 'ward_subject_out_of_pvt',
            'council_subject_pos_gvt', 'council_subject_out_of_gvt',
            'council_subject_pos_pvt', 'council_subject_out_of_pvt',
            'region_subject_pos_gvt', 'region_subject_out_of_gvt',
            'region_subject_pos_pvt', 'region_subject_out_of_pvt',
            'subject_grade', 'overall_marks'
        ]
        self.GRADES_DATA = None
        self.DIVISIONS_DATA = None
        self.STUDENT_SUBJECTS_DF = None

    async def load_grades(self):
        async with aiomysql.create_pool(
            host=self.settings.DB_HOST,
            port=self.settings.DB_PORT,
            user=self.settings.DB_USER,
            password=self.settings.DB_PASSWORD,
            db=self.settings.DB_NAME,
            maxsize=5
        ) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT exam_id, grade, lower_value, highest_value, grade_points, division_points FROM exam_grades WHERE exam_id = %s",
                        (self.exam_id,)
                    )
                    return await cur.fetchall()

    async def load_divisions(self):
        async with aiomysql.create_pool(
            host=self.settings.DB_HOST,
            port=self.settings.DB_PORT,
            user=self.settings.DB_USER,
            password=self.settings.DB_PASSWORD,
            db=self.settings.DB_NAME,
            maxsize=5
        ) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT exam_id, division, lowest_points, highest_points FROM exam_divisions WHERE exam_id = %s",
                        (self.exam_id,)
                    )
                    return await cur.fetchall()

    def lookup_grade_and_division(self, marks: float | None = None, grade: str | None = None, points: int | None = None, return_type: str = "grade"):
        if not self.exam_id or (self.GRADES_DATA is None and return_type != "division") or (self.DIVISIONS_DATA is None and return_type == "division"):
            return None

        if marks is not None and return_type in ["grade", "grade_points", "division_points"]:
            for row in self.GRADES_DATA:
                if row[0] == self.exam_id and row[2] <= marks <= row[3]:
                    if return_type == "grade":
                        return row[1]
                    elif return_type == "grade_points":
                        return row[4]
                    elif return_type == "division_points":
                        return row[5]
            return None

        elif grade is not None and return_type in ["grade_points", "division_points"]:
            for row in self.GRADES_DATA:
                if row[0] == self.exam_id and row[1] == grade:
                    if return_type == "grade_points":
                        return row[4]
                    elif return_type == "division_points":
                        return row[5]
            return None

        elif points is not None and return_type == "division":
            min_points = min([row[2] for row in self.DIVISIONS_DATA if row[0] == self.exam_id], default=float('inf'))
            if points < min_points:
                return None
            for row in self.DIVISIONS_DATA:
                if row[0] == self.exam_id and row[2] <= points <= row[3]:
                    return row[1]
            return None

        return None

    async def load_student_subjects(self):
        async with aiomysql.create_pool(
            host=self.settings.DB_HOST,
            port=self.settings.DB_PORT,
            user=self.settings.DB_USER,
            password=self.settings.DB_PASSWORD,
            db=self.settings.DB_NAME,
            maxsize=5
        ) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """SELECT id, exam_id, student_global_id, centre_number, subject_code,
                        theory_marks, practical_marks, overall_marks, subject_pos, subject_out_of,
                        ward_subject_pos, ward_subject_out_of, council_subject_pos, council_subject_out_of,
                        region_subject_pos, region_subject_out_of, ward_subject_pos_gvt, ward_subject_pos_pvt,
                        council_subject_pos_gvt, council_subject_pos_pvt, region_subject_pos_gvt,
                        region_subject_pos_pvt, subject_grade,
                        ward_subject_out_of_gvt, ward_subject_out_of_pvt,
                        council_subject_out_of_gvt, council_subject_out_of_pvt,
                        region_subject_out_of_gvt, region_subject_out_of_pvt
                        FROM student_subjects 
                        WHERE exam_id = %s""",
                        (self.exam_id,)
                    )
                    rows = await cur.fetchall()
                    columns = [
                        'id', 'exam_id', 'student_global_id', 'centre_number', 'subject_code',
                        'theory_marks', 'practical_marks', 'overall_marks', 'subject_pos', 'subject_out_of',
                        'ward_subject_pos', 'ward_subject_out_of', 'council_subject_pos', 'council_subject_out_of',
                        'region_subject_pos', 'region_subject_out_of', 'ward_subject_pos_gvt', 'ward_subject_pos_pvt',
                        'council_subject_pos_gvt', 'council_subject_pos_pvt', 'region_subject_pos_gvt',
                        'region_subject_pos_pvt', 'subject_grade',
                        'ward_subject_out_of_gvt', 'ward_subject_out_of_pvt',
                        'council_subject_out_of_gvt', 'council_subject_out_of_pvt',
                        'region_subject_out_of_gvt', 'region_subject_out_of_pvt'
                    ]
                    return pd.DataFrame(rows, columns=columns)

    async def load_schools(self):
        async with aiomysql.create_pool(
            host=self.settings.DB_HOST,
            port=self.settings.DB_PORT,
            user=self.settings.DB_USER,
            password=self.settings.DB_PASSWORD,
            db=self.settings.DB_NAME,
            maxsize=5
        ) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT centre_number, school_name, region_id, council_id, ward_id, region_name, council_name, ward_name, school_type FROM schools"
                    )
                    rows = await cur.fetchall()
                    columns = ['centre_number', 'school_name', 'region_id', 'council_id', 'ward_id', 'region_name', 'council_name', 'ward_name', 'school_type']
                    return pd.DataFrame(rows, columns=columns)

    async def load_exam_subjects(self):
        async with aiomysql.create_pool(
            host=self.settings.DB_HOST,
            port=self.settings.DB_PORT,
            user=self.settings.DB_USER,
            password=self.settings.DB_PASSWORD,
            db=self.settings.DB_NAME,
            maxsize=5
        ) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT exam_id, subject_code, has_practical FROM exam_subjects WHERE exam_id = %s",
                        (self.exam_id,)
                    )
                    rows = await cur.fetchall()
                    columns = ['exam_id', 'subject_code', 'has_practical']
                    return pd.DataFrame(rows, columns=columns)

    async def calculate_grades_and_marks(self):
        self.GRADES_DATA = await self.load_grades()
        self.DIVISIONS_DATA = await self.load_divisions()
        student_subjects_df = await self.load_student_subjects()
        logging.info(f"Loaded {len(student_subjects_df)} student subject records")
        schools_df = await self.load_schools()
        logging.info(f"Loaded {len(schools_df)} school records")
        exam_subjects_df = await self.load_exam_subjects()
        logging.info(f"Loaded {len(exam_subjects_df)} exam subject records")

        # Merge student_subjects with exam_subjects to get has_practical
        df = student_subjects_df.merge(exam_subjects_df, on=['exam_id', 'subject_code'], how='left')
        logging.info(f"Merged DataFrame has {len(df)} records")

        # Calculate overall_marks
        def calculate_overall_marks(row):
            if row['has_practical']:
                if pd.notnull(row['theory_marks']) or pd.notnull(row['practical_marks']):
                    theory = row['theory_marks'] if pd.notnull(row['theory_marks']) else 0
                    practical = row['practical_marks'] if pd.notnull(row['practical_marks']) else 0
                    return (theory + practical) * 2 / 3
                return None
            return row['theory_marks']

        df['overall_marks'] = df.apply(calculate_overall_marks, axis=1)

        # Calculate subject_grade
        def calculate_subject_grade(row):
            if pd.notnull(row['overall_marks']):
                return self.lookup_grade_and_division(
                    marks=row['overall_marks'],
                    return_type="grade"
                )
            return None

        df['subject_grade'] = df.apply(calculate_subject_grade, axis=1)

        # Merge with schools to get region_name, council_name, ward_name, school_type
        df = df.merge(schools_df[['centre_number', 'region_name', 'council_name', 'ward_name', 'school_type']],
                      on='centre_number', how='left')

        self.STUDENT_SUBJECTS_DF = df

    def calculate_rankings(self):
        start_time = time.time()
        logging.info("Starting optimized ranking calculation...")

        df = self.STUDENT_SUBJECTS_DF
        valid_mask = df['overall_marks'].notnull() & (df['overall_marks'] >= 0)
        df_valid = df[valid_mask].copy()
        logging.info(f"Valid records: {len(df_valid)}")

        # Initialize ranking columns
        def init_columns(columns):
            for col in columns:
                df[col] = pd.NA

        rank_cols = [
            'subject_pos', 'subject_out_of',
            'ward_subject_pos', 'ward_subject_out_of',
            'council_subject_pos', 'council_subject_out_of',
            'region_subject_pos', 'region_subject_out_of',
            'ward_subject_pos_gvt', 'ward_subject_out_of_gvt',
            'ward_subject_pos_pvt', 'ward_subject_out_of_pvt',
            'council_subject_pos_gvt', 'council_subject_out_of_gvt',
            'council_subject_pos_pvt', 'council_subject_out_of_pvt',
            'region_subject_pos_gvt', 'region_subject_out_of_gvt',
            'region_subject_pos_pvt', 'region_subject_out_of_pvt'
        ]
        init_columns(rank_cols)

        # Apply ranking with optional filtering
        def apply_ranking(group_keys, rank_col, out_of_col=None, mask=None):
            if mask is not None:
                data = df[mask]
            else:
                data = df_valid

            grouped = data.groupby(group_keys, sort=False)
            df.loc[data.index, rank_col] = grouped['overall_marks'].rank(method='min', ascending=False).astype('Int32')
            if out_of_col:
                df.loc[data.index, out_of_col] = grouped['overall_marks'].transform('count').astype('Int32')

        # Subject-level
        apply_ranking(['subject_code'], 'subject_pos', 'subject_out_of')

        # Ward-level
        ward_mask = valid_mask & df['ward_name'].notnull()
        apply_ranking(['subject_code', 'ward_name'], 'ward_subject_pos', 'ward_subject_out_of', mask=ward_mask)

        # Council-level
        council_mask = valid_mask & df['council_name'].notnull()
        apply_ranking(['subject_code', 'council_name'], 'council_subject_pos', 'council_subject_out_of', mask=council_mask)

        # Region-level
        region_mask = valid_mask & df['region_name'].notnull()
        apply_ranking(['subject_code', 'region_name'], 'region_subject_pos', 'region_subject_out_of', mask=region_mask)

        # Rank by school type (GOVERNMENT/PRIVATE) within area
        def rank_by_type(level, area_column):
            for school_type in ['GOVERNMENT', 'PRIVATE']:
                suffix = 'gvt' if school_type == 'GOVERNMENT' else 'pvt'
                mask = (
                    valid_mask &
                    df[area_column].notnull() &
                    df['school_type'].notnull() &
                    (df['school_type'].str.upper() == school_type)
                )
                rank_col = f"{level}_subject_pos_{suffix}"
                out_of_col = f"{level}_subject_out_of_{suffix}"
                apply_ranking(['subject_code', area_column], rank_col, out_of_col=out_of_col, mask=mask)

        rank_by_type('ward', 'ward_name')
        rank_by_type('council', 'council_name')
        rank_by_type('region', 'region_name')

        # Convert <NA> to None for export or compatibility
        df = df.where(pd.notna(df), None)

        # Final selection of fields
        self.STUDENT_SUBJECTS_DF = df[[
            'id', 'exam_id', 'student_global_id', 'centre_number', 'subject_code',
            'theory_marks', 'practical_marks', 'overall_marks',
            'subject_pos', 'subject_out_of',
            'ward_subject_pos', 'ward_subject_out_of',
            'council_subject_pos', 'council_subject_out_of',
            'region_subject_pos', 'region_subject_out_of',
            'ward_subject_pos_gvt', 'ward_subject_out_of_gvt',
            'ward_subject_pos_pvt', 'ward_subject_out_of_pvt',
            'council_subject_pos_gvt', 'council_subject_out_of_gvt',
            'council_subject_pos_pvt', 'council_subject_out_of_pvt',
            'region_subject_pos_gvt', 'region_subject_out_of_gvt',
            'region_subject_pos_pvt', 'region_subject_out_of_pvt',
            'subject_grade'
        ]]

        logging.info(f"Ranking calculation completed in {time.time() - start_time:.2f} seconds")

    async def update_student_subjects_rankings(self, batch_size: int = 5000) -> int:
        df = self.STUDENT_SUBJECTS_DF.copy()
        df = df.replace({np.nan: None})  # Replace NaN with None for MySQL

        updated_count = 0
        total = len(df)

        pool = await aiomysql.create_pool(
            host=self.settings.DB_HOST,
            port=self.settings.DB_PORT,
            user=self.settings.DB_USER,
            password=self.settings.DB_PASSWORD,
            db=self.settings.DB_NAME,
            maxsize=5,
            autocommit=True
        )

        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for start in range(0, total, batch_size):
                    end = min(start + batch_size, total)
                    batch = df.iloc[start:end]

                    update_values = []
                    for _, row in batch.iterrows():
                        update_values.append([
                            *(row.get(col) for col in self.RANKING_COLUMNS),
                            row['id']
                        ])

                    set_clause = ", ".join([f"{col} = %s" for col in self.RANKING_COLUMNS])
                    query = f"""
                        UPDATE student_subjects
                        SET {set_clause}
                        WHERE id = %s
                    """

                    await cur.executemany(query, update_values)
                    logging.info(f"Updated batch {start}–{end - 1}: {len(update_values)} records")
                    updated_count += len(update_values)

        pool.close()
        await pool.wait_closed()
        return updated_count

    async def export_subject_data(self, subject_code: str, filename: str = "subject_011_only.csv"):
        if self.STUDENT_SUBJECTS_DF is not None:
            self.STUDENT_SUBJECTS_DF[self.STUDENT_SUBJECTS_DF['subject_code'] == subject_code].to_csv(filename, index=False)
            logging.info(f"Exported data for subject {subject_code} to {filename}")
            return filename
        return None

    def get_first_row(self):
        if self.STUDENT_SUBJECTS_DF is not None:
            first_row = self.STUDENT_SUBJECTS_DF.iloc[0]
            return {column: value for column, value in first_row.items()}
        return None

    async def process_all(self, subject_code: str = "011", export_filename: str = "subject_011_only.csv") -> dict:
        """
        Performs the entire processing workflow and returns a dictionary with basic data.
        """
        start_time = time.time()
        logging.info(f"Starting process_all for exam_id: {self.exam_id}")

        result = {
            "exam_id": self.exam_id,
            "student_subjects_count": 0,
            "schools_count": 0,
            "exam_subjects_count": 0,
            "updated_records": 0,
            "exported_file": None,
            "first_row": None,
            "processing_time_seconds": 0.0,
            "success": False,
            "error": None
        }

        try:
            # Calculate grades and marks
            await self.calculate_grades_and_marks()
            result["student_subjects_count"] = len(self.STUDENT_SUBJECTS_DF) if self.STUDENT_SUBJECTS_DF is not None else 0
            result["schools_count"] = len(await self.load_schools())
            result["exam_subjects_count"] = len(await self.load_exam_subjects())

            # Calculate rankings
            self.calculate_rankings()

            # Update database
            result["updated_records"] = await self.update_student_subjects_rankings(batch_size=5000)

            # Export data
            result["exported_file"] = await self.export_subject_data(subject_code, export_filename)

            # Get first row
            result["first_row"] = self.get_first_row()

            result["processing_time_seconds"] = time.time() - start_time
            result["success"] = True
            logging.info(f"process_all completed in {result['processing_time_seconds']:.2f} seconds")

        except Exception as e:
            result["error"] = str(e)
            logging.error(f"process_all failed with error: {str(e)}")

        return result