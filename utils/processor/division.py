import pandas as pd
import numpy as np
import aiomysql
import asyncio
import uuid
import time
from typing import Dict, List, Any, Tuple
import logging
from app.core.config import settings

# Configure logging to file
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='division_processor.log',
    filemode='a'
)

class DivisionProcessor:
    def __init__(self, exam_id: str):
        self.exam_id = exam_id
        self.logger = logging.getLogger(__name__)
        self.logger.debug(f"Initialized DivisionProcessor with exam_id: {exam_id}")

    async def get_pool(self) -> aiomysql.Pool:
        self.logger.debug("Creating aiomysql connection pool")
        pool = await aiomysql.create_pool(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            db=settings.DB_NAME,
            autocommit=True
        )
        self.logger.debug("Connection pool created successfully")
        return pool

    async def validate_centres(self, pool: aiomysql.Pool) -> List[str]:
        self.logger.debug("Starting centre validation")
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                self.logger.debug(f"Executing query to find invalid centres for exam_id: {self.exam_id}")
                await cursor.execute("""
                    SELECT DISTINCT s.centre_number
                    FROM students s
                    WHERE s.exam_id = %s
                    AND s.centre_number NOT IN (SELECT centre_number FROM schools)
                """, (self.exam_id,))
                result = await cursor.fetchall()
                invalid_centres = [row[0] for row in result]
                self.logger.debug(f"Found {len(invalid_centres)} invalid centres: {invalid_centres}")
                return invalid_centres

    async def load_data(self, pool: aiomysql.Pool) -> Tuple[pd.DataFrame, ...]:
        self.logger.debug("Starting data loading")
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Students data
                self.logger.debug(f"Loading students data for exam_id: {self.exam_id}")
                await cursor.execute("""
                    SELECT s.student_global_id, s.student_id, s.full_name, s.sex, s.exam_id, 
                           s.centre_number, sc.region_name, sc.council_name, sc.ward_name, 
                           sc.school_type
                    FROM students s
                    INNER JOIN schools sc ON s.centre_number = sc.centre_number
                    WHERE s.exam_id = %s
                """, (self.exam_id,))
                students_data = await cursor.fetchall()
                students = pd.DataFrame(students_data)
                self.logger.debug(f"Loaded students: {students.shape[0]} rows, columns: {list(students.columns)}")
                if not students.empty:
                    self.logger.debug(f"Students sample: {students.head(2).to_dict(orient='records')}")

                # Exams data
                self.logger.debug("Loading exams data")
                await cursor.execute("SELECT exam_id, avg_style FROM exams WHERE exam_id = %s", (self.exam_id,))
                exams_data = await cursor.fetchall()
                exams = pd.DataFrame(exams_data)
                self.logger.debug(f"Loaded exams: {exams.shape[0]} rows, columns: {list(exams.columns)}")
                if not exams.empty:
                    self.logger.debug(f"Exams sample: {exams.head(2).to_dict(orient='records')}")

                # Exam subjects
                self.logger.debug("Loading exam subjects")
                await cursor.execute("SELECT exam_id, subject_code FROM exam_subjects WHERE exam_id = %s", (self.exam_id,))
                exam_subjects_data = await cursor.fetchall()
                exam_subjects = pd.DataFrame(exam_subjects_data)
                self.logger.debug(f"Loaded exam subjects: {exam_subjects.shape[0]} rows, columns: {list(exam_subjects.columns)}")
                if not exam_subjects.empty:
                    self.logger.debug(f"Exam subjects sample: {exam_subjects.head(2).to_dict(orient='records')}")

                # Student subjects
                self.logger.debug("Loading student subjects")
                await cursor.execute("""
                    SELECT student_global_id, exam_id, centre_number, subject_code, overall_marks 
                    FROM student_subjects 
                    WHERE exam_id = %s
                """, (self.exam_id,))
                student_subjects_data = await cursor.fetchall()
                student_subjects = pd.DataFrame(student_subjects_data)
                self.logger.debug(f"Loaded student subjects: {student_subjects.shape[0]} rows, columns: {list(student_subjects.columns)}")
                if not student_subjects.empty:
                    self.logger.debug(f"Student subjects sample: {student_subjects.head(2).to_dict(orient='records')}")

                # Exam grades
                self.logger.debug("Loading exam grades")
                await cursor.execute("""
                    SELECT exam_id, grade, lower_value, division_points 
                    FROM exam_grades 
                    WHERE exam_id = %s
                """, (self.exam_id,))
                exam_grades_data = await cursor.fetchall()
                exam_grades = pd.DataFrame(exam_grades_data)
                self.logger.debug(f"Loaded exam grades: {exam_grades.shape[0]} rows, columns: {list(exam_grades.columns)}")
                if not exam_grades.empty:
                    self.logger.debug(f"Exam grades sample: {exam_grades.head(2).to_dict(orient='records')}")
                    self.logger.debug(f"Exam grades nulls: {exam_grades[['lower_value', 'division_points']].isna().sum().to_dict()}")

                # Exam divisions
                self.logger.debug("Loading exam divisions")
                await cursor.execute("""
                    SELECT exam_id, division, lowest_points 
                    FROM exam_divisions 
                    WHERE exam_id = %s
                """, (self.exam_id,))
                exam_divisions_data = await cursor.fetchall()
                exam_divisions = pd.DataFrame(exam_divisions_data)
                self.logger.debug(f"Loaded exam divisions: {exam_divisions.shape[0]} rows, columns: {list(exam_divisions.columns)}")
                if not exam_divisions.empty:
                    self.logger.debug(f"Exam divisions sample: {exam_divisions.head(2).to_dict(orient='records')}")
                    self.logger.debug(f"Exam divisions nulls: {exam_divisions[['lowest_points', 'division']].isna().sum().to_dict()}")

        return students, exams, exam_subjects, student_subjects, exam_grades, exam_divisions

    def handle_absent_cases(self, df: pd.DataFrame, is_old_curriculum: bool, min_subjects: int) -> pd.DataFrame:
        self.logger.debug("Handling absent and invalid cases")
        best_cols = [f'best_{i}' for i in range(1, min_subjects + 1)]
        rank_cols = [
            'pos', 'out_of', 'ward_pos', 'ward_out_of', 'ward_pos_gvt', 'ward_pos_pvt',
            'council_pos', 'council_out_of', 'council_pos_gvt', 'council_pos_pvt',
            'region_pos', 'region_out_of', 'region_pos_gvt', 'region_pos_pvt', 
            'school_pos', 'school_out_of'
        ]
        null_cols = ['avg_marks', 'total_marks', 'avg_grade', 'total_points'] + rank_cols
        
        # Count valid marks
        valid_marks_count = df[best_cols].count(axis=1)
        
        # Absent: No valid marks
        absent_mask = valid_marks_count == 0
        df.loc[absent_mask, 'division'] = 'ABS'
        df.loc[absent_mask, null_cols] = np.nan
        n_abs = absent_mask.sum()
        self.logger.debug(f"Marked {n_abs} students as ABS (no valid marks)")
        if n_abs > 0:
            self.logger.debug(f"Sample ABS students: {df[absent_mask][['student_global_id', 'division'] + best_cols].head(2).to_dict(orient='records')}")

        # Incomplete: Fewer than required subjects
        incomplete_mask = (valid_marks_count > 0) & (valid_marks_count < min_subjects)
        df.loc[incomplete_mask, 'division'] = 'INC'
        df.loc[incomplete_mask, null_cols] = np.nan
        n_inc = incomplete_mask.sum()
        self.logger.debug(f"Marked {n_inc} students as INC (fewer than {min_subjects} valid marks)")
        if n_inc > 0:
            self.logger.debug(f"Sample INC students: {df[incomplete_mask][['student_global_id', 'division'] + best_cols].head(2).to_dict(orient='records')}")

        # Invalid avg_marks: < 0 or null
        invalid_mask = (df['avg_marks'] < 0) | df['avg_marks'].isna()
        df.loc[invalid_mask, 'division'] = 'ABS'
        df.loc[invalid_mask, null_cols] = np.nan
        n_invalid = invalid_mask.sum()
        self.logger.debug(f"Marked {n_invalid} students as ABS (invalid avg_marks)")
        if n_invalid > 0:
            self.logger.debug(f"Sample invalid avg_marks students: {df[invalid_mask][['student_global_id', 'avg_marks', 'division']].head(2).to_dict(orient='records')}")

        return df

    async def process_data(self, students: pd.DataFrame, exams: pd.DataFrame, 
                          exam_subjects: pd.DataFrame, student_subjects: pd.DataFrame,
                          exam_grades: pd.DataFrame, exam_divisions: pd.DataFrame) -> pd.DataFrame:
        self.logger.debug("Starting data processing")
        
        # Check curriculum based on subject '011'
        is_old_curriculum = exam_subjects[exam_subjects['exam_id'] == self.exam_id]['subject_code'].eq('011').any()
        min_subjects = 7 if is_old_curriculum else 8
        self.logger.debug(f"Curriculum: {'Old (7 subjects)' if is_old_curriculum else 'New (8 subjects)'}, min_subjects: {min_subjects}")

        # Merge students with exams
        self.logger.debug("Merging students with exams")
        df = students.merge(exams[['exam_id', 'avg_style']], on='exam_id', how='left')
        self.logger.debug(f"Merged DataFrame shape: {df.shape}, columns: {list(df.columns)}")

        # Pivot student subjects
        self.logger.debug("Pivoting student subjects")
        subject_pivot = student_subjects.pivot_table(
            index=['student_global_id', 'exam_id', 'centre_number'],
            columns='subject_code',
            values='overall_marks',
            aggfunc='first'
        ).reset_index()
        self.logger.debug(f"Pivoted subjects shape: {subject_pivot.shape}, columns: {list(subject_pivot.columns)}")
        df = df.merge(subject_pivot, on=['student_global_id', 'exam_id', 'centre_number'], how='left')
        self.logger.debug(f"After subject merge, DataFrame shape: {df.shape}, columns: {list(df.columns)}")

        # Initialize best subjects
        subject_codes = exam_subjects[exam_subjects['exam_id'] == self.exam_id]['subject_code'].tolist()
        subject_codes_dict = {self.exam_id: subject_codes}
        self.logger.debug(f"Subject codes for exam_id {self.exam_id}: {subject_codes}")
        n_subjects = min_subjects
        for i in range(1, n_subjects + 1):
            df[f'best_{i}'] = np.nan
            df[f'best_{i}_subject'] = np.nan

        # Fill best subjects
        self.logger.debug(f"Calculating top {n_subjects} subject marks and subjects")
        marks_matrix = df[subject_codes].to_numpy()
        subject_indices = np.arange(len(subject_codes))
        filled = np.where(np.isnan(marks_matrix), -1e9, marks_matrix)
        sorted_indices = np.argsort(-filled, axis=1)[:, :n_subjects]
        row_indices = np.arange(len(df))[:, None]
        top_marks = marks_matrix[row_indices, sorted_indices]
        top_subjects = np.array(subject_codes)[sorted_indices]
        for i in range(n_subjects):
            df[f'best_{i+1}'] = top_marks[:, i]
            df[f'best_{i+1}_subject'] = top_subjects[:, i]
        self.logger.debug(f"Assigned top {n_subjects} subjects and marks, columns added: {[f'best_{i+1}' for i in range(n_subjects)] + [f'best_{i+1}_subject' for i in range(n_subjects)]}")
        sample_best = df[['student_global_id'] + [f'best_{i+1}' for i in range(n_subjects)] + [f'best_{i+1}_subject' for i in range(n_subjects)]].head(2)
        self.logger.debug(f"Sample best subjects: {sample_best.to_dict(orient='records')}")

        # Calculate division points
        self.logger.debug("Creating division points lookup")
        if exam_grades.empty or exam_grades[['lower_value', 'division_points']].isna().any().any():
            self.logger.error("Exam grades empty or contains nulls, cannot create division lookup")
            raise ValueError("Invalid exam_grades data: empty or contains null values")
        division_lookup = {
            exam_id: [(lower, points) for lower, points in group.sort_values('lower_value', ascending=False)[['lower_value', 'division_points']].itertuples(index=False, name=None) if lower is not None and points is not None]
            for exam_id, group in exam_grades.groupby('exam_id')
        }
        self.logger.debug(f"Division lookup keys: {list(division_lookup.keys())}")
        if self.exam_id in division_lookup:
            self.logger.debug(f"Division lookup sample for {self.exam_id}: {division_lookup[self.exam_id][:2]}")
        else:
            self.logger.error(f"No division lookup data for exam_id {self.exam_id}")
            raise ValueError(f"No division lookup data for exam_id {self.exam_id}")

        def map_points_vectorized(best_marks: pd.Series, exam_ids: pd.Series, col_name: str) -> List[float]:
            self.logger.debug(f"Mapping points for column {col_name}, marks count: {best_marks.count()}")
            results = []
            for idx, (mark, exam_id) in enumerate(zip(best_marks, exam_ids)):
                if pd.isna(mark) or exam_id not in division_lookup:
                    results.append(np.nan)
                    continue
                if mark < 0 or mark > 100:
                    self.logger.warning(f"Outlier mark {mark} for student at index {idx} in {col_name}, assigned null points")
                    results.append(np.nan)
                    continue
                try:
                    lookup = division_lookup[exam_id]
                    for lower, points in lookup:
                        if mark >= lower:
                            results.append(points)
                            if idx < 2:
                                self.logger.debug(f"Assigned points {points} for mark {mark} in {col_name}")
                            break
                    else:
                        results.append(np.nan)
                        if idx < 2:
                            self.logger.debug(f"Assigned null points for mark {mark} in {col_name} (below lowest threshold)")
                except Exception as e:
                    self.logger.error(f"Error mapping points at index {idx}, mark {mark}, exam_id {exam_id}: {str(e)}", exc_info=True)
                    results.append(np.nan)
            self.logger.debug(f"Completed mapping points for {col_name}, results length: {len(results)}")
            return results

        for i in range(1, n_subjects + 1):
            self.logger.debug(f"Calculating points for best_{i}")
            df[f'best_{i}_points'] = map_points_vectorized(df[f'best_{i}'], df['exam_id'], f'best_{i}')
            self.logger.debug(f"best_{i}_points assigned, non-null count: {df[f'best_{i}_points'].count()}")

        # Define points_cols before calculating total_points
        points_cols = [f'best_{i}_points' for i in range(1, n_subjects + 1)]
        self.logger.debug(f"Points columns defined: {points_cols}")

        # Calculate total points
        self.logger.debug("Calculating total points")
        df['total_points'] = df[points_cols].sum(axis=1, skipna=True)
        df.loc[df[points_cols].count(axis=1) < min_subjects, 'total_points'] = -1
        self.logger.debug(f"Total points calculated, non-null count: {df['total_points'].count()}")
        sample_points = df[['student_global_id'] + points_cols + ['total_points']].head(2)
        self.logger.debug(f"Sample total points: {sample_points.to_dict(orient='records')}")

        # Calculate divisions
        self.logger.debug("Creating division lookup")
        if exam_divisions.empty or exam_divisions[['lowest_points', 'division']].isna().any().any():
            self.logger.error("Exam divisions empty or contains nulls, cannot create division lookup")
            raise ValueError("Invalid exam_divisions data: empty or contains null values")
        division_lookup = {
            exam_id: [(low, div) for low, div in group.sort_values('lowest_points', ascending=False)[['lowest_points', 'division']].itertuples(index=False, name=None) if low is not None and div is not None]
            for exam_id, group in exam_divisions.groupby('exam_id')
        }
        self.logger.debug(f"Division lookup keys: {list(division_lookup.keys())}")
        if self.exam_id in division_lookup:
            self.logger.debug(f"Division lookup sample for {self.exam_id}: {division_lookup[self.exam_id][:2]}")
        else:
            self.logger.error(f"No division lookup data for exam_id {self.exam_id}")
            raise ValueError(f"No division lookup data for exam_id {self.exam_id}")

        def map_divisions_vectorized(total_points: pd.Series, exam_ids: pd.Series) -> List[Any]:
            self.logger.debug(f"Mapping divisions, points count: {total_points.count()}")
            results = []
            for idx, (points, exam_id) in enumerate(zip(total_points, exam_ids)):
                if points == -1:
                    results.append('INC')
                    if idx < 2:
                        self.logger.debug(f"Assigned division INC for total_points {points}")
                    continue
                if pd.isna(points) or exam_id not in division_lookup:
                    results.append('ABS')
                    if idx < 2:
                        self.logger.debug(f"Assigned division ABS for total_points {points}")
                    continue
                try:
                    lookup = division_lookup[exam_id]
                    for low, div in lookup:
                        if points >= low:
                            results.append(div)
                            if idx < 2:
                                self.logger.debug(f"Assigned division {div} for total_points {points}")
                            break
                    else:
                        results.append('ABS')
                        if idx < 2:
                            self.logger.debug(f"Assigned division ABS for total_points {points} (below lowest threshold)")
                except Exception as e:
                    self.logger.error(f"Error mapping division at index {idx}, points {points}, exam_id {exam_id}: {str(e)}", exc_info=True)
                    results.append('ABS')
            self.logger.debug(f"Completed mapping divisions, results length: {len(results)}")
            return results

        df['division'] = map_divisions_vectorized(df['total_points'], df['exam_id'])
        self.logger.debug(f"Divisions assigned, non-null count: {df['division'].count()}")
        sample_divisions = df[['student_global_id', 'total_points', 'division']].head(2)
        self.logger.debug(f"Sample divisions: {sample_divisions.to_dict(orient='records')}")

        # Calculate avg_marks and total_marks
        self.logger.debug("Calculating average and total marks")
        all_subjects = sorted(set(col for cols in subject_codes_dict.values() for col in cols))
        self.logger.debug(f"All subject codes: {all_subjects}")
        df[all_subjects] = df[all_subjects].apply(pd.to_numeric, errors='coerce')
        marks_matrix = df[all_subjects].to_numpy(dtype=np.float32)

        subject_mask = np.zeros_like(marks_matrix, dtype=bool)
        for exam_id, cols in subject_codes_dict.items():
            rows = df['exam_id'] == exam_id
            col_ids = [all_subjects.index(c) for c in cols if c in all_subjects]
            subject_mask[np.ix_(rows, col_ids)] = True

        masked = np.where(subject_mask, marks_matrix, np.nan)
        sorted_marks = -np.sort(-np.where(np.isnan(masked), -np.inf, masked), axis=1)
        valid = ~np.isinf(sorted_marks)

        df['avg_marks'] = np.nan
        df['total_marks'] = np.nan
        style = df['avg_style'].str.upper().fillna('')
        self.logger.debug(f"Unique avg_style values: {style.unique()}")

        for avg_style, n_subjects_avg in [('SEVEN_BEST', 7), ('EIGHT_BEST', 8)]:
            mask = style == avg_style
            self.logger.debug(f"Processing avg_style {avg_style}, rows: {mask.sum()}")
            top = sorted_marks[mask, :n_subjects_avg]
            valid_top = valid[mask, :n_subjects_avg]
            totals = np.sum(np.where(valid_top, top, 0), axis=1)
            df.loc[mask, 'total_marks'] = totals
            df.loc[mask, 'avg_marks'] = totals / n_subjects_avg

        mask = style == 'AUTO'
        self.logger.debug(f"Processing avg_style AUTO, rows: {mask.sum()}")
        top = sorted_marks[mask]
        valid_top = valid[mask]
        totals = np.sum(np.where(valid_top, top, 0), axis=1)
        counts = valid_top.sum(axis=1)
        df.loc[mask, 'total_marks'] = totals
        df.loc[mask, 'avg_marks'] = totals / np.maximum(min_subjects, counts)
        self.logger.debug(f"Average marks calculated, non-null count: {df['avg_marks'].count()}")
        sample_avg = df[['student_global_id', 'total_marks', 'avg_marks']].head(2)
        self.logger.debug(f"Sample avg_marks: {sample_avg.to_dict(orient='records')}")

        # Calculate avg_grade
        self.logger.debug("Creating average grade lookup")
        if exam_grades.empty or exam_grades[['lower_value', 'grade']].isna().any().any():
            self.logger.error("Exam grades empty or contains nulls, cannot create grade lookup")
            raise ValueError("Invalid exam_grades data: empty or contains null values")
        avg_grade_lookup = {
            exam_id: [(lower, grade) for lower, grade in group.sort_values('lower_value', ascending=False)[['lower_value', 'grade']].itertuples(index=False, name=None) if lower is not None and grade is not None]
            for exam_id, group in exam_grades.groupby('exam_id')
        }
        self.logger.debug(f"Average grade lookup keys: {list(avg_grade_lookup.keys())}")
        if self.exam_id in avg_grade_lookup:
            self.logger.debug(f"Average grade lookup sample for {self.exam_id}: {avg_grade_lookup[self.exam_id][:2]}")
        else:
            self.logger.error(f"No grade lookup data for exam_id {self.exam_id}")
            raise ValueError(f"No grade lookup data for exam_id {self.exam_id}")

        def map_avg_grade_vectorized(avg_marks: pd.Series, exam_ids: pd.Series) -> List[Any]:
            self.logger.debug(f"Mapping average grades, marks count: {avg_marks.count()}")
            grades = []
            for idx, (mark, exam_id) in enumerate(zip(avg_marks, exam_ids)):
                if pd.isna(mark) or exam_id not in avg_grade_lookup or mark < 0 or mark > 100:
                    if not pd.isna(mark) and (mark < 0 or mark > 100):
                        self.logger.warning(f"Outlier avg_mark {mark} for student at index {idx}, assigned null grade")
                    grades.append(np.nan)
                    continue
                try:
                    lookup = avg_grade_lookup[exam_id]
                    for lower, grade in lookup:
                        if mark >= lower:
                            grades.append(grade)
                            if idx < 2:
                                self.logger.debug(f"Assigned grade {grade} for avg_mark {mark}")
                            break
                    else:
                        grades.append(np.nan)
                        if idx < 2:
                            self.logger.debug(f"Assigned null grade for avg_mark {mark} (below lowest threshold)")
                except Exception as e:
                    self.logger.error(f"Error mapping grade at index {idx}, mark {mark}, exam_id {exam_id}: {str(e)}", exc_info=True)
                    grades.append(np.nan)
            self.logger.debug(f"Completed mapping grades, results length: {len(grades)}")
            return grades

        df['avg_grade'] = map_avg_grade_vectorized(df['avg_marks'], df['exam_id'])
        self.logger.debug(f"Average grades assigned, non-null count: {df['avg_grade'].count()}")
        sample_grades = df[['student_global_id', 'avg_marks', 'avg_grade']].head(2)
        self.logger.debug(f"Sample avg_grade: {sample_grades.to_dict(orient='records')}")

        # Handle absent and invalid cases
        df = self.handle_absent_cases(df, is_old_curriculum, min_subjects)

        # Calculate rankings
        self.logger.debug("Calculating rankings")
        df_valid = df[(df['avg_marks'] >= 0) & (~df['avg_marks'].isna())].copy()
        self.logger.debug(f"Valid rows for ranking: {df_valid.shape[0]}")
        rank_cols = [
            'pos', 'out_of', 'ward_pos', 'ward_out_of', 'ward_pos_gvt', 'ward_pos_pvt',
            'council_pos', 'council_out_of', 'council_pos_gvt', 'council_pos_pvt',
            'region_pos', 'region_out_of', 'region_pos_gvt', 'region_pos_pvt', 
            'school_pos', 'school_out_of'
        ]
        df[rank_cols] = np.nan

        df.loc[df_valid.index, 'pos'] = df_valid['avg_marks'].rank(method='min', ascending=False)
        df.loc[df_valid.index, 'out_of'] = len(df_valid)
        self.logger.debug(f"Overall ranking assigned, pos non-null: {df['pos'].count()}")

        def rank_in_group(df_main: pd.DataFrame, df_sub: pd.DataFrame, level_prefix: str):
            idx = df_sub.index
            ranks = df_sub['avg_marks'].rank(method='min', ascending=False)
            df_main.loc[idx, f'{level_prefix}_pos'] = ranks
            df_main.loc[idx, f'{level_prefix}_out_of'] = len(df_sub)
            self.logger.debug(f"Ranked {level_prefix}, rows: {len(df_sub)}")
            for sch_type, suffix in [('GOVERNMENT', 'gvt'), ('PRIVATE', 'pvt')]:
                mask = df_sub['school_type'] == sch_type
                if mask.any():
                    sub_idx = df_sub[mask].index
                    sub_ranks = df_sub.loc[mask, 'avg_marks'].rank(method='min', ascending=False)
                    df_main.loc[sub_idx, f'{level_prefix}_pos_{suffix}'] = sub_ranks
                    self.logger.debug(f"Ranked {level_prefix}_pos_{suffix}, rows: {mask.sum()}")

        for level, group in [
            ('council', df_valid.groupby('council_name')),
            ('region', df_valid.groupby('region_name')),
            ('ward', df_valid.groupby(['council_name', 'ward_name'])),
            ('school', df_valid.groupby('centre_number'))
        ]:
            self.logger.debug(f"Processing rankings for level: {level}")
            for key, subgroup in group:
                if isinstance(key, tuple) and any(pd.isna(x) for x in key):
                    self.logger.debug(f"Skipping group with null key: {key}")
                    continue
                if not isinstance(key, tuple) and pd.isna(key):
                    self.logger.debug(f"Skipping group with null key: {key}")
                    continue
                prefix = 'ward' if level == 'ward' else level
                rank_in_group(df, subgroup, prefix)

        # Rename best subject columns
        self.logger.debug("Renaming best subject columns")
        df.rename(columns={
            f'best_{i}': name for i, name in enumerate(
                ['first', 'second', 'third', 'fourth', 'fifth', 'sixth', 'seventh', 'eighth'][:n_subjects], 1
            )
        }, inplace=True)

        # Add ID column
        self.logger.debug("Adding ID column")
        df.insert(0, 'id', [str(uuid.uuid4()) for _ in range(len(df))])
        self.logger.debug(f"Processed DataFrame final shape: {df.shape}, columns: {list(df.columns)}")

        # Save records for centre_number=S0867 as CSV
        self.logger.debug("Saving records for centre_number=S0867 to CSV")
        s0867_df = df[df['centre_number'] == 'S0867']
        if not s0867_df.empty:
            s0867_df.to_csv('results_S0867.csv', index=False)
            self.logger.debug(f"Saved {s0867_df.shape[0]} records for centre_number=S0867 to results_S0867.csv")
        else:
            self.logger.warning("No records found for centre_number=S0867")

        return df

    async def save_results(self, df: pd.DataFrame, pool: aiomysql.Pool) -> Tuple[int, int, int]:
        self.logger.debug("Starting save_results")
        columns_to_keep = [
            'id', 'exam_id', 'student_global_id', 'centre_number', 'avg_marks',
            'total_marks', 'division', 'total_points', 'pos', 'out_of', 'ward_pos', 'ward_out_of',
            'council_pos', 'council_out_of', 'region_pos', 'region_out_of',
            'ward_pos_gvt', 'ward_pos_pvt', 'council_pos_gvt', 'council_pos_pvt',
            'region_pos_gvt', 'region_pos_pvt', 'avg_grade', 'school_pos', 'school_out_of'
        ]
        insert_df = df[columns_to_keep].copy().replace({np.nan: None})
        self.logger.debug(f"Prepared insert DataFrame, shape: {insert_df.shape}, columns: {columns_to_keep}")

        table_name = 'results'
        cols = insert_df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(cols))
        columns_str = ', '.join(f'`{col}`' for col in cols)
        update_str = ', '.join(f'`{col}` = VALUES(`{col}`)' for col in cols if col != 'id')
        insert_query = f"""
            INSERT INTO `{table_name}` ({columns_str})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_str};
        """
        self.logger.debug("Insert query prepared")

        batch_size = 1000
        data = [tuple(row) for row in insert_df.to_numpy()]
        total_inserted = 0
        total_updated = 0

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    self.logger.debug(f"Inserting batch {i//batch_size + 1}, size: {len(batch)}")
                    await cursor.executemany(insert_query, batch)
                    affected = cursor.rowcount
                    estimated_updates = max(0, affected - len(batch))
                    estimated_inserts = len(batch) - estimated_updates
                    total_inserted += estimated_inserts
                    total_updated += estimated_updates
                    self.logger.debug(f"Batch {i//batch_size + 1}: Inserted {estimated_inserts}, Updated {estimated_updates}, Total {len(batch)}")

        self.logger.debug(f"Save completed: {total_inserted} inserted, {total_updated} updated")
        return total_inserted, total_updated, len(insert_df)

    async def process_exam(self) -> Dict[str, Any]:
        self.logger.debug("Starting process_exam")
        start_time = time.time()
        result = {
            "status": "failed",
            "message": "Processing failed or no data processed",
            "execution_time_seconds": 0.0,
            "inserted": 0,
            "updated": 0,
            "row_count": 0,
            "missing_columns": [],
            "no_nulls_centre_number": False,
            "no_nulls_school_type": False,
            "invalid_centre_numbers": []
        }

        try:
            async with await self.get_pool() as pool:
                # Validate centres
                self.logger.debug("Validating centres")
                invalid_centres = await self.validate_centres(pool)
                result["invalid_centre_numbers"] = invalid_centres
                if invalid_centres:
                    self.logger.warning(f"Found {len(invalid_centres)} invalid centre_number(s): {invalid_centres}")
                    result["message"] = f"Found {len(invalid_centres)} invalid centre numbers"
                    return result

                # Load data
                self.logger.debug("Loading data")
                students, exams, exam_subjects, student_subjects, exam_grades, exam_divisions = await self.load_data(pool)

                if students.empty:
                    self.logger.warning("No student data found")
                    result["message"] = "No student data found for the given exam_id"
                    return result

                # Process data
                self.logger.debug("Processing data")
                df = await self.process_data(students, exams, exam_subjects, student_subjects, exam_grades, exam_divisions)
                
                if df.empty:
                    self.logger.warning("Processed DataFrame is empty")
                    result["message"] = "Processed DataFrame is empty"
                    return result

                # Save results
                self.logger.debug("Saving results")
                inserted, updated, row_count = await self.save_results(df, pool)
                self.logger.debug(f"Results saved: {inserted} inserted, {updated} updated, {row_count} total rows")

                # Validation checks
                self.logger.debug("Performing validation checks")
                expected_columns = [
                    'id', 'student_global_id', 'student_id', 'full_name', 'sex', 'exam_id', 'centre_number',
                    'region_name', 'council_name', 'ward_name', 'school_type', 'avg_style', 'first', 'second',
                    'third', 'fourth', 'fifth', 'sixth', 'seventh', 'eighth', 'best_1_points', 'best_2_points',
                    'best_3_points', 'best_4_points', 'best_5_points', 'best_6_points', 'best_7_points',
                    'best_8_points', 'total_points', 'division', 'avg_marks', 'total_marks', 'avg_grade',
                    'pos', 'out_of', 'ward_pos', 'ward_out_of', 'ward_pos_gvt', 'ward_pos_pvt',
                    'council_pos', 'council_out_of', 'council_pos_gvt', 'council_pos_pvt',
                    'region_pos', 'region_out_of', 'region_pos_gvt', 'region_pos_pvt', 'school_pos', 'school_out_of'
                ]
                missing_cols = [col for col in expected_columns if col not in df.columns]
                self.logger.debug(f"Missing columns: {missing_cols}")
                self.logger.debug(f"Centre number null count: {df['centre_number'].isna().sum()}")
                self.logger.debug(f"School type null count: {df['school_type'].isna().sum()}")

                result.update({
                    "status": "success",
                    "message": "Processing completed successfully",
                    "execution_time_seconds": time.time() - start_time,
                    "inserted": inserted,
                    "updated": updated,
                    "row_count": row_count,
                    "missing_columns": missing_cols,
                    "no_nulls_centre_number": not df['centre_number'].isna().any(),
                    "no_nulls_school_type": not df['school_type'].isna().any(),
                    "invalid_centre_numbers": invalid_centres
                })
                self.logger.debug("process_exam completed successfully")

        except Exception as e:
            self.logger.error(f"Processing error: {str(e)}", exc_info=True)
            result.update({
                "message": f"Error processing exam data: {str(e)}",
                "execution_time_seconds": time.time() - start_time
            })

        return result

async def main():
    logging.debug("Starting main function")
    processor = DivisionProcessor(exam_id='1f0656e3-8756-680b-ac24-8d5b3e217521')
    result = await processor.process_exam()
    logging.debug(f"Main function completed with result: {result}")
    print(result)

if __name__ == "__main__":
    asyncio.run(main())