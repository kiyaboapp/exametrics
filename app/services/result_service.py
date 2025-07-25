
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.db.models.result import Result as ResultModel
from app.db.schemas.result import ResultCreate, Result
from uuid6 import uuid6
import pandas as pd
import numpy as np
import time
from sqlalchemy import text
from app.db.models.result import Result



async def create_result(db: AsyncSession, result: ResultCreate) -> Result:
    existing_result = await db.execute(select(ResultModel).filter(ResultModel.exam_id == result.exam_id, ResultModel.student_global_id == result.student_global_id, ResultModel.centre_number == result.centre_number))
    if existing_result.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Result already exists for this exam, student, and centre"
        )
    result_data = result.model_dump()
    result_data["id"] = str(uuid6())
    db_result = ResultModel(**result_data)
    db.add(db_result)
    await db.commit()
    await db.refresh(db_result)
    return Result.model_validate(db_result)

async def get_result(db: AsyncSession, result_id: str) -> Result:
    result = await db.execute(select(ResultModel).filter(ResultModel.id == result_id))
    result_obj = result.scalars().first()
    if not result_obj:
        raise HTTPException(status_code=404, detail="Result not found")
    return Result.model_validate(result_obj)

async def get_results(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[Result]:
    result = await db.execute(select(ResultModel).offset(skip).limit(limit))
    return [Result.model_validate(result_obj) for result_obj in result.scalars().all()]




async def prepare_results_df(exam_id: str, db: AsyncSession) -> pd.DataFrame:
    """Prepare a comprehensive results DataFrame from multiple database tables."""
    start_time = time.time()
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Preparing results DataFrame for exam_id: {exam_id}")
    
    # Get a raw connection
    conn = await db.connection()
    
    # 1. Validate centre_number data
    result = await conn.execute(
        text("""
            SELECT DISTINCT s.centre_number
            FROM students s
            WHERE s.exam_id = :exam_id
            AND s.centre_number NOT IN (SELECT centre_number FROM schools)
        """),
        {"exam_id": exam_id}
    )
    invalid_centres = pd.DataFrame(result.fetchall(), columns=result.keys())
    
    if not invalid_centres.empty:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Warning: Found {len(invalid_centres)} invalid centres: {invalid_centres['centre_number'].tolist()}")
    
    # 2. Load all required data
    queries = {
        'students': """
            SELECT s.student_global_id, s.student_id, s.full_name, s.sex, s.exam_id, 
                   s.centre_number, sc.region_name, sc.council_name, sc.ward_name, sc.school_type 
            FROM students s
            INNER JOIN schools sc ON s.centre_number = sc.centre_number 
            WHERE s.exam_id = :exam_id
        """,
        'exams': "SELECT exam_id, avg_style FROM exams WHERE exam_id = :exam_id",
        'exam_subjects': "SELECT exam_id, subject_code FROM exam_subjects WHERE exam_id = :exam_id",
        'student_subjects': """
            SELECT student_global_id, exam_id, centre_number, subject_code, overall_marks 
            FROM student_subjects 
            WHERE exam_id = :exam_id
        """,
        'exam_grades': """
            SELECT exam_id, grade, lower_value, division_points 
            FROM exam_grades 
            WHERE exam_id = :exam_id
        """,
        'exam_divisions': """
            SELECT exam_id, division, lowest_points 
            FROM exam_divisions 
            WHERE exam_id = :exam_id
        """
    }
    
    # Execute all queries asynchronously
    data = {}
    for name, query in queries.items():
        result = await conn.execute(text(query), {"exam_id": exam_id})
        data[name] = pd.DataFrame(result.fetchall(), columns=result.keys())
    
    # 3. Process and merge data
    df = data['students'].merge(data['exams'], on='exam_id', how='left')
    
    # Pivot student subjects
    subject_pivot = data['student_subjects'].pivot_table(
        index=['student_global_id', 'exam_id', 'centre_number'], 
        columns='subject_code', 
        values='overall_marks',
        aggfunc='first'
    ).reset_index()
    df = df.merge(subject_pivot, on=['student_global_id', 'exam_id', 'centre_number'], how='left')
    
    # 4. Calculate best subjects and points
    subject_codes = data['exam_subjects']['subject_code'].unique()
    for i in range(1, 9):
        df[f'best_{i}'] = np.nan
    
    marks_matrix = df[subject_codes].to_numpy()
    filled = np.where(np.isnan(marks_matrix), -1e9, marks_matrix)
    sorted_indices = np.argsort(-filled, axis=1)[:, :8]
    row_indices = np.arange(len(df))[:, None]
    top8 = marks_matrix[row_indices, sorted_indices]
    df[[f'best_{i+1}' for i in range(8)]] = top8
    
    # 5. Calculate division points
    division_lookup = {}
    for _, row in data['exam_grades'].iterrows():
        exam_id = row['exam_id']
        if exam_id not in division_lookup:
            division_lookup[exam_id] = []
        division_lookup[exam_id].append((row['lower_value'], row['division_points']))
    
    for exam_id in division_lookup:
        division_lookup[exam_id].sort(reverse=True, key=lambda x: x[0])
    
    for i in range(1, 9):
        points = []
        for _, row in df.iterrows():
            mark = row[f'best_{i}']
            exam_id = row['exam_id']
            if pd.isna(mark) or exam_id not in division_lookup:
                points.append(np.nan)
                continue
            for lower, point in division_lookup[exam_id]:
                if mark >= lower:
                    points.append(point)
                    break
            else:
                points.append(division_lookup[exam_id][-1][1])
        df[f'best_{i}_points'] = points
    
    # 6. Calculate total points and division
    df['total_points'] = df[[f'best_{i}_points' for i in range(1, 9)]].sum(axis=1)
    df.loc[df[[f'best_{i}' for i in range(1, 9)]].count(axis=1) < 7, 'total_points'] = -1
    
    division_map = {}
    for _, row in data['exam_divisions'].iterrows():
        exam_id = row['exam_id']
        if exam_id not in division_map:
            division_map[exam_id] = []
        division_map[exam_id].append((row['lowest_points'], row['division']))
    
    for exam_id in division_map:
        division_map[exam_id].sort(reverse=True, key=lambda x: x[0])
    
    divisions = []
    for _, row in df.iterrows():
        points = row['total_points']
        exam_id = row['exam_id']
        if points == -1 or pd.isna(points) or exam_id not in division_map:
            divisions.append(np.nan)
            continue
        for low, div in division_map[exam_id]:
            if points >= low:
                divisions.append(div)
                break
        else:
            divisions.append(division_map[exam_id][-1][1])
    df['division'] = divisions
    
    # 7. Calculate averages and totals based on style
    style = df['avg_style'].str.upper().fillna('')
    subject_cols = [col for col in df.columns if col in subject_codes]
    marks = df[subject_cols].values
    
    df['avg_marks'] = np.nan
    df['total_marks'] = np.nan
    
    # Seven best subjects
    mask = style == 'SEVEN_BEST'
    if mask.any():
        top7 = np.sort(marks[mask], axis=1)[:, -7:]
        df.loc[mask, 'total_marks'] = top7.sum(axis=1)
        df.loc[mask, 'avg_marks'] = top7.sum(axis=1) / 7
    
    # Eight best subjects
    mask = style == 'EIGHT_BEST'
    if mask.any():
        top8 = np.sort(marks[mask], axis=1)[:, -8:]
        df.loc[mask, 'total_marks'] = top8.sum(axis=1)
        df.loc[mask, 'avg_marks'] = top8.sum(axis=1) / 8
    
    # Automatic calculation
    mask = style == 'AUTO'
    if mask.any():
        valid_marks = np.where(np.isnan(marks[mask]), 0, marks[mask])
        counts = (~np.isnan(marks[mask])).sum(axis=1)
        df.loc[mask, 'total_marks'] = valid_marks.sum(axis=1)
        df.loc[mask, 'avg_marks'] = valid_marks.sum(axis=1) / np.maximum(7, counts)
    
    # 8. Calculate rankings
    df_valid = df[df['avg_marks'] >= 0].copy()
    
    # National ranking
    df['pos'] = df_valid['avg_marks'].rank(method='min', ascending=False)
    df['out_of'] = len(df_valid)
    
    # Helper function for grouped rankings
    def calculate_group_rankings(df_main, df_sub, group_cols, prefix):
        if len(df_sub) == 0:
            return
        
        # Overall group ranking
        ranks = df_sub['avg_marks'].rank(method='min', ascending=False)
        df_main.loc[df_sub.index, f'{prefix}_pos'] = ranks
        df_main.loc[df_sub.index, f'{prefix}_out_of'] = len(df_sub)
        
        # Government/private breakdown
        for sch_type, suffix in [('GOVERNMENT', 'gvt'), ('PRIVATE', 'pvt')]:
            mask = df_sub['school_type'] == sch_type
            if mask.any():
                sub_ranks = df_sub.loc[mask, 'avg_marks'].rank(method='min', ascending=False)
                df_main.loc[df_sub[mask].index, f'{prefix}_pos_{suffix}'] = sub_ranks
    
    # Regional rankings
    for region, group in df_valid.groupby('region_name'):
        if pd.notna(region):
            calculate_group_rankings(df, group, ['region_name'], 'region')
    
    # Council rankings
    for council, group in df_valid.groupby('council_name'):
        if pd.notna(council):
            calculate_group_rankings(df, group, ['council_name'], 'council')
    
    # Ward rankings
    for (council, ward), group in df_valid.groupby(['council_name', 'ward_name']):
        if pd.notna(council) and pd.notna(ward):
            calculate_group_rankings(df, group, ['council_name', 'ward_name'], 'ward')
    
    # School rankings
    for centre, group in df_valid.groupby('centre_number'):
        if pd.notna(centre):
            df.loc[group.index, 'school_pos'] = group['avg_marks'].rank(method='min', ascending=False)
            df.loc[group.index, 'school_out_of'] = len(group)
    
    # 9. Final formatting
    rename_dict = {
        'best_1': 'first', 'best_2': 'second', 'best_3': 'third', 'best_4': 'fourth',
        'best_5': 'fifth', 'best_6': 'sixth', 'best_7': 'seventh', 'best_8': 'eighth'
    }
    df = df.rename(columns=rename_dict)
    
    # Add UUIDs
    df.insert(0, 'id', [str(uuid6()) for _ in range(len(df))])
    
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] DataFrame preparation completed in {time.time() - start_time:.2f}s")
    return df

async def execute_results_insert(df: pd.DataFrame, db: AsyncSession) -> dict:
    """Execute bulk insert of results into the database."""
    start_time = time.time()
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting bulk insert of {len(df)} records")
    
    # Prepare mappings
    result_mappings = df[[
        'id', 'exam_id', 'student_global_id', 'centre_number',
        'avg_marks', 'total_marks', 'division', 'total_points',
        'pos', 'out_of', 'ward_pos', 'ward_out_of', 'council_pos', 'council_out_of',
        'region_pos', 'region_out_of', 'ward_pos_gvt', 'ward_pos_pvt',
        'council_pos_gvt', 'council_pos_pvt', 'region_pos_gvt', 'region_pos_pvt',
        'school_pos', 'school_out_of'
    ]].to_dict('records')
    
    try:
        # Execute bulk insert
        await db.bulk_insert_mappings(Result, result_mappings)
        await db.commit()
        
        time_taken = time.time() - start_time
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Successfully inserted {len(result_mappings)} records in {time_taken:.2f}s")
        
        return {
            "status": "success",
            "inserted_records": len(result_mappings),
            "time_taken": f"{time_taken:.2f} seconds"
        }
    except Exception as e:
        await db.rollback()
        error_msg = f"Bulk insert failed: {str(e)}"
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)




