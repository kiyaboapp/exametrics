import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.sql import text
import asyncio
import time
import logging
from sqlalchemy.exc import OperationalError
from app.core.config import Settings

# Configure logging
logger = logging.getLogger('utils.processor.subjects_ranker')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

class SubjectRanker:
    def __init__(self, settings: Settings, exam_id: str):
        self.engine = create_async_engine(settings.DATABASE_URL, echo=False)
        self.exam_id = exam_id
        self.ranking_columns = [
            'school_pos_F', 'school_pos_M', 'school_out_of_F', 'school_out_of_M',
            'ward_subject_pos_F', 'ward_subject_pos_M', 'ward_subject_out_of_F', 'ward_subject_out_of_M',
            'ward_subject_pos_gvt_F', 'ward_subject_pos_gvt_M', 'ward_subject_out_of_gvt_F', 'ward_subject_out_of_gvt_M',
            'ward_subject_pos_pvt_F', 'ward_subject_pos_pvt_M', 'ward_subject_out_of_pvt_F', 'ward_subject_out_of_pvt_M',
            'council_subject_pos_F', 'council_subject_pos_M', 'council_subject_out_of_F', 'council_subject_out_of_M',
            'council_subject_pos_gvt_F', 'council_subject_pos_gvt_M', 'council_subject_out_of_gvt_F', 'council_subject_out_of_gvt_M',
            'council_subject_pos_pvt_F', 'council_subject_pos_pvt_M', 'council_subject_out_of_pvt_F', 'council_subject_out_of_pvt_M',
            'region_subject_pos_F', 'region_subject_pos_M', 'region_subject_out_of_F', 'region_subject_out_of_M',
            'region_subject_pos_gvt_F', 'region_subject_pos_gvt_M', 'region_subject_out_of_gvt_F', 'region_subject_out_of_gvt_M',
            'region_subject_pos_pvt_F', 'region_subject_pos_pvt_M', 'region_subject_out_of_pvt_F', 'region_subject_out_of_pvt_M'
        ]

    def _format_duration(self, seconds):
        if seconds >= 60:
            minutes = seconds / 60
            return f"{minutes:.2f} minutes"
        return f"{seconds:.2f} seconds"

    async def fetch_data(self):
        start_time = time.time()
        logger.info(f"Fetching data for exam_id: {self.exam_id}")
        query = """
        SELECT ss.id, ss.student_global_id, ss.centre_number, ss.subject_code, ss.overall_marks, s.sex,
               ss.school_pos_F, ss.school_pos_M, ss.school_out_of_F, ss.school_out_of_M,
               ss.ward_subject_pos_F, ss.ward_subject_pos_M, ss.ward_subject_out_of_F, ss.ward_subject_out_of_M,
               ss.ward_subject_pos_gvt_F, ss.ward_subject_pos_gvt_M, ss.ward_subject_out_of_gvt_F, ss.ward_subject_out_of_gvt_M,
               ss.ward_subject_pos_pvt_F, ss.ward_subject_pos_pvt_M, ss.ward_subject_out_of_pvt_F, ss.ward_subject_out_of_pvt_M,
               ss.council_subject_pos_F, ss.council_subject_pos_M, ss.council_subject_out_of_F, ss.council_subject_out_of_M,
               ss.council_subject_pos_gvt_F, ss.council_subject_pos_gvt_M, ss.council_subject_out_of_gvt_F, ss.council_subject_out_of_gvt_M,
               ss.council_subject_pos_pvt_F, ss.council_subject_pos_pvt_M, ss.council_subject_out_of_pvt_F, ss.council_subject_out_of_pvt_M,
               ss.region_subject_pos_F, ss.region_subject_pos_M, ss.region_subject_out_of_F, ss.region_subject_out_of_M,
               ss.region_subject_pos_gvt_F, ss.region_subject_pos_gvt_M, ss.region_subject_out_of_gvt_F, ss.region_subject_out_of_gvt_M,
               ss.region_subject_pos_pvt_F, ss.region_subject_pos_pvt_M, ss.region_subject_out_of_pvt_F, ss.region_subject_out_of_pvt_M,
               sch.ward_name, sch.council_name, sch.region_name, sch.school_type
        FROM student_subjects ss
        JOIN students s ON ss.student_global_id = s.student_global_id
        LEFT JOIN schools sch ON ss.centre_number = sch.centre_number
        WHERE ss.exam_id = :exam_id
        """
        try:
            async with AsyncSession(self.engine) as session:
                async with session.begin():
                    result = await session.execute(text(query), {'exam_id': self.exam_id})
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
            duration = time.time() - start_time
            logger.info(f"Fetched {len(df)} records in {self._format_duration(duration)}")
            logger.info(f"Initial Data Sample:\n{df[['id', 'student_global_id', 'centre_number', 'subject_code', 'overall_marks', 'sex', 'school_pos_F', 'school_pos_M', 'ward_name', 'council_name', 'region_name', 'school_type']].iloc[0].to_string()}")
            return df, duration
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            raise

    async def clear_rankings(self):
        start_time = time.time()
        logger.info("Clearing sex-based subject rankings")
        update_query = f"""
        UPDATE student_subjects
        SET {', '.join([f'{col} = NULL' for col in self.ranking_columns])}
        WHERE exam_id = :exam_id
        """
        try:
            async with AsyncSession(self.engine) as session:
                async with session.begin():
                    await session.execute(text(update_query), {'exam_id': self.exam_id})
            duration = time.time() - start_time
            logger.info(f"Cleared rankings in {self._format_duration(duration)}")
            return duration
        except Exception as e:
            logger.error(f"Error clearing rankings: {str(e)}")
            raise

    async def compute_school_rankings(self, df):
        start_time = time.time()
        logger.info("Computing school subject rankings")
        try:
            df_valid = df[df['overall_marks'].notnull() & (df['overall_marks'] >= 0) & 
                          df['overall_marks'].apply(lambda x: not pd.isna(x) and x != float('inf') and x != -float('inf'))].copy()
            df[['school_pos_F', 'school_pos_M', 'school_out_of_F', 'school_out_of_M']] = pd.NA
            df_valid.loc[df_valid['sex'] == 'F', 'school_pos_F'] = df_valid[df_valid['sex'] == 'F'].groupby(['centre_number', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid.loc[df_valid['sex'] == 'M', 'school_pos_M'] = df_valid[df_valid['sex'] == 'M'].groupby(['centre_number', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid.loc[df_valid['sex'] == 'F', 'school_out_of_F'] = df_valid[df_valid['sex'] == 'F'].groupby(['centre_number', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid.loc[df_valid['sex'] == 'M', 'school_out_of_M'] = df_valid[df_valid['sex'] == 'M'].groupby(['centre_number', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df.update(df_valid[['id', 'school_pos_F', 'school_pos_M', 'school_out_of_F', 'school_out_of_M']])
            duration = time.time() - start_time
            sample_row = df[df['school_pos_F'].notna() | df['school_pos_M'].notna()].iloc[0] if not df[df['school_pos_F'].notna() | df['school_pos_M'].notna()].empty else df.iloc[0]
            logger.info(f"Computed school rankings in {self._format_duration(duration)}")
            logger.info(f"School Rankings Sample:\n{sample_row[['id', 'student_global_id', 'centre_number', 'subject_code', 'sex', 'overall_marks', 'school_pos_F', 'school_pos_M', 'school_out_of_F', 'school_out_of_M']].to_string()}")
            return df, duration
        except Exception as e:
            logger.error(f"Error computing school rankings: {str(e)}")
            raise

    async def compute_location_rankings(self, df):
        start_time = time.time()
        logger.info("Computing location subject rankings")
        try:
            df[self.ranking_columns] = pd.NA
            df_valid = df[df['overall_marks'].notnull() & (df['overall_marks'] >= 0) & 
                          df['overall_marks'].apply(lambda x: not pd.isna(x) and x != float('inf') and x != -float('inf'))].copy()
            df_valid_region = df_valid[df_valid['region_name'].notnull()]
            df_valid_council = df_valid[df_valid['council_name'].notnull() & df_valid['region_name'].notnull()]
            df_valid_ward = df_valid[df_valid['ward_name'].notnull() & df_valid['council_name'].notnull() & df_valid['region_name'].notnull()]

            # Ward rankings
            df_valid_ward.loc[df_valid_ward['sex'] == 'F', 'ward_subject_pos_F'] = df_valid_ward[df_valid_ward['sex'] == 'F'].groupby(['ward_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_ward.loc[df_valid_ward['sex'] == 'M', 'ward_subject_pos_M'] = df_valid_ward[df_valid_ward['sex'] == 'M'].groupby(['ward_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_ward.loc[df_valid_ward['sex'] == 'F', 'ward_subject_out_of_F'] = df_valid_ward[df_valid_ward['sex'] == 'F'].groupby(['ward_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid_ward.loc[df_valid_ward['sex'] == 'M', 'ward_subject_out_of_M'] = df_valid_ward[df_valid_ward['sex'] == 'M'].groupby(['ward_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid_ward.loc[(df_valid_ward['sex'] == 'F') & (df_valid_ward['school_type'] == 'GOVERNMENT'), 'ward_subject_pos_gvt_F'] = df_valid_ward[(df_valid_ward['sex'] == 'F') & (df_valid_ward['school_type'] == 'GOVERNMENT')].groupby(['ward_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_ward.loc[(df_valid_ward['sex'] == 'M') & (df_valid_ward['school_type'] == 'GOVERNMENT'), 'ward_subject_pos_gvt_M'] = df_valid_ward[(df_valid_ward['sex'] == 'M') & (df_valid_ward['school_type'] == 'GOVERNMENT')].groupby(['ward_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_ward.loc[(df_valid_ward['sex'] == 'F') & (df_valid_ward['school_type'] == 'PRIVATE'), 'ward_subject_pos_pvt_F'] = df_valid_ward[(df_valid_ward['sex'] == 'F') & (df_valid_ward['school_type'] == 'PRIVATE')].groupby(['ward_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_ward.loc[(df_valid_ward['sex'] == 'M') & (df_valid_ward['school_type'] == 'PRIVATE'), 'ward_subject_pos_pvt_M'] = df_valid_ward[(df_valid_ward['sex'] == 'M') & (df_valid_ward['school_type'] == 'PRIVATE')].groupby(['ward_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_ward.loc[(df_valid_ward['sex'] == 'F') & (df_valid_ward['school_type'] == 'GOVERNMENT'), 'ward_subject_out_of_gvt_F'] = df_valid_ward[(df_valid_ward['sex'] == 'F') & (df_valid_ward['school_type'] == 'GOVERNMENT')].groupby(['ward_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid_ward.loc[(df_valid_ward['sex'] == 'M') & (df_valid_ward['school_type'] == 'GOVERNMENT'), 'ward_subject_out_of_gvt_M'] = df_valid_ward[(df_valid_ward['sex'] == 'M') & (df_valid_ward['school_type'] == 'GOVERNMENT')].groupby(['ward_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid_ward.loc[(df_valid_ward['sex'] == 'F') & (df_valid_ward['school_type'] == 'PRIVATE'), 'ward_subject_out_of_pvt_F'] = df_valid_ward[(df_valid_ward['sex'] == 'F') & (df_valid_ward['school_type'] == 'PRIVATE')].groupby(['ward_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid_ward.loc[(df_valid_ward['sex'] == 'M') & (df_valid_ward['school_type'] == 'PRIVATE'), 'ward_subject_out_of_pvt_M'] = df_valid_ward[(df_valid_ward['sex'] == 'M') & (df_valid_ward['school_type'] == 'PRIVATE')].groupby(['ward_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')

            # Council rankings
            df_valid_council.loc[df_valid_council['sex'] == 'F', 'council_subject_pos_F'] = df_valid_council[df_valid_council['sex'] == 'F'].groupby(['council_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_council.loc[df_valid_council['sex'] == 'M', 'council_subject_pos_M'] = df_valid_council[df_valid_council['sex'] == 'M'].groupby(['council_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_council.loc[df_valid_council['sex'] == 'F', 'council_subject_out_of_F'] = df_valid_council[df_valid_council['sex'] == 'F'].groupby(['council_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid_council.loc[df_valid_council['sex'] == 'M', 'council_subject_out_of_M'] = df_valid_council[df_valid_council['sex'] == 'M'].groupby(['council_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid_council.loc[(df_valid_council['sex'] == 'F') & (df_valid_council['school_type'] == 'GOVERNMENT'), 'council_subject_pos_gvt_F'] = df_valid_council[(df_valid_council['sex'] == 'F') & (df_valid_council['school_type'] == 'GOVERNMENT')].groupby(['council_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_council.loc[(df_valid_council['sex'] == 'M') & (df_valid_council['school_type'] == 'GOVERNMENT'), 'council_subject_pos_gvt_M'] = df_valid_council[(df_valid_council['sex'] == 'M') & (df_valid_council['school_type'] == 'GOVERNMENT')].groupby(['council_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_council.loc[(df_valid_council['sex'] == 'F') & (df_valid_council['school_type'] == 'PRIVATE'), 'council_subject_pos_pvt_F'] = df_valid_council[(df_valid_council['sex'] == 'F') & (df_valid_council['school_type'] == 'PRIVATE')].groupby(['council_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_council.loc[(df_valid_council['sex'] == 'M') & (df_valid_council['school_type'] == 'PRIVATE'), 'council_subject_pos_pvt_M'] = df_valid_council[(df_valid_council['sex'] == 'M') & (df_valid_council['school_type'] == 'PRIVATE')].groupby(['council_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_council.loc[(df_valid_council['sex'] == 'F') & (df_valid_council['school_type'] == 'GOVERNMENT'), 'council_subject_out_of_gvt_F'] = df_valid_council[(df_valid_council['sex'] == 'F') & (df_valid_council['school_type'] == 'GOVERNMENT')].groupby(['council_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid_council.loc[(df_valid_council['sex'] == 'M') & (df_valid_council['school_type'] == 'GOVERNMENT'), 'council_subject_out_of_gvt_M'] = df_valid_council[(df_valid_council['sex'] == 'M') & (df_valid_council['school_type'] == 'GOVERNMENT')].groupby(['council_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid_council.loc[(df_valid_council['sex'] == 'F') & (df_valid_council['school_type'] == 'PRIVATE'), 'council_subject_out_of_pvt_F'] = df_valid_council[(df_valid_council['sex'] == 'F') & (df_valid_council['school_type'] == 'PRIVATE')].groupby(['council_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid_council.loc[(df_valid_council['sex'] == 'M') & (df_valid_council['school_type'] == 'PRIVATE'), 'council_subject_out_of_pvt_M'] = df_valid_council[(df_valid_council['sex'] == 'M') & (df_valid_council['school_type'] == 'PRIVATE')].groupby(['council_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')

            # Region rankings
            df_valid_region.loc[df_valid_region['sex'] == 'F', 'region_subject_pos_F'] = df_valid_region[df_valid_region['sex'] == 'F'].groupby(['region_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_region.loc[df_valid_region['sex'] == 'M', 'region_subject_pos_M'] = df_valid_region[df_valid_region['sex'] == 'M'].groupby(['region_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_region.loc[df_valid_region['sex'] == 'F', 'region_subject_out_of_F'] = df_valid_region[df_valid_region['sex'] == 'F'].groupby(['region_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid_region.loc[df_valid_region['sex'] == 'M', 'region_subject_out_of_M'] = df_valid_region[df_valid_region['sex'] == 'M'].groupby(['region_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid_region.loc[(df_valid_region['sex'] == 'F') & (df_valid_region['school_type'] == 'GOVERNMENT'), 'region_subject_pos_gvt_F'] = df_valid_region[(df_valid_region['sex'] == 'F') & (df_valid_region['school_type'] == 'GOVERNMENT')].groupby(['region_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_region.loc[(df_valid_region['sex'] == 'M') & (df_valid_region['school_type'] == 'GOVERNMENT'), 'region_subject_pos_gvt_M'] = df_valid_region[(df_valid_region['sex'] == 'M') & (df_valid_region['school_type'] == 'GOVERNMENT')].groupby(['region_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_region.loc[(df_valid_region['sex'] == 'F') & (df_valid_region['school_type'] == 'PRIVATE'), 'region_subject_pos_pvt_F'] = df_valid_region[(df_valid_region['sex'] == 'F') & (df_valid_region['school_type'] == 'PRIVATE')].groupby(['region_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_region.loc[(df_valid_region['sex'] == 'M') & (df_valid_region['school_type'] == 'PRIVATE'), 'region_subject_pos_pvt_M'] = df_valid_region[(df_valid_region['sex'] == 'M') & (df_valid_region['school_type'] == 'PRIVATE')].groupby(['region_name', 'subject_code'])['overall_marks'].rank(ascending=False, method='min').astype('Int64')
            df_valid_region.loc[(df_valid_region['sex'] == 'F') & (df_valid_region['school_type'] == 'GOVERNMENT'), 'region_subject_out_of_gvt_F'] = df_valid_region[(df_valid_region['sex'] == 'F') & (df_valid_region['school_type'] == 'GOVERNMENT')].groupby(['region_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid_region.loc[(df_valid_region['sex'] == 'M') & (df_valid_region['school_type'] == 'GOVERNMENT'), 'region_subject_out_of_gvt_M'] = df_valid_region[(df_valid_region['sex'] == 'M') & (df_valid_region['school_type'] == 'GOVERNMENT')].groupby(['region_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid_region.loc[(df_valid_region['sex'] == 'F') & (df_valid_region['school_type'] == 'PRIVATE'), 'region_subject_out_of_pvt_F'] = df_valid_region[(df_valid_region['sex'] == 'F') & (df_valid_region['school_type'] == 'PRIVATE')].groupby(['region_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')
            df_valid_region.loc[(df_valid_region['sex'] == 'M') & (df_valid_region['school_type'] == 'PRIVATE'), 'region_subject_out_of_pvt_M'] = df_valid_region[(df_valid_region['sex'] == 'M') & (df_valid_region['school_type'] == 'PRIVATE')].groupby(['region_name', 'subject_code'])['overall_marks'].transform('count').astype('Int64')

            df.update(df_valid_ward[[c for c in df_valid_ward.columns if c.startswith('ward_')]])
            df.update(df_valid_council[[c for c in df_valid_council.columns if c.startswith('council_')]])
            df.update(df_valid_region[[c for c in df_valid_region.columns if c.startswith('region_')]])
            duration = time.time() - start_time
            sample_row = df[df['ward_subject_pos_F'].notna() | df['ward_subject_pos_M'].notna()].iloc[0] if not df[df['ward_subject_pos_F'].notna() | df['ward_subject_pos_M'].notna()].empty else df.iloc[0]
            logger.info(f"Computed location rankings in {self._format_duration(duration)}")
            logger.info(f"Location Rankings Sample:\n{sample_row[['id', 'student_global_id', 'subject_code', 'sex', 'overall_marks', 'ward_subject_pos_F', 'ward_subject_pos_M', 'council_subject_pos_F', 'council_subject_pos_M', 'region_subject_pos_F', 'region_subject_pos_M']].to_string()}")
            return df, duration
        except Exception as e:
            logger.error(f"Error computing location rankings: {str(e)}")
            raise

    async def update_rankings(self, df):
        start_time = time.time()
        logger.info("Updating database with rankings")
        update_query = """
        UPDATE student_subjects
        SET 
            school_pos_F = :school_pos_F, school_pos_M = :school_pos_M, 
            school_out_of_F = :school_out_of_F, school_out_of_M = :school_out_of_M,
            ward_subject_pos_F = :ward_subject_pos_F, ward_subject_pos_M = :ward_subject_pos_M,
            ward_subject_out_of_F = :ward_subject_out_of_F, ward_subject_out_of_M = :ward_subject_out_of_M,
            ward_subject_pos_gvt_F = :ward_subject_pos_gvt_F, ward_subject_pos_gvt_M = :ward_subject_pos_gvt_M,
            ward_subject_out_of_gvt_F = :ward_subject_out_of_gvt_F, ward_subject_out_of_gvt_M = :ward_subject_out_of_gvt_M,
            ward_subject_pos_pvt_F = :ward_subject_pos_pvt_F, ward_subject_pos_pvt_M = :ward_subject_pos_pvt_M,
            ward_subject_out_of_pvt_F = :ward_subject_out_of_pvt_F, ward_subject_out_of_pvt_M = :ward_subject_out_of_pvt_M,
            council_subject_pos_F = :council_subject_pos_F, council_subject_pos_M = :council_subject_pos_M,
            council_subject_out_of_F = :council_subject_out_of_F, council_subject_out_of_M = :council_subject_out_of_M,
            council_subject_pos_gvt_F = :council_subject_pos_gvt_F, council_subject_pos_gvt_M = :council_subject_pos_gvt_M,
            council_subject_out_of_gvt_F = :council_subject_out_of_gvt_F, council_subject_out_of_gvt_M = :council_subject_out_of_gvt_M,
            council_subject_pos_pvt_F = :council_subject_pos_pvt_F, council_subject_pos_pvt_M = :council_subject_pos_pvt_M,
            council_subject_out_of_pvt_F = :council_subject_out_of_pvt_F, council_subject_out_of_pvt_M = :council_subject_out_of_pvt_M,
            region_subject_pos_F = :region_subject_pos_F, region_subject_pos_M = :region_subject_pos_M,
            region_subject_out_of_F = :region_subject_out_of_F, region_subject_out_of_M = :region_subject_out_of_M,
            region_subject_pos_gvt_F = :region_subject_pos_gvt_F, region_subject_pos_gvt_M = :region_subject_pos_gvt_M,
            region_subject_out_of_gvt_F = :region_subject_out_of_gvt_F, region_subject_out_of_gvt_M = :region_subject_out_of_gvt_M,
            region_subject_pos_pvt_F = :region_subject_pos_pvt_F, region_subject_pos_pvt_M = :region_subject_pos_pvt_M,
            region_subject_out_of_pvt_F = :region_subject_out_of_pvt_F, region_subject_out_of_pvt_M = :region_subject_out_of_pvt_M
        WHERE id = :id
        """
        max_retries = 3
        chunk_size = 5000
        total_updated = 0
        try:
            for start in range(0, len(df), chunk_size):
                df_chunk = df[start:start + chunk_size]
                async with AsyncSession(self.engine) as session:
                    for _, row in df_chunk.iterrows():
                        params = {'id': row['id']}
                        for col in self.ranking_columns:
                            value = row[col]
                            params[col] = None if pd.isna(value) or value is None else int(value)
                        for attempt in range(max_retries):
                            try:
                                async with session.begin():
                                    await session.execute(text(update_query), params)
                                total_updated += 1
                                break
                            except OperationalError as e:
                                if attempt < max_retries - 1:
                                    logger.warning(f"Retry {attempt + 1} for record {row['id']} due to: {str(e)}")
                                    await asyncio.sleep(1)
                                    continue
                                logger.error(f"Failed to update record {row['id']}: {str(e)}")
                                raise e
                logger.info(f"Inserted {start + chunk_size} records")
            duration = time.time() - start_time
            logger.info(f"Updated {total_updated} records in {self._format_duration(duration)}")
            return total_updated, duration
        except Exception as e:
            logger.error(f"Error updating rankings: {str(e)}")
            raise

    async def verify_rankings(self):
        start_time = time.time()
        logger.info("Verifying rankings (first 5 records)")
        query = """
        SELECT ss.id, ss.student_global_id, ss.centre_number, ss.subject_code, ss.overall_marks, s.sex,
               ss.school_pos_F, ss.school_pos_M, ss.ward_subject_pos_F, ss.ward_subject_pos_M,
               ss.council_subject_pos_F, ss.council_subject_pos_M, ss.region_subject_pos_F, ss.region_subject_pos_M
        FROM student_subjects ss
        JOIN students s ON ss.student_global_id = s.student_global_id
        WHERE ss.exam_id = :exam_id
        LIMIT 5
        """
        try:
            async with AsyncSession(self.engine) as session:
                async with session.begin():
                    result = await session.execute(text(query), {'exam_id': self.exam_id})
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
            duration = time.time() - start_time
            logger.info(f"Verified 5 records in {self._format_duration(duration)}")
            return df, duration
        except Exception as e:
            logger.error(f"Error verifying rankings: {str(e)}")
            raise

    async def run(self):
        response = {
            'status': 'success',
            'exam_id': self.exam_id,
            'timings': {},
            'counts': {'fetched_records': 0, 'updated_records': 0},
            'error': None
        }
        total_start_time = time.time()
        try:
            # Fetch data
            df, fetch_duration = await self.fetch_data()
            response['timings']['fetch_data'] = self._format_duration(fetch_duration)
            response['counts']['fetched_records'] = len(df)

            # Clear rankings
            clear_duration = await self.clear_rankings()
            response['timings']['clear_rankings'] = self._format_duration(clear_duration)

            # Compute school rankings
            df, school_rank_duration = await self.compute_school_rankings(df)
            response['timings']['compute_school_rankings'] = self._format_duration(school_rank_duration)

            # Compute location rankings
            df, location_rank_duration = await self.compute_location_rankings(df)
            response['timings']['compute_location_rankings'] = self._format_duration(location_rank_duration)

            # Update rankings
            updated_count, update_duration = await self.update_rankings(df)
            response['timings']['update_rankings'] = self._format_duration(update_duration)
            response['counts']['updated_records'] = updated_count

            total_duration = time.time() - total_start_time
            response['timings']['total'] = self._format_duration(total_duration)
            logger.info(f"Ranking process completed in {self._format_duration(total_duration)}")
            return response
        except Exception as e:
            response['status'] = 'failure'
            response['error'] = str(e)
            total_duration = time.time() - total_start_time
            response['timings']['total'] = self._format_duration(total_duration)
            logger.error(f"Ranking process failed: {str(e)}")
            return response

async def main():
    settings = Settings()
    ranker = SubjectRanker(settings, '1f0656e3-8756-680b-ac24-8d5b3e217521')
    response = await ranker.run()
    print("Run Response:", response)
    # Optionally verify rankings
    df_verify, verify_duration = await ranker.verify_rankings()
    print(f"Verification (5 records) took {verify_duration:.2f} seconds")
    print("Verified Rankings:\n", df_verify.to_string())

if __name__ == "__main__":
    asyncio.run(main())