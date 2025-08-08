import pandas as pd
import asyncio
import time
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from app.core.config import Settings
from sqlalchemy.exc import OperationalError

class RankingSexWise:
    def __init__(self, settings: Settings):
        """Initialize with database settings."""
        self.settings = settings
        self.engine = create_async_engine(settings.DATABASE_URL, echo=False)
        self.async_session = sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
        self.chunk_size = 10000  # Adjustable chunk size
        self.max_retries = 3
        self.retry_delay = 1  # Seconds

    async def fetch_data(self, exam_id: str) -> pd.DataFrame:
        """Fetch data for the given exam_id."""
        query = """
        SELECT r.student_global_id, r.centre_number, r.avg_marks, s.sex,
               r.school_pos_F, r.school_pos_M, r.school_out_of_F, r.school_out_of_M,
               r.ward_pos_F, r.ward_pos_M, r.ward_out_of_F, r.ward_out_of_M,
               r.ward_pos_gvt_F, r.ward_pos_gvt_M, r.ward_pos_pvt_F, r.ward_pos_pvt_M,
               r.council_pos_F, r.council_pos_M, r.council_out_of_F, r.council_out_of_M,
               r.council_pos_gvt_F, r.council_pos_gvt_M, r.council_pos_pvt_F, r.council_pos_pvt_M,
               r.region_pos_F, r.region_pos_M, r.region_out_of_F, r.region_out_of_M,
               r.region_pos_gvt_F, r.region_pos_gvt_M, r.region_pos_pvt_F, r.region_pos_pvt_M,
               sch.ward_name, sch.council_name, sch.region_name, sch.school_type
        FROM results r
        JOIN students s ON r.student_global_id = s.student_global_id
        LEFT JOIN schools sch ON r.centre_number = sch.centre_number
        WHERE r.exam_id = :exam_id
        """
        async with self.async_session() as session:
            result = await session.execute(text(query), {'exam_id': exam_id})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

    async def clear_rankings(self, exam_id: str):
        """Clear existing sex-based rankings for the exam_id."""
        update_query = """
        UPDATE results
        SET 
            school_pos_F = NULL, school_pos_M = NULL, school_out_of_F = NULL, school_out_of_M = NULL,
            ward_pos_F = NULL, ward_pos_M = NULL, ward_out_of_F = NULL, ward_out_of_M = NULL,
            ward_pos_gvt_F = NULL, ward_pos_gvt_M = NULL, ward_pos_pvt_F = NULL, ward_pos_pvt_M = NULL,
            council_pos_F = NULL, council_pos_M = NULL, council_out_of_F = NULL, council_out_of_M = NULL,
            council_pos_gvt_F = NULL, council_pos_gvt_M = NULL, council_pos_pvt_F = NULL, council_pos_pvt_M = NULL,
            region_pos_F = NULL, region_pos_M = NULL, region_out_of_F = NULL, region_out_of_M = NULL,
            region_pos_gvt_F = NULL, region_pos_gvt_M = NULL, region_pos_pvt_F = NULL, region_pos_pvt_M = NULL
        WHERE exam_id = :exam_id
        """
        async with self.async_session() as session:
            async with session.begin():
                await session.execute(text(update_query), {'exam_id': exam_id})

    def compute_rankings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute school-wise and location-wise rankings."""
        # Filter for valid avg_marks (>= 0 and finite)
        df_valid = df[df['avg_marks'].notnull() & (df['avg_marks'] >= 0) & 
                      df['avg_marks'].apply(lambda x: not pd.isna(x) and x != float('inf') and x != -float('inf'))].copy()

        # Initialize ranking columns to NaN
        ranking_columns = [
            'school_pos_F', 'school_pos_M', 'school_out_of_F', 'school_out_of_M',
            'ward_pos_F', 'ward_pos_M', 'ward_out_of_F', 'ward_out_of_M',
            'ward_pos_gvt_F', 'ward_pos_gvt_M', 'ward_pos_pvt_F', 'ward_pos_pvt_M',
            'council_pos_F', 'council_pos_M', 'council_out_of_F', 'council_out_of_M',
            'council_pos_gvt_F', 'council_pos_gvt_M', 'council_pos_pvt_F', 'council_pos_pvt_M',
            'region_pos_F', 'region_pos_M', 'region_out_of_F', 'region_out_of_M',
            'region_pos_gvt_F', 'region_pos_gvt_M', 'region_pos_pvt_F', 'region_pos_pvt_M'
        ]
        df[ranking_columns] = pd.NA

        # School-wise rankings
        df_valid.loc[df_valid['sex'] == 'F', 'school_pos_F'] = df_valid[df_valid['sex'] == 'F'].groupby('centre_number')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid.loc[df_valid['sex'] == 'M', 'school_pos_M'] = df_valid[df_valid['sex'] == 'M'].groupby('centre_number')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid.loc[df_valid['sex'] == 'F', 'school_out_of_F'] = df_valid[df_valid['sex'] == 'F'].groupby('centre_number')['avg_marks'].transform('count').astype('Int64')
        df_valid.loc[df_valid['sex'] == 'M', 'school_out_of_M'] = df_valid[df_valid['sex'] == 'M'].groupby('centre_number')['avg_marks'].transform('count').astype('Int64')

        # Filter for valid location hierarchies
        df_valid_region = df_valid[df_valid['region_name'].notnull()]
        df_valid_council = df_valid[df_valid['council_name'].notnull() & df_valid['region_name'].notnull()]
        df_valid_ward = df_valid[df_valid['ward_name'].notnull() & df_valid['council_name'].notnull() & df_valid['region_name'].notnull()]

        # Ward rankings
        df_valid_ward.loc[df_valid_ward['sex'] == 'F', 'ward_pos_F'] = df_valid_ward[df_valid_ward['sex'] == 'F'].groupby('ward_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid_ward.loc[df_valid_ward['sex'] == 'M', 'ward_pos_M'] = df_valid_ward[df_valid_ward['sex'] == 'M'].groupby('ward_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid_ward.loc[df_valid_ward['sex'] == 'F', 'ward_out_of_F'] = df_valid_ward[df_valid_ward['sex'] == 'F'].groupby('ward_name')['avg_marks'].transform('count').astype('Int64')
        df_valid_ward.loc[df_valid_ward['sex'] == 'M', 'ward_out_of_M'] = df_valid_ward[df_valid_ward['sex'] == 'M'].groupby('ward_name')['avg_marks'].transform('count').astype('Int64')

        # Ward government/private rankings
        df_valid_ward.loc[(df_valid_ward['sex'] == 'F') & (df_valid_ward['school_type'] == 'GOVERNMENT'), 'ward_pos_gvt_F'] = df_valid_ward[(df_valid_ward['sex'] == 'F') & (df_valid_ward['school_type'] == 'GOVERNMENT')].groupby('ward_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid_ward.loc[(df_valid_ward['sex'] == 'M') & (df_valid_ward['school_type'] == 'GOVERNMENT'), 'ward_pos_gvt_M'] = df_valid_ward[(df_valid_ward['sex'] == 'M') & (df_valid_ward['school_type'] == 'GOVERNMENT')].groupby('ward_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid_ward.loc[(df_valid_ward['sex'] == 'F') & (df_valid_ward['school_type'] == 'PRIVATE'), 'ward_pos_pvt_F'] = df_valid_ward[(df_valid_ward['sex'] == 'F') & (df_valid_ward['school_type'] == 'PRIVATE')].groupby('ward_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid_ward.loc[(df_valid_ward['sex'] == 'M') & (df_valid_ward['school_type'] == 'PRIVATE'), 'ward_pos_pvt_M'] = df_valid_ward[(df_valid_ward['sex'] == 'M') & (df_valid_ward['school_type'] == 'PRIVATE')].groupby('ward_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')

        # Council rankings
        df_valid_council.loc[df_valid_council['sex'] == 'F', 'council_pos_F'] = df_valid_council[df_valid_council['sex'] == 'F'].groupby('council_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid_council.loc[df_valid_council['sex'] == 'M', 'council_pos_M'] = df_valid_council[df_valid_council['sex'] == 'M'].groupby('council_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid_council.loc[df_valid_council['sex'] == 'F', 'council_out_of_F'] = df_valid_council[df_valid_council['sex'] == 'F'].groupby('council_name')['avg_marks'].transform('count').astype('Int64')
        df_valid_council.loc[df_valid_council['sex'] == 'M', 'council_out_of_M'] = df_valid_council[df_valid_council['sex'] == 'M'].groupby('council_name')['avg_marks'].transform('count').astype('Int64')

        # Council government/private rankings
        df_valid_council.loc[(df_valid_council['sex'] == 'F') & (df_valid_council['school_type'] == 'GOVERNMENT'), 'council_pos_gvt_F'] = df_valid_council[(df_valid_council['sex'] == 'F') & (df_valid_council['school_type'] == 'GOVERNMENT')].groupby('council_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid_council.loc[(df_valid_council['sex'] == 'M') & (df_valid_council['school_type'] == 'GOVERNMENT'), 'council_pos_gvt_M'] = df_valid_council[(df_valid_council['sex'] == 'M') & (df_valid_council['school_type'] == 'GOVERNMENT')].groupby('council_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid_council.loc[(df_valid_council['sex'] == 'F') & (df_valid_council['school_type'] == 'PRIVATE'), 'council_pos_pvt_F'] = df_valid_council[(df_valid_council['sex'] == 'F') & (df_valid_council['school_type'] == 'PRIVATE')].groupby('council_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid_council.loc[(df_valid_council['sex'] == 'M') & (df_valid_council['school_type'] == 'PRIVATE'), 'council_pos_pvt_M'] = df_valid_council[(df_valid_council['sex'] == 'M') & (df_valid_council['school_type'] == 'PRIVATE')].groupby('council_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')

        # Region rankings
        df_valid_region.loc[df_valid_region['sex'] == 'F', 'region_pos_F'] = df_valid_region[df_valid_region['sex'] == 'F'].groupby('region_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid_region.loc[df_valid_region['sex'] == 'M', 'region_pos_M'] = df_valid_region[df_valid_region['sex'] == 'M'].groupby('region_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid_region.loc[df_valid_region['sex'] == 'F', 'region_out_of_F'] = df_valid_region[df_valid_region['sex'] == 'F'].groupby('region_name')['avg_marks'].transform('count').astype('Int64')
        df_valid_region.loc[df_valid_region['sex'] == 'M', 'region_out_of_M'] = df_valid_region[df_valid_region['sex'] == 'M'].groupby('region_name')['avg_marks'].transform('count').astype('Int64')

        # Region government/private rankings
        df_valid_region.loc[(df_valid_region['sex'] == 'F') & (df_valid_region['school_type'] == 'GOVERNMENT'), 'region_pos_gvt_F'] = df_valid_region[(df_valid_region['sex'] == 'F') & (df_valid_region['school_type'] == 'GOVERNMENT')].groupby('region_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid_region.loc[(df_valid_region['sex'] == 'M') & (df_valid_region['school_type'] == 'GOVERNMENT'), 'region_pos_gvt_M'] = df_valid_region[(df_valid_region['sex'] == 'M') & (df_valid_region['school_type'] == 'GOVERNMENT')].groupby('region_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid_region.loc[(df_valid_region['sex'] == 'F') & (df_valid_region['school_type'] == 'PRIVATE'), 'region_pos_pvt_F'] = df_valid_region[(df_valid_region['sex'] == 'F') & (df_valid_region['school_type'] == 'PRIVATE')].groupby('region_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')
        df_valid_region.loc[(df_valid_region['sex'] == 'M') & (df_valid_region['school_type'] == 'PRIVATE'), 'region_pos_pvt_M'] = df_valid_region[(df_valid_region['sex'] == 'M') & (df_valid_region['school_type'] == 'PRIVATE')].groupby('region_name')['avg_marks'].rank(ascending=False, method='min').astype('Int64')

        # Update original DataFrame
        df.update(df_valid_ward[['student_global_id', 'ward_pos_F', 'ward_pos_M', 'ward_out_of_F', 'ward_out_of_M',
                                'ward_pos_gvt_F', 'ward_pos_gvt_M', 'ward_pos_pvt_F', 'ward_pos_pvt_M']])
        df.update(df_valid_council[['student_global_id', 'council_pos_F', 'council_pos_M', 'council_out_of_F', 'council_out_of_M',
                                   'council_pos_gvt_F', 'council_pos_gvt_M', 'council_pos_pvt_F', 'council_pos_pvt_M']])
        df.update(df_valid_region[['student_global_id', 'region_pos_F', 'region_pos_M', 'region_out_of_F', 'region_out_of_M',
                                  'region_pos_gvt_F', 'region_pos_gvt_M', 'region_pos_pvt_F', 'region_pos_pvt_M']])
        df.update(df_valid[['student_global_id', 'school_pos_F', 'school_pos_M', 'school_out_of_F', 'school_out_of_M']])
        return df

    async def update_rankings(self, df: pd.DataFrame, exam_id: str):
        """Update the database with new rankings using chunked updates."""
        # Prepare DataFrame for updates
        df_update = df[['student_global_id', 'school_pos_F', 'school_pos_M', 'school_out_of_F', 'school_out_of_M',
                        'ward_pos_F', 'ward_pos_M', 'ward_out_of_F', 'ward_out_of_M',
                        'ward_pos_gvt_F', 'ward_pos_gvt_M', 'ward_pos_pvt_F', 'ward_pos_pvt_M',
                        'council_pos_F', 'council_pos_M', 'council_out_of_F', 'council_out_of_M',
                        'council_pos_gvt_F', 'council_pos_gvt_M', 'council_pos_pvt_F', 'council_pos_pvt_M',
                        'region_pos_F', 'region_pos_M', 'region_out_of_F', 'region_out_of_M',
                        'region_pos_gvt_F', 'region_pos_gvt_M', 'region_pos_pvt_F', 'region_pos_pvt_M']].copy()
        df_update['exam_id'] = exam_id
        df_update = df_update.where(df_update.notnull(), None)  # Convert NaN to None for SQL

        update_query = """
        UPDATE results
        SET 
            school_pos_F = :school_pos_F,
            school_pos_M = :school_pos_M,
            school_out_of_F = :school_out_of_F,
            school_out_of_M = :school_out_of_M,
            ward_pos_F = :ward_pos_F,
            ward_pos_M = :ward_pos_M,
            ward_out_of_F = :ward_out_of_F,
            ward_out_of_M = :ward_out_of_M,
            ward_pos_gvt_F = :ward_pos_gvt_F,
            ward_pos_gvt_M = :ward_pos_gvt_M,
            ward_pos_pvt_F = :ward_pos_pvt_F,
            ward_pos_pvt_M = :ward_pos_pvt_M,
            council_pos_F = :council_pos_F,
            council_pos_M = :council_pos_M,
            council_out_of_F = :council_out_of_F,
            council_out_of_M = :council_out_of_M,
            council_pos_gvt_F = :council_pos_gvt_F,
            council_pos_gvt_M = :council_pos_gvt_M,
            council_pos_pvt_F = :council_pos_pvt_F,
            council_pos_pvt_M = :council_pos_pvt_M,
            region_pos_F = :region_pos_F,
            region_pos_M = :region_pos_M,
            region_out_of_F = :region_out_of_F,
            region_out_of_M = :region_out_of_M,
            region_pos_gvt_F = :region_pos_gvt_F,
            region_pos_gvt_M = :region_pos_gvt_M,
            region_pos_pvt_F = :region_pos_pvt_F,
            region_pos_pvt_M = :region_pos_pvt_M
        WHERE student_global_id = :student_global_id AND exam_id = :exam_id
        """

        async with self.async_session() as session:
            async with session.begin():
                for start in range(0, len(df_update), self.chunk_size):
                    chunk = df_update.iloc[start:start + self.chunk_size]
                    records = chunk.to_dict('records')
                    
                    for attempt in range(self.max_retries):
                        try:
                            async with session.begin_nested():
                                await session.execute(text(update_query), records)
                                print(f"Updated chunk {start}:{start + self.chunk_size} successfully.")
                            break
                        except OperationalError as e:
                            if "Deadlock" in str(e) or "Lock wait timeout" in str(e):
                                if attempt < self.max_retries - 1:
                                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                                    continue
                                else:
                                    raise Exception(f"Failed to update chunk {start}:{start + self.chunk_size} after {self.max_retries} retries: {e}")
                            else:
                                raise e
                        except Exception as e:
                            await session.rollback()
                            raise e

    async def rank_results(self, exam_id: str) -> pd.DataFrame:
        """Perform all ranking actions for the given exam_id."""
        # Fetch data
        df = await self.fetch_data(exam_id)
        print("Initial Data:")
        print(df[['student_global_id', 'centre_number', 'avg_marks', 'sex', 'school_pos_F', 'school_pos_M', 
                 'ward_name', 'council_name', 'region_name', 'school_type']].head())

        # Clear existing rankings
        await self.clear_rankings(exam_id)
        print("Sex-based rankings cleared.")

        # Compute rankings
        df = self.compute_rankings(df)
        print("Computed Rankings:")
        print(df[['student_global_id', 'centre_number', 'sex', 'avg_marks', 'school_pos_F', 'school_pos_M',
                 'ward_pos_F', 'ward_pos_M', 'council_pos_F', 'council_pos_M', 'region_pos_F', 'region_pos_M']].head())

        # Update database
        await self.update_rankings(df, exam_id)
        print(f"Database updated with new rankings ({len(df)} rows in chunks of {self.chunk_size}).")

        return df

    async def close(self):
        """Dispose of the engine."""
        await self.engine.dispose()


async def main():
    settings = Settings()
    exam_id = '1f0656e3-8756-680b-ac24-8d5b3e217521'  # Example exam_id
    ranker = RankingSexWise(settings)

    try:
        start_time = time.time()
        await ranker.rank_results(exam_id)
        end_time = time.time()
        print(f"Ranking completed in {end_time - start_time:.2f} seconds.")
    finally:
        await ranker.close()

if __name__ == "__main__":
    asyncio.run(main())