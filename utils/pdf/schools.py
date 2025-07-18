import os
import asyncio
import aiomysql
from typing import Dict, List, Optional, Union
import aiohttp
from pydantic import BaseModel
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class SchoolInfo(BaseModel):
    centre_number: str
    school_name: str
    region_name: Optional[str] = None
    council_name: Optional[str] = None
    ward_name: Optional[str] = None
    school_type: str = 'unknown'

class KiyaboAPIClient:
    def __init__(self, base_url: str = "https://api.kiyabo.com/api/v1"):
        self.base_url = base_url
        self.token = None
        self.pool = None
        self.session = None
    
    async def initialize(self):
        """Initialize both database and HTTP session"""
        await self.initialize_db()
        await self.initialize_http_session()
    
    async def initialize_db(self):
        """Initialize the database connection pool"""
        try:
            self.pool = await aiomysql.create_pool(
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT", 3306)),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                db=os.getenv("DB_NAME"),
                minsize=1,
                maxsize=int(os.getenv("DB_POOL_SIZE", 5)),
                autocommit=True
            )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize database pool: {str(e)}")
    
    async def initialize_http_session(self):
        """Initialize aiohttp client session"""
        try:
            self.session = aiohttp.ClientSession()
        except Exception as e:
            raise RuntimeError(f"Failed to initialize HTTP session: {str(e)}")
    
    async def close(self):
        """Clean up all resources"""
        try:
            await self.close_db()
            await self.close_http_session()
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
    
    async def close_db(self):
        """Close the database connection pool"""
        if self.pool:
            try:
                self.pool.close()
                await self.pool.wait_closed()
            except Exception as e:
                logger.error(f"Error closing database pool: {str(e)}")
            finally:
                self.pool = None
    
    async def close_http_session(self):
        """Close aiohttp client session"""
        if self.session:
            try:
                await self.session.close()
            except Exception as e:
                logger.error(f"Error closing HTTP session: {str(e)}")
            finally:
                self.session = None
    
    async def login(self, email: str, password: str) -> Dict:
        """Authenticate with the API and get a bearer token"""
        if not self.session:
            raise RuntimeError("HTTP session not initialized")
        
        try:
            url = f"{self.base_url}/auth/login"
            data = {
                "username": email,
                "password": password,
            }
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json"
            }
            
            async with self.session.post(url, data=data, headers=headers) as response:
                response.raise_for_status()
                result = await response.json()
                self.token = result["access_token"]
                logger.info("Login successful")
                return result
        except aiohttp.ClientError as e:
            logger.error(f"Login failed: {str(e)}")
            raise RuntimeError(f"Login failed: {str(e)}")
    
    async def get_all_schools(self, **filters) -> Dict:
        """Get ALL schools matching filters by following pagination"""
        if not self.token:
            raise ValueError("You must login first")
        if not self.session:
            raise RuntimeError("HTTP session not initialized")

        all_schools = []
        skip = 0
        limit = 100  # Max per request
        has_more = True
        
        while has_more:
            try:
                url = f"{self.base_url}/tamisemi/schools/"
                headers = {
                    "Authorization": f"Bearer {self.token}",
                    "Accept": "application/json"
                }
                
                # Prepare query parameters
                params = {k: v.lower() if isinstance(v, str) else v 
                         for k, v in filters.items() if v is not None}
                params['skip'] = skip
                params['limit'] = limit
                
                logger.info(f"Fetching schools with params: {params}")
                
                async with self.session.get(url, headers=headers, params=params) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    # Add schools to our collection
                    schools = data.get('schools', [])
                    all_schools.extend(schools)
                    
                    # Check if we've reached the end
                    if len(schools) < limit:
                        has_more = False
                    else:
                        skip += limit
                    
                    # Small delay to be polite to the API
                    await asyncio.sleep(0.5)
            
            except aiohttp.ClientError as e:
                logger.error(f"Failed to fetch schools: {str(e)}")
                raise RuntimeError(f"Failed to fetch schools: {str(e)}")
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                raise RuntimeError(f"Unexpected error: {str(e)}")
        
        return {
            'count': len(all_schools),
            'schools': all_schools,
            'current': None,
            'previous': None,
            'next': None
        }

async def main():
    client = KiyaboAPIClient()
    
    try:
        # Initialize both database and HTTP session
        await client.initialize()
        
        # Login to API
        login_response = await client.login(os.getenv("API_EMAIL"), os.getenv("API_PASSWORD"))
        
        # Fetch ALL schools from API for given regions and level
        regions = [
            "Arusha",
            "Dar es Salaam",
            "Dodoma",
            "Geita",
            "Iringa",
            "Kagera",
            "Katavi",
            "Kigoma",
            "Kilimanjaro",
            "Lindi",
            "Manyara",
            "Mara",
            "Mbeya",
            "Morogoro",
            "Mtwara",
            "Mwanza",
            "Njombe",
            "Pwani",
            "Rukwa",
            "Ruvuma",
            "Shinyanga",
            "Simiyu",
            "Singida",
            "Songwe",
            "Tabora",
            "Tanga"
        ]
        all_schools = []
        
        for region in regions:
            logger.info(f"Fetching ALL schools for {region}...")
            try:
                result = await client.get_all_schools(region=region, level="olevel-only")
                logger.info(f"Found {len(result['schools'])} schools in {region}")
                all_schools.extend(result['schools'])
            except Exception as e:
                logger.error(f"Error fetching schools for {region}: {str(e)}")
                continue
        
        logger.info(f"Total schools collected: {len(all_schools)}")
        
        if all_schools:
            logger.info("Sample of fetched schools (first 5):")
            for school in all_schools[:5]:
                logger.info(f"- {school.get('school_name')} ({school.get('centre_number')})")
        else:
            logger.warning("No schools found")
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
    finally:
        # Clean up
        await client.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")