import asyncio
from utils.excel.excel import export_to_excel

async def main(
        exam_id:str,
        centre_number:str="",
        council_name:str="",
        region_name:str="",
        school_type:str="",
        practical_mode:int=0,
        marks_filler:str=""

        ):
    
    """Main function to export student data to an Excel file."""
    file_path = await export_to_excel(
    exam_id=exam_id,
    council_name=council_name,
    region_name=region_name,
    school_type=school_type,
    practical_mode=practical_mode,
    marks_filler=marks_filler,
    centre_number=centre_number
    )
    return file_path

if __name__ == "__main__":
    exam_id='1f0656e3-8756-680b-ac24-8d5b3e217521'
    centre_number='S0112'
    asyncio.run(main(
        exam_id=exam_id,
        centre_number=centre_number)
    )