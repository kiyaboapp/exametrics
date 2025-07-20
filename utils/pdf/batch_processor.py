# app/utils/batch_processor.py

import os
import pandas as pd
from typing import List, Tuple
from utils.pdf.pdf_processor import PDFTableProcessor
import traceback
from datetime import datetime
import numpy as np


class BatchPDFProcessor:

    @staticmethod
    def process_pdf_files(pdf_paths: List[str], split_names: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Process multiple PDF files and return combined student data and report data.
        """
        print(f"[BatchPDFProcessor] Starting processing of {len(pdf_paths)} PDF files...")
        all_student_data = []
        report_data = []

        for pdf_path in pdf_paths:
            print(f"[BatchPDFProcessor] Processing file: {pdf_path}")
            try:
                student_data, _, school_info = PDFTableProcessor.extract_tables(pdf_path)

                # Remove repeater column if exists (accepting common typos)
                for col in ['REPEATER', 'RETAEPER']:
                    if col in student_data.columns:
                        student_data = student_data.drop(columns=[col])
                        print(f"[BatchPDFProcessor] Removed column: {col}")

                # Find first subject column index (subject headers are numeric strings)
                subject_col_idx = None
                for idx, col in enumerate(student_data.columns):
                    if col.isdigit():
                        subject_col_idx = idx
                        break

                # Columns to insert
                school_info_cols = ['EXAM_TYPE', 'CENTRE_NUMBER', 'EXAM_YEAR', 'SCHOOL_NAME']
                insert_df = pd.DataFrame({
                    key: [school_info.get(key, '')] * len(student_data)
                    for key in school_info_cols
                })

                # Insert school info columns before first subject column or at start if no subject found
                if subject_col_idx is not None:
                    insert_pos = subject_col_idx
                else:
                    insert_pos = 0  # fallback: insert at beginning

                left = student_data.iloc[:, :insert_pos]
                right = student_data.iloc[:, insert_pos:]
                student_data = pd.concat([left, insert_df, right], axis=1)

                all_student_data.append(student_data)

                report_data.append({
                    'SCHOOL NAME': school_info.get('SCHOOL_NAME', ''),
                    'CENTRE NUMBER': school_info.get('CENTRE_NUMBER', ''),
                    'SCHOOL TYPE': school_info.get('SCHOOL_TYPE', ''),
                    'EXAM TYPE': school_info.get('EXAM_TYPE', ''),
                    'EXAM YEAR': school_info.get('EXAM_YEAR', ''),
                    'STATUS': 'Success',
                    'FILE NAME': os.path.basename(pdf_path),
                    'STUDENT COUNT': len(student_data)
                })

                print(f"[BatchPDFProcessor] Processed {pdf_path}: {len(student_data)} students")

            except Exception as e:
                error_message = f"Failed to process {pdf_path}: {e}"
                traceback.print_exc()
                report_data.append({
                    'SCHOOL NAME': '',
                    'CENTRE NUMBER': '',
                    'SCHOOL TYPE': '',
                    'EXAM TYPE': '',
                    'EXAM YEAR': '',
                    'STATUS': error_message,
                    'FILE NAME': os.path.basename(pdf_path),
                    'STUDENT NAME': 0
                })
                print(f"[BatchPDFProcessor] {error_message}")
                continue

        student_df = pd.concat(all_student_data, ignore_index=True) if all_student_data else pd.DataFrame()
        report_df = pd.DataFrame(report_data)

        print("[BatchPDFProcessor] Processing complete.")
        return student_df, report_df

    @staticmethod
    def process_pdf_folder(folder_path: str, split_names: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Process all PDF files in a given folder.
        """
        print(f"[BatchPDFProcessor] Scanning folder: {folder_path}")
        pdf_files = [
            os.path.join(folder_path, file)
            for file in os.listdir(folder_path)
            if file.lower().endswith('.pdf')
        ]

        if not pdf_files:
            error_message = "No PDF files found in the specified folder."
            print(f"[BatchPDFProcessor] {error_message}")
            raise ValueError(error_message)

        print(f"[BatchPDFProcessor] Found {len(pdf_files)} PDF files.")
        return BatchPDFProcessor.process_pdf_files(pdf_files, split_names)

    @staticmethod
    def generate_output_filename() -> str:
        """
        Generate a unique output filename for the Excel file.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Batch_NECTA_Results_{timestamp}.xlsx"
        print(f"[BatchPDFProcessor] Generated output filename: {filename}")
        return filename

    @staticmethod
    def save_to_excel(
        student_data: pd.DataFrame,
        report_data: pd.DataFrame,
        output_path: str,
        split_names: bool = False
    ) -> str:
        """
        Save combined student data and report data to an Excel file.
        """

        print(f"[BatchPDFProcessor] Saving output to Excel: {output_path}")

        # Replace nan and inf values with None to avoid ExcelWriter errors
        student_data = student_data.replace([np.nan, np.inf, -np.inf], None)
        report_data = report_data.replace([np.nan, np.inf, -np.inf], None)

        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            workbook = writer.book

            # Define formats
            border_format = workbook.add_format({'border': 1, 'valign': 'top'})
            header_format = workbook.add_format({
                'bold': True, 'border': 1, 'valign': 'top',
                'align': 'center', 'fg_color': '#4472C4', 'font_color': 'white'
            })
            center_format = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'top'})

            # ----------------- Students Sheet -----------------
            student_data.to_excel(writer, sheet_name='Students', index=False, startrow=1, header=False)
            student_sheet = writer.sheets['Students']

            # Write header row
            for col_num, value in enumerate(student_data.columns.values):
                student_sheet.write(0, col_num, value, header_format)

            max_row = len(student_data)
            max_col = len(student_data.columns) - 1

            for row in range(1, max_row + 1):
                for col in range(max_col + 1):
                    value = student_data.iloc[row - 1, col]
                    col_name = student_data.columns[col]

                    # Center alignment for numeric column names or specific columns
                    if str(col_name).isdigit() and len(str(col_name)) == 3:
                        student_sheet.write(row, col, value, center_format)
                    elif col_name in ['CANDIDATE', 'SEX']:
                        student_sheet.write(row, col, value, center_format)
                    else:
                        student_sheet.write(row, col, value, border_format)

            # Adjust column widths
            for col_num, col_name in enumerate(student_data.columns):
                if str(col_name).isdigit() and len(col_name) == 3:
                    student_sheet.set_column(col_num, col_num, 8)
                elif col_name in ['CANDIDATE', 'SEX']:
                    student_sheet.set_column(col_num, col_num, 12)
                else:
                    max_len = max(student_data[col_name].astype(str).map(len).max(), len(str(col_name))) + 2
                    student_sheet.set_column(col_num, col_num, max_len)

            # ----------------- Report Sheet -----------------
            report_data.to_excel(writer, sheet_name='Report', index=False, startrow=1, header=False)
            report_sheet = writer.sheets['Report']

            for col_num, value in enumerate(report_data.columns.values):
                report_sheet.write(0, col_num, value, header_format)

            max_row = len(report_data)
            max_col = len(report_data.columns) - 1

            for row in range(1, max_row + 1):
                for col in range(max_col + 1):
                    value = report_data.iloc[row - 1, col]
                    report_sheet.write(row, col, value, border_format)

            # ----------------- Formatting Both Sheets -----------------
            for sheet, title in [(student_sheet, "Combined Student Data"), (report_sheet, "Conversion Report")]:
                sheet.freeze_panes(1, 0)
                sheet.set_landscape()
                sheet.fit_to_pages(1, 0)
                sheet.set_margins(left=0.5, right=0.5, top=0.5, bottom=0.5)
                sheet.repeat_rows(0)
                sheet.set_footer('&RPage &P of &N')
                sheet.autofilter(0, 0, 0, max_col)
                sheet.set_header(f'&C&"Calibri,Bold"&14{title}')

        print(f"[BatchPDFProcessor] Excel saved successfully: {output_path}")
        return output_path
