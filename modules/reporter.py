"""
Report Generator Module - PRODUCTION READY
Generates Excel reports with multiple sheets and analysis.
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
import logging

# Setup logger
logger = logging.getLogger(__name__)


def generate_report(transactions: List[Dict[str, Any]], output_path: str):
    """
    Generate comprehensive Excel report with multiple sheets.
    
    Args:
        transactions: List of all transactions
        output_path: Path to output Excel file
        
    Raises:
        ValueError: If no transactions provided
        Exception: If report generation fails
    """
    if not transactions:
        raise ValueError("No transactions to report")
    
    try:
        logger.info(f"Generating report with {len(transactions)} transactions")
        
        # Convert to DataFrame
        df = pd.DataFrame(transactions)
        
        # Ensure required columns exist
        if 'category' not in df.columns:
            logger.warning("Column 'category' not found, creating from category_manual/category_auto")
            df['category'] = df.get('category_manual', df.get('category_auto', 'UNCATEGORIZED'))
        
        if 'type' not in df.columns:
            logger.warning("Column 'type' not found, inferring from amount")
            df['type'] = df['amount'].apply(lambda x: 'income' if x > 0 else 'expense')
        
        # Ensure output directory exists
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Create Excel writer
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Sheet 1: Full Data
            _write_full_data(df, writer)
            logger.debug("Full data sheet written successfully")
            
            # Sheet 2: Monthly Summary
            _write_monthly_summary(df, writer)
            logger.debug("Monthly summary sheet written successfully")
            
            # Sheet 3: Category Summary
            _write_category_summary(df, writer)
            logger.debug("Category summary sheet written successfully")
            
            # Sheet 4: Review Required
            _write_review_required(df, writer)
            logger.debug("Review required sheet written successfully")
            
            # Sheet 5: Dashboard
            _write_dashboard(df, writer)
            logger.debug("Dashboard sheet written successfully")
        
        logger.info(f"Report generated successfully: {output_path}")
        
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        raise


def _write_full_data(df: pd.DataFrame, writer):
    """Write full transaction data sheet."""
    try:
        # Select and order columns
        columns = [
            'date', 'description_raw', 'amount', 'category', 
            'subcategory', 'type', 'balance', 'review_flag'
        ]
        
        # Filter to available columns
        available_cols = [col for col in columns if col in df.columns]
        
        df_full = df[available_cols].copy()
        df_full = df_full.sort_values('date', ascending=False)
        
        df_full.to_excel(writer, sheet_name='FULL_DATA', index=False)
        
        # Auto-adjust column widths (FIXED: handle float/int properly)
        worksheet = writer.sheets['FULL_DATA']
        for idx, col in enumerate(df_full.columns):
            try:
                # Convert to string and get max length
                col_values = df_full[col].astype(str)
                max_length = max(
                    col_values.str.len().max(),
                    len(str(col))
                )
            except Exception as e:
                logger.warning(f"Could not calculate width for column {col}: {e}")
                max_length = 15  # Default width
            
            # Excel column letters: A=65, B=66, etc.
            if idx < 26:
                col_letter = chr(65 + idx)
            else:
                # For columns beyond Z (AA, AB, etc.)
                col_letter = f"A{chr(65 + idx - 26)}"
            
            worksheet.column_dimensions[col_letter].width = min(max_length + 2, 50)
            
    except Exception as e:
        logger.error(f"Error writing full data sheet: {e}")
        raise


def _write_monthly_summary(df: pd.DataFrame, writer):
    """Write monthly summary sheet."""
    try:
        df_monthly = df.copy()
        df_monthly['date'] = pd.to_datetime(df_monthly['date'])
        df_monthly['month'] = df_monthly['date'].dt.to_period('M')
        
        # Calculate monthly totals
        summary = df_monthly.groupby(['month', 'type'])['amount'].sum().unstack(fill_value=0)
        
        # Ensure income and expense columns exist
        if 'income' not in summary.columns:
            summary['income'] = 0
        if 'expense' not in summary.columns:
            summary['expense'] = 0
        
        # Calculate net savings
        summary['net_savings'] = summary['income'] + summary['expense']  # expense is negative
        
        # Reset index and format
        summary = summary.reset_index()
        summary['month'] = summary['month'].astype(str)
        
        # Reorder columns
        summary = summary[['month', 'income', 'expense', 'net_savings']]
        
        summary.to_excel(writer, sheet_name='MONTHLY_SUMMARY', index=False)
        
        # Format worksheet
        worksheet = writer.sheets['MONTHLY_SUMMARY']
        for idx in range(min(len(summary.columns), 26)):
            col_letter = chr(65 + idx)
            worksheet.column_dimensions[col_letter].width = 15
            
    except Exception as e:
        logger.error(f"Error writing monthly summary: {e}")
        raise


def _write_category_summary(df: pd.DataFrame, writer):
    """Write category summary sheet."""
    try:
        # Group by category and type
        summary = df.groupby(['category', 'type'])['amount'].agg(['sum', 'count']).reset_index()
        summary.columns = ['category', 'type', 'total_amount', 'transaction_count']
        
        # Sort by total amount
        summary = summary.sort_values('total_amount', ascending=False)
        
        summary.to_excel(writer, sheet_name='CATEGORY_SUMMARY', index=False)
        
        # Format worksheet
        worksheet = writer.sheets['CATEGORY_SUMMARY']
        for idx in range(min(len(summary.columns), 26)):
            col_letter = chr(65 + idx)
            worksheet.column_dimensions[col_letter].width = 20
            
    except Exception as e:
        logger.error(f"Error writing category summary: {e}")
        raise


def _write_review_required(df: pd.DataFrame, writer):
    """Write transactions requiring review."""
    try:
        df_review = df[df['review_flag'] == 1].copy() if 'review_flag' in df.columns else pd.DataFrame()
        
        if df_review.empty:
            # Create empty sheet with message
            df_review = pd.DataFrame({'message': ['No transactions require review']})
        else:
            columns = [
                'date', 'description_raw', 'amount', 'category', 
                'subcategory', 'type'
            ]
            available_cols = [col for col in columns if col in df_review.columns]
            df_review = df_review[available_cols]
            df_review = df_review.sort_values('date', ascending=False)
        
        df_review.to_excel(writer, sheet_name='REVIEW_REQUIRED', index=False)
        
        # Format worksheet
        worksheet = writer.sheets['REVIEW_REQUIRED']
        for idx in range(min(len(df_review.columns), 26)):
            col_letter = chr(65 + idx)
            worksheet.column_dimensions[col_letter].width = 20
            
    except Exception as e:
        logger.error(f"Error writing review required: {e}")
        raise


def _write_dashboard(df: pd.DataFrame, writer):
    """Write dashboard with key metrics."""
    try:
        # Calculate key metrics
        total_income = df[df['type'] == 'income']['amount'].sum() if 'type' in df.columns else 0
        total_expenses = df[df['type'] == 'expense']['amount'].sum() if 'type' in df.columns else 0
        net_savings = total_income + total_expenses  # expenses are negative
        
        transaction_count = len(df)
        date_range = f"{df['date'].min()} to {df['date'].max()}"
        
        # Top expense categories
        if 'type' in df.columns and 'category' in df.columns:
            expense_by_category = df[df['type'] == 'expense'].groupby('category')['amount'].sum()
            expense_by_category = expense_by_category.sort_values()  # Most negative first
            top_expenses = expense_by_category.head(10)
        else:
            top_expenses = pd.Series()
        
        # Create dashboard data
        dashboard_data = {
            'Metric': [
                'Total Income',
                'Total Expenses',
                'Net Savings',
                'Transaction Count',
                'Date Range',
                '',
                'Top Expense Categories:'
            ],
            'Value': [
                f"{total_income:.2f}",
                f"{total_expenses:.2f}",
                f"{net_savings:.2f}",
                str(transaction_count),
                date_range,
                '',
                ''
            ]
        }
        
        # Add top expenses
        for category, amount in top_expenses.items():
            dashboard_data['Metric'].append(f"  {category}")
            dashboard_data['Value'].append(f"{amount:.2f}")
        
        df_dashboard = pd.DataFrame(dashboard_data)
        df_dashboard.to_excel(writer, sheet_name='DASHBOARD', index=False)
        
        # Format worksheet
        worksheet = writer.sheets['DASHBOARD']
        worksheet.column_dimensions['A'].width = 30
        worksheet.column_dimensions['B'].width = 20
        
    except Exception as e:
        logger.error(f"Error writing dashboard: {e}")
        raise
