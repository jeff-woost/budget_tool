"""
Enhanced Budget Manager - Database Module
Handles all database operations and data models
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    class _PandasStub:
        class DataFrame:
            def __init__(self, *_, **__):
                pass
    pd = _PandasStub()

# Categories structure
EXPENSE_CATEGORIES = {
    'Housing': ['Mortgage', 'Special Assessment', 'Additional Principal', 'Lima Apartment Wires', 
                'Lima Apartment Fees', 'Escrow', 'HOA', 'Reserves', 'Condo Insurance', 
                'Property Taxes', 'Labor'],
    'Utilities': ['Optimum', 'PSEG', 'Cell Phone', 'Car Insurance', 'Gloria', 'Insurance', 
                  'Taxi / Transit', 'Bus Pass', 'Misc Utility'],
    'Food': ['Food (Groceries)', 'Food (Take Out)', 'Food (Dining Out)', 'Food (Other)', 
             'Food (Party)', 'Food (Guests)', 'Food (Work)', 'Food (Special Occasion)'],
    'Healthcare': ['Jeff Doctor', 'Prescriptions', 'Vitamins', 'Other Doctor Visits', 
                   'Haircut', 'Hygenie', 'Family', 'Fertility', 'Co-Pay', 'Baker', 
                   'HC Subscriptions', 'Joaquin Health Care', 'Zoe Health Care', 'Misc Health Care'],
    'Childcare': ['Village Classes', 'Baby Sitting', 'Clothing', 'Diapers', 'Necessities', 
                  'Accessories', 'Toys', 'Food / Snacks', 'Haircut', 'Activities', 
                  'Uber / Lyft', 'Misc.'],
    'Vehicles': ['Vehicle Fixes', 'Vehicle Other', 'Gas', 'DMV', 'Parts', 'Tires / Wheels', 
                 'Insurance', 'Oil Changes', 'Car Wash', 'Parking', 'Tolls'],
    'Home': ['Home Necessities', 'Home Décor', 'House Cleaning', 'Bathroom', 'Bedrooms', 
             'Kitchen', 'Tools / Hardware', 'Storage', 'Homeware', 'Subscriptions'],
    'Other': ['Gifts', 'Taxes', 'Donations', 'Gatherings', 'Parties', 'Clothes', 'Shoes', 
              'Pets', 'Target AutoPay', 'Stupid Tax', 'Amazon Prime', 'Fees', 'Reversal', 
              'Entertainment', 'Other'],
    'Vacation': ['Flights/Travel', 'Rental Car', 'Airport', 'Taxi', 'Food', 'Eating Out', 
                 'Gas', 'Activities', 'Bedding', 'Fees', 'Physical Goods', 'Housing', 
                 'Necessities']
}

class EnhancedBudgetDatabase:
    """Enhanced database handler with comprehensive financial tracking"""
    
    def __init__(self, db_name='enhanced_budget_v2.db'):
        self.connection = sqlite3.connect(db_name)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
        
        # Enable optimizations
        self.cursor.execute("PRAGMA foreign_keys = ON")
        self.cursor.execute("PRAGMA journal_mode = WAL")
        self.cursor.execute("PRAGMA synchronous = NORMAL")
        
        self.create_tables()
        self.create_indexes()
        
    def create_tables(self):
        """Create all necessary tables"""
        
        # Income table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS income (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                source TEXT NOT NULL,
                amount REAL NOT NULL,
                person TEXT NOT NULL,
                account TEXT NOT NULL,
                description TEXT,
                is_transfer BOOLEAN DEFAULT 0,
                from_account TEXT,
                month TEXT NOT NULL,
                year INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Expenses table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                category TEXT NOT NULL,
                subcategory TEXT NOT NULL,
                amount REAL NOT NULL,
                person TEXT NOT NULL,
                description TEXT,
                account TEXT NOT NULL,
                cleared BOOLEAN DEFAULT 1,
                month TEXT NOT NULL,
                year INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Investment accounts
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS investment_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_name TEXT NOT NULL UNIQUE,
                account_type TEXT NOT NULL,
                liquidity TEXT NOT NULL,
                current_balance REAL DEFAULT 0,
                previous_balance REAL DEFAULT 0,
                last_updated TEXT NOT NULL,
                person TEXT,
                institution TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Savings goals
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS savings_goals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                target_amount REAL NOT NULL,
                current_amount REAL DEFAULT 0,
                monthly_contribution REAL DEFAULT 0,
                target_date TEXT,
                priority INTEGER DEFAULT 1,
                is_active BOOLEAN DEFAULT 1,
                category TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Monthly summaries
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS monthly_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                month TEXT NOT NULL,
                year INTEGER NOT NULL,
                total_income REAL DEFAULT 0,
                total_expenses REAL DEFAULT 0,
                total_saved REAL DEFAULT 0,
                net_worth REAL DEFAULT 0,
                budget_variance REAL DEFAULT 0,
                savings_rate REAL DEFAULT 0,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(month, year)
            )
        ''')
        
        # Budget plans
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS budget_plans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                month TEXT NOT NULL,
                year INTEGER NOT NULL,
                category TEXT NOT NULL,
                subcategory TEXT NOT NULL,
                planned_amount REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(month, year, category, subcategory)
            )
        ''')
        
        # Real assets
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS real_assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_name TEXT NOT NULL UNIQUE,
                asset_type TEXT NOT NULL,
                current_value REAL DEFAULT 0,
                purchase_price REAL DEFAULT 0,
                purchase_date TEXT,
                last_updated TEXT NOT NULL,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create update triggers
        tables_with_updated = ['income', 'expenses', 'savings_goals', 'budget_plans']
        for table in tables_with_updated:
            self.cursor.execute(f'''
                CREATE TRIGGER IF NOT EXISTS update_{table}_timestamp 
                AFTER UPDATE ON {table}
                BEGIN
                    UPDATE {table} SET updated_at = CURRENT_TIMESTAMP 
                    WHERE id = NEW.id;
                END
            ''')
        
        self.connection.commit()
        self._initialize_default_data()
    
    def create_indexes(self):
        """Create indexes for performance"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_income_date ON income(date)",
            "CREATE INDEX IF NOT EXISTS idx_income_month_year ON income(month, year)",
            "CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(date)",
            "CREATE INDEX IF NOT EXISTS idx_expenses_month_year ON expenses(month, year)",
            "CREATE INDEX IF NOT EXISTS idx_expenses_category ON expenses(category, subcategory)",
            "CREATE INDEX IF NOT EXISTS idx_monthly_summary_date ON monthly_summary(year, month)",
            "CREATE INDEX IF NOT EXISTS idx_budget_plans_date ON budget_plans(year, month, category)"
        ]
        
        for index in indexes:
            self.cursor.execute(index)
        
        self.connection.commit()
        
    def _initialize_default_data(self):
        """Initialize default data"""
        
        # Default investment accounts
        default_investments = [
            ("Jeff's Roth Accounts", "Retirement", "Non-Liquid", "Jeff", "Vanguard"),
            ("Jeff's 401k Accounts", "Retirement", "Non-Liquid", "Jeff", "Employer"),
            ("Vanessa's Roth 401k Accounts", "Retirement", "Non-Liquid", "Vanessa", "Employer"),
            ("Vanessa's Roth Accounts", "Retirement", "Non-Liquid", "Vanessa", "Vanguard"),
            ("Emergency Fund", "Savings", "Liquid", "Joint", "Bank of America"),
            ("House Fund", "Savings", "Semi-Liquid", "Joint", "Bank of America"),
            ("Vacation Fund", "Savings", "Liquid", "Joint", "Bank of America"),
            ("Baby Fund", "Savings", "Liquid", "Joint", "Bank of America"),
            ("HSA", "Health Savings", "Semi-Liquid", "Jeff", "Health Equity"),
            ("Betterment", "Investment", "Semi-Liquid", "Joint", "Betterment"),
            ("Checking", "Cash", "Liquid", "Joint", "Bank of America")
        ]
        
        for name, acc_type, liquidity, person, institution in default_investments:
            try:
                self.cursor.execute('''
                    INSERT OR IGNORE INTO investment_accounts 
                    (account_name, account_type, liquidity, person, institution, 
                     current_balance, previous_balance, last_updated)
                    VALUES (?, ?, ?, ?, ?, 0, 0, ?)
                ''', (name, acc_type, liquidity, person, institution, 
                      datetime.now().strftime('%Y-%m-%d')))
            except:
                pass
                
        # Default real assets
        default_assets = [
            ("Primary Residence", "Real Estate", 548584, datetime.now().strftime('%Y-%m-%d')),
            ("Lima Apartment", "Real Estate", 52000, datetime.now().strftime('%Y-%m-%d')),
            ("Tiguan", "Vehicle", 38379, datetime.now().strftime('%Y-%m-%d'))
        ]
        
        for name, asset_type, value, date in default_assets:
            try:
                self.cursor.execute('''
                    INSERT OR IGNORE INTO real_assets 
                    (asset_name, asset_type, current_value, last_updated)
                    VALUES (?, ?, ?, ?)
                ''', (name, asset_type, value, date))
            except:
                pass
        
        # Default savings goals
        default_goals = [
            ("Vacation", 3000, 1, "Travel fund"),
            ("Emergency Fund", 24000, 1, "6 months expenses"),
            ("Baby Fund", 8000, 2, "Child expenses"),
            ("House Down Payment", 80000, 3, "Future home"),
        ]
        
        for name, target, priority, notes in default_goals:
            try:
                self.cursor.execute('''
                    INSERT OR IGNORE INTO savings_goals 
                    (name, target_amount, priority, notes, target_date)
                    VALUES (?, ?, ?, ?, ?)
                ''', (name, target, priority, notes, 
                      (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')))
            except:
                pass
                
        self.connection.commit()
    
    # Income methods
    def add_income(self, date: str, source: str, amount: float, person: str, 
                   account: str, description: str = "", is_transfer: bool = False,
                   from_account: str = "") -> int:
        """Add income entry"""
        month = date.split('-')[1]
        year = int(date.split('-')[0])
        
        self.cursor.execute('''
            INSERT INTO income 
            (date, source, amount, person, account, description, is_transfer, 
             from_account, month, year)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (date, source, amount, person, account, description, 
              is_transfer, from_account, month, year))
        
        self.connection.commit()
        return self.cursor.lastrowid
    
    def update_income(self, income_id: int, data: Dict):
        """Update income entry"""
        month = data['date'].split('-')[1]
        year = int(data['date'].split('-')[0])
        
        self.cursor.execute('''
            UPDATE income SET date = ?, source = ?, amount = ?, person = ?,
            account = ?, description = ?, is_transfer = ?, from_account = ?,
            month = ?, year = ?
            WHERE id = ?
        ''', (data['date'], data['source'], data['amount'], data['person'],
              data['account'], data['description'], data['is_transfer'],
              data['from_account'], month, year, income_id))
        self.connection.commit()
    
    def delete_income(self, income_id: int):
        """Delete income entry"""
        self.cursor.execute('DELETE FROM income WHERE id = ?', (income_id,))
        self.connection.commit()
    
    # Expense methods
    def add_expense(self, date: str, category: str, subcategory: str, amount: float,
                    person: str, description: str, account: str, cleared: bool = True) -> int:
        """Add expense entry"""
        month = date.split('-')[1]
        year = int(date.split('-')[0])
        
        self.cursor.execute('''
            INSERT INTO expenses 
            (date, category, subcategory, amount, person, description, account, 
             cleared, month, year)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (date, category, subcategory, amount, person, description, 
              account, cleared, month, year))
        
        self.connection.commit()
        return self.cursor.lastrowid
    
    def update_expense(self, expense_id: int, data: Dict):
        """Update expense entry"""
        month = data['date'].split('-')[1]
        year = int(data['date'].split('-')[0])
        
        self.cursor.execute('''
            UPDATE expenses SET date = ?, category = ?, subcategory = ?,
            amount = ?, person = ?, description = ?, account = ?, cleared = ?,
            month = ?, year = ?
            WHERE id = ?
        ''', (data['date'], data['category'], data['subcategory'], 
              data['amount'], data['person'], data['description'],
              data['account'], data['cleared'], month, year, expense_id))
        self.connection.commit()
    
    def delete_expense(self, expense_id: int):
        """Delete expense entry"""
        self.cursor.execute('DELETE FROM expenses WHERE id = ?', (expense_id,))
        self.connection.commit()
    
    # Data retrieval methods
    def get_monthly_data(self, month: str, year: int) -> Dict:
        """Get comprehensive monthly financial data"""
        data = {
            'income': [],
            'expenses': [],
            'summary': {},
            'category_totals': {},
            'subcategory_totals': {}
        }
        
        # Get income
        self.cursor.execute('''
            SELECT * FROM income WHERE month = ? AND year = ?
            ORDER BY date DESC
        ''', (month, year))
        
        data['income'] = [dict(row) for row in self.cursor.fetchall()]
        
        # Get expenses
        self.cursor.execute('''
            SELECT * FROM expenses WHERE month = ? AND year = ?
            ORDER BY date DESC
        ''', (month, year))
        
        data['expenses'] = [dict(row) for row in self.cursor.fetchall()]
        
        # Calculate totals by category
        self.cursor.execute('''
            SELECT category, subcategory, SUM(amount) as total
            FROM expenses 
            WHERE month = ? AND year = ?
            GROUP BY category, subcategory
        ''', (month, year))
        
        for row in self.cursor.fetchall():
            cat = row['category']
            subcat = row['subcategory']
            total = row['total']
            
            if cat not in data['category_totals']:
                data['category_totals'][cat] = 0
            data['category_totals'][cat] += total
            
            if cat not in data['subcategory_totals']:
                data['subcategory_totals'][cat] = {}
            data['subcategory_totals'][cat][subcat] = total
        
        # Calculate summary
        total_income = sum(item['amount'] for item in data['income'] if not item['is_transfer'])
        total_expenses = sum(item['amount'] for item in data['expenses'])
        
        data['summary'] = {
            'total_income': total_income,
            'total_expenses': total_expenses,
            'net_balance': total_income - total_expenses,
            'transfer_in': sum(item['amount'] for item in data['income'] if item['is_transfer']),
            'cleared_expenses': sum(item['amount'] for item in data['expenses'] if item['cleared']),
            'pending_expenses': sum(item['amount'] for item in data['expenses'] if not item['cleared'])
        }
        
        return data
    
    def get_budget_vs_actual(self, month: str, year: int) -> Dict:
        """Get budget vs actual comparison"""
        comparison = {}
        
        # Get planned amounts
        self.cursor.execute('''
            SELECT category, subcategory, planned_amount
            FROM budget_plans
            WHERE month = ? AND year = ?
        ''', (month, year))
        
        for row in self.cursor.fetchall():
            cat = row['category']
            subcat = row['subcategory']
            planned = row['planned_amount']
            
            if cat not in comparison:
                comparison[cat] = {}
            comparison[cat][subcat] = {
                'planned': planned,
                'actual': 0,
                'variance': 0,
                'percentage': 0
            }
        
        # Get actual amounts
        self.cursor.execute('''
            SELECT category, subcategory, SUM(amount) as actual
            FROM expenses
            WHERE month = ? AND year = ?
            GROUP BY category, subcategory
        ''', (month, year))
        
        for row in self.cursor.fetchall():
            cat = row['category']
            subcat = row['subcategory']
            actual = row['actual']
            
            if cat not in comparison:
                comparison[cat] = {}
            if subcat not in comparison[cat]:
                comparison[cat][subcat] = {
                    'planned': 0,
                    'actual': actual,
                    'variance': -actual,
                    'percentage': 0
                }
            else:
                comparison[cat][subcat]['actual'] = actual
                planned = comparison[cat][subcat]['planned']
                comparison[cat][subcat]['variance'] = planned - actual
                if planned > 0:
                    comparison[cat][subcat]['percentage'] = (actual / planned) * 100
        
        return comparison
    
    def get_net_worth_summary(self) -> Dict:
        """Calculate comprehensive net worth summary"""
        summary = {
            'liquid_assets': 0,
            'semi_liquid_assets': 0,
            'non_liquid_assets': 0,
            'real_assets': 0,
            'total_assets': 0,
            'by_account_type': {},
            'by_person': {},
            'month_over_month_change': 0
        }
        
        # Investment accounts
        self.cursor.execute('''
            SELECT account_type, liquidity, person, current_balance, previous_balance
            FROM investment_accounts
        ''')
        
        for row in self.cursor.fetchall():
            acc_type = row['account_type']
            liquidity = row['liquidity']
            person = row['person']
            current = row['current_balance']
            previous = row['previous_balance']
            
            if liquidity == 'Liquid':
                summary['liquid_assets'] += current
            elif liquidity == 'Semi-Liquid':
                summary['semi_liquid_assets'] += current
            else:
                summary['non_liquid_assets'] += current
            
            if acc_type not in summary['by_account_type']:
                summary['by_account_type'][acc_type] = 0
            summary['by_account_type'][acc_type] += current
            
            if person:
                if person not in summary['by_person']:
                    summary['by_person'][person] = 0
                summary['by_person'][person] += current
            
            summary['month_over_month_change'] += (current - previous)
        
        # Real assets
        self.cursor.execute('SELECT SUM(current_value) FROM real_assets')
        result = self.cursor.fetchone()
        summary['real_assets'] = result[0] if result and result[0] else 0
        
        summary['total_assets'] = (summary['liquid_assets'] + 
                                   summary['semi_liquid_assets'] + 
                                   summary['non_liquid_assets'] + 
                                   summary['real_assets'])
        
        return summary
    
    def update_investment_account(self, account_name: str, new_balance: float):
        """Update investment account balance"""
        # Get current balance first
        self.cursor.execute('''
            SELECT current_balance FROM investment_accounts WHERE account_name = ?
        ''', (account_name,))
        
        result = self.cursor.fetchone()
        previous_balance = result['current_balance'] if result else 0
        
        self.cursor.execute('''
            UPDATE investment_accounts
            SET previous_balance = ?, current_balance = ?, last_updated = ?
            WHERE account_name = ?
        ''', (previous_balance, new_balance, datetime.now().strftime('%Y-%m-%d'), 
              account_name))
        
        self.connection.commit()
    
    def get_investment_accounts(self) -> List[Dict]:
        """Get all investment accounts"""
        self.cursor.execute('''
            SELECT * FROM investment_accounts
            ORDER BY account_type, account_name
        ''')
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_real_assets(self) -> List[Dict]:
        """Get all real assets"""
        self.cursor.execute('''
            SELECT * FROM real_assets
            ORDER BY asset_type, asset_name
        ''')
        return [dict(row) for row in self.cursor.fetchall()]
    
    def update_real_asset(self, asset_name: str, new_value: float):
        """Update real asset value"""
        self.cursor.execute('''
            UPDATE real_assets
            SET current_value = ?, last_updated = ?
            WHERE asset_name = ?
        ''', (new_value, datetime.now().strftime('%Y-%m-%d'), asset_name))
        self.connection.commit()
    
    def get_savings_goals(self) -> List[Dict]:
        """Get all active savings goals"""
        self.cursor.execute('''
            SELECT * FROM savings_goals
            WHERE is_active = 1
            ORDER BY priority, target_date
        ''')
        return [dict(row) for row in self.cursor.fetchall()]
    
    def update_savings_goal(self, goal_id: int, current_amount: float):
        """Update savings goal progress"""
        self.cursor.execute('''
            UPDATE savings_goals
            SET current_amount = ?
            WHERE id = ?
        ''', (current_amount, goal_id))
        self.connection.commit()
    
    def save_monthly_summary(self, month: str, year: int, data: Dict):
        """Save monthly summary for historical tracking"""
        self.cursor.execute('''
            INSERT OR REPLACE INTO monthly_summary
            (month, year, total_income, total_expenses, total_saved, net_worth,
             budget_variance, savings_rate, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (month, year, data.get('total_income', 0), data.get('total_expenses', 0),
              data.get('total_saved', 0), data.get('net_worth', 0),
              data.get('budget_variance', 0), data.get('savings_rate', 0),
              data.get('notes', '')))
        
        self.connection.commit()
    
    def get_historical_trends(self, months: int = 12) -> List[Dict]:
        """Get historical financial trends"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=months * 30)
        
        self.cursor.execute('''
            SELECT * FROM monthly_summary
            WHERE year * 100 + CAST(month AS INTEGER) >= ?
            ORDER BY year, month
        ''', (start_date.year * 100 + start_date.month,))
        
        return [dict(row) for row in self.cursor.fetchall()]
    
    def update_budget_plan(self, month: str, year: int, category: str, 
                          subcategory: str, amount: float):
        """Update or create budget plan"""
        self.cursor.execute('''
            INSERT OR REPLACE INTO budget_plans
            (month, year, category, subcategory, planned_amount)
            VALUES (?, ?, ?, ?, ?)
        ''', (month, year, category, subcategory, amount))
        self.connection.commit()
    
    def import_csv_data(self, filepath: str, data_type: str = 'expenses') -> Dict:
        """Import data from CSV file"""
        try:
            import csv
            imported = 0
            errors = []
            
            with open(filepath, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                for row in reader:
                    try:
                        if data_type == 'expenses':
                            # Map CSV columns to expense fields
                            self.add_expense(
                                date=row.get('Date', datetime.now().strftime('%Y-%m-%d')),
                                category=row.get('Category', 'Other'),
                                subcategory=row.get('Sub Category', 'Other'),
                                amount=float(row.get('Amount', 0)),
                                person=row.get('Person', 'Joint'),
                                description=row.get('Description', ''),
                                account=row.get('Account', 'Checking'),
                                cleared=row.get('Cleared', 'True').lower() == 'true'
                            )
                        elif data_type == 'income':
                            # Map CSV columns to income fields
                            self.add_income(
                                date=row.get('Date', datetime.now().strftime('%Y-%m-%d')),
                                source=row.get('Source', 'Other'),
                                amount=float(row.get('Amount', 0)),
                                person=row.get('Person', 'Joint'),
                                account=row.get('Account', 'Checking'),
                                description=row.get('Description', ''),
                                is_transfer=row.get('IsTransfer', 'False').lower() == 'true',
                                from_account=row.get('FromAccount', '')
                            )
                        imported += 1
                    except Exception as e:
                        errors.append(f"Row {imported + 1}: {str(e)}")
            
            return {
                'success': True,
                'imported': imported,
                'errors': errors
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def export_to_csv(self, filepath: str, month: str, year: int) -> bool:
        """Export monthly data to CSV"""
        try:
            import csv
            
            data = self.get_monthly_data(month, year)
            
            with open(filepath, 'w', newline='', encoding='utf-8') as file:
                # Write income section
                writer = csv.writer(file)
                writer.writerow(['INCOME'])
                writer.writerow(['Date', 'Source', 'Amount', 'Person', 'Account', 
                               'Description', 'Is Transfer'])
                
                for item in data['income']:
                    writer.writerow([
                        item['date'], item['source'], item['amount'],
                        item['person'], item['account'], item.get('description', ''),
                        'Yes' if item.get('is_transfer') else 'No'
                    ])
                
                writer.writerow([])  # Empty row
                
                # Write expenses section
                writer.writerow(['EXPENSES'])
                writer.writerow(['Date', 'Category', 'Subcategory', 'Amount', 
                               'Person', 'Description', 'Account', 'Cleared'])
                
                for item in data['expenses']:
                    writer.writerow([
                        item['date'], item['category'], item['subcategory'],
                        item['amount'], item['person'], item.get('description', ''),
                        item['account'], 'Yes' if item['cleared'] else 'No'
                    ])
                
                return True
        except Exception as e:
            print(f"Export error: {e}")
            return False
    
    def add_savings_goal(self, name: str, target_amount: float, monthly_contribution: float = 0,
                        target_date: str = None, priority: int = 1, category: str = None, 
                        notes: str = "") -> int:
        """Add a new savings goal"""
        if target_date is None:
            target_date = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
        
        self.cursor.execute('''
            INSERT INTO savings_goals 
            (name, target_amount, current_amount, monthly_contribution, 
             target_date, priority, is_active, category, notes)
            VALUES (?, ?, 0, ?, ?, ?, 1, ?, ?)
        ''', (name, target_amount, monthly_contribution, target_date, 
              priority, category, notes))
        
        self.connection.commit()
        return self.cursor.lastrowid
    
    def update_savings_goal_contribution(self, goal_id: int, amount: float):
        """Add contribution to savings goal"""
        self.cursor.execute('''
            UPDATE savings_goals
            SET current_amount = current_amount + ?
            WHERE id = ?
        ''', (amount, goal_id))
        self.connection.commit()
    
    def deactivate_savings_goal(self, goal_id: int):
        """Deactivate a savings goal"""
        self.cursor.execute('''
            UPDATE savings_goals
            SET is_active = 0
            WHERE id = ?
        ''', (goal_id,))
        self.connection.commit()
    
    def get_budget_plans(self, month: str, year: int) -> Dict:
        """Get all budget plans for a specific month"""
        self.cursor.execute('''
            SELECT category, subcategory, planned_amount
            FROM budget_plans
            WHERE month = ? AND year = ?
            ORDER BY category, subcategory
        ''', (month, year))
        
        plans = {}
        for row in self.cursor.fetchall():
            cat = row['category']
            if cat not in plans:
                plans[cat] = {}
            plans[cat][row['subcategory']] = row['planned_amount']
        
        return plans
    
    def copy_budget_from_previous_month(self, target_month: str, target_year: int):
        """Copy budget plans from previous month"""
        # Calculate previous month
        if int(target_month) == 1:
            prev_month = '12'
            prev_year = target_year - 1
        else:
            prev_month = str(int(target_month) - 1).zfill(2)
            prev_year = target_year
        
        # Get previous month's budget
        self.cursor.execute('''
            SELECT category, subcategory, planned_amount
            FROM budget_plans
            WHERE month = ? AND year = ?
        ''', (prev_month, prev_year))
        
        # Insert into target month
        for row in self.cursor.fetchall():
            self.update_budget_plan(
                target_month, target_year, 
                row['category'], row['subcategory'], 
                row['planned_amount']
            )
    
    def get_year_to_date_summary(self, year: int) -> Dict:
        """Get year-to-date financial summary"""
        self.cursor.execute('''
            SELECT 
                SUM(total_income) as ytd_income,
                SUM(total_expenses) as ytd_expenses,
                SUM(total_saved) as ytd_saved,
                AVG(savings_rate) as avg_savings_rate
            FROM monthly_summary
            WHERE year = ?
        ''', (year,))
        
        result = self.cursor.fetchone()
        return {
            'ytd_income': result['ytd_income'] or 0,
            'ytd_expenses': result['ytd_expenses'] or 0,
            'ytd_saved': result['ytd_saved'] or 0,
            'avg_savings_rate': result['avg_savings_rate'] or 0
        }
    
    def close(self):
        """Close database connection"""
        self.connection.close()