"""
Main Window Module
Contains the main application window and tab management.
"""

import sys
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QVBoxLayout, QWidget, QTabWidget
)

from .tabs.spending_tab import SpendingTab
from .tabs.zero_based_budget_tab import ZeroBasedBudgetTab
from .tabs.reports_tab import ReportsTab
from .tabs.payment_tracking_tab import PaymentTrackingTab
from ..data.data_manager import DataManager


class BudgetTracker(QMainWindow):
    """Main application window for the Budget Tracker."""
    
    def __init__(self):
        super().__init__()
        self.data_manager = DataManager()
        self.data_manager.load_data()
        self.init_ui()
        
    def init_ui(self):
        """Initialize the user interface."""
        self.setWindowTitle("Budget Spending Tracker")
        self.setGeometry(100, 100, 1000, 700)
        
        # Set up main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        
        # Create tab widget
        tab_widget = QTabWidget()
        main_layout = QVBoxLayout(main_widget)
        main_layout.addWidget(tab_widget)
        
        # Create tabs
        self.spending_tab = SpendingTab(self.data_manager)
        self.budget_tab = ZeroBasedBudgetTab(self.data_manager)
        self.reports_tab = ReportsTab(self.data_manager)
        self.payment_tab = PaymentTrackingTab(self.data_manager)
        
        tab_widget.addTab(self.spending_tab, "Transactions")
        tab_widget.addTab(self.budget_tab, "Zero-Based Budget")
        tab_widget.addTab(self.reports_tab, "Reports")
        tab_widget.addTab(self.payment_tab, "Payment Tracking")
        
        # Connect signals for data updates
        self.spending_tab.data_changed.connect(self.on_data_changed)
        self.budget_tab.data_changed.connect(self.on_data_changed)
        self.payment_tab.data_changed.connect(self.on_data_changed)
        
        # Apply styling
        self.apply_styling()
        
    def on_data_changed(self):
        """Handle data changes by updating all tabs."""
        self.spending_tab.refresh_data()
        self.budget_tab.refresh_data()
        self.reports_tab.refresh_data()
        self.payment_tab.refresh_data()
        
    def apply_styling(self):
        """Apply custom styling to the application."""
        self.setStyleSheet("""
            /* Main Window Styling - Dark Theme */
            QMainWindow {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #2c3e50, stop:1 #34495e);
                font-family: 'Segoe UI', Arial, sans-serif;
                color: #ecf0f1;
            }
            
            /* Tab Widget Styling */
            QTabWidget::pane {
                border: 2px solid #7f8c8d;
                border-radius: 8px;
                background-color: #34495e;
                margin-top: 5px;
            }
            
            QTabWidget::tab-bar {
                alignment: center;
            }
            
            QTabBar::tab {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #3498db, stop:1 #2980b9);
                color: white;
                border: none;
                padding: 12px 24px;
                margin-right: 2px;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
                font-weight: 600;
                font-size: 11pt;
                min-width: 120px;
            }
            
            QTabBar::tab:selected {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #27ae60, stop:1 #229954);
                border-bottom: 3px solid #f39c12;
            }
            
            QTabBar::tab:hover:!selected {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #16a085, stop:1 #138d75);
            }
            
            /* Group Box Styling */
            QGroupBox {
                font-weight: 600;
                font-size: 12pt;
                border: 2px solid #7f8c8d;
                border-radius: 10px;
                margin: 15px 5px;
                padding-top: 20px;
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #34495e, stop:1 #2c3e50);
                color: #ecf0f1;
            }
            
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 15px;
                padding: 5px 10px;
                background-color: #3498db;
                color: white;
                border-radius: 5px;
                font-weight: bold;
            }
            
            /* Button Styling */
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #27ae60, stop:1 #229954);
                color: white;
                border: none;
                padding: 12px 20px;
                border-radius: 8px;
                font-weight: 600;
                font-size: 10pt;
                min-height: 20px;
            }
            
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #2ecc71, stop:1 #27ae60);
                border: 2px solid #f39c12;
            }
            
            QPushButton:pressed {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #1e8449, stop:1 #196f3d);
                border: 2px solid #e67e22;
            }
            
            /* Special button colors */
            QPushButton[objectName="csv_upload_button"] {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #e67e22, stop:1 #d35400);
            }
            
            QPushButton[objectName="csv_upload_button"]:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #f39c12, stop:1 #e67e22);
            }
            
            QPushButton[objectName="delete_button"] {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #e74c3c, stop:1 #c0392b);
                padding: 8px 16px;
                font-size: 9pt;
            }
            
            QPushButton[objectName="delete_button"]:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #ec7063, stop:1 #e74c3c);
            }
            
            QPushButton[objectName="mark_paid_button"] {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #27ae60, stop:1 #229954);
                padding: 8px 16px;
                font-size: 9pt;
            }
            
            QPushButton[objectName="mark_paid_button"]:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #2ecc71, stop:1 #27ae60);
            }
            
            /* Input Field Styling - Dark Theme with High Contrast */
            QLineEdit, QDoubleSpinBox, QComboBox, QDateEdit {
                padding: 10px;
                border: 2px solid #7f8c8d;
                border-radius: 6px;
                background-color: #2c3e50;
                color: #ecf0f1;
                font-size: 10pt;
                font-weight: 500;
                selection-background-color: #3498db;
                selection-color: white;
            }
            
            QLineEdit:focus, QDoubleSpinBox:focus, QComboBox:focus, QDateEdit:focus {
                border-color: #3498db;
                background-color: #34495e;
                color: #ffffff;
            }
            
            QComboBox::drop-down {
                border: none;
                border-top-right-radius: 6px;
                border-bottom-right-radius: 6px;
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #3498db, stop:1 #2980b9);
                width: 30px;
            }
            
            QComboBox::down-arrow {
                image: none;
                border-left: 5px solid transparent;
                border-right: 5px solid transparent;
                border-top: 5px solid white;
                margin: 10px;
            }
            
            QComboBox QAbstractItemView {
                background-color: #2c3e50;
                color: #ecf0f1;
                border: 2px solid #3498db;
                selection-background-color: #3498db;
                selection-color: white;
            }
            
            /* Table Styling - Dark Theme */
            QTableWidget {
                gridline-color: #7f8c8d;
                background-color: #2c3e50;
                border: 1px solid #7f8c8d;
                border-radius: 8px;
                font-size: 10pt;
                color: #ecf0f1;
            }
            
            QHeaderView::section {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #34495e, stop:1 #2c3e50);
                color: #ecf0f1;
                padding: 12px;
                border: none;
                font-weight: 600;
                font-size: 10pt;
            }
            
            QTableWidget::item {
                padding: 8px;
                border-bottom: 1px solid #7f8c8d;
                color: #ecf0f1;
            }
            
            QTableWidget::item:selected {
                background-color: #3498db;
                color: white;
            }
            
            QTableWidget::item:hover {
                background-color: #34495e;
            }
            
            /* Radio Button Styling - Dark Theme */
            QRadioButton {
                font-weight: 500;
                font-size: 10pt;
                spacing: 8px;
                color: #ecf0f1;
            }
            
            QRadioButton::indicator {
                width: 16px;
                height: 16px;
                border-radius: 8px;
                border: 2px solid #7f8c8d;
                background-color: #2c3e50;
            }
            
            QRadioButton::indicator:checked {
                background-color: #3498db;
                border-color: #2980b9;
            }
            
            /* Progress Bar Styling - Dark Theme */
            QProgressBar {
                border: 2px solid #7f8c8d;
                border-radius: 8px;
                text-align: center;
                font-weight: 600;
                font-size: 10pt;
                background-color: #2c3e50;
                color: #ecf0f1;
                height: 25px;
            }
            
            QProgressBar::chunk {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 #27ae60, stop:0.5 #2ecc71, stop:1 #16a085);
                border-radius: 6px;
                margin: 2px;
            }
            
            /* Label Styling - Dark Theme */
            QLabel {
                color: #ecf0f1;
                font-size: 10pt;
                font-weight: 500;
            }
            
            /* Scroll Bar Styling - Dark Theme */
            QScrollBar:vertical {
                border: none;
                background-color: #2c3e50;
                width: 12px;
                border-radius: 6px;
            }
            
            QScrollBar::handle:vertical {
                background-color: #7f8c8d;
                border-radius: 6px;
                min-height: 20px;
            }
            
            QScrollBar::handle:vertical:hover {
                background-color: #95a5a6;
            }
            
            /* Dialog Styling */
            QDialog {
                background-color: #2c3e50;
                color: #ecf0f1;
            }
            
            QDialogButtonBox QPushButton {
                min-width: 80px;
                padding: 8px 16px;
            }
        """)


def main():
    """Main application entry point."""
    app = QApplication(sys.argv)
    
    # Set application properties
    app.setApplicationName("Budget Spending Tracker")
    app.setOrganizationName("Budget Tracker")
    
    window = BudgetTracker()
    window.show()
    
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
