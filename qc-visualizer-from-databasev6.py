#!/usr/bin/env python3

import sys
import csv
import subprocess
import datetime

# NumPy and Pandas
import numpy as np
import pandas as pd

# Matplotlib and related libraries
import matplotlib
matplotlib.use('Qt5Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.colors as mcolors
import matplotlib.lines as mlines
from matplotlib.lines import Line2D
from matplotlib.collections import PathCollection
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from matplotlib.patches import Rectangle
from matplotlib.legend_handler import HandlerBase
import mplcursors

# Seaborn
import seaborn as sns

# PyQt5
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QListWidget, 
    QListWidgetItem, QAbstractItemView, QDateTimeEdit, QLabel, QLineEdit, 
    QComboBox, QMessageBox, QProgressBar, QCheckBox, QFileDialog, 
    QTableWidget, QTableWidgetItem, QHeaderView, QDesktopWidget, QMenu,
    QScrollArea, QTableView
)
from PyQt5.QtCore import (
    Qt, QDateTime, QThread, pyqtSignal, QTimer, QPointF, QSortFilterProxyModel, QSortFilterProxyModel, QVariant
)
from PyQt5.QtGui import (
    QColor, QPainter, QPen, QPolygonF, QStandardItemModel, QStandardItem
)

# XML parsing
from lxml import etree as ET

# MySQL connector
import mysql.connector
from mysql.connector import Error

matplotlib.use('Qt5Agg')

matplotlib.use('Qt5Agg')
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

# Define ToggleHandler
class ToggleHandler(HandlerBase):
    def __init__(self, plot):
        self.plot = plot
        super().__init__()

    def create_artists(self, legend, orig_handle, x0, y0, width, height, fontsize, trans):
        rect = Rectangle((x0, y0), width, height, facecolor='white', edgecolor='black', alpha=0.8)
        self.update_prop(rect, orig_handle, legend)
        return [rect]

    def update_prop(self, legend_handle, orig_handle, legend):
        legend_handle.set_facecolor('lightgray' if not orig_handle.get_visible() else 'white')

class DataFetchThread(QThread):
    progress_update = pyqtSignal(int)
    data_ready = pyqtSignal(object)
    error_occurred = pyqtSignal(str)

    def __init__(self, command, estimated_total_size):
        super().__init__()
        self.command = command
        self.estimated_total_size = estimated_total_size

    def run(self):
        try:
            process = subprocess.Popen(self.command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, universal_newlines=True)
            output = ""
            for line in iter(process.stdout.readline, ''):
                output += line
                progress = min(int((len(output) / self.estimated_total_size) * 100), 100)
                self.progress_update.emit(progress)
            
            if process.wait() != 0:
                error = process.stderr.read()
                self.error_occurred.emit(f"Command failed with error:\n{error}")
            else:
                self.data_ready.emit(output)
        except Exception as e:
            self.error_occurred.emit(str(e))

    class CustomSortProxyModel(QSortFilterProxyModel):
        def lessThan(self, left, right):
            left_data = self.sourceModel().data(left, Qt.UserRole)
            right_data = self.sourceModel().data(right, Qt.UserRole)
            
            if isinstance(left_data, QVariant) and left_data.isNull():
                return False
            if isinstance(right_data, QVariant) and right_data.isNull():
                return True
            
            if isinstance(left_data, (int, float)) and isinstance(right_data, (int, float)):
                return left_data < right_data
            
            return super().lessThan(left, right)

class ColorMarkerLabel(QLabel):
    def __init__(self, color, marker, size=20):
        super().__init__()
        self.color = self.tuple_to_qcolor(color)
        self.marker = marker
        self.setFixedSize(size, size)

    def tuple_to_qcolor(self, color_tuple):
        if isinstance(color_tuple, tuple):
            return QColor.fromRgbF(*color_tuple)
        elif isinstance(color_tuple, str):
            return QColor(color_tuple)
        else:
            return QColor('black')

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        
        pen = QPen(self.color)
        pen.setWidth(2)
        painter.setPen(pen)
        painter.setBrush(self.color)
        
        center = QPointF(self.width() / 2, self.height() / 2)
        size = 8  # Adjust this for marker size

        if self.marker == 'o':
            painter.drawEllipse(center, size/2, size/2)
        elif self.marker == 's':
            painter.drawRect(int(center.x() - size/2), int(center.y() - size/2), size, size)
        elif self.marker == 'D':
            diamond = QPolygonF([
                QPointF(center.x(), center.y() - size/2),
                QPointF(center.x() + size/2, center.y()),
                QPointF(center.x(), center.y() + size/2),
                QPointF(center.x() - size/2, center.y())
            ])
            painter.drawPolygon(diamond)
        elif self.marker == '^':
            triangle = QPolygonF([
                QPointF(center.x(), center.y() - size/2),
                QPointF(center.x() - size/2, center.y() + size/2),
                QPointF(center.x() + size/2, center.y() + size/2)
            ])
            painter.drawPolygon(triangle)
        elif self.marker == 'v':
            triangle = QPolygonF([
                QPointF(center.x(), center.y() + size/2),
                QPointF(center.x() - size/2, center.y() - size/2),
                QPointF(center.x() + size/2, center.y() - size/2)
            ])
            painter.drawPolygon(triangle)
        elif self.marker in ['<', '>', 'p', 'h', '8', 'H']:
            # For complex shapes, we'll just draw a filled circle
            painter.drawEllipse(center, size/2, size/2)
        elif self.marker in ['+', 'x']:
            painter.drawLine(int(center.x() - size/2), int(center.y()), int(center.x() + size/2), int(center.y()))
            painter.drawLine(int(center.x()), int(center.y() - size/2), int(center.x()), int(center.y() + size/2))
            if self.marker == 'x':
                painter.drawLine(int(center.x() - size/2), int(center.y() - size/2), int(center.x() + size/2), int(center.y() + size/2))
                painter.drawLine(int(center.x() - size/2), int(center.y() + size/2), int(center.x() + size/2), int(center.y() - size/2))
        else:
            # Default to a small line for any unhandled markers
            painter.drawLine(int(center.x() - size/2), int(center.y()), int(center.x() + size/2), int(center.y()))
        
        painter.end()

class SeisCompGUI(QWidget):
    def __init__(self):
        super().__init__()
        self.db_connection = None
        self.plot_window = None
        self.table_window = None
        self.connect_to_database()
        self.initUI()

    def connect_to_database(self):
        try:
            self.db_connection = mysql.connector.connect(
                host="127.0.0.1",
                user="sysop",
                password="sysop",
                database="seiscomp",
                port=3306
            )
            print("Successfully connected to the database")
        except Error as e:
            print(f"Error connecting to MySQL database: {e}")

    def execute_query(self, query):
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result
        except Error as e:
            print(f"Error executing query: {e}")
            return []

    def initUI(self):
        main_layout = QVBoxLayout()
        self.setLayout(main_layout)

        self.setup_datetime_section(main_layout)
        self.setup_network_station_section(main_layout)
        self.setup_parameters_section(main_layout)
        self.setup_location_channel_section(main_layout)
        self.setup_plot_options_section(main_layout)
        self.setup_run_button(main_layout)
        self.setup_average_button(main_layout)
        self.setup_progress_bar(main_layout)

        self.update_network_codes()

        self.setWindowTitle('SeisComP QC Data Visualization')
        self.setGeometry(300, 300, 600, 800)

    def setup_datetime_section(self, layout):
        datetime_layout = QHBoxLayout()
        
        self.start_time = QDateTimeEdit(QDateTime.currentDateTimeUtc().addSecs(-3600))
        self.end_time = QDateTimeEdit(QDateTime.currentDateTimeUtc())
        
        for dt in (self.start_time, self.end_time):
            dt.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
            dt.setCalendarPopup(True)
        
        datetime_layout.addWidget(QLabel("Start Time:"))
        datetime_layout.addWidget(self.start_time)
        datetime_layout.addWidget(QLabel("End Time:"))
        datetime_layout.addWidget(self.end_time)
        
        layout.addLayout(datetime_layout)

    def get_default_channels_and_locations(self):
        query = """
        SELECT
            cs.networkCode AS network,
            cs.stationCode AS station,
            pm_stream.value AS detecStream,
            pm_locid.value AS detecLocid
        FROM
            ConfigModule cm
            JOIN ConfigStation cs ON cs._parent_oid=cm._oid AND cm.name='trunk'
            JOIN Setup su ON su._parent_oid=cs._oid AND su.name='default'
            JOIN PublicObject po ON po.publicID=su.parameterSetID
            JOIN Parameter pm_stream ON pm_stream._parent_oid=po._oid AND pm_stream.name='detecStream'
            LEFT JOIN Parameter pm_locid ON pm_locid._parent_oid=po._oid AND pm_locid.name='detecLocid'
        """
        results = self.execute_query(query)
        default_channels = {}
        
        def append_z_if_needed(channel):
            two_letter_codes = ['BH', 'HH', 'SH', 'EH', 'LH', 'CH']  # Add more if needed
            if any(channel.startswith(code) for code in two_letter_codes) and len(channel) == 2:
                return channel + 'Z'
            return channel

        for row in results:
            network = row[0]
            station = row[1]
            channel_code = row[2]
            location_code = row[3]

            # Decode byte strings if necessary
            if isinstance(channel_code, bytes):
                channel_code = channel_code.decode('utf-8')
            if isinstance(location_code, bytes):
                location_code = location_code.decode('utf-8')

            # Append 'Z' to two-letter channel codes if needed
            channel_code = append_z_if_needed(channel_code)

            # Use empty string for NULL or empty location codes
            location_code = location_code if location_code and location_code.strip() else ''

            key = f"{network}.{station}"
            default_channels[key] = {
                'channelCode': channel_code,
                'locationCode': location_code
            }

        return default_channels

    def get_all_stations(self, selected_network_codes, start_time, end_time):
        network_codes_str = "', '".join(selected_network_codes)
        query = f"""
        SELECT DISTINCT 
            Network.code AS network_code, 
            Station.code AS station_code
        FROM 
            Network
        JOIN 
            Station ON Network._oid = Station._parent_oid
        JOIN
            SensorLocation ON Station._oid = SensorLocation._parent_oid
        JOIN
            Stream ON SensorLocation._oid = Stream._parent_oid
        WHERE
            Network.code IN ('{network_codes_str}')
            AND (Network.end IS NULL OR Network.end > '{start_time}')
            AND (Station.end IS NULL OR Station.end > '{start_time}')
            AND (SensorLocation.end IS NULL OR SensorLocation.end > '{start_time}')
            AND (Stream.end IS NULL OR Stream.end > '{start_time}')
            AND Network.start <= '{end_time}'
            AND Station.start <= '{end_time}'
            AND SensorLocation.start <= '{end_time}'
            AND Stream.start <= '{end_time}'
            AND (
                Stream.code LIKE '%Z'  -- Vertical component channels
                OR Stream.code LIKE 'ED_'  -- EDH channels
                OR Stream.code LIKE 'BD_'  -- BDF channels
            )
        ORDER BY 
            Network.code, Station.code
        """
        return self.execute_query(query)

    def setup_network_station_section(self, layout):
        network_station_layout = QHBoxLayout()
        
        # Network section
        network_layout = QVBoxLayout()
        network_layout.addWidget(QLabel("Network Code:"))
        self.network_code = QListWidget()
        self.network_code.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.network_code.itemSelectionChanged.connect(self.update_station_codes)
        network_layout.addWidget(self.network_code)
        network_station_layout.addLayout(network_layout)
        
        # Station section
        station_layout = QVBoxLayout()
        station_layout.addWidget(QLabel("Station Code:"))
        self.station_code = QListWidget()
        self.station_code.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.station_code.itemSelectionChanged.connect(self.update_location_channel_codes)
        station_layout.addWidget(self.station_code)
        network_station_layout.addLayout(station_layout)
        
        layout.addLayout(network_station_layout)

    def setup_parameters_section(self, layout):
        layout.addWidget(QLabel("Parameters:"))
        self.parameters = QListWidget()
        self.parameters.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.parameters.addItems([
            'latency', 'delay', 'timing', 'offset', 'rms', 'availability',
            'gaps count', 'gaps interval', 'gaps length', 'overlaps count',
            'overlaps interval', 'overlaps length', 'spikes count',
            'spikes interval', 'spikes amplitude'
        ])
        layout.addWidget(self.parameters)

    def setup_location_channel_section(self, layout):
        loc_chan_layout = QHBoxLayout()
        
        self.location_code = QLineEdit()
        self.location_code.setPlaceholderText("Location codes (comma-separated, * for all)")
        
        self.channel_code = QLineEdit()
        self.channel_code.setPlaceholderText("Channel codes (comma-separated, * for all)")
        
        loc_chan_layout.addWidget(QLabel("Location Code:"))
        loc_chan_layout.addWidget(self.location_code)
        loc_chan_layout.addWidget(QLabel("Channel Code:"))
        loc_chan_layout.addWidget(self.channel_code)
        
        layout.addLayout(loc_chan_layout)

    def setup_plot_options_section(self, layout):
        options_layout = QHBoxLayout()
        
        layout.addWidget(QLabel("Plot Type:"))
        self.plot_type = QComboBox()
        self.plot_type.addItems(['line', 'scatter', 'area', 'heatmap', 'violin', 'box'])
        options_layout.addWidget(self.plot_type)
        
        self.normalize_cb = QCheckBox("Normalize Data")
        options_layout.addWidget(self.normalize_cb)
        
        self.log_scale_cb = QCheckBox("Log Scale")
        options_layout.addWidget(self.log_scale_cb)
        
        layout.addLayout(options_layout)

    def setup_run_button(self, layout):
        run_button = QPushButton("Run")
        run_button.clicked.connect(self.run_command)
        layout.addWidget(run_button)

    def setup_average_button(self, layout):
        average_button = QPushButton("Calculate Station Averages")
        average_button.clicked.connect(self.calculate_station_averages)
        layout.addWidget(average_button)

    def setup_progress_bar(self, layout):
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        layout.addWidget(self.progress_bar)

    def update_network_codes(self):
        query = "SELECT DISTINCT code FROM Network ORDER BY code"
        network_codes = [row[0] for row in self.execute_query(query)]
        self.network_code.clear()
        self.network_code.addItems(network_codes)
        print(f"Added {len(network_codes)} network codes to the list.")

    def update_station_codes(self):
        network_codes = [item.text() for item in self.network_code.selectedItems()]
        if not network_codes:
            self.station_code.clear()
            print("No network codes selected, clearing station codes.")
            return

        default_channels = self.get_default_channels_and_locations()

        network_list = "','".join(network_codes)
        query = f"""
        SELECT DISTINCT Network.code, Station.code
        FROM Station 
        JOIN Network ON Station._parent_oid = Network._oid 
        WHERE Network.code IN ('{network_list}')
        ORDER BY Network.code, Station.code
        """
        stations = self.execute_query(query)
        
        self.station_code.clear()
        for network, station in stations:
            key = f"{network}.{station}"
            default = default_channels.get(key, {'channelCode': 'BHZ', 'locationCode': ''})
            loc_display = default['locationCode'] if default['locationCode'] else '--'
            item_text = f"{station} (Default: {loc_display}.{default['channelCode']})"
            self.station_code.addItem(item_text)
        
        print(f"Added {len(stations)} station codes to the list.")

    def update_location_channel_codes(self):
        network_codes = [item.text() for item in self.network_code.selectedItems()]
        station_codes = [item.text() for item in self.station_code.selectedItems()]
        
        if not network_codes or not station_codes:
            return

        network_list = "','".join(network_codes)
        station_list = "','".join(station_codes)
        query = f"""
        SELECT DISTINCT 
            IFNULL(SensorLocation.code, '--') as location_code, 
            Stream.code as channel_code
        FROM Network
        JOIN Station ON Network._oid = Station._parent_oid
        JOIN SensorLocation ON Station._oid = SensorLocation._parent_oid
        JOIN Stream ON SensorLocation._oid = Stream._parent_oid
        WHERE Network.code IN ('{network_list}')
        AND Station.code IN ('{station_list}')
        AND (
            Stream.code LIKE '%Z'
            OR Stream.code LIKE 'ED_'
            OR Stream.code LIKE 'BD_'
        )
        ORDER BY location_code, channel_code
        """
        results = self.execute_query(query)
        location_codes = sorted(set(row[0] for row in results))
        channel_codes = sorted(set(row[1] for row in results))
        
        self.location_code.setText(",".join(location_codes))
        self.channel_code.setText(",".join(channel_codes))
        
        print(f"Location codes: {location_codes}")
        print(f"Channel codes: {channel_codes}")
                
    def run_command(self):
        selected_networks = [item.text() for item in self.network_code.selectedItems()]
        selected_stations = [item.text().split()[0] for item in self.station_code.selectedItems()]
        location_codes = [loc.strip() for loc in self.location_code.text().split(',') if loc.strip()]
        channel_codes = [chan.strip() for chan in self.channel_code.text().split(',') if chan.strip()]
        
        default_channels = self.get_default_channels_and_locations()

        start_time = self.start_time.dateTime().toString("yyyy-MM-dd HH:mm:ss")
        end_time = self.end_time.dateTime().toString("yyyy-MM-dd HH:mm:ss")

        parameters = [f'"{item.text()}"' if ' ' in item.text() else item.text() for item in self.parameters.selectedItems()]
        if not parameters:
            QMessageBox.warning(self, "Warning", "Please select at least one parameter.")
            return
        parameters = ','.join(parameters)

        def get_channel_codes(default_channel):
            if default_channel.startswith(('EDH', 'BDF')):
                return [default_channel]
            elif len(default_channel) == 2:
                return [default_channel + 'Z']
            elif len(default_channel) == 3:
                return [default_channel]
            else:
                return ['BHZ', 'HHZ', 'EHZ', 'SHZ', 'EDH', 'BDF']

        stream_patterns = set()  # Use a set to avoid duplications
        for network in selected_networks:
            for station in selected_stations:
                key = f"{network}.{station}"
                default = default_channels.get(key, {'channelCode': 'BHZ', 'locationCode': ''})
                
                locs = location_codes if location_codes else [default['locationCode']]
                chans = channel_codes if channel_codes else get_channel_codes(default['channelCode'])
                
                for loc in locs:
                    for chan in chans:
                        loc_str = loc if loc else ''
                        chan_str = chan
                        
                        # Handle byte strings
                        if isinstance(loc_str, bytes):
                            loc_str = loc_str.decode('utf-8')
                        if isinstance(chan_str, bytes):
                            chan_str = chan_str.decode('utf-8')
                        
                        stream_patterns.add(f"{network}.{station}.{loc_str}.{chan_str}")

        if not stream_patterns:
            QMessageBox.warning(self, "Warning", "No matching streams found for the selected criteria.")
            return

        i_parameter = ','.join(stream_patterns)

        # Estimate the total data size
        estimated_total_size = self.estimate_data_size(start_time, end_time, list(stream_patterns), parameters)

        command = f"scqueryqc -d mysql://sysop:sysop@127.0.0.1:3306/seiscomp -f -b '{start_time}' -e '{end_time}' -p {parameters} -i {i_parameter}"
        print(f"Running command: {command}")

        self.progress_bar.setValue(0)
        self.data_thread = DataFetchThread(command, estimated_total_size)
        self.data_thread.progress_update.connect(self.update_progress)
        self.data_thread.data_ready.connect(self.process_data)
        self.data_thread.error_occurred.connect(self.show_error)
        self.data_thread.start()

    def estimate_data_size(self, start_time, end_time, stream_patterns, parameters):
        # Convert start_time and end_time to datetime objects
        start = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        end = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        
        # Calculate the time range in hours
        time_range_hours = (end - start).total_seconds() / 3600
        
        # Estimate the number of data points per stream per parameter
        data_points_per_stream_param = time_range_hours * 6  # Assuming 6 data points per hour
        
        # Calculate the total estimated data points
        total_data_points = len(stream_patterns) * len(parameters.split(',')) * data_points_per_stream_param
        
        # Estimate the size of each data point in bytes (adjust this based on your actual data)
        bytes_per_data_point = 50
        
        # Calculate the total estimated size in bytes
        estimated_total_size = total_data_points * bytes_per_data_point
        
        return estimated_total_size

    def calculate_station_averages(self):
        selected_networks = [item.text() for item in self.network_code.selectedItems()]
        selected_stations = [item.text().split()[0] for item in self.station_code.selectedItems()]
        location_codes = [loc.strip() for loc in self.location_code.text().split(',') if loc.strip()]
        channel_codes = [chan.strip() for chan in self.channel_code.text().split(',') if chan.strip()]
        
        default_channels = self.get_default_channels_and_locations()

        start_time = self.start_time.dateTime().toString("yyyy-MM-dd HH:mm:ss")
        end_time = self.end_time.dateTime().toString("yyyy-MM-dd HH:mm:ss")

        parameters = [item.text() for item in self.parameters.selectedItems()]
        if not parameters:
            QMessageBox.warning(self, "Warning", "Please select at least one parameter.")
            return

        def get_channel_codes(default_channel):
            if default_channel.startswith(('EDH', 'BDF')):
                return [default_channel]
            elif len(default_channel) == 2:
                return [default_channel + 'Z']
            elif len(default_channel) == 3:
                return [default_channel]
            else:
                return ['BHZ', 'HHZ', 'EHZ', 'SHZ', 'EDH', 'BDF']

        stream_patterns = set()  # Use a set to avoid duplications
        for network in selected_networks:
            for station in selected_stations:
                key = f"{network}.{station}"
                default = default_channels.get(key, {'channelCode': 'BHZ', 'locationCode': ''})
                
                locs = location_codes if location_codes else [default['locationCode']]
                chans = channel_codes if channel_codes else get_channel_codes(default['channelCode'])
                
                for loc in locs:
                    for chan in chans:
                        loc_str = loc if loc else ''
                        chan_str = chan
                        
                        # Handle byte strings
                        if isinstance(loc_str, bytes):
                            loc_str = loc_str.decode('utf-8')
                        if isinstance(chan_str, bytes):
                            chan_str = chan_str.decode('utf-8')
                        
                        stream_patterns.add(f"{network}.{station}.{loc_str}.{chan_str}")

        if not stream_patterns:
            QMessageBox.warning(self, "Warning", "No matching streams found for the selected criteria.")
            return

        i_parameter = ','.join(stream_patterns)

        # Estimate the total data size
        estimated_total_size = self.estimate_data_size(start_time, end_time, list(stream_patterns), ','.join(parameters))

        command = f"scqueryqc -d mysql://sysop:sysop@127.0.0.1:3306/seiscomp -f -b '{start_time}' -e '{end_time}' -p {','.join(parameters)} -i {i_parameter}"
        print(f"Running command for station averages: {command}")

        self.progress_bar.setValue(0)
        self.data_thread = DataFetchThread(command, estimated_total_size)
        self.data_thread.progress_update.connect(self.update_progress)
        self.data_thread.data_ready.connect(self.process_average_data)
        self.data_thread.error_occurred.connect(self.show_error)
        self.data_thread.start()

    def process_average_data(self, xml_data):
        try:
            data_dict = self.parse_xml_data(xml_data)
            if not data_dict:
                QMessageBox.warning(self, "Warning", "No data found for the selected criteria.")
                return
            self.calculate_and_display_averages(data_dict)
        except ET.ParseError as e:
            QMessageBox.critical(self, "Error", f"Failed to parse XML data:\n{str(e)}")

    def get_stream_combinations(self):
        selected_networks = [item.text() for item in self.network_code.selectedItems()]
        selected_stations = [item.text() for item in self.station_code.selectedItems()]
        location_codes = self.location_code.text().split(',')
        channel_codes = self.channel_code.text().split(',')

        if not channel_codes:
            channel_codes = ['*Z', 'EDH', 'BDF']

        query = f"""
        SELECT DISTINCT 
            Network.code, 
            Station.code, 
            IFNULL(SensorLocation.code, '--') as location_code, 
            Stream.code as channel_code
        FROM Network
        JOIN Station ON Network._oid = Station._parent_oid
        JOIN SensorLocation ON Station._oid = SensorLocation._parent_oid
        JOIN Stream ON SensorLocation._oid = Stream._parent_oid
        WHERE Network.code IN ('{"','".join(selected_networks)}')
        AND Station.code IN ('{"','".join(selected_stations)}')
        AND (SensorLocation.code IN ('{"','".join(location_codes)}') OR SensorLocation.code IS NULL)
        AND (
            Stream.code LIKE '%Z'
            OR Stream.code LIKE 'ED%'
            OR Stream.code LIKE 'BD%'
        )
        """
        return self.execute_query(query)

    def estimate_data_size(self, start_time, end_time, stream_patterns, parameters):
        # Convert start_time and end_time to datetime objects
        start = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        end = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        
        # Calculate the time range in hours
        time_range_hours = (end - start).total_seconds() / 3600
        
        # Estimate the number of data points per stream per parameter
        data_points_per_stream_param = time_range_hours * 6  # Assuming 6 data points per hour
        
        # Calculate the total estimated data points
        total_data_points = len(stream_patterns) * len(parameters.split(',')) * data_points_per_stream_param
        
        # Estimate the size of each data point in bytes (adjust this based on your actual data)
        bytes_per_data_point = 50
        
        # Calculate the total estimated size in bytes
        estimated_total_size = total_data_points * bytes_per_data_point
        
        return estimated_total_size

    def update_progress(self, value):
        self.progress_bar.setValue(value)

    def process_data(self, xml_data):
        try:
            data_dict = self.parse_xml_data(xml_data)
            if not data_dict:
                QMessageBox.warning(self, "Warning", "No data found for the selected criteria.")
                return
            # Use QTimer to call plot_data from the main thread
            QTimer.singleShot(0, lambda: self.plot_data(data_dict))
        except ET.ParseError as e:
            QMessageBox.critical(self, "Error", f"Failed to parse XML data:\n{str(e)}")

    def show_error(self, error_message):
        QMessageBox.critical(self, "Error", error_message)

    def process_data(self, xml_data):
        try:
            data_dict = self.parse_xml_data(xml_data)
            if not data_dict:
                QMessageBox.warning(self, "Warning", "No data found for the selected criteria.")
                return
            # Use QTimer to call plot_data from the main thread
            QTimer.singleShot(0, lambda: self.plot_data(data_dict))
        except ET.ParseError as e:
            QMessageBox.critical(self, "Error", f"Failed to parse XML data:\n{str(e)}")


    def parse_xml_data(self, xml_data):
        ns_map = {'ns': 'http://geofon.gfz-potsdam.de/ns/seiscomp3-schema/0.12'}
        root = ET.fromstring(xml_data.encode('utf-8'))
        
        data_dict = {}
        selected_parameters = [item.text() for item in self.parameters.selectedItems()]

        for waveform_quality in root.xpath('//ns:waveformQuality', namespaces=ns_map):
            elements = {elem: waveform_quality.find(f'ns:{elem}', namespaces=ns_map) 
                        for elem in ['start', 'value', 'parameter', 'waveformID']}

            if any(element is None for element in elements.values()):
                continue

            start_time = datetime.datetime.strptime(elements['start'].text, "%Y-%m-%dT%H:%M:%S.%fZ")
            value = float(elements['value'].text)
            parameter = elements['parameter'].text
            waveform_id = elements['waveformID']
            
            station_code = waveform_id.get('stationCode')
            network_code = waveform_id.get('networkCode')
            location_code = waveform_id.get('locationCode') or ''
            channel_code = waveform_id.get('channelCode')

            if parameter in selected_parameters:
                key = f"{network_code}.{station_code}.{location_code}.{channel_code}"
                if key not in data_dict:
                    data_dict[key] = {}
                if parameter not in data_dict[key]:
                    data_dict[key][parameter] = {'time': [], 'value': []}

                data_dict[key][parameter]['time'].append(start_time)
                data_dict[key][parameter]['value'].append(value)

        return data_dict

    def plot_data(self, data_dict):
        plot_type = self.plot_type.currentText()
        normalize = self.normalize_cb.isChecked()
        log_scale = self.log_scale_cb.isChecked()

        if plot_type in ['line', 'scatter', 'area']:
            self.plot_time_series(data_dict, plot_type, normalize, log_scale)
        elif plot_type == 'heatmap':
            self.plot_heatmap(data_dict, normalize, log_scale)
        elif plot_type in ['violin', 'box']:
                self.plot_distribution(data_dict, plot_type, normalize, log_scale)

    def plot_time_series(self, data_dict, plot_type, normalize, log_scale):
        if not data_dict:
            print("No data to plot")
            QMessageBox.warning(self, "Warning", "No data to plot.")
            return

        # Get screen size
        screen = QDesktopWidget().screenNumber(self)
        screen_size = QDesktopWidget().availableGeometry(screen).size()

        # Calculate figure size based on screen size
        width = min(screen_size.width() * 0.9, 1400)  # Max width of 1200 pixels
        height = min(screen_size.height() * 0.9, 900)  # Max height of 800 pixels

        fig = Figure(figsize=(width/100, height/100))  # Convert pixels to inches
        canvas = FigureCanvas(fig)
        ax = fig.add_subplot(111)

        markers = ['o', 's', 'D', '^', 'v', '<', '>', 'p', 'h', '8', '*', 'H', '+', 'x', 'd', '|', '_']
        colors = plt.colormaps['tab20']  # Use this instead of plt.cm.get_cmap('tab20')

        legend_elements = []
        for i, (key, parameters) in enumerate(data_dict.items()):
            network, station, location, channel = key.split('.')
            
            for j, (parameter, data) in enumerate(parameters.items()):
                times = mdates.date2num(data['time'])
                values = np.array(data['value'])
                
                if normalize:
                    values = (values - np.min(values)) / (np.max(values) - np.min(values))
                
                label = f'{network}.{station}.{location}.{channel} - {parameter}'
                color = colors(i % 20)
                marker = markers[j % len(markers)]
                
                line = None
                if plot_type == 'line':
                    line, = ax.plot(times, values, linestyle='-', label=label, color=color, marker=marker, markersize=4)
                elif plot_type == 'scatter':
                    line = ax.scatter(times, values, label=label, color=color, marker=marker, s=20)
                elif plot_type == 'area':
                    ax.fill_between(times, values, label=label, color=color, alpha=0.3)
                    line, = ax.plot(times, values, linestyle='-', color=color, marker=marker, markersize=4)

                legend_elements.append(line)

        ax.set_title("Quality Parameters Over Time")
        ax.set_xlabel('Time')
        ax.set_ylabel('Value' if not normalize else 'Normalized Value')
        if log_scale:
            ax.set_yscale('log')
        ax.grid(True)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
        fig.autofmt_xdate()  # Rotate and align the tick labels

        # Add hover annotations
        cursor = mplcursors.cursor(legend_elements, hover=True)

        @cursor.connect("add")
        def on_add(sel):
            artist = sel.artist
            label = artist.get_label()
            sel.annotation.set_text(label)
            sel.annotation.get_bbox_patch().set(fc="white", alpha=0.8)

        # Adjust layout and display
        fig.tight_layout()
        plot_window = QWidget()
        plot_layout = QVBoxLayout()
        plot_layout.addWidget(canvas)

        # Create a button to show the legend
        legend_button = QPushButton("Show Legend")
        legend_button.clicked.connect(lambda: self.show_legend_window(legend_elements))
        plot_layout.addWidget(legend_button)

        plot_window.setLayout(plot_layout)
        plot_window.setWindowTitle("Time Series Plot")
        plot_window.resize(width, height)
        plot_window.show()
        
        # Keep a reference to the plot window
        self.plot_window = plot_window
        
        print("Plot window created and shown")

    def create_legend_window(self, legend_elements):
        legend_window = QWidget()
        legend_window.setWindowTitle("Legend")
        layout = QVBoxLayout()

        scroll_area = QScrollArea()
        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)

        for line in legend_elements:
            label = line.get_label()
            color = line.get_color()
            
            # Determine the correct marker
            if isinstance(line, Line2D):
                marker = line.get_marker()
            elif isinstance(line, PathCollection):
                paths = line.get_paths()
                if paths:
                    vertex_codes = paths[0].codes
                    if vertex_codes is not None and len(vertex_codes) > 0:
                        if vertex_codes[0] == mpl.path.Path.MOVETO and vertex_codes[1] == mpl.path.Path.LINETO:
                            marker = 's'  # square for scatter plot
                        else:
                            marker = 'o'  # default to circle
                    else:
                        marker = 'o'  # default to circle
                else:
                    marker = 'o'  # default to circle
            else:
                marker = 'o'  # default to circle for unknown types
            
            item_widget = QWidget()
            item_layout = QHBoxLayout(item_widget)
            
            color_marker_label = ColorMarkerLabel(color, marker)
            
            text_label = QLabel(label)
            text_label.setWordWrap(True)  # Allow long labels to wrap
            
            item_layout.addWidget(color_marker_label)
            item_layout.addWidget(text_label, 1)  # Give the text label more space
            item_layout.addStretch()
            
            scroll_layout.addWidget(item_widget)

        scroll_content.setLayout(scroll_layout)
        scroll_area.setWidget(scroll_content)
        scroll_area.setWidgetResizable(True)
        
        layout.addWidget(scroll_area)
        legend_window.setLayout(layout)
        legend_window.resize(400, 600)  # Set an initial size
        return legend_window

    def show_legend_window(self, legend_elements):
        if hasattr(self, 'legend_window') and self.legend_window.isVisible():
            self.legend_window.raise_()
            self.legend_window.activateWindow()
        else:
            self.legend_window = self.create_legend_window(legend_elements)
            self.legend_window.show()


    def plot_heatmap(self, data_dict, normalize, log_scale):
        df = self.create_dataframe(data_dict)
        if df.empty:
            QMessageBox.warning(self, "Warning", "Not enough data for heatmap visualization.")
            return
        
        fig = Figure(figsize=(12, 8))
        canvas = FigureCanvas(fig)
        ax = fig.add_subplot(111)
        
        if normalize:
            df = (df - df.min()) / (df.max() - df.min())
        if log_scale:
            df = np.log1p(df)
        
        sns.heatmap(df, cmap='viridis', annot=True, fmt='.2f', ax=ax)
        ax.set_title("Heatmap of Quality Parameters")
        fig.tight_layout()
        
        plot_window = QWidget()
        plot_layout = QVBoxLayout()
        plot_layout.addWidget(canvas)
        plot_window.setLayout(plot_layout)
        plot_window.show()

    def plot_distribution(self, data_dict, plot_type, normalize, log_scale):
        df = self.create_dataframe(data_dict)
        if df.empty:
            QMessageBox.warning(self, "Warning", "Not enough data for distribution visualization.")
            return
        
        fig = Figure(figsize=(12, 8))
        canvas = FigureCanvas(fig)
        ax = fig.add_subplot(111)
        
        if normalize:
            df = (df - df.min()) / (df.max() - df.min())
        if log_scale:
            df = np.log1p(df)
        
        if plot_type == 'violin':
            sns.violinplot(data=df, ax=ax)
        elif plot_type == 'box':
            sns.boxplot(data=df, ax=ax)
        
        ax.set_title(f"{plot_type.capitalize()} Plot of Quality Parameters")
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
        fig.tight_layout()
        
        plot_window = QWidget()
        plot_layout = QVBoxLayout()
        plot_layout.addWidget(canvas)
        plot_window.setLayout(plot_layout)
        plot_window.show()

    def create_dataframe(self, data_dict):
        data_list = []
        for key, parameters in data_dict.items():
            for parameter, data in parameters.items():
                for time, value in zip(data['time'], data['value']):
                    data_list.append({
                        'time': time,
                        'value': value,
                        'parameter': parameter,
                        'key': key
                    })
        return pd.DataFrame(data_list)

    def export_to_csv(self, table_view):
        filename, _ = QFileDialog.getSaveFileName(self, "Save CSV", "", "CSV Files (*.csv)")
        if filename:
            with open(filename, 'w', newline='') as file:
                writer = csv.writer(file)
                model = table_view.model()

                # Write headers
                headers = [model.headerData(i, Qt.Horizontal) for i in range(model.columnCount())]
                writer.writerow(headers)

                # Write data
                for row in range(model.rowCount()):
                    row_data = []
                    for column in range(model.columnCount()):
                        index = model.index(row, column)
                        item = model.data(index, Qt.DisplayRole)
                        if isinstance(item, float):
                            item = f"{item:.4f}"
                        row_data.append(str(item))
                    writer.writerow(row_data)

            QMessageBox.information(self, "Export Successful", f"Data exported to {filename}")

    def setup_table_context_menu(self, table_view):
        table_view.setContextMenuPolicy(Qt.CustomContextMenu)
        table_view.customContextMenuRequested.connect(lambda pos: self.show_table_context_menu(pos, table_view))

    def show_table_context_menu(self, pos, table_view):
        context_menu = QMenu(self)
        copy_action = context_menu.addAction("Copy")
        action = context_menu.exec_(table_view.mapToGlobal(pos))
        if action == copy_action:
            self.copy_selection(table_view)

    def copy_selection(self, table_view):
        selection = table_view.selectionModel().selection()
        if selection:
            rows = sorted(set(index.row() for index in selection.indexes()))
            columns = sorted(set(index.column() for index in selection.indexes()))
            
            data = []
            for row in rows:
                row_data = []
                for column in columns:
                    index = table_view.model().index(row, column)
                    row_data.append(str(table_view.model().data(index)))
                data.append('\t'.join(row_data))
            
            clipboard = QApplication.clipboard()
            clipboard.setText('\n'.join(data))

    def calculate_and_display_averages(self, data_dict):
        # Get selected network codes
        selected_network_codes = [item.text() for item in self.network_code.selectedItems()]
        
        # Get start and end times
        start_time = self.start_time.dateTime().toString("yyyy-MM-dd HH:mm:ss")
        end_time = self.end_time.dateTime().toString("yyyy-MM-dd HH:mm:ss")
        
        # Get all active stations from the database for selected networks
        all_stations = self.get_all_stations(selected_network_codes, start_time, end_time)
        station_averages = {f"{net}.{sta}": {} for net, sta in all_stations}

        # Calculate averages for stations with data
        for key, parameters in data_dict.items():
            network, station, _, _ = key.split('.')
            station_key = f"{network}.{station}"
            
            if station_key in station_averages:
                for parameter, data in parameters.items():
                    if parameter not in station_averages[station_key]:
                        station_averages[station_key][parameter] = {'values': [], 'count': 0}
                    station_averages[station_key][parameter]['values'].extend(data['value'])
                    station_averages[station_key][parameter]['count'] += len(data['value'])
        
        # Calculate averages
        parameters = set()
        for station, params in station_averages.items():
            for param, data in params.items():
                parameters.add(param)
                if data['count'] > 0:
                    data['average'] = np.mean(data['values'])
                else:
                    data['average'] = None

        # Create a new window for the table
        table_window = QWidget()
        table_window.setWindowTitle("Station Averages")
        table_layout = QVBoxLayout()
        table_window.setLayout(table_layout)

        # Create the model and set up headers
        model = QStandardItemModel()
        headers = ['Station']
        for param in sorted(parameters):
            headers.extend([f"{param} (Avg)", f"{param} (Count)"])
        model.setHorizontalHeaderLabels(headers)

        # Populate the model
        for station, params in sorted(station_averages.items()):
            row_items = [QStandardItem(station)]
            for param in sorted(parameters):
                if param in params:
                    avg = params[param].get('average')
                    count = params[param]['count']
                    if avg is not None:
                        avg_item = QStandardItem(f"{avg:.4f}")
                        avg_item.setData(avg, Qt.UserRole)
                    else:
                        avg_item = QStandardItem("N/A")
                        avg_item.setData(QVariant(), Qt.UserRole)
                    count_item = QStandardItem(str(count))
                    count_item.setData(count, Qt.UserRole)
                else:
                    avg_item = QStandardItem("N/A")
                    avg_item.setData(QVariant(), Qt.UserRole)
                    count_item = QStandardItem("0")
                    count_item.setData(0, Qt.UserRole)
                row_items.extend([avg_item, count_item])
            model.appendRow(row_items)

        # Create a custom proxy model for sorting
        proxy_model = CustomSortProxyModel()
        proxy_model.setSourceModel(model)

        # Create a QTableView and set its model to the proxy model
        table_view = QTableView()
        table_view.setModel(proxy_model)
        table_view.setSortingEnabled(True)
        
        # Adjust column widths
        table_view.resizeColumnsToContents()
        table_view.setColumnWidth(0, 150)  # Set width for station column
        
        # Set stretch for other columns
        header = table_view.horizontalHeader()
        for i in range(1, model.columnCount()):
            header.setSectionResizeMode(i, QHeaderView.Stretch)

        # Add the table view to the layout
        table_layout.addWidget(table_view)

        # Create an export button
        export_button = QPushButton("Export to CSV")
        export_button.clicked.connect(lambda: self.export_to_csv(table_view))
        table_layout.addWidget(export_button)

        # Set the window size
        table_window.resize(1000, 600)

        # Show the window
        table_window.show()

        # Keep a reference to the window
        self.table_window = table_window

        # Set up context menu for copying
        self.setup_table_context_menu(table_view)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    ex = SeisCompGUI()
    ex.show()
    sys.exit(app.exec_())
