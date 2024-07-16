import sys
import csv
import subprocess
import datetime
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.colors as mcolors
import matplotlib.lines as mlines
import mplcursors
from matplotlib.lines import Line2D
from matplotlib.collections import PathCollection
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from matplotlib.patches import Rectangle
from matplotlib.legend_handler import HandlerBase

from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QListWidget, 
    QListWidgetItem, QAbstractItemView, QDateTimeEdit, QLabel, QLineEdit, 
    QComboBox, QMessageBox, QProgressBar, QCheckBox, QFileDialog, 
    QTableWidget, QTableWidgetItem, QHeaderView, QDesktopWidget, QMenu,
    QScrollArea
)
from PyQt5.QtCore import Qt, QDateTime, QThread, pyqtSignal, QTimer, QPointF
from PyQt5.QtGui import QColor, QPainter, QPen, QPolygonF

from lxml import etree as ET
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

        network_list = "','".join(network_codes)
        query = f"""
        SELECT DISTINCT Station.code
        FROM Station 
        JOIN Network ON Station._parent_oid = Network._oid 
        WHERE Network.code IN ('{network_list}')
        ORDER BY Station.code
        """
        station_codes = [row[0] for row in self.execute_query(query)]
        self.station_code.clear()
        self.station_code.addItems(station_codes)
        print(f"Added {len(station_codes)} station codes to the list.")

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
        selected_stations = [item.text() for item in self.station_code.selectedItems()]
        location_codes = [loc.strip() for loc in self.location_code.text().split(',') if loc.strip()] or ['*']
        channel_codes = [chan.strip() for chan in self.channel_code.text().split(',') if chan.strip()]
        
        if not channel_codes:
            channel_codes = ['*Z', 'ED*', 'BD*']  # Default to all Z, EDH, and BDF channels if none selected

        start_time = self.start_time.dateTime().toString("yyyy-MM-dd HH:mm:ss")
        end_time = self.end_time.dateTime().toString("yyyy-MM-dd HH:mm:ss")

        parameters = [f'"{item.text()}"' if ' ' in item.text() else item.text() for item in self.parameters.selectedItems()]
        if not parameters:
            QMessageBox.warning(self, "Warning", "Please select at least one parameter.")
            return
        parameters = ','.join(parameters)

        self.stream_combinations = self.get_stream_combinations()
        stream_patterns = [f"{net}.{sta}.{loc}.{cha}" for net, sta, loc, cha in self.stream_combinations]

        if not stream_patterns:
            QMessageBox.warning(self, "Warning", "No matching streams found for the selected criteria.")
            return

        i_parameter = ','.join(stream_patterns)

        # Estimate the total data size
        estimated_total_size = self.estimate_data_size(start_time, end_time, stream_patterns, parameters)

        command = f"scqueryqc -d mysql://sysop:sysop@127.0.0.1:3306/seiscomp -f -b '{start_time}' -e '{end_time}' -p {parameters} -i {i_parameter}"
        print(f"Running command: {command}")

        self.progress_bar.setValue(0)
        self.data_thread = DataFetchThread(command, estimated_total_size)
        self.data_thread.progress_update.connect(self.update_progress)
        self.data_thread.data_ready.connect(self.process_data)
        self.data_thread.error_occurred.connect(self.show_error)
        self.data_thread.start()

    def calculate_station_averages(self):
        network_codes = [item.text() for item in self.network_code.selectedItems()]
        station_codes = [item.text() for item in self.station_code.selectedItems()]
        location_codes = [loc.strip() for loc in self.location_code.text().split(',') if loc.strip()] or ['*']
        channel_codes = [chan.strip() for chan in self.channel_code.text().split(',') if chan.strip()] or ['*Z', 'ED*', 'BD*']
        start_time = self.start_time.dateTime().toString("yyyy-MM-dd HH:mm:ss")
        end_time = self.end_time.dateTime().toString("yyyy-MM-dd HH:mm:ss")

        parameters = [item.text() for item in self.parameters.selectedItems()]
        if not parameters:
            QMessageBox.warning(self, "Warning", "Please select at least one parameter.")
            return

        self.stream_combinations = self.get_stream_combinations()
        stream_patterns = [f"{net}.{sta}.{loc}.{cha}" for net, sta, loc, cha in self.stream_combinations]

        if not stream_patterns:
            QMessageBox.warning(self, "Warning", "No matching streams found for the selected criteria.")
            return

        i_parameter = ','.join(stream_patterns)

        # Estimate the total data size
        estimated_total_size = self.estimate_data_size(start_time, end_time, stream_patterns, ','.join(parameters))

        command = f"scqueryqc -d mysql://sysop:sysop@127.0.0.1:3306/seiscomp -f -b '{start_time}' -e '{end_time}' -p {','.join(parameters)} -i {i_parameter}"
        
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
            channel_codes = ['*Z', 'ED*', 'BD*']

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
            location_code = waveform_id.get('locationCode') or '--'  # Use '--' for empty location codes
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

    def export_to_csv(self, table):
        filename, _ = QFileDialog.getSaveFileName(self, "Save CSV", "", "CSV Files (*.csv)")
        if filename:
            with open(filename, 'w', newline='') as file:
                writer = csv.writer(file)
                
                # Write headers
                headers = []
                for col in range(table.columnCount()):
                    headers.append(table.horizontalHeaderItem(col).text())
                writer.writerow(headers)
                
                # Write data
                for row in range(table.rowCount()):
                    row_data = []
                    for col in range(table.columnCount()):
                        item = table.item(row, col)
                        if item is not None:
                            row_data.append(item.text())
                        else:
                            row_data.append("")
                    writer.writerow(row_data)
            
            QMessageBox.information(self, "Export Successful", f"Data exported to {filename}")

    def setup_table_context_menu(self, table):
        table.setContextMenuPolicy(Qt.CustomContextMenu)
        table.customContextMenuRequested.connect(lambda pos: self.show_table_context_menu(pos, table))

    def show_table_context_menu(self, pos, table):
        context_menu = QMenu(self)
        copy_action = context_menu.addAction("Copy")
        action = context_menu.exec_(table.mapToGlobal(pos))
        if action == copy_action:
            self.copy_selection(table)

    def copy_selection(self, table):
        selection = table.selectedRanges()
        if selection:
            rows = sorted(index for range_ in selection for index in range(range_.topRow(), range_.bottomRow() + 1))
            columns = sorted(index for range_ in selection for index in range(range_.leftColumn(), range_.rightColumn() + 1))
            
            data = []
            for row in rows:
                row_data = []
                for col in columns:
                    item = table.item(row, col)
                    if item is not None:
                        row_data.append(item.text())
                    else:
                        row_data.append('')
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

        # Create the table
        table = QTableWidget()
        table_layout.addWidget(table)

        # Set up the table
        table.setColumnCount(1 + 2 * len(parameters))  # 1 for station, 2 for each parameter (avg and count)
        table.setRowCount(len(station_averages))

        # Set the headers
        headers = ['Station']
        for param in sorted(parameters):
            headers.extend([f"{param} (Avg)", f"{param} (Count)"])
        table.setHorizontalHeaderLabels(headers)

        # Populate the table
        for row, (station, params) in enumerate(sorted(station_averages.items())):
            table.setItem(row, 0, QTableWidgetItem(station))
            col = 1
            row_has_na = False
            for param in sorted(parameters):
                if param in params:
                    avg = params[param].get('average')
                    count = params[param]['count']
                    avg_item = QTableWidgetItem(f"{avg:.4f}" if avg is not None else "N/A")
                    count_item = QTableWidgetItem(str(count))
                    table.setItem(row, col, avg_item)
                    table.setItem(row, col + 1, count_item)
                    if avg is None:
                        row_has_na = True
                else:
                    table.setItem(row, col, QTableWidgetItem("N/A"))
                    table.setItem(row, col + 1, QTableWidgetItem("0"))
                    row_has_na = True
                col += 2
            
            # Change row color if it contains N/A
            if row_has_na:
                for col in range(table.columnCount()):
                    item = table.item(row, col)
                    if item:
                        item.setBackground(QColor(240, 240, 240))  # Light grey background

        # Resize columns to content
        table.resizeColumnsToContents()

        # Make the horizontal header stretch to fill available space
        header = table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Stretch)

        # Set up context menu for copying
        self.setup_table_context_menu(table)

        # Create an export button
        export_button = QPushButton("Export to CSV")
        export_button.clicked.connect(lambda: self.export_to_csv(table))
        table_layout.addWidget(export_button)

        # Set the window size
        table_window.resize(1000, 600)  # Increased width to accommodate more columns

        # Show the window
        table_window.show()

        # Keep a reference to the window
        self.table_window = table_window
        
        # Keep a reference to the window
        self.table_window = table_window
        self.setup_table_context_menu(table)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    ex = SeisCompGUI()
    ex.show()
    sys.exit(app.exec_())
