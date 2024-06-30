import sys
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QComboBox, QDateTimeEdit, QLabel, QListWidget, QListWidgetItem, QAbstractItemView
from PyQt5.QtCore import Qt
import requests
import xml.etree.ElementTree as ET
import datetime
import subprocess

# Set the Matplotlib backend to 'qt'
import matplotlib
matplotlib.use('qt5agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

class MyApp(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()
        self.setLayout(layout)

        self.network_code = QComboBox()
        self.station_code = QComboBox()
        self.location_code = QComboBox()
        self.channel_code = QComboBox()
        self.start_time = QDateTimeEdit()
        self.end_time = QDateTimeEdit()
        self.parameters = QListWidget()
        self.parameters.setSelectionMode(QAbstractItemView.ExtendedSelection)

        for parameter in [
            'latency',
            'delay',
            'timing',
            'offset',
            'rms',
            'availability',
            'gaps count',
            'gaps interval',
            'gaps length',
            'overlaps count',
            'overlaps interval',
            'overlaps length',
            'spikes count',
            'spikes interval',
            'spikes amplitude'
        ]:
            item = QListWidgetItem(parameter)
            self.parameters.addItem(item)

        layout.addWidget(QLabel("Network Code:"))
        layout.addWidget(self.network_code)
        layout.addWidget(QLabel("Station Code:"))
        layout.addWidget(self.station_code)
        layout.addWidget(QLabel("Location Code:"))
        layout.addWidget(self.location_code)
        layout.addWidget(QLabel("Channel Code:"))
        layout.addWidget(self.channel_code)
        layout.addWidget(QLabel("Start Time:"))
        layout.addWidget(self.start_time)
        layout.addWidget(QLabel("End Time:"))
        layout.addWidget(self.end_time)
        layout.addWidget(QLabel("Parameters:"))
        layout.addWidget(self.parameters)

        run_button = QPushButton("Run")
        run_button.clicked.connect(self.run_command)
        layout.addWidget(run_button)

        self.network_code.currentIndexChanged.connect(self.update_station_codes)
        self.station_code.currentIndexChanged.connect(self.update_location_codes)
        self.location_code.currentIndexChanged.connect(self.update_channel_codes)

        self.update_network_codes()

    def fetch_codes(self, url, column):
        response = requests.get(url)
        data = response.text.split("\n")
        codes = [line.split("|")[column] for line in data if line]
        return codes

    def update_network_codes(self):
        self.network_code.addItems(self.fetch_codes("http://localhost:8081/fdsnws/station/1/query?level=network&format=text&nodata=404", 0))

    def update_station_codes(self):
        network_code = self.network_code.currentText()
        self.station_code.clear()
        self.station_code.addItems(self.fetch_codes(f"http://localhost:8081/fdsnws/station/1/query?network={network_code}&level=station&format=text&nodata=404", 1))

    def update_location_codes(self):
        network_code = self.network_code.currentText()
        station_code = self.station_code.currentText()
        self.location_code.clear()
        self.location_code.addItems(self.fetch_codes(f"http://localhost:8081/fdsnws/station/1/query?network={network_code}&station={station_code}&level=channel&format=text&nodata=404", 2))

    def update_channel_codes(self):
        network_code = self.network_code.currentText()
        station_code = self.station_code.currentText()
        location_code = self.location_code.currentText()
        self.channel_code.clear()
        self.channel_code.addItems(self.fetch_codes(f"http://localhost:8081/fdsnws/station/1/query?network={network_code}&station={station_code}&location={location_code}&level=channel&format=text&nodata=404", 3))

    def parse_and_visualize(self, xml_data):
        # Parse the XML data
        root = ET.fromstring(xml_data)

        # Prepare a dictionary to store the data for each parameter
        data = {
            'latency': [],
            'delay': [],
            'timing': [],
            'offset': [],
            'rms': [],
            'availability': [],
            'gaps count': [],
            'gaps interval': [],
            'gaps length': [],
            'overlaps count': [],
            'overlaps interval': [],
            'overlaps length': [],
            'spikes count': [],
            'spikes interval': [],
            'spikes amplitude': []
        }

        # Find all 'waveformQuality' elements
        for waveform_quality in root.findall('.//{http://geofon.gfz-potsdam.de/ns/seiscomp3-schema/0.13}waveformQuality'):
            # Extract the parameters
            start_time = waveform_quality.find('{http://geofon.gfz-potsdam.de/ns/seiscomp3-schema/0.13}start').text
            parameter = waveform_quality.find('{http://geofon.gfz-potsdam.de/ns/seiscomp3-schema/0.13}parameter').text
            value = float(waveform_quality.find('{http://geofon.gfz-potsdam.de/ns/seiscomp3-schema/0.13}value').text)

            # Convert the start time to a datetime object and add it to the list
            start_time = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ")

            # Add the start time and value to the list for the parameter
            data[parameter].append((start_time, value))

        # Create a plot for all parameters
        plt.figure(figsize=(10, 6))
        for parameter, values in data.items():
            if values:
                times, values = zip(*values)  # Unzip the list of tuples
                plt.plot(times, values, marker='o', label=parameter)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
        plt.gca().xaxis.set_major_locator(mdates.MinuteLocator(interval=15))
        plt.gcf().autofmt_xdate()
        plt.title('Parameters over Time')
        plt.xlabel('Time')
        plt.ylabel('Value')
        plt.grid(True)
        plt.legend()
        plt.show()

    def run_command(self):
        network_code = self.network_code.currentText()
        station_code = self.station_code.currentText()
        location_code = self.location_code.currentText()
        channel_code = self.channel_code.currentText()
        start_time = self.start_time.dateTime().toString(Qt.ISODate)
        end_time = self.end_time.dateTime().toString(Qt.ISODate)

        # Get the selected parameters
#        parameters = [item.text() for item in self.parameters.selectedItems()]
#        parameters = ','.join(parameters)
        parameters = ['"{}"'.format(item.text()) if ' ' in item.text() else item.text() for item in self.parameters.selectedItems()]
        parameters = ','.join(parameters)

        command = f"scqueryqc -d mysql://sysop:sysop@localhost/seiscomp -b {start_time} -e {end_time} -p {parameters} -i {network_code}.{station_code}.{location_code}.{channel_code}"
        print(f"Running command: {command}")
        # Run the command here
        # For example, if you're using the subprocess module to run the command:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        # Then you can parse and visualize the output like this:
        self.parse_and_visualize(result.stdout)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    ex = MyApp()
    ex.show()
    sys.exit(app.exec_())
