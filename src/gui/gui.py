#!/usr/bin/env python3
import os
import subprocess
import json
import sys
import tempfile
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                            QLabel, QLineEdit, QRadioButton, QPushButton, QProgressBar, 
                            QTabWidget, QTextEdit, QFrame, QGroupBox, QMessageBox, QFileDialog,
                            QButtonGroup, QComboBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import numpy as np
import uuid
from PyQt5.QtGui import QIntValidator

class MapReduceWorker(QThread):
    finished = pyqtSignal(str, str, int)
    
    def __init__(self, cmd):
        super().__init__()
        self.cmd = cmd
        
    def run(self):
        try:
            process = subprocess.Popen(
                self.cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            stdout, stderr = process.communicate()
            self.finished.emit(stdout, stderr, process.returncode)
        except Exception as e:
            self.finished.emit("", str(e), -1)

class HadoopFileWorker(QThread):
    finished = pyqtSignal(str, str, int)
    
    def __init__(self, operation, local_path=None, hdfs_path=None):
        super().__init__()
        self.operation = operation  # 'upload' or 'list'
        self.local_path = local_path
        self.hdfs_path = hdfs_path
        
    def run(self):
        try:
            if self.operation == 'upload':
                cmd = f"hadoop fs -put '{self.local_path}' {self.hdfs_path}"
                process = subprocess.Popen(
                    cmd,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )
                stdout, stderr = process.communicate()
                self.finished.emit(stdout, stderr, process.returncode)
                
            elif self.operation == 'list':
                cmd = "hadoop fs -ls /user/student/us-accidents/data | awk '{print $8}'"
                process = subprocess.Popen(
                    cmd,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )
                stdout, stderr = process.communicate()
                self.finished.emit(stdout, stderr, process.returncode)
                
        except Exception as e:
            self.finished.emit("", str(e), -1)

class BigDataAnalysisApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Kaza Analizi Uygulaması")
        self.setGeometry(100, 100, 1000, 700)
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.output_dir = f"/user/student/us-accidents/outputs/{self.get_selected_stat()}_{uuid.uuid4().hex[:6]}"
        self.hadoop_data_dir = "/user/student/us-accidents/data"
        
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.main_layout = QVBoxLayout(self.central_widget)
        
        self.create_file_selection()
        self.create_stat_selection()
        self.create_column_selection()
        self.create_run_button()
        self.create_results_area()
        
        self.worker = None
        self.hadoop_worker = None
        self.clear_results()
        
        # Hadoop'daki mevcut dosyaları listele
        self.list_hadoop_files()
    
    def create_file_selection(self):
        file_group = QGroupBox("Veri Seçimi")
        file_layout = QVBoxLayout()
        
        # Local file selection
        local_file_layout = QHBoxLayout()
        local_file_layout.addWidget(QLabel("Yerel Dosya:"))
        self.local_file_path = QLineEdit()
        local_file_layout.addWidget(self.local_file_path)
        self.browse_button = QPushButton("Gözat...")
        self.browse_button.clicked.connect(self.browse_local_file)
        local_file_layout.addWidget(self.browse_button)
        self.upload_button = QPushButton("Hadoop'a Yükle")
        self.upload_button.clicked.connect(self.upload_to_hadoop)
        local_file_layout.addWidget(self.upload_button)
        file_layout.addLayout(local_file_layout)
        
        # Hadoop file selection
        hadoop_file_layout = QHBoxLayout()
        hadoop_file_layout.addWidget(QLabel("HDFS Dosyası:"))
        self.hadoop_file_combo = QComboBox()
        self.hadoop_file_combo.setEditable(True)
        self.hadoop_file_combo.setMinimumWidth(300)
        hadoop_file_layout.addWidget(self.hadoop_file_combo)
        self.refresh_button = QPushButton("Listeyi Yenile")
        self.refresh_button.clicked.connect(self.list_hadoop_files)
        hadoop_file_layout.addWidget(self.refresh_button)
        file_layout.addLayout(hadoop_file_layout)
        
        file_group.setLayout(file_layout)
        self.main_layout.addWidget(file_group)
    
    def create_stat_selection(self):
        stats_group = QGroupBox("İstatistik Fonksiyonları")
        stats_layout = QVBoxLayout()
        row_layouts = [QHBoxLayout(), QHBoxLayout()]
        stats_layout.addLayout(row_layouts[0])
        stats_layout.addLayout(row_layouts[1])
        
        self.stat_button_group = QButtonGroup(self)
        stats = [
            ("Ortalama Kaza Şiddeti", "mean"),
            ("Maksimum Kaza Şiddeti", "max"),
            ("Standart Sapma", "stddev"),
            ("Min-Max Normalizasyon", "minmax"),
            ("Çarpıklık (Skewness)", "skewness")
        ]
        
        for i, (text, value) in enumerate(stats):
            radio = QRadioButton(text)
            radio.setProperty("value", value)
            self.stat_button_group.addButton(radio)
            row_idx = 0 if i < 3 else 1
            row_layouts[row_idx].addWidget(radio)
            if i == 0:
                radio.setChecked(True)
        
        stats_group.setLayout(stats_layout)
        self.main_layout.addWidget(stats_group)
    
    def create_column_selection(self):
        column_group = QGroupBox("Sütun Seçimi")
        column_layout = QHBoxLayout()
        
        column_layout.addWidget(QLabel("Analiz Edilecek Sütun İndeksi:"))
        self.column_index = QLineEdit()
        self.column_index.setPlaceholderText("2 (Varsayılan)")
        self.column_index.setValidator(QIntValidator(0, 100))  # Only allow numbers
        self.column_index.setMaximumWidth(50)
        column_layout.addWidget(self.column_index)
        column_layout.addStretch()
        
        column_group.setLayout(column_layout)
        self.main_layout.addWidget(column_group)
    
    def create_run_button(self):
        run_layout = QVBoxLayout()
        self.run_button = QPushButton("MapReduce İşini Çalıştır")
        self.run_button.clicked.connect(self.run_mapreduce_job)
        run_layout.addWidget(self.run_button)
        self.progress = QProgressBar()
        self.progress.setRange(0, 0)
        self.progress.setVisible(False)
        run_layout.addWidget(self.progress)
        self.main_layout.addLayout(run_layout)
    
    def create_results_area(self):
        results_group = QGroupBox("Sonuçlar")
        results_layout = QVBoxLayout()
        self.tab_widget = QTabWidget()
        
        self.text_tab = QWidget()
        text_layout = QVBoxLayout(self.text_tab)
        self.result_text = QTextEdit()
        self.result_text.setReadOnly(True)
        text_layout.addWidget(self.result_text)
        self.tab_widget.addTab(self.text_tab, "Metin Sonuçları")
        
        self.graph_tab = QWidget()
        graph_layout = QVBoxLayout(self.graph_tab)
        self.fig = plt.Figure(figsize=(7, 4))
        self.canvas = FigureCanvas(self.fig)
        graph_layout.addWidget(self.canvas)
        self.tab_widget.addTab(self.graph_tab, "Grafik")
        
        results_layout.addWidget(self.tab_widget)
        results_group.setLayout(results_layout)
        self.main_layout.addWidget(results_group)
    
    def clear_results(self):
        self.result_text.clear()
        self.fig.clear()
        self.canvas.draw()
    
    def get_selected_stat(self):
        try:
            selected_button = self.stat_button_group.checkedButton()
            if selected_button:
                return selected_button.property("value")
            return "mean"
        except:
            return "mean"
    
    def get_column_index(self):
        try:
            col_text = self.column_index.text()
            if col_text:
                return int(col_text)
            return 2  # Default value
        except:
            return 2  # Default value
    
    def browse_local_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "CSV Dosyası Seçin", "", "CSV Files (*.csv);;All Files (*)"
        )
        if file_path:
            self.local_file_path.setText(file_path)
    
    def upload_to_hadoop(self):
        local_path = self.local_file_path.text()
        if not local_path:
            QMessageBox.warning(self, "Uyarı", "Lütfen önce bir dosya seçin")
            return
        
        if not os.path.isfile(local_path):
            QMessageBox.critical(self, "Hata", "Dosya bulunamadı!")
            return
        
        filename = os.path.basename(local_path)
        hdfs_path = f"{self.hadoop_data_dir}/{filename}"
        
        # Check if file already exists in HDFS
        check_cmd = f"hadoop fs -test -e {hdfs_path}"
        if subprocess.call(check_cmd, shell=True) == 0:
            reply = QMessageBox.question(
                self, "Dosya Mevcut", 
                f"{hdfs_path} zaten var. Üzerine yazılsın mı?",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            )
            if reply == QMessageBox.No:
                return
        
        self.result_text.append(f"{local_path} dosyası {hdfs_path} konumuna yükleniyor...")
        self.progress.setVisible(True)
        self.set_buttons_enabled(False)
        
        self.hadoop_worker = HadoopFileWorker('upload', local_path, hdfs_path)
        self.hadoop_worker.finished.connect(self.upload_finished)
        self.hadoop_worker.start()
    
    def list_hadoop_files(self):
        self.result_text.append("Hadoop'daki veri dosyaları listeleniyor...")
        self.progress.setVisible(True)
        self.set_buttons_enabled(False)
        
        self.hadoop_worker = HadoopFileWorker('list')
        self.hadoop_worker.finished.connect(self.list_files_finished)
        self.hadoop_worker.start()
    
    def upload_finished(self, stdout, stderr, return_code):
        self.progress.setVisible(False)
        self.set_buttons_enabled(True)
        
        if return_code == 0:
            self.result_text.append("Dosya başarıyla yüklendi!\n")
            # Listeyi güncelle
            self.list_hadoop_files()
        else:
            error_msg = f"HATA (Kod: {return_code}):\n{stderr}"
            self.result_text.append(error_msg)
            QMessageBox.critical(self, "Yükleme Başarısız", error_msg)
    
    def list_files_finished(self, stdout, stderr, return_code):
        self.progress.setVisible(False)
        self.set_buttons_enabled(True)
        
        if return_code == 0:
            files = stdout.strip().split('\n')
            # Sadece CSV dosyalarını filtrele
            csv_files = [f for f in files if f.lower().endswith('.csv')]
            
            self.hadoop_file_combo.clear()
            if csv_files:
                for file in csv_files:
                    self.hadoop_file_combo.addItem(file)
                self.result_text.append("Hadoop'daki CSV dosyaları listelendi.\n")
            else:
                self.result_text.append("Hadoop'da CSV dosyası bulunamadı. Varsayılan dosya kullanılacak.\n")
        else:
            error_msg = f"HATA (Kod: {return_code}):\n{stderr}"
            self.result_text.append(error_msg)
            QMessageBox.critical(self, "Listeleme Başarısız", error_msg)
    
    def set_buttons_enabled(self, enabled):
        self.browse_button.setEnabled(enabled)
        self.upload_button.setEnabled(enabled)
        self.refresh_button.setEnabled(enabled)
        self.run_button.setEnabled(enabled)
    
    def run_mapreduce_job(self):
        try:
            self.clear_results()
            stat_type = self.get_selected_stat()
            column_index = self.get_column_index()
            hdfs_path = self.hadoop_file_combo.currentText()
            self.output_dir = f"/user/student/us-accidents/outputs/{stat_type}_{uuid.uuid4().hex[:6]}"
            
            scripts = {
                "mean": "mean_severity.py",
                "max": "max_severity.py",
                "stddev": "stddev_severity.py",
                "minmax": "minmax_normalization.py",
                "skewness": "skewness.py"
            }
            
            cmd = ["python", f'{self.script_dir}/../mapreduce/{scripts[stat_type]}',
                  "-r", "hadoop", f'hdfs://{hdfs_path}', "--output-dir", f'hdfs://{self.output_dir}',
                  "--column", str(column_index)]
            
            self.result_text.append(f"Komut çalıştırılıyor: {' '.join(cmd)}\n")
            self.result_text.append(f"Analiz edilen sütun indeksi: {column_index}\n")
            self.set_buttons_enabled(False)
            self.progress.setVisible(True)
            
            self.worker = MapReduceWorker(cmd)
            self.worker.finished.connect(self.job_finished)
            self.worker.start()
        except Exception as e:
            QMessageBox.critical(self, "Hata", str(e))
            self.set_buttons_enabled(True)
            self.progress.setVisible(False)
    
    def job_finished(self, stdout, stderr, return_code):
        self.set_buttons_enabled(True)
        self.progress.setVisible(False)
        
        if return_code != 0:
            error_msg = f"HATA (Kod: {return_code}):\n{stderr}"
            self.result_text.append(error_msg)
            QMessageBox.critical(self, "İşlem Başarısız", error_msg)
            return
        
        try:
            stat_type = self.get_selected_stat()
            result_cmd = f"hadoop fs -cat {self.output_dir}/part-*"
            hdfs_result = subprocess.check_output(result_cmd, shell=True).decode()
            self.result_text.append(f"Sonuçlar ({self.output_dir}):\n{hdfs_result}")
            
            # Parse output based on stat type
            if stat_type == "minmax":
                results = []
                for line in hdfs_result.strip().split('\n'):
                    if line.strip():
                        try:
                            parts = line.split('\t')
                            if len(parts) >= 2:
                                key = parts[0]
                                value = json.loads('\t'.join(parts[1:]))
                                results.append((key, value))
                        except:
                            continue
                result_data = dict(results[:10])  # Take first 10 samples
            else:
                # For other stats, take the last line and try to parse as JSON
                last_line = hdfs_result.strip().split('\n')[-1]
                if '\t' in last_line:
                    key, value = last_line.split('\t', 1)
                    try:
                        value = json.loads(value)
                    except json.JSONDecodeError:
                        value = value.strip('"')
                    result_data = {key.strip('"'): value}
                else:
                    try:
                        result_data = json.loads(last_line)
                    except json.JSONDecodeError:
                        result_data = {"raw_output": hdfs_result}
            
            self.display_results(stat_type, result_data)
            
        except Exception as e:
            error_msg = f"Sonuç işlenirken hata: {str(e)}\n\nMapReduce Logları:\n{stdout}"
            self.result_text.append(error_msg)
            QMessageBox.warning(self, "Sonuç Alma Hatası", error_msg)

    def display_results(self, stat_type, output):
        try:
            self.result_text.append(f"\nİstatistik Türü: {stat_type}\n")
            
            if isinstance(output, dict):
                self.result_text.append(json.dumps(output, indent=2, ensure_ascii=False))
            else:
                self.result_text.append(str(output))
            
            self.plot_results(stat_type, output)
            
        except Exception as e:
            self.result_text.append(f"\nSonuç işlenirken hata oluştu: {str(e)}")
            self.result_text.append(f"\nHam çıktı:\n{output}")

    def plot_results(self, stat_type, result_data):
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        
        if stat_type == "mean":
            mean_val = result_data.get("mean_severity", 0)
            ax.bar(["Ortalama"], [mean_val], color='blue')
            ax.set_ylabel("Şiddet Değeri")
            ax.set_title("Ortalama Kaza Şiddeti")
            ax.set_ylim(0, 4)
            
        elif stat_type == "max":
            max_val = result_data.get("max_severity", 0)
            ax.bar(["Maksimum"], [max_val], color='red')
            ax.set_ylabel("Şiddet Değeri")
            ax.set_title("Maksimum Kaza Şiddeti")
            ax.set_ylim(0, 4)
            
        elif stat_type == "stddev":
            if isinstance(result_data, dict):
                if "statistics" in result_data:
                    stats = result_data["statistics"]
                    mean_val = stats.get("mean", 0)
                    std_val = stats.get("std_dev", 0)
                else:
                    mean_val = result_data.get("mean", 0)
                    std_val = result_data.get("std_dev", 0)
                
                ax.bar(["Ortalama", "Std. Sapma"], [mean_val, std_val], color=['blue', 'orange'])
                ax.set_ylabel("Değer")
                ax.set_title("Kaza Şiddeti İstatistikleri")
                ax.set_ylim(0, max(mean_val, std_val) * 1.2)
            
        elif stat_type == "minmax":
            if isinstance(result_data, dict):
                samples = list(result_data.items())[:10]  # Show first 10 samples
                original_values = [v.get("original", 0) for k, v in samples]
                normalized_values = [v.get("normalized", 0) for k, v in samples]
                labels = [f"Örnek {i+1}" for i in range(len(samples))]
                
                x = np.arange(len(labels))
                width = 0.35
                
                ax.bar(x - width/2, original_values, width, label='Orijinal')
                ax.bar(x + width/2, normalized_values, width, label='Normalize')
                
                ax.set_xticks(x)
                ax.set_xticklabels(labels)
                ax.legend()
                ax.set_ylabel("Değer")
                ax.set_title("Min-Max Normalizasyon Sonuçları (İlk 10 Örnek)")
                plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
            
        elif stat_type == "skewness":
            skew_results = {}
            if isinstance(result_data, dict):
                if "skewness_result" in result_data:
                    if isinstance(result_data["skewness_result"], dict):
                        skew_results = result_data["skewness_result"]
                    elif isinstance(result_data["skewness_result"], str):
                        try:
                            skew_results = json.loads(result_data["skewness_result"])
                        except:
                            pass
            
            skew_val = skew_results.get("skewness", 0)
            interpretation = skew_results.get("interpretation", "")
            
            # Generate skewed data for visualization
            x = np.random.normal(0, 1, 1000)
            if skew_val > 0:
                x = np.exp(x) - 1  # Positive skew
            elif skew_val < 0:
                x = -np.exp(x) + 1  # Negative skew
            
            ax.hist(x, bins=30, color='green', alpha=0.7)
            ax.set_title(f"Çarpıklık Dağılımı: {skew_val:.4f}")
            if interpretation:
                ax.text(0.05, 0.95, interpretation, 
                       transform=ax.transAxes, verticalalignment='top',
                       bbox=dict(facecolor='white', alpha=0.8))
        
        self.fig.tight_layout()
        self.canvas.draw()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = BigDataAnalysisApp()
    window.show()
    sys.exit(app.exec_())