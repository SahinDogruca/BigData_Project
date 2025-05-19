#!/usr/bin/env python3
import time
import subprocess
import argparse
import matplotlib.pyplot as plt
import numpy as np
import json
from tabulate import tabulate
from datetime import datetime

def parse_arguments():
    """Komut satırı argümanlarını işle"""
    parser = argparse.ArgumentParser(description='Hadoop MapReduce performans değerlendirmesi')
    parser.add_argument('--input', required=True, help='HDFS input path')
    parser.add_argument('--output', default='performance_results.json', help='Sonuçları kaydetmek için dosya yolu')
    parser.add_argument('--iterations', type=int, default=3, help='Her test için çalıştırma sayısı')
    parser.add_argument('--sample-sizes', type=str, default='0.1,0.5,1.0', 
                      help='Test edilecek veri setinin boyut oranları (virgülle ayrılmış)')
    return parser.parse_args()

def check_hdfs_file_exists(path):
    """HDFS'de dosyanın var olup olmadığını kontrol et"""
    cmd = f"hadoop fs -test -e {path}; echo $?"
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return result.stdout.strip() == '0'

def create_sample_datasets(input_path, sample_sizes):
    """Farklı boyutlarda örnek veri setleri oluştur"""
    samples = {}
    
    for size in sample_sizes:
        size_str = str(size).replace('.', '_')
        output_path = f"/user/student/us-accidents/data/sample_{size_str}_data.csv"
        
        if check_hdfs_file_exists(output_path):
            print(f"{output_path} zaten mevcut, atlanıyor...")
            samples[size] = output_path
            continue
        
        cmd = f"hadoop fs -cat {input_path} 2>/dev/null | wc -l"
        try:
            total_lines = int(subprocess.check_output(cmd, shell=True).decode().strip())
            sample_lines = int(total_lines * size) + 1
            
            cmd = f"hadoop fs -cat {input_path} 2>/dev/null | head -n {sample_lines} | hadoop fs -put - {output_path} 2>/dev/null"
            subprocess.run(cmd, shell=True, check=True)
            samples[size] = output_path
            print(f"Oluşturuldu: {output_path} ({sample_lines} satır, orijinal verinin %{size*100}'i)")
        except subprocess.CalledProcessError as e:
            print(f"Hata: {e.stderr.decode() if e.stderr else str(e)}")
            continue
    
    return samples

def run_mapreduce_job(script_path, input_path, iteration, total_iterations):
    """MapReduce işini çalıştır ve süresini ölç"""
    print(f"\n{'='*50}")
    print(f"İterasyon {iteration}/{total_iterations} çalıştırılıyor...")
    print(f"Script: {script_path}")
    print(f"Input: {input_path}")
    
    start_time = time.time()
    
    cmd = f"python {script_path} -r hadoop hdfs://{input_path}"
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    elapsed_time = time.time() - start_time
    
    if process.returncode != 0:
        print(f"! HATA ! Kod: {process.returncode}")
        print("Çıktı:", process.stdout)
        print("Hata:", process.stderr)
        return None
    
    print(f"Başarıyla tamamlandı. Süre: {elapsed_time:.2f} saniye")
    return elapsed_time

def evaluate_performance(sample_datasets, iterations=3):
    """Performans değerlendirmesi yap"""
    scripts = {
        "~/Desktop/bigdata/src/mapreduce/mean_severity.py": "Ortalama",
        "~/Desktop/bigdata/src/mapreduce/max_severity.py": "Maksimum",
        "~/Desktop/bigdata/src/mapreduce/stddev_severity.py": "Standart Sapma",
        "~/Desktop/bigdata/src/mapreduce/minmax_normalization.py": "Min-Max Normalizasyon",
        "~/Desktop/bigdata/src/mapreduce/skewness.py": "Çarpıklık"
    }
    
    results = {}
    
    for script_name, script_desc in scripts.items():
        script_results = {}
        print(f"\n{'#'*50}")
        print(f"## {script_desc} fonksiyonu için performans testi")
        print(f"{'#'*50}")
        
        for size, path in sample_datasets.items():
            times = []
            print(f"\nBoyut: %{size*100} - {path}")
            
            for i in range(1, iterations+1):
                duration = run_mapreduce_job(script_name, path, i, iterations)
                if duration is not None:
                    times.append(duration)
            
            if times:
                script_results[size] = {
                    "times": times,
                    "average": sum(times)/len(times),
                    "min": min(times),
                    "max": max(times),
                    "last_run": datetime.now().isoformat()
                }
        
        if script_results:
            results[script_name] = {
                "description": script_desc,
                "results": script_results
            }
    
    return results

def save_results(results, output_file):
    """Sonuçları JSON dosyasına kaydet"""
    try:
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\nSonuçlar başarıyla kaydedildi: {output_file}")
        return True
    except Exception as e:
        print(f"\nHATA: Sonuçlar kaydedilemedi - {str(e)}")
        return False

def plot_performance(results, output_image="performance_plot.png"):
    """Performans sonuçlarını görselleştir"""
    if not results:
        print("Görselleştirme için yeterli veri yok")
        return
    
    plt.figure(figsize=(12, 7))
    
    for script, data in results.items():
        sizes = []
        averages = []
        for size, metrics in data["results"].items():
            sizes.append(f"%{int(float(size)*100)}")
            averages.append(metrics["average"])
        
        plt.plot(sizes, averages, 'o-', label=data["description"])
    
    plt.xlabel('Veri Seti Boyutu')
    plt.ylabel('Ortalama Çalışma Süresi (saniye)')
    plt.title('MapReduce Performans Karşılaştırması')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    
    try:
        plt.savefig(output_image)
        print(f"Grafik kaydedildi: {output_image}")
    except Exception as e:
        print(f"Grafik kaydedilemedi: {str(e)}")

def generate_report(results):
    """Performans raporu oluştur"""
    if not results:
        print("Rapor oluşturmak için yeterli veri yok")
        return None
    
    report = []
    headers = ["Fonksiyon", "Veri Boyutu", "Ortalama (s)", "Min (s)", "Max (s)", "Çalıştırma Sayısı"]
    
    for script, data in results.items():
        for size, metrics in data["results"].items():
            report.append([
                data["description"],
                f"%{int(float(size)*100)}",
                f"{metrics['average']:.2f}",
                f"{metrics['min']:.2f}",
                f"{metrics['max']:.2f}",
                len(metrics["times"])
            ])
    
    print("\nPERFORMANS RAPORU:")
    print(tabulate(report, headers=headers, tablefmt="grid"))
    return report

def main():
    args = parse_arguments()
    sample_sizes = [float(s) for s in args.sample_sizes.split(',')]
    
    print("\nÖrnek veri setleri oluşturuluyor...")
    samples = create_sample_datasets(args.input, sample_sizes)
    
    if not samples:
        print("Hata: Örnek veri setleri oluşturulamadı")
        return
    
    print("\nPerformans testleri başlıyor...")
    results = evaluate_performance(samples, args.iterations)
    
    if not results:
        print("Hata: Performans testleri çalıştırılamadı")
        return
    
    if save_results(results, args.output):
        generate_report(results)
        plot_performance(results)

if __name__ == "__main__":
    main()
