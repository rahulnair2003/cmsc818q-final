import matplotlib.pyplot as plt
import numpy as np
import json
import os

def plot_query_times(distributions, filename):
    """Plot query times for each distribution and the merged sketch."""
    labels = [d['label'] for d in distributions]
    query_times = [d['query_time_us'] for d in distributions]

    plt.figure(figsize=(10, 6))
    x = np.arange(len(labels))
    width = 0.6

    rects = plt.bar(x, query_times, width, color=['#2ecc71', '#3498db', '#e74c3c', '#9b59b6'])
    plt.xlabel('Sketch')
    plt.ylabel('Query Time (μs)')
    plt.title('Query Time for Each Distribution and Merged Sketch')
    plt.xticks(x, labels)
    plt.grid(True, alpha=0.3)

    # Add value labels on top of bars
    for i, v in enumerate(query_times):
        plt.text(i, v, f'{v:.1f}', ha='center', va='bottom')

    plt.tight_layout()
    plt.savefig(os.path.join('profiling_results', filename))
    plt.close()

def plot_quantile_accuracy(distributions, filename):
    """Plot quantile accuracy comparison across distributions and the merged sketch."""
    quantiles = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
    x = np.arange(len(quantiles))
    width = 0.2

    plt.figure(figsize=(15, 8))
    for i, dist in enumerate(distributions):
        errors = [e * 100 for e in dist['errors']]
        plt.bar(x + i*width, errors, width, label=dist['label'])

    plt.xlabel('Quantile')
    plt.ylabel('Relative Error (%)')
    plt.title('Quantile Accuracy Across Distributions and Merged Sketch')
    plt.xticks(x + width, [f'p{q*100}' for q in quantiles])
    plt.legend()
    plt.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(os.path.join('profiling_results', filename))
    plt.close()

def plot_query_time_vs_data_size(data):
    data_sizes = sorted(set(d['data_size'] for d in data))
    ingestion_times = [d['query_time_us'] for d in data if d['label'] == 'Ingestion Pipeline']
    raw_times = [d['query_time_us'] for d in data if d['label'] == 'Raw Computation']

    plt.figure(figsize=(10, 6))
    plt.plot(data_sizes, ingestion_times, marker='o', label='Ingestion Pipeline')
    plt.plot(data_sizes, raw_times, marker='s', label='Raw Computation')
    plt.xscale('log')
    plt.xlabel('Data Size')
    plt.ylabel('Query Time (μs)')
    plt.title('Query Time vs. Data Size')
    plt.legend()
    plt.grid(True)
    plt.savefig('profiling_results/query_time_vs_data_size.png')
    plt.close()

def plot_quantile_error_vs_data_size(data):
    data_sizes = sorted(set(d['data_size'] for d in data if d['label'] == 'Ingestion Pipeline'))
    quantiles = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
    quantile_labels = [f"p{int(q*100)}" for q in quantiles]

    # For each quantile, collect the error across data sizes for DDSketch only
    errors_by_quantile = [[] for _ in quantiles]
    for d in data:
        if d['label'] == 'Ingestion Pipeline':
            for i, e in enumerate(d['errors']):
                errors_by_quantile[i].append(e)

    plt.figure(figsize=(10, 6))
    for i, q_label in enumerate(quantile_labels):
        plt.plot(data_sizes, errors_by_quantile[i], marker='o', label=q_label)
    plt.xscale('log')
    plt.xlabel('Data Size')
    plt.ylabel('Quantile Error')
    plt.title('Quantile Error vs. Data Size (DDSketch Only)')
    plt.legend(title='Quantile')
    plt.grid(True)
    plt.savefig('profiling_results/quantile_error_vs_data_size.png')
    plt.close()

def plot_quantile_errors(data):
    quantiles = data['quantiles']
    uniform_errors = data['uniform_errors']
    normal_errors = data['normal_errors']
    exponential_errors = data['exponential_errors']
    merged_errors = data['merged_errors']
    data_size = data['data_size']

    plt.figure(figsize=(10, 6))
    plt.plot(quantiles, uniform_errors, marker='o', label='Uniform', color='#2ecc71')
    plt.plot(quantiles, normal_errors, marker='s', label='Normal', color='#3498db')
    plt.plot(quantiles, exponential_errors, marker='^', label='Exponential', color='#e74c3c')
    plt.plot(quantiles, merged_errors, marker='*', label='Merged', color='#9b59b6')
    plt.xlabel('Quantile')
    plt.ylabel('Relative Error')
    plt.title(f'DDSketch Quantile Error (Data Size: {data_size:,})')
    plt.grid(True, alpha=0.3)
    plt.xticks(quantiles, [f'{q*100:.0f}%' for q in quantiles])
    plt.legend()
    plt.tight_layout()
    plt.savefig('profiling_results/quantile_errors.png')
    plt.close()

def plot_quantile_errors_100M(data):
    quantiles = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
    # Find the Ingestion Pipeline entry for 100M
    entry = next((d for d in data if d['label'] == 'Ingestion Pipeline' and d['data_size'] == 100000000), None)
    if entry is None:
        print("No Ingestion Pipeline result for 100M found.")
        return
    errors = entry['errors']
    plt.figure(figsize=(10, 6))
    plt.plot(quantiles, errors, marker='o', color='#2ecc71', linewidth=2)
    plt.xlabel('Quantile')
    plt.ylabel('Relative Error')
    plt.title('DDSketch Quantile Error (Data Size: 100,000,000)')
    plt.grid(True, alpha=0.3)
    plt.xticks(quantiles, [f'{q*100:.0f}%' for q in quantiles])
    for q, e in zip(quantiles, errors):
        plt.text(q, e, f'{e:.2%}', ha='center', va='bottom')
    plt.tight_layout()
    plt.savefig('profiling_results/quantile_errors_100M.png')
    plt.close()

def plot_quantile_errors_10M(data):
    quantiles = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
    # Find the Ingestion Pipeline entry for 10M
    entry = next((d for d in data if d['label'] == 'Ingestion Pipeline' and d['data_size'] == 10000000), None)
    if entry is None:
        print("No Ingestion Pipeline result for 10M found.")
        return
    errors = entry['errors']
    plt.figure(figsize=(10, 6))
    plt.plot(quantiles, errors, marker='o', color='#2ecc71', linewidth=2)
    plt.xlabel('Quantile')
    plt.ylabel('Relative Error')
    plt.title('DDSketch Quantile Error (Data Size: 10,000,000)')
    plt.grid(True, alpha=0.3)
    plt.xticks(quantiles, [f'{q*100:.0f}%' for q in quantiles])
    for q, e in zip(quantiles, errors):
        plt.text(q, e, f'{e:.2%}', ha='center', va='bottom')
    plt.tight_layout()
    plt.savefig('profiling_results/quantile_errors_10M.png')
    plt.close()

def plot_query_performance():
    distributions = ['Exponential', 'Normal', 'Uniform']
    ddsketch_times = [23.33, 7.53, 17.52]  # in ms
    raw_times = [8304.16, 7926.36, 8358.61]  # in ms
    
    x = np.arange(len(distributions))
    width = 0.35
    
    plt.figure(figsize=(10, 6))
    plt.bar(x - width/2, ddsketch_times, width, label='DDSketch', color='#2ecc71')
    plt.bar(x + width/2, raw_times, width, label='Raw Array', color='#e74c3c')
    
    plt.xlabel('Distribution')
    plt.ylabel('Query Time (ms)')
    plt.title('Query Performance: DDSketch vs Raw Array')
    plt.xticks(x, distributions)
    plt.yscale('log')  # Use log scale due to large difference in times
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # Add value labels on top of bars
    for i, v in enumerate(ddsketch_times):
        plt.text(i - width/2, v, f'{v:.2f}', ha='center', va='bottom')
    for i, v in enumerate(raw_times):
        plt.text(i + width/2, v, f'{v:.2f}', ha='center', va='bottom')
    
    plt.tight_layout()
    plt.savefig('profiling_results/query_performance.png')
    plt.close()

def main():
    plot_query_performance()

if __name__ == '__main__':
    main() 