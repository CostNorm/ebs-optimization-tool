# Configuration settings specific to the EBS Optimizer Lambda function.

# CloudWatch metric collection settings
EBS_METRIC_PERIOD = 86400  # Daily data (in seconds)

# Criteria for detecting idle EBS volumes
EBS_IDLE_VOLUME_CRITERIA = {
    'days_to_check': 7,                   # Detection period (days)
    'idle_time_threshold': 95,            # Idle time threshold (%)
    'io_ops_threshold': 10,               # Daily average IO operations threshold
    'throughput_threshold': 5 * 1024 * 1024,  # Daily average throughput threshold (5MB)
    'burst_balance_threshold': 90,        # Burst balance threshold (%)
    'detached_days_threshold': 7          # Threshold for days kept in detached state
}

# Criteria for detecting overprovisioned EBS volumes
EBS_OVERPROVISIONED_CRITERIA = {
    'days_to_check': 30,                 # Detection period (days)
    'disk_usage_threshold': 20,          # Disk usage threshold (%)
    'resize_buffer_percent': 0.3,        # Buffer percentage for resize recommendations (30%)
    'resize_min_buffer_gb': 10,          # Minimum buffer size in GB for resize
    'iops_usage_threshold_percent': 0.5, # IOPS usage threshold (50% of provisioned)
    'throughput_usage_threshold_percent': 0.5 # Throughput usage threshold (50% of provisioned)
}

# Regional EBS pricing (USD/GB/month) - Consider using AWS Price List API for dynamic pricing
# Structure adjusted for gp3 IOPS/Throughput pricing
EBS_PRICING = {
    'us-east-1': {
        'gp2': {'storage': 0.10},
        'gp3': {'storage': 0.08, 'iops': 0.005, 'throughput': 0.04}, # $/IOPS-mo, $/MiBps-mo
        'io1': {'storage': 0.125, 'iops': 0.065},
        'io2': {'storage': 0.125, 'iops': 0.065}, # io2 Block Express has different pricing
        'st1': {'storage': 0.045},
        'sc1': {'storage': 0.025},
        'standard': {'storage': 0.05}
    },
    'ap-northeast-2': {
        'gp2': {'storage': 0.114},
        'gp3': {'storage': 0.0912, 'iops': 0.0057, 'throughput': 0.0456},
        'io1': {'storage': 0.138, 'iops': 0.072},
        'io2': {'storage': 0.138, 'iops': 0.072},
        'st1': {'storage': 0.051},
        'sc1': {'storage': 0.028},
        'standard': {'storage': 0.08}
    },
    # Add other regions as needed
    'default': { # Default prices if region not found
        'gp2': {'storage': 0.10},
        'gp3': {'storage': 0.08, 'iops': 0.005, 'throughput': 0.04},
        'io1': {'storage': 0.125, 'iops': 0.065},
        'io2': {'storage': 0.125, 'iops': 0.065},
        'st1': {'storage': 0.045},
        'sc1': {'storage': 0.025},
        'standard': {'storage': 0.05}
    }
}

# Add other Lambda-specific configurations if needed 